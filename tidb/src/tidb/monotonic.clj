(ns tidb.monotonic
  "Establishes a collection of integers identified by keys. Monotonically
  increments individual keys via read-write transactions, and reads keys in
  small groups. We verify that the order of transactions implied by each key
  are mutually consistent; e.g. no transaction can observe key x increase, but
  key y decrease."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [elle.core :as elle]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [history :as h]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as append]
            [jepsen.tests.cycle.wr :as wr]
            [tidb [sql :as c :refer :all]
                  [txn :as txn]
                  [util :as util]]
            [tidb.sql :as c]))

(defn read-key
  "Read a specific key's value from the table. Missing values are represented
  as -1."
  ([c test k]
   (read-key c test k false))
  ([c test k lock?]
   (-> (c/query c [(str "select "
                        (when (:index-lookup test)
                          "/*+ index_lookup_pushdown(cycle, cycle_sk) */")
                        "(val) from cycle where "
                        (if (:use-index test) "sk" "pk") " = ?" (when lock? " for update"))
                   k])
       first
       (:val -1))))

(defn read-keys
  "Read several keys values from the table, returning a map of keys to values."
  [c test ks]
  (->> (map (partial read-key c test) ks)
       (zipmap ks)
       (into (sorted-map))))
  ;(zipmap ks (map (partial read-key c test) ks)))

(defn single-stmt-inc! [conn op]
  (let [k (:value op)
        q (str "insert into cycle values (?, ?, 0) "
               "on duplicate key update val = values(val)+1")]
    (c/execute! conn [q k k] {:transaction? false})
    (assoc op :type :ok, :value {})))

(defrecord IncrementClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/with-conn-failure-retry conn
      (c/execute! conn ["create table if not exists cycle
                        (pk  int not null primary key,
                         sk  int not null,
                         val int)"])
      (when (:use-index test)
        (c/create-index! conn [(str "create index cycle_sk on cycle (sk"
                                    (when-not (:index-lookup test) ", val")
                                    ")")]))
      (when (:table-cache test)
        (c/execute! conn ["alter table cycle cache"]))))

  (invoke! [this test op]
    (if (and (= :inc (:f op)) (:single-stmt-write test))
      (c/with-error-handling op (single-stmt-inc! conn op))
      (c/with-txn op [c conn {:isolation (util/isolation-level test)
                              :before-hook (partial c/rand-init-txn! test conn)}]
        (case (:f op)
          :read (let [v (read-keys c test (shuffle (keys (:value op))))]
                  (assoc op :type :ok, :value v))
          :inc (let [k (:value op)]
                 (if (:update-in-place test)
                   ; Update directly
                   (do (when (= [0] (c/execute!
                                     c [(str "update cycle set val = val + 1"
                                             " where pk = ?") k]))
                         ; That failed; insert
                         (c/insert! c "cycle" {:pk k, :sk k, :val 0}))
                       ; We can't place any constraints on the values since we
                       ; didn't read anything
                       (assoc op :type :ok, :value {}))

                   ; Update via separate r/w
                   (let [v (read-key c test k (not= "optimistic" (:txn-mode test)))]
                     (if (= -1 v)
                       (c/insert! c "cycle" {:pk k, :sk k, :val 0})
                       (c/update! c "cycle" {:val (inc v)},
                                  [(str (if (:use-index test) "sk" "pk") " = ?")
                                   k]))
                     ; The monotonic value constraint isn't actually enough to
                     ; capture all the ordering dependencies here: an increment
                     ; from x->y must fall after every read of x, and before
                     ; every read of y, but the monotonic order relation can only
                     ; enforce one of those. We'll return the written value here.
                     ; Still better than nothing.
                     (assoc op :type :ok :value {k (inc v)}))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn reads [key-count]
  (fn [] {:type  :invoke
          :f     :read
          :value (-> (range key-count)
                     ;jepsen.util/random-nonempty-subset
                     (zipmap (repeat nil)))}))

(defn incs [key-count]
  (fn [] {:type :invoke,
          :f :inc
          :value (rand-int key-count)}))

(defn inc-workload
  [opts]
  (let [key-count 8]
    {:client (IncrementClient. nil)
     :checker (checker/compose
                {:cycle (cycle/checker
                          (elle/combine elle/monotonic-key-graph
                                        elle/realtime-graph))
                 :timeline (timeline/html)})
     :generator (->> (gen/mix [(incs key-count)
                               (reads key-count)]))}))

(defn consistency-model
  "TiDB's REPEATABLE READ uses snapshots with real-time ordering. Preserve
  the old checker's realtime edges while asking Elle to include all SI
  anomalies, including G1. Strong SI still permits concurrent write skew;
  the default strict-serializable model would incorrectly reject it."
  [opts]
  (case (util/isolation-level opts)
    :read-committed :read-committed
    :repeatable-read :strong-snapshot-isolation))

(defn indexed-checker
  "Precompute the history index before Elle starts concurrent folds. This
  avoids Elle 0.2.7's deadlock on valid sparse histories; remove after upgrading
  to a release containing jepsen-io/elle commit fa0e699ec3488b9dfc660be4fcf7e9547bbea4e0."
  [c]
  (reify checker/Checker
    (check [_ test history opts]
      (h/ensure-pair-index history)
      (checker/check c test history opts))))

(defn txn-workload
  [opts]
  (-> (wr/test {:min-txn-length 2
                 :max-txn-length 5
                 :key-count 5
                 :max-writes-per-key 32
                 :consistency-models [(consistency-model opts)]})
      (assoc :client (txn/client {:val-type "int"}))
      (update :checker indexed-checker)))

(defn append-client
  "Wraps a TxnClient, translating string lists back into integers."
  [client]
  (reify client/Client
    (open! [this test node]
      (append-client (client/open! client test node)))

    (setup! [this test]
      (append-client (client/setup! client test)))

    (invoke! [this test op]
      (let [op' (client/invoke! client test op)
            txn' (mapv (fn [[f k v :as mop]]
                         (if (= f :r)
                           ; Rewrite reads to convert "1,2,3" to [1 2 3].
                           [f k (when v (mapv #(Long/parseLong %)
                                              (str/split v #",")))]
                           mop))
                       (:value op'))]
        (assoc op' :value txn')))

    (teardown! [this test]
      (client/teardown! client test))

    (close! [this test]
      (client/close! client test))))

(defn append-workload
  [opts]
  (-> (append/test {:min-txn-length 1
                     :max-txn-length 4
                     :key-count 5
                     :max-writes-per-key 16
                     :consistency-models [(consistency-model opts)]})
      (assoc :client (append-client (txn/client {:val-type "text"})))
      (update :checker indexed-checker)))
