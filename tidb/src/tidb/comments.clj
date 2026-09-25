(ns tidb.comments
  "Checks for a strict serializability anomaly in which T1 < T2, but T2 is
  visible without T1.

  We perform concurrent blind inserts across n tables, and meanwhile, perform
  reads of both tables in a transaction. To verify, we replay the history,
  tracking the writes which were known to have completed before the invocation
  of any write w_i. If w_i is visible, and some w_j < w_i is *not* visible,
  we've found a violation of strict serializability.

  Splits keys up onto different tables to make sure they fall in different
  shard ranges"
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [core :as jepsen]
             [generator :as gen]
             [independent :as independent]
             [util :refer [meh]]]
            [jepsen.tests.causal-reverse :as cr]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [tidb.sql :as c :refer :all]
            [tidb.util :as util]
            [knossos.model :as model]
            [knossos.op :as op]
            [clojure.core.reducers :as r]))

(def table-prefix "String prepended to all table names." "comment_")

(defn table-names
  "Names of all tables"
  [table-count]
  (map (partial str table-prefix) (range table-count)))

(defn id->table
  "Turns an id into a table id"
  [table-count id]
  (str table-prefix (mod (hash id) table-count)))

(defrecord CommentsClient [table-count tbl-created? conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (c/with-conn-failure-retry conn
          (info "Creating tables" (pr-str (table-names table-count)))
          (doseq [t (table-names table-count)]
            (c/execute! conn [(str "create table " t
                                   " (id int primary key,
                                       tkey int)")])
            (when (:table-cache test)
              (c/execute! conn [(str "alter table " t " cache")]))
            (info "Created table" t))))))


  (invoke! [this test op]
    (case (:f op)
      :write (let [[k id] (:value op)
                   table (id->table table-count id)
                   single-stmt-write? (:single-stmt-write test)]
               (c/rand-init-txn! test conn)
               (when single-stmt-write?
                 (c/set-auto-commit! conn true))
               (c/insert! conn table {:id id, :tkey k}
                          {:transaction? (not single-stmt-write?)})
               (c/attach-txn-info conn (assoc op :type :ok)))

      :read (with-txn op [c conn {:isolation (util/isolation-level test)}]
              (->> (table-names table-count)
                   (mapcat (fn [table]
                             (c/query c [(str "select id from "
                                              table
                                              " where tkey = ?")
                                         (key (:value op))])))
                   (map :id)
                   (into (sorted-set))
                   (independent/tuple (key (:value op)))
                   (assoc op :type :ok, :value)))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (c/close! conn)))

(defn generator
  "Preserves TiDB's read coverage and globally unique SQL primary keys."
  [opts]
  (let [limit (:per-key-limit opts 500)]
    (independent/concurrent-generator
      (count (:nodes opts))
      (range)
      (fn [k]
        ; Each key emits at most limit operations, hence at most limit writes.
        ; The independent checker allows per-key IDs; our SQL schema does not.
        (let [writes (map cr/w (range (* k limit) (* (inc k) limit)))]
          (->> (gen/mix [(gen/repeat (cr/r)) writes])
               (gen/stagger 1/100)
               (gen/limit limit)))))))

(defn workload
  [opts]
  (assoc (cr/workload opts)
         :name      "comments"
         :generator (generator opts)
         :client    (CommentsClient. 10 (atom false) nil)))
