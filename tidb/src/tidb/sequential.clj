(ns tidb.sequential
  "A sequential consistency test.

  Verify that client order is consistent with DB order by performing queries
  (in four distinct transactions) like

  A: insert x
  A: insert y
  B: read y
  B: read x

  A's process order enforces that x must be visible before y; we should never
  observe y alone.

  Splits keys up onto different tables to make sure they fall in different
  shard ranges"
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [core :as jepsen]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh]]]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [tidb.sql :as c :refer :all]
            [tidb.basic :as basic]
            [knossos.model :as model]
            [knossos.op :as op]
            [clojure.core.reducers :as r]))

(def table-prefix "String prepended to all table names." "seq_")

(defn table-names
  "Names of all tables"
  [table-count]
  (map (partial str table-prefix) (range table-count)))

(defn key->table
  "Turns a key into a table id"
  [table-count k]
  (str table-prefix (mod (hash k) table-count)))

(defn subkeys
  "The subkeys used for a given key, in order."
  [key-count k]
  (mapv (partial str k "_") (range key-count)))

(defrecord SequentialClient [table-count tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (locking SequentialClient
      (c/with-conn-failure-retry conn
        (info "Creating tables" (pr-str (table-names table-count)))
        (doseq [t (table-names table-count)]
          (c/execute! conn [(str "create table if not exists " t
                                 " (tkey varchar(255) primary key)")])
          (when (:table-cache test)
            (c/execute! conn [(str "alter table " t " cache")]))
          (info "Created table" t)))))

  (invoke! [this test op]
    (let [ks (subkeys (:key-count test) (:value op))]
      (case (:f op)
        :write
        (do (doseq [k ks]
              (let [table (key->table table-count k)]
                (with-txn-retries
                  (c/rand-init-txn! test conn)
                  (c/insert! conn table {:tkey k}))))
            (c/attach-txn-info conn (assoc op :type :ok)))
        :read
        (->> ks
             reverse
             (mapv (fn [k]
                     (first
                       (with-txn-retries
                         (c/query conn [(str "select tkey from "
                                             (key->table table-count k)
                                             " where tkey = ?") k]
                                  {:row-fn :tkey})))))
             (vector (:value op))
             (assoc op :type :ok, :value)))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn trailing-nil?
  "Does the given sequence contain a nil anywhere after a non-nil element?"
  [coll]
  (some nil? (drop-while nil? coll)))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (assert (integer? (:key-count test)))
      (let [reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %)))
                       (r/map :value)
                       (into []))
            none (filter (comp (partial every? nil?) second) reads)
            some (filter (comp (partial some nil?) second) reads)
            bad  (filter (comp trailing-nil? second) reads)
            all  (filter (fn [[k ks]]
                           (= (subkeys (:key-count test) k)
                              (reverse ks)))
                         reads)]
        {:valid?      (not (seq bad))
         :all-count   (count all)
         :some-count  (count some)
         :none-count  (count none)
         :bad-count   (count bad)
         :bad         bad}))))

(defn writes
  "Emit distinct keys. The sequence advances only when an op is consumed."
  []
  (map (fn [k] {:type :invoke, :f :write, :value k}) (range)))

(defn reads
  "Read a recently invoked write, waiting until at least one exists."
  []
  (reify gen/Generator
    (op [this test ctx]
      (if-let [ks (seq (::recent-writes ctx))]
        [(gen/fill-in-op {:f :read, :value (rand-nth (vec ks))} ctx) this]
        [:pending this]))
    (update [this test ctx event] this)))

(defn gen
  "Basic generator with n writers, tracking their last 2n invoked keys."
  [n]
  (gen/track ::recent-writes clojure.lang.PersistentQueue/EMPTY
             (fn [ks op]
               (if (and (= :invoke (:type op)) (= :write (:f op)))
                 (conj (if (= (* 2 n) (count ks)) (pop ks) ks)
                       (:value op))
                 ks))
             (gen/reserve n (writes) (reads))))

(defn workload
  [opts]
  (let [c         (:concurrency opts)
        gen       (gen (quot (inc c) 2))
        keyrange (atom {})]
      {:key-count 5
       :keyrange  keyrange
       :client    (SequentialClient. c (atom false) nil)
       :generator (gen/stagger 1/100 gen)
       :checker   (checker)}))
