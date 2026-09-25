(ns tidb.comments-migration-test
  (:require [clojure.test :refer :all]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gen.test]
            [tidb.comments :as comments]))

(defn operations [concurrency]
  (->> (:generator (comments/workload {:nodes ["n1" "n2"]
                                 :per-key-limit 50}))
       (gen/limit 200)
       gen/clients
       (gen.test/perfect (gen.test/n+nemesis-context concurrency))))

(deftest reads-continue-throughout-each-key
  ;; A single map is consumed once by the pure generator API; later writes
  ;; would then have no reads with which to check their visibility.
  (doseq [concurrency [2 4]]
    (let [by-key (group-by (comp key :value) (operations concurrency))
          completed-keys (filter #(= 50 (count (val %))) by-key)]
      (is (<= 3 (count completed-keys)))
      (doseq [[k ops] completed-keys]
        (is (< 1 (count (filter #(= :read (:f %)) ops))) (str "key " k))))))

(deftest write-identifiers-are-unique-across-keys
  ;; Clients such as tidb.comments use the write ID as a SQL primary key,
  ;; with the independent key stored in a separate column. Check sequential
  ;; and concurrent key groups, including reuse of workers for fresh keys.
  (doseq [concurrency [2 4]]
    (let [writes (filter #(= :write (:f %)) (operations concurrency))
          ids (map (comp val :value) writes)]
      (is (<= 4 (count (set (map (comp key :value) writes)))))
      (is (= (count ids) (count (set ids)))))))
