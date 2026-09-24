(ns tidb.workloads-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gen.test]
            [jepsen.history :as h]
            [tidb.monotonic :as monotonic]
            [tidb.sequential :as sequential]
            [tidb.sets :as sets]
            [tidb.table :as table]))

(defn history [ops]
  (h/dense-history (map-indexed (fn [i op]
                           (assoc op :index i :time (* i 1000000)))
                         ops)))

(defn aborted-read [write-fn writer-type]
  (let [value (if (= :append write-fn) [7] 7)]
    (history [{:process 0 :type :invoke :f :txn :value [[write-fn 0 7]]}
              {:process 0 :type writer-type :f :txn :value [[write-fn 0 7]]}
              {:process 1 :type :invoke :f :txn :value [[:r 0 nil]]}
              {:process 1 :type :ok :f :txn :value [[:r 0 value]]}])))

(defn check-workload [workload opts history]
  (checker/check (:checker (workload opts))
                 {:name "tidb-workload-test"
                  :start-time (str (java.util.UUID/randomUUID))}
                 history {}))

(deftest failed-writes-must-not-be-visible
  (doseq [[workload write-fn] [[monotonic/append-workload :append]
                              [monotonic/txn-workload :w]]
          opts [{} {:isolation :repeatable-read}
                   {:isolation :read-committed}]]
    (let [result (check-workload workload opts (aborted-read write-fn :fail))]
      (is (false? (:valid? result)) (pr-str [opts result]))
      (is (some #{:G1a} (:anomaly-types result)) (pr-str result)))))

(deftest indeterminate-writes-may-have-committed
  (doseq [[workload write-fn] [[monotonic/append-workload :append]
                              [monotonic/txn-workload :w]]]
    (let [result (check-workload workload {} (aborted-read write-fn :info))]
      ;; The register analyzer can return :unknown for this tiny history
      ;; because it cannot infer an edge from the indeterminate writer.
      (is (not= false (:valid? result)) (pr-str result))
      (is (not-any? #{:G1a} (:anomaly-types result)) (pr-str result)))))

(deftest snapshot-isolation-allows-write-skew
  ;; Both transactions read the initial snapshot and update different keys.
  ;; Rejecting this would silently require serializability from TiDB RR.
  (let [h (history [{:process 0 :type :invoke :f :txn :value [[:r 0 nil] [:w 1 1]]}
                    {:process 1 :type :invoke :f :txn :value [[:r 1 nil] [:w 0 1]]}
                    {:process 0 :type :ok :f :txn :value [[:r 0 nil] [:w 1 1]]}
                    {:process 1 :type :ok :f :txn :value [[:r 1 nil] [:w 0 1]]}])]
    (is (true? (:valid? (check-workload monotonic/txn-workload {} h))))))

(deftest recovery-reads-include-retired-keys
  (doseq [workload [monotonic/append-workload monotonic/txn-workload]]
    (let [{:keys [wrap-generator final-generator]} (workload {})
          generator (wrap-generator
                      (gen/phases
                        (gen/once {:f :txn :value [[:w 0 1]]})
                        (gen/once {:f :txn :value [[:w 9 1]]})
                        final-generator))
          reads (->> generator gen/clients gen.test/perfect
                     (mapcat :value) (filter #(= :r (first %)))
                     (map second) set)]
      (is (= #{0 1 2 3 4 5 6 7 8 9} reads)))))

(deftest sequential-generator-retains-recent-writes
  (let [ops (->> (sequential/gen 1) (gen/limit 40)
                 gen/clients gen.test/perfect)
        writes (filter #(= :write (:f %)) ops)
        reads (filter #(= :read (:f %)) ops)]
    (is (seq writes))
    (is (seq reads))
    (is (= (count writes) (count (set (map :value writes)))))
    (doseq [read reads]
      (is (some #(and (= (:value %) (:value read))
                      (<= (:time %) (:time read))) writes)))))

(deftest set-generator-continues-reading
  ;; A literal operation is consumed once in the pure generator API.
  (let [ops (->> (:generator (sets/workload {:concurrency 2}))
                 (gen/limit 30) gen/clients gen.test/perfect)]
    (is (< 1 (count (filter #(= :read (:f %)) ops))))))

(deftest table-creation-generator-is-immutable
  (let [generator (table/generator)
        ctx (gen.test/n+nemesis-context 2)
        [first-op next-generator] (gen/op generator {} ctx)
        [same-op _] (gen/op generator {} ctx)
        [next-op _] (gen/op next-generator {} ctx)]
    (is (= (:value first-op) (:value same-op)))
    (is (= 1 (:value first-op)))
    (is (= 2 (:value next-op)))))

(deftest sparse-history-checking-does-not-deadlock
  ;; Histories partitioned by independent/checker preserve non-contiguous
  ;; indices. Elle 0.2.7 can deadlock while building their lazy index in a fold.
  (doseq [[workload write-fn] [[monotonic/append-workload :append]
                              [monotonic/txn-workload :w]]]
    (let [history (->> (aborted-read write-fn :fail)
                       (map-indexed #(assoc %2 :index (* 2 %1)))
                       h/sparse-history)
          check (future (check-workload workload {} history))
          result (deref check 10000 ::timeout)]
      (future-cancel check)
      (is (not= ::timeout result) "Checking a four-operation history timed out")
      (when (map? result)
        (is (false? (:valid? result)))
        (is (some #{:G1a} (:anomaly-types result)))))))

(deftest completed-writes-are-visible-to-later-snapshots
  ;; The old RR append checker included realtime edges. Preserve that
  ;; guarantee without strengthening SI all the way to serializability.
  (doseq [[workload write-fn] [[monotonic/append-workload :append]
                              [monotonic/txn-workload :w]]]
    (let [hist (history [{:process 0 :type :invoke :f :txn :value [[write-fn 0 7]]}
                         {:process 0 :type :ok :f :txn :value [[write-fn 0 7]]}
                         {:process 1 :type :invoke :f :txn :value [[:r 0 nil]]}
                         {:process 1 :type :ok :f :txn :value [[:r 0 nil]]}])]
      (is (false? (:valid? (check-workload workload {} hist))))
      ;; RC's previous anomaly selection did not impose this constraint.
      (is (not= false (:valid? (check-workload workload
                                             {:isolation :read-committed}
                                             hist)))))))
