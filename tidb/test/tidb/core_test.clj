(ns tidb.core-test
  (:require [clojure.test :refer :all]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gen.test]
            [jepsen.tests.cycle.core :as cycle]
            [tidb.core :as core]
            [tidb.nemesis :as nemesis]))

(def options
  {:version "test" :workload :fixture :time-limit 1
   :concurrency 2 :nodes ["n1" "n2"] :nemesis {:interval 10}
   :auto-retry-limit 0 :recovery-time 2 :final-recovery-time 0})

(defn generated-history [workload]
  (with-redefs [core/workloads {:fixture (fn [_] workload)}
                nemesis/nemesis (fn [_]
                                  {:generator nil
                                   :final-generator [{:type :info :f :heal}]})]
    (gen.test/simulate
      (:generator (core/test options))
      (fn [_ op]
        (-> op
            (assoc :type (if (= :invoke (:type op)) :ok :info))
            (update :time + (if (= :sleep (:type op))
                              (long (* 1000000000 (:value op)))
                              10)))))))

(deftest final-reads-use-the-workload-key-tracker
  (let [h (generated-history
            {:generator [{:f :txn :value [[:append 0 1]]}
                         {:f :txn :value [[:append 9 1]]}]
             :wrap-generator cycle/max-key-tracker
             :final-generator (cycle/final-gen)})
        keys (->> h (filter #(= :invoke (:type %)))
                  (mapcat :value) (filter #(= :r (first %)))
                  (map second) set)]
    (is (= #{0 1 2 3 4 5 6 7 8 9} keys))))

(deftest faults-are-healed-without-a-final-read-workload
  (let [h (generated-history {:generator [{:f :read}]})]
    (is (some #(= :heal (:f %)) h))))

(deftest recovery-time-applies-before-final-reads
  (let [h (generated-history {:generator [{:f :read}]
                             :final-generator [{:f :final-read}]})
        heal (last (filter #(= :heal (:f %)) h))
        read (first (filter #(and (= :invoke (:type %))
                                 (= :final-read (:f %))) h))]
    (is (some? heal))
    (is (some? read))
    (is (<= 2000000000 (- (:time read) (:time heal))))))
