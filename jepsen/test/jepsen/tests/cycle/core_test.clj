(ns jepsen.tests.cycle.core-test
  (:require [clojure.test :refer :all]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gen.test]
            [jepsen.tests.cycle.core :as cycle]))

(deftest final-generator-reads-highest-key-and-partial-batch
  (doseq [[max-key expected] [[0 #{0}]
                             [7 #{0 1 2 3 4 5 6 7}]
                             [9 #{0 1 2 3 4 5 6 7 8 9}]]]
    (let [ops (->> (gen/phases
                     (gen/once {:f :txn :value [[:append max-key 1]]})
                     (cycle/final-gen))
                   cycle/max-key-tracker gen/clients gen.test/perfect)
          read-keys (->> ops (mapcat :value)
                         (filter #(= :r (first %))) (map second) set)]
      (is (= expected read-keys)))))
