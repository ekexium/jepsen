(ns jepsen.tests.bank-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [jepsen.tests.bank :as bank]))

(deftest foreign-key-records-balance-test
  (let [test {:accounts [0 1], :total-amount 100}
        check (fn [amount]
                (checker/check
                  (bank/checker {}) test
                  (h/history [{:type :invoke, :process 0, :f :read}
                              {:type :ok, :process 0, :f :read,
                               :value {0 60, 1 40}, :total-moved amount}]) {}))]
    (doseq [amount [nil 0]]
      (is (true? (:valid? (check amount)))))
    (doseq [amount [-5 1]]
      (let [result (check amount)]
        (is (false? (:valid? result)))
        (is (= 1 (:error-count result)))
        (is (= amount (get-in result [:errors :non-zero-total-moved :first :total-moved])))
        (is (= amount (get-in result [:errors :non-zero-total-moved :worst :total-moved])))))))
