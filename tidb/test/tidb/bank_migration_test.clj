(ns tidb.bank-migration-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [tidb.bank :as bank]))

(deftest multitable-teardown-before-history-test
  (let [client (bank/->MultiBankClient {:tidb.sql/node "n1"} (atom false))]
    (is (nil? (client/teardown! client {:nodes ["n1"]})))))

(deftest multitable-failure-collects-mvcc-diagnostics-test
  (let [workload (bank/multitable-workload {})
        test {:nodes ["n1"], :accounts [0 1], :total-amount 100}
        bank-checker (get-in workload [:checker :checkers :SI])
        reads (fn [balances]
                (h/history [{:type :invoke, :process 0, :f :read}
                            {:type :ok, :process 0, :f :read, :value balances}]))
        requested (atom [])]
    (with-redefs [clojure.core/slurp (fn [url]
                                     (swap! requested conj url)
                                     "{}")]
      (is (true? (:valid? (checker/check bank-checker test (reads {0 60, 1 40}) {}))))
      (is (empty? @requested))
      (is (false? (:valid? (checker/check bank-checker test (reads {0 60, 1 30}) {}))))
      (is (= #{"http://n1:10080/mvcc/key/test/accounts0/0"
               "http://n1:10080/mvcc/key/test/accounts1/0"}
             (set @requested))))))
