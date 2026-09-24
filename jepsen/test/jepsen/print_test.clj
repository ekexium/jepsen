(ns jepsen.print-test
  (:require [clojure [pprint]
                     [test :refer :all]]
            [jepsen [history :as h]
                    [print :as p]]))

; We don't want to print out jepsen.history.op everywhere; it's much harder to
; read
(deftest op-pprint-test
  (let [op (h/op {:index 0
                  :time 1
                  :process 2
                  :type :ok
                  :f :read
                  :value "hi"})]
    (testing "Clojure pprint"
      (is (= "{:process 2, :type :ok, :f :read, :value \"hi\", :index 0, :time 1}\n"
             (with-out-str (clojure.pprint/pprint op)))))

    (testing "Jepsen Fipp pprint"
      (is (= "{:index 0, :time 1, :type :ok, :process 2, :f :read, :value \"hi\"}\n"
             (with-out-str (p/pprint op)))))))


(deftest op-transaction-info-test
  (let [op (h/op {:index 0, :time 1, :process 2, :type :ok, :f :txn,
                  :value [[:append 0 7]],
                  :txn-info {:start_ts 100, :commit_ts 110},
                  :error :connection-lost})]
    (doseq [rendered [(p/op->str op) (with-out-str (p/prn-op op))]]
      (is (re-find #":start_ts 100" rendered))
      (is (re-find #":commit_ts 110" rendered))
      (is (re-find #"connection-lost" rendered)))
    (is (= "2\t:ok\t:txn\t[[:append 0 7]]"
           (p/op->str (dissoc op :txn-info :error))))))
