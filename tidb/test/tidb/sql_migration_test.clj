(ns tidb.sql-migration-test
  (:require [clojure.test :refer :all]
            [tidb.sql :as sql]))

(def op {:type :invoke :f :txn :value [[:append 0 7]]})

(defn classify [e]
  (try
    (sql/with-error-handling op (throw e))
    (catch java.sql.SQLException _ {:type :unhandled})))

(deftest known-aborts-do-not-depend-on-jdbc-subclass
  (doseq [error [(java.sql.SQLException. "Write conflict [try again later]")
                 (java.sql.BatchUpdateException. "Write conflict [try again later]" (int-array [-3]))
                 (java.sql.SQLTransactionRollbackException. "Write conflict [try again later]")]]
    (is (= {:type :fail :error :conflict}
           (select-keys (classify error) [:type :error])))))

(deftest uncertain-outcomes-remain-uncertain
  (is (= :info (:type (classify (java.sql.BatchUpdateException. "Query timed out" (int-array [-3]))))))
  (is (= :info (:type (classify (java.sql.SQLNonTransientConnectionException. "Connection timed out")))))
  (is (= :unhandled (:type (classify (java.sql.SQLException. "unknown commit outcome"))))))
