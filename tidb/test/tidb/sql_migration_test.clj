(ns tidb.sql-migration-test
  (:require [clojure.test :refer :all]
            [tidb.sql :as sql]))

(def op {:type :invoke :f :txn :value [[:append 0 7]]})

(defn classify [e]
  (try
    (sql/with-error-handling op (throw e))
    (catch java.sql.SQLException _ {:type :unhandled})))

(def exception-constructors
  [#(java.sql.SQLException. ^String %)
   #(java.sql.BatchUpdateException. ^String % (int-array [-3]))
   #(java.sql.SQLTransactionRollbackException. ^String %)
   #(java.sql.SQLNonTransientConnectionException. ^String %)
   #(java.sql.SQLTimeoutException. ^String %)])

(defn thrown-value [f]
  (try (f)
       (catch Throwable e e)))

(deftest known-aborts-do-not-depend-on-jdbc-subclass
  (doseq [make-exception exception-constructors
          message ["Write conflict [try again later]"
                   "Deadlock found when trying to get lock; try restarting transaction"
                   "can not retry select for update statement"]]
    (let [error (make-exception message)]
      (testing (str (class error) " " message)
        (is (= {:type :fail :error :conflict}
               (select-keys (classify error) [:type :error])))))))

(deftest missing-sql-message-preserves-original-exception
  (doseq [make-exception exception-constructors]
    (let [error (make-exception nil)]
      (testing (str (class error))
        (is (identical? error
                        (thrown-value #(sql/capture-txn-abort (throw error)))))
        (is (identical? error
                        (thrown-value #(sql/with-error-handling op
                                         (throw error)))))))))

(deftest uncertain-outcomes-remain-uncertain
  (is (= :info (:type (classify (java.sql.BatchUpdateException. "Query timed out" (int-array [-3]))))))
  (is (= :info (:type (classify (java.sql.SQLNonTransientConnectionException. "Connection timed out")))))
  (is (= :unhandled (:type (classify (java.sql.SQLException. "unknown commit outcome"))))))
