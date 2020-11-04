(ns tidb.util
  (:require [clojure.string :as str]
            [jepsen.generator :as gen]))

(gen/defgenerator WithDDL [g]
  [g]
  (op [_ test process]
    (condp > (rand)
      0.06 {:type  :invoke
            :f     :ddl
            :value :add-index}
      0.12 {:type  :invoke
            :f     :ddl
            :value :drop-index}
      ; 0.13 {:type  :invoke
      ;       :f     :ddl
      ;       :value :cancel-job}
      (gen/op g test process))))

(defn with-ddl [g] (WithDDL. g))

(defn select-for-update? [test] (= "FOR UPDATE" (:read-lock test)))

(defn isolation-level [test] (get test :isolation :repeatable-read))
