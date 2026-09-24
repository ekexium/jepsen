(ns jepsen.generator.worker-node-test
  (:require [clojure.test :refer :all]
            [jepsen.generator.interpreter :as interpreter])
  (:import (java.util.concurrent ArrayBlockingQueue TimeUnit)))

(deftest worker-log-identifies-node-test
  (doseq [[id node] [[0 "n1"] [3 "n2"] [:nemesis nil]]]
    (let [out (ArrayBlockingQueue. 1)
          worker (reify interpreter/Worker
                   (open [this _ _] this)
                   (invoke! [_ _ op]
                     (assoc op :value (.getName (Thread/currentThread))))
                   (close! [_ _]))
          running (interpreter/spawn-worker {:nodes ["n1" "n2"]} out worker id)]
      (try
        (.put ^ArrayBlockingQueue (:in running) {:type :invoke, :f :thread-name})
        (let [completion (.poll out 5 TimeUnit/SECONDS)]
          (is (some? completion) "The real worker dispatches the operation")
          (when completion
            (if node
              (is (.contains ^String (:value completion) node)
                  "Operation logs identify the node assigned to this worker")
              (is (.contains ^String (:value completion) "nemesis")))))
        (finally
          (.put ^ArrayBlockingQueue (:in running) {:type :exit})
          (is (not= ::timeout (deref (:future running) 5000 ::timeout))))))))
