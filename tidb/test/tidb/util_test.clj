(ns tidb.util-test
  (:require [clojure.test :refer :all]
            [elle.core :as elle]
            [tidb.util :as util]))

(deftest tso-graph-preserves-known-order-test
  (let [a {:txn-info {:start_ts 10, :commit_ts 20}}
        b {:txn-info {:start_ts 21, :commit_ts 30}}
        reader {:txn-info {:start_ts 31}}
        unknown {:txn-info {:start_ts util/max-ts}}
        [graph explainer] (util/tso-graph [reader unknown b a])]
    (is (= :tso (.edge graph a b)))
    (is (= :tso (.edge graph b reader)))
    (is (not (.contains (.vertices graph) unknown)))
    (is (= "a's commit-ts 20 < b's start-ts 21"
           (elle/explain-pair explainer "a" a "b" b)))))
