(ns tidb.nemesis-test
  (:require [clojure.test :refer :all]
            [clj-commons.slingshot :refer [try+]]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gen.test]
            [jepsen.nemesis :as nemesis]
            [jepsen.net :as net]
            [tidb.db :as db]
            [tidb.nemesis :as n]
            [tidb.util :as tu]))

(defn operations
  "Run the real generator with one nemesis worker, advancing simulated time
  for both nemesis calls and sleeps. No SSH or wall-clock sleeps are needed."
  [generator]
  (let [ops (atom [])]
    (gen.test/simulate
      (gen/nemesis generator)
      (fn [_ op]
        (swap! ops conj op)
        (update op :time + (if (= :sleep (:type op))
                            (long (* 1e9 (:value op)))
                            10))))
    @ops))

(deftest fault-recovery-alternates-test
  ;; Finite map generators must not exhaust a fault profile after one cycle.
  (doseq [[fault recovery] [[:kill-pd :start-pd]
                            [:kill-kv :start-kv]
                            [:kill-db :start-db]
                            [:kill-tikv-worker :start-tikv-worker]
                            [:stop-pd :start-pd]
                            [:stop-kv :start-kv]
                            [:stop-db :start-db]
                            [:stop-tikv-worker :start-tikv-worker]
                            [:pause-pd :resume-pd]
                            [:pause-kv :resume-kv]
                            [:pause-db :resume-db]
                            [:pause-tikv-worker :resume-tikv-worker]
                            [:shuffle-leader :del-shuffle-leader]
                            [:shuffle-region :del-shuffle-region]
                            [:random-merge :del-random-merge]
                            [:enable-failpoint :disable-failpoint]
                            [:start-netem :stop-netem]]]
    (testing (name fault)
      (let [ops (operations (gen/limit 6
                             (n/mixed-generator
                               {fault true :schedule :fixed :interval 1})))]
        (is (= [fault recovery fault recovery fault recovery]
               (mapv :f ops)))
        (is (every? #(>= % 1000000000)
                    (map - (rest (map :time ops)) (map :time ops))))))))

(deftest every-partition-profile-recovers-test
  (doseq [fault [:partition-one :partition-pd-leader
                :partition-half :partition-ring]]
    (testing (name fault)
      (is (= [:stop-partition]
             (mapv :f (operations (n/final-generator {fault true}))))))))

(deftest partitions-alternate-with-healing-test
  (doseq [[fault kind] [[:partition-one :single-node]
                       [:partition-pd-leader :pd-leader]
                       [:partition-half :half]
                       [:partition-ring :ring]]]
    (let [ops (operations (gen/limit 4
                           (n/mixed-generator
                             {fault true :schedule :fixed :interval 1})))
          starts (filter #(= :start-partition (:f %)) ops)]
      (is (= [:start-partition :stop-partition
              :start-partition :stop-partition]
             (mapv :f ops)))
      (is (= [kind kind] (mapv :partition-type starts)))
      (when-not (= :pd-leader kind)
        (is (every? #(seq (:value %)) starts))))))

(deftest worker-faults-require-tidbx-test
  (doseq [fault [:kill-tikv-worker :stop-tikv-worker :pause-tikv-worker]]
    (is (= :rejected
           (try+
             (n/nemesis {:enable-tidbx false
                         :nemesis {fault true :interval 1}})
             :accepted
             (catch [:type :tikv-worker-nemesis-requires-tidbx] _
               :rejected))))))

(deftest shorthand-worker-gating-test
  (doseq [[fault regular worker recovery]
          [[:kill #{:kill-pd :kill-kv :kill-db} :kill-tikv-worker :start-tikv-worker]
           [:stop #{:stop-pd :stop-kv :stop-db} :stop-tikv-worker :start-tikv-worker]
           [:pause #{:pause-pd :pause-kv :pause-db} :pause-tikv-worker :resume-tikv-worker]]
          enabled? [false true]]
    (let [opts {:enable-tidbx enabled?
                :nemesis {fault true :interval 1 :schedule :fixed}}
          {:keys [generator final-generator]} (n/nemesis opts)
          fs (set (map :f (operations (gen/limit 200 generator))))
          recoveries (set (map :f (operations final-generator)))]
      (is (every? fs regular))
      (is (= enabled? (contains? fs worker)))
      (is (= enabled? (contains? recoveries recovery))))))

(deftest failpoint-profile-enables-and-cleans-up-test
  ;; Mock only HTTP calls: the real composite nemesis chooses ports, reports
  ;; results, and excludes the persistent enableTestAPI failpoint on cleanup.
  (let [active (atom {})
        opts {:nemesis {:failpoint true :interval 1 :schedule :fixed
                        :failpoints [["tidb" "tikvclient/rpcFailOnRecv" "return(\"write\")"]]}}
        {:keys [nemesis generator final-generator]} (n/nemesis opts)
        test {:nodes ["n1"]}
        starts (operations (gen/limit 1 generator))
        cleanup (operations final-generator)]
    (with-redefs [tu/fail-enable! (fn [node port point action]
                                  (swap! active assoc [node port point] action))
                  tu/fail-list (fn [node port]
                                 (cons ["server/enableTestAPI" "return(true)"]
                                       (for [[[n p point] action] @active
                                             :when (= [node port] [n p])]
                                         [point action])))
                  tu/fail-disable! (fn [node port point]
                                    (is (not= "server/enableTestAPI" point))
                                    (swap! active dissoc [node port point]))]
      (is (= [:enable-failpoint] (mapv :f starts)))
      (is (= [:disable-failpoint] (mapv :f cleanup)))
      (doseq [op starts] (nemesis/invoke! nemesis test op))
      (is (= {["n1" 10080 "tikvclient/rpcFailOnRecv"] "return(\"write\")"}
             @active))
      (doseq [op cleanup] (nemesis/invoke! nemesis test op))
      (is (empty? @active)))))

(deftest restart-kv-without-pd-completes-test
  ;; Wrapping only the old seq in a vector would repeat :kill-kv forever,
  ;; because functions in the modern generator API are themselves infinite.
  (let [ops (operations (gen/limit 10 (n/restart-kv-without-pd-generator)))
        faults (remove #(= :sleep (:type %)) ops)]
    (is (= [:kill-kv :pause-pd :start-kv :resume-pd] (mapv :f faults)))
    (is (= [["n1" "n2"] ["n1" "n2"]]
           (mapv :value (take 2 faults))))
    (is (>= (- (:time (last faults)) (:time (nth faults 2)))
            70000000000))))

(deftest long-recovery-repeats-test
  (let [ops (operations (gen/limit 10
                         (n/full-generator {:enable-failpoint true
                                            :long-recovery true
                                            :schedule :fixed :interval 50})))
        sleeps (keep-indexed #(when (= :sleep (:type %2)) %1) ops)]
    (is (= 2 (count sleeps)))
    (doseq [i sleeps]
      (is (= :disable-failpoint (:f (nth ops (dec i)))))
      (is (= 60 (:value (nth ops i)))))
    (is (= :enable-failpoint (:f (nth ops (inc (first sleeps))))))
    (is (>= (- (:time (nth ops (inc (first sleeps))))
               (:time (nth ops (first sleeps))))
            60000000000))))

(deftest random-schedule-keeps-bounded-delays-test
  ;; Upstream stagger now defaults to an exponential distribution. TiDB's
  ;; existing random profile uses uniform delays bounded by twice the interval.
  (let [times (map :time
                  (operations
                    (gen/limit 200 (n/mixed-generator
                                     {:enable-failpoint true
                                      :schedule :random :interval 1}))))
        gaps (map - (rest times) times)]
    (is (= 199 (count gaps)))
    (is (every? #(<= 0 % 2000000000) gaps))))

(deftest pd-leader-is-discovered-at-invocation-test
  ;; A slow/unavailable PD must not block the central generator and clients.
  ;; Querying when the fault executes also avoids selecting a stale leader.
  (let [queries (atom [])
        isolated (atom nil)
        test {:nodes ["n1" "n2" "n3"]}
        leader (atom "n1")]
    (with-redefs [gen.test/default-test test
                  db/pd-leader-node (fn [_ _]
                                      (swap! queries conj @leader)
                                      @leader)
                  net/drop-all! (fn [_ grudge] (reset! isolated grudge))]
      (let [op (first (operations
                       (gen/limit 1 (n/mixed-generator
                                      {:partition-pd-leader true
                                       :interval 1 :schedule :fixed}))))]
        (is (empty? @queries))
        (reset! leader "n2")
        (nemesis/invoke! (n/full-nemesis {}) test op)
        (is (= ["n2"] @queries))
        (is (= #{"n1" "n3"} (get @isolated "n2")))
        (is (= #{"n2"} (get @isolated "n1")))
        (is (= #{"n2"} (get @isolated "n3")))))))

(deftest interrupted-restart-kv-without-pd-recovers-test
  ;; End the special sequence just after it stops KV and pauses PD. The
  ;; final phase must recover both even though the ordinary sequence ended.
  (let [{:keys [generator final-generator]}
        (n/nemesis {:nemesis {:restart-kv-without-pd true}})
        ops (operations (gen/phases (gen/limit 3 generator)
                                    final-generator))]
    (is (= [:kill-kv :pause-pd :resume-pd :start-kv]
           (mapv :f (remove #(= :sleep (:type %)) ops))))))
