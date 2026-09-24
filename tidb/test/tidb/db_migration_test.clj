(ns tidb.db-migration-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [jepsen.control :as control]
            [jepsen.control.util :as cu]
            [jepsen.db :as db]
            [tidb.db :as tidb]))

(defn setup-until-install []
  (try
    (db/setup! (tidb/db) {} "n1")
    (catch clojure.lang.ExceptionInfo e (:stage (ex-data e)))))

(deftest a-new-node-reaches-installation
  (with-redefs [tidb/stop! (fn [& _])
                cu/exists? (constantly false)
                cu/ls (fn [_] (throw (ex-info "missing directory"
                                      {:type :jepsen.control/nonzero-exit :exit 1})))
                tidb/install! (fn [& _] (throw (ex-info "installation reached" {:stage :install})))]
    (is (= :install (setup-until-install)))))

(deftest resetting-a-node-retains-its-binary-directory
  (let [commands (atom [])]
    (with-redefs [tidb/stop! (fn [& _])
                  cu/exists? (constantly true)
                  cu/ls (fn [_] ["bin" "data" "db.log"])
                  control/exec (fn [& args] (swap! commands conj args))
                  tidb/install! (fn [& _] (throw (ex-info "installation reached" {:stage :install})))]
      (is (= :install (setup-until-install)))
      (is (= [[:rm :-rf ["/opt/tidb/data" "/opt/tidb/db.log"]]] @commands)))))

(deftest flat-binary-layout-survives-data-reset
  (let [dir (.toFile (java.nio.file.Files/createTempDirectory
                       "tidb-layout-" (make-array java.nio.file.attribute.FileAttribute 0)))
        bin (io/file dir "bin")
        executable (io/file bin "pd-server")
        exec (fn [& args]
               (when-not (= :sync (first args))
                 (let [r (apply shell/sh (map #(if (keyword? %) (name %) (str %))
                                              (flatten args)))]
                   (when-not (zero? (:exit r)) (throw (ex-info "local command failed" r)))
                   (:out r))))]
    (try
      (with-redefs [tidb/tidb-dir (.getPath dir)
                    tidb/tidb-bin-dir (.getPath bin)
                    cu/exists? #(.exists (io/file %))
                    cu/ls #(mapv (fn [f] (.getName f)) (.listFiles (io/file %)))
                    cu/install-archive! (fn [_ _] (spit (io/file dir "pd-server") "binary-marker"))
                    control/exec exec]
        (tidb/install! {:force-reinstall true :tarball-url "http://test/binaries.tar.gz"} "n1")
        (with-redefs [tidb/stop! (fn [& _])
                      tidb/install! (fn [& _] (throw (ex-info "installation reached" {:stage :install})))]
          (is (= :install (setup-until-install)))))
      (is (.isFile executable) "Resetting data must not delete the binaries")
      (when (.isFile executable) (is (= "binary-marker" (slurp executable))))
      (finally (shell/sh "rm" "-rf" (.getPath dir))))))
