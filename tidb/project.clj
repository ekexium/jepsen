(defproject tidb "0.1.1-SNAPSHOT"
  :description "Jepsen testing for TiDB"
  :url "https://pingcap.com/index"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main tidb.core
  :jvm-opts ["-Djava.awt.headless=true"]
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [clj-http "3.10.0"]
                 [cheshire "5.8.1"]
                 [org.clojars.pingcap/jepsen "0.3.14-SNAPSHOT"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [org.mariadb.jdbc/mariadb-java-client "2.4.1"]
                 [javax.xml.bind/jaxb-api "2.3.1"]
                 [org.glassfish.jaxb/jaxb-runtime "2.3.1"]]
  :aot [tidb.core clojure.tools.logging.impl])
