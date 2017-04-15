(defproject ruwaterloo-clj-scraper "0.1.0-SNAPSHOT"
  :description "Scraper for /r/uwaterloo"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.442"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/java.jdbc "0.7.0-alpha3"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.xerial/sqlite-jdbc "3.16.1"]
                 [clj-http "2.3.0"]
                 [com.taoensso/timbre "4.8.0"]]
  :main ^:skip-aot ruwaterloo-clj-scraper.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
