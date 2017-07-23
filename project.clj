(defproject bitcoin-stream "0.1.0-SNAPSHOT"
  :description "Exercise to practise websockets, JSON and data streaming processing"
  :url "http://example.com/FIXME"
  :license {:name "GNU General Public License (GPL) version 3"
            :url "https://www.gnu.org/licenses/gpl.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]
                 [aleph "0.4.3"]
                 [org.clojure/data.json "0.2.6"]
                 [manifold "0.1.6"]]
  :main ^:skip-aot bitcoin-stream.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
