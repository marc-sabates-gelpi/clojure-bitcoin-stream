(ns bitcoin-stream.core
  (:require [clojure.data.json :as json]
            [aleph.http :as http]
            [aleph.http.client :as http-client]
            [manifold.stream :as s]
            [clojure.core.async :refer [timeout <! go-loop alt! chan go >!]]
            [clj-time.local :as l]
            [medley.core :as medley])
  (:gen-class))

;; Objectives
;; * Use websockets client
;; * Parse JSON data
;; * Work with data streams
;; * Try to convert a stream to a lazy coll with (s/stream->seq conn)

(def ^:const END_POINT "wss://ws.blockchain.info/inv")
(def ^:const TIMEOUT 30000)
(def ^:const ops {:ping (json/write-str {"op" "ping"})
                  :transactions (json/write-str {"op" "unconfirmed_sub"})
                  :blocks (json/write-str {"op" "blocks_sub"})
                  :last-transaction (json/write-str {"op" "ping_tx"})
                  :last-block (json/write-str {"op" "ping_block"})})
(def network-activity (chan 128))

(defmacro websocket-write [c data]
  `(do
     (go (>! network-activity ~data))
     (s/put! ~c ~data)))

(defmacro ping [c]
  `(s/put! ~c (:ping ops)))

(defn keep-alive [conn]
  (go-loop []
    (alt! [(timeout TIMEOUT)] (ping conn)
          [network-activity] nil) 
    (recur)))

(defmulti filter-response :op)

(defmethod filter-response "block" [resp] (medley/dissoc-in resp [:x :txIndexes]))

(defmethod filter-response :default [resp] resp)

(defn -main
  [& args]
  (let [[op-key & _] args]
    (if-let [k (keyword op-key)]
      (if-let [op (k ops)]
        (let [conn @(http/websocket-client END_POINT {:max-frame-payload 256000 :max-frame-size 512000})]
          (keep-alive conn)  
          (websocket-write conn op)
          (loop []
            (if-let [response @(s/take! conn)]
              (-> response
                  (json/read-str :key-fn keyword)
                  filter-response
                  (#(str "\n" (l/local-now) " Message:\n" %))
                  println))
            (recur)))))))
