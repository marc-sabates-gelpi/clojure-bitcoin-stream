(ns bitcoin-stream.core
  (:require [clojure.data.json :as json]
            [aleph.http :as http]
            [manifold.stream :as s]
            [clojure.core.async :refer [timeout <! go-loop alt! chan go >!]])
  (:gen-class))

;; Objectives
;; * Use websockets client
;; * Parse JSON data
;; * Work with data streams

;;FIXME There is a design flaw:
;; If we do more than one put operation in a row we are filling the queue and then we will set as many timeouts as puts

(def END_POINT "wss://ws.blockchain.info/inv")
;; (def END_POINT "wss://echo.websocket.org")
(def TIMEOUT 30000)
(def ops {:error (json/write-str {"error" true})
          :ping (json/write-str {"op" "ping"})
          :transactions (json/write-str {"op" "unconfirmed_sub"})
          :blocks (json/write-str {"op" "blocks_sub"})
          :last-transaction (json/write-str {"op" "ping_tx"})
          :last-block (json/write-str {"op" "ping_block"})})

(defmacro websocket-write [c activity data]
  `(do
     (go (>! ~activity true))
     (s/put! ~c ~data)))

(defmacro ping [c]
  `(s/put! ~c (:ping ops)))

(defn keep-alive [{:keys [activity conn]}]
  (go-loop []
    (alt! [(timeout TIMEOUT)] (ping conn)
          [activity] nil) 
    (recur)))

(defn -main
  [& args]
  (let [conn @(http/websocket-client END_POINT) activity (chan 128) [op-key & _] args]
    (keep-alive {:activity activity :conn conn})
    (if-let [k (keyword op-key)]
      (if-let [op (k ops)]
        (websocket-write conn activity op)))
    (loop []
      (println (str "Message: " (json/read-str
                                 @(s/take! conn (:error ops))
                                 :key-fn keyword)))
      (recur))))
