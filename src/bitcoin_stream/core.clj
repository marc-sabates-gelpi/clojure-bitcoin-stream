(ns bitcoin-stream.core
  (:require [clojure.data.json :as json]
            [aleph.http :as http]
            [manifold.stream :as s])
  (:gen-class))

;; Objectives
;; * Use websockets client
;; * Parse JSON data
;; * Work with data streams

;; (def END_POINT "wss://ws.blockchain.info/inv")
(def END_POINT "wss://echo.websocket.org")

(defn -main
  [& args]
  (let [conn @(http/websocket-client END_POINT)]
    (s/put! conn (json/write-str {"op" "blocks_sub"}))
    (json/read-str @(s/take! conn))))
