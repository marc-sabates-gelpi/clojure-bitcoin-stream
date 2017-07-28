(ns bitcoin-stream.core
  (:require [clojure.data.json :as json]
            [aleph.http :as http]
            [manifold.stream :as s]
            [clojure.core.async :refer [timeout <! go-loop alt! chan go >!]]
            [clj-time.local :as l])
  (:gen-class))

;; Objectives
;; * Use websockets client
;; * Parse JSON data
;; * Work with data streams

(def ^:const END_POINT "wss://ws.blockchain.info/inv")
(def ^:const ops {:error (json/write-str {"op" "error"})
          :ping (json/write-str {"op" "ping"})
          :transactions (json/write-str {"op" "unconfirmed_sub"})
          :blocks (json/write-str {"op" "blocks_sub"})
          :last-transaction (json/write-str {"op" "ping_tx"})
          :last-block (json/write-str {"op" "ping_block"})})
(def ^:const TIMEOUT 30000)

(defmacro websocket-write [c activity data]
  `(do
     (go (>! ~activity ~data))
     (s/put! ~c ~data)))

(defmacro ping [c]
  `(s/put! ~c (:ping ops)))

(defn keep-alive [{:keys [activity conn]}]
  (go-loop []
    (alt! [(timeout TIMEOUT)] (ping conn)
          [activity] nil) 
    (recur)))

(defmulti filter-response :op)

(defmethod filter-response "block"
  [{:keys [op x]}]
  {:op op :x (dissoc x :txIndexes)})

(defmethod filter-response :default [resp] resp)

(defn -main
  [& args]
  (let [conn @(http/websocket-client END_POINT) activity (chan 128) [op-key & _] args]
    (keep-alive {:activity activity :conn conn})
    (if-let [k (keyword op-key)]
      (if-let [op (k ops)]
        (websocket-write conn activity op)))
    (loop []
      ;; (-> @(s/take! conn (:error ops))
      ;;     (fn [x] (json/read-str x :key-fn keyword))
      ;;     filter-response
      ;;     (fn [y] (str (l/local-now) " Message: " y))
      ;;     println)
      (println (str "\n" (l/local-now) " Message:\n" (filter-response (json/read-str @(s/take! conn (:error ops)) :key-fn keyword))))
      (recur))))
