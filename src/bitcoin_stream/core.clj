(ns bitcoin-stream.core
  (:require [clojure.data.json :as json]
            [aleph.http :as http]
            [aleph.http.client :as http-client]
            [manifold.stream :as s]
            [clojure.core.async :refer [timeout <! go-loop alt! chan go >!]]
            [clj-time.local :as l]
            [clj-time.coerce :as c]
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

(defn- satoshi-to-bitcoin
  [x]
  (format "%.8f" (double (/ x 100000000))))

(defmulti filter-response :op)

(defmethod filter-response "block" [resp] (medley/dissoc-in resp [:x :txIndexes]))

(defmethod filter-response "utx"
  [{{t :time [{{addr-in :addr value-in :value} :prev_out} & _] :inputs o :out} :x op :op}]
  {:op op :trans-time (str (c/from-long t)) :addr-in addr-in :value-in value-in :out (map #(select-keys % [:addr :value]) o)})

(defmulti tabulate :op)
(defmethod tabulate "utx"
  [trans]
  (str "Transaction time: " (:trans-time trans) "\n"
       "In\taddr:\t" (:addr-in trans) "\tvalue:\t" (satoshi-to-bitcoin (:value-in trans)) "BTC\n"
       "\nOut" (reduce #(str % "\taddr:\t" (:addr %2) "\tvalue:\t" (satoshi-to-bitcoin (:value %2)) "BTC\n") "" (:out trans))
       "\n"))

(defmethod filter-response :default [resp] resp)

(defn -main
  [& args]
  (let [[op-key & _] args]
    (if-let [k (keyword op-key)]
      (if-let [op (k ops)]
        (try
          (let [conn @(http/websocket-client END_POINT ;; {:max-frame-payload 0 :max-frame-size 512000}
                                                )]
               (keep-alive conn)  
               (websocket-write conn op)
               (loop []
                 (if-let [response @(s/take! conn)]
                   (-> response
                       (json/read-str :key-fn keyword)
                       filter-response
                       tabulate
                       println))
                 (recur)))
          (catch Exception e (str "caught exception: " (.getMessage e))))))))
