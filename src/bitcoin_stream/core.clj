(ns bitcoin-stream.core
  (:require [clojure.data.json :as json]
            [aleph.http :as http]
            [aleph.http.client :as http-client]
            [manifold.stream :as s]
            [clojure.core.async :refer [timeout <! go-loop alt! chan go >!]]
            [clj-time.local :as l]
            [clj-time.coerce :as c]
            [clostache.parser :refer [render-resource]])
  (:gen-class))

;; Objectives
;; * Use websockets client
;; * Parse JSON data
;; * Work with data streams
;; * Try to convert a stream to a lazy coll with (s/stream->seq conn)

(def ^:const MAX_INT 2147483647)
(def ^:const END_POINT "wss://ws.blockchain.info/inv")
(def ^:const TIMEOUT 30000)
(def ^:const TRANSACTION_TMPL "templates/transaction.mustache")
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

(defmacro posix->millis [posix]
  `(str (c/from-long (* ~posix 1000))))

(defn satoshi->btc [satoshi]
  (format "%.8f" (double (/ satoshi 100000000))))

(defn get-addr-value-formatted [m]
  (update-in (select-keys m [:addr :value]) [:value] satoshi->btc))

(defmulti filter-response :op)
(defmethod filter-response :default [resp]
  resp)
(defmethod filter-response "block" [{{t :time block-index :blockIndex [coin-base & _] :txIndexes} :x op :op}]
  {:op op
   :block-time (posix->millis t)
   :block-index block-index
   :coinbase coin-base})
(defmethod filter-response "utx" [{{t :time [{{addr-in :addr value-in :value} :prev_out} & _] :inputs out :out tx-index :tx_index} :x op :op}]
  {:op op
   :trans-time (posix->millis t)
   :addr-in addr-in
   :value-in (satoshi->btc value-in)
   :out (map get-addr-value-formatted out)
   :tx-index tx-index})

(defmulti tabulate :op)
(defmethod tabulate :default [resp]
  resp)
(defmethod tabulate "utx" [trans]
  (render-resource TRANSACTION_TMPL trans))

(defmulti update-data (fn [resp data] (:op resp)))
(defmethod update-data "utx" [resp data]
  (if (some #{(:tx-index resp)} (:coinbase data))
    (-> data
        (update :miners-data conj resp)
        (assoc :current-op :coinbase))
    data))
(defmethod update-data "block" [resp data]
  (println "[update-data] BLOCK")
  (-> data
      (update :coinbase conj (:coinbase resp))
      (assoc :current-op :block)))
(defmethod update-data :default [resp data]
  (println (str "[update-data] DEFAULT"))
  (assoc data :current-op :none))

(defn tee-to-show-update [data]
  (if-not (= :none (:current-op data)) (println data))
  data)

(defn -main [& args]
  (let [[op-key & _] args]
    (if-let [k (keyword op-key)]
      (if-let [op (k ops)]
        (try
          (let [conn @(http/websocket-client END_POINT {:max-frame-payload MAX_INT})]
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

(defn miners []
  "Keeps track of the miners that have mined blocks"
  (let [conn @(http/websocket-client END_POINT {:max-frame-payload MAX_INT})]
    (keep-alive conn)  
    (websocket-write conn (:blocks ops))
    (websocket-write conn (:transactions ops))
    (loop [data {:miners-data {} :coinbase [] :current-op :none}]
      (let [response @(s/take! conn)]
        (recur
         (-> response
             (json/read-str :key-fn keyword)
             filter-response
             (update-data data)
             tee-to-show-update))))))
