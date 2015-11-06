(ns rethinkdb.net
  (:require [cheshire.core :as cheshire]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [manifold.deferred :as d]
            [manifold.stream :as s]
            [rethinkdb.query-builder :refer [parse-query]]
            [rethinkdb.types :as types]
            [byte-streams :as stream]
            [rethinkdb.response :refer [parse-response]]
            [rethinkdb.utils :refer [buff->bytes str->bytes
                                      int->bytes bytes->int
                                      pp-bytes sub-bytes]]
            [clj-tcp.client :as tcp]
    [gloss.core :as gloss]
    [gloss.io :as io])

  (:import [java.io Closeable]
           [io.netty.buffer Unpooled UnpooledByteBufAllocator ByteBufAllocator]
           [io.netty.buffer ByteBuf]))

(def protocol (gloss/compile-frame
                  (gloss/finite-block  :uint64-le)
                  (gloss/finite-frame (gloss/prefix :int32-le) (gloss/string :utf-8)) pr-str))

(defn wrap-duplex-stream
  [protocol s]
  (let [out (s/stream)]
    (s/connect
      (s/map #(io/encode protocol %) out)
      s)

    (s/splice
      out
      (io/decode-stream s protocol))))

(def ^ByteBufAllocator allocator UnpooledByteBufAllocator/DEFAULT)
(declare send-continue-query send-stop-query)

(defn close
  "Clojure proxy for java.io.Closeable's close."
  [^Closeable x]
  (.close x))

(deftype Cursor [conn token coll]
  Closeable
  (close [this] (and (send-stop-query conn token) :closed))
  clojure.lang.Seqable
  (seq [this] (do
                (Thread/sleep 250)
                (lazy-seq (concat coll (send-continue-query conn token))))))

(defn send-int [out i n]
  (tcp/write! out (int->bytes i n)))

(defn send-str [out s]
    (tcp/write! out (str->bytes s)))

(defn read-init-response [^ByteBuf buff init-ch]
  (-> buff
   (buff->bytes 0 8)
   String.
   (clojure.string/replace  #"\W*$" "")
   (->> (async/>!! init-ch))))

(defn read-query-response [^ByteBuf buff read-ch]
  (let [
        bytes-arr   (buff->bytes buff 0 (.readableBytes buff))
        recvd-token (bytes->int (sub-bytes bytes-arr 0 7) 8)
        length (bytes->int (sub-bytes bytes-arr 8 11) 4)]
       (when (<= length (count bytes-arr))
        (async/>!! read-ch [recvd-token
          (String. (sub-bytes bytes-arr 12 (+ length 12)))]))))

(defn read-response [^ByteBuf buff read-ch init-ch]
(if (= 8 (.readableBytes buff))
    (read-init-response buff init-ch)
    (async/>!! read-ch buff)))

(defn write-query [channel [token json]]
  (tcp/write! channel
  (byte-array (concat
    (int->bytes token 8)
    (int->bytes (count json) 4)
    (str->bytes json)))))

(defn make-connection-loops [{:keys [read-ch error-ch] :as channel}]
 (let [parsed-in (async/chan (async/sliding-buffer 100))
       ;pub (async/pub read-ch first)
        connection-errors
        (async/go-loop []
        (when-let [[e v] (async/<! error-ch)]
          (log/error e "Network error occured: ")
          (recur)))]
  {:loops [connection-errors]
   :parsed-in parsed-in
   ;:pub pub
}))

(defn send-query* [conn token query]
  (let [{:keys [channel pub parsed-in]} @conn
        chan (async/timeout 5000)]
    ;(async/sub pub token chan)
    (write-query channel [token query])
    (>break!)
    (let [[recvd-token json]
          (async/<!! chan)]
    (assert (= recvd-token token) "Must not receive response with different token")
    (println (str "token is " token))
    (async/unsub pub token chan)
    (cheshire/parse-string json true))))

(defn send-query [conn token query]
  (let [{:keys [db]} @conn
        query (if (and db (= 2 (count query))) ;; If there's only 1 element in query then this is a continue or stop query.
                ;; TODO: Could provide other global optargs too
                (concat query [{:db [(types/tt->int :DB) [db]]}])
                query)
        json (cheshire/generate-string query)
        {type :t resp :r :as json-resp} (send-query* conn token json)
        resp (parse-response resp)]
    (condp get type
      #{1} (first resp) ;; Success Atom, Query returned a single RQL datatype
      #{2} (do ;; Success Sequence, Query returned a sequence of RQL datatypes.
             (swap! (:conn conn) update-in [:waiting] #(disj % token))
             resp)
      #{3 5} (if (get (:waiting @conn) token) ;; Success Partial, Query returned a partial sequence of RQL datatypes
               (lazy-seq (concat resp (send-continue-query conn token)))
               (do
                 (swap! (:conn conn) update-in [:waiting] #(conj % token))
                 (Cursor. conn token resp)))
      (let [ex (ex-info (str (first resp)) json-resp)]
        (log/error ex)
        (throw ex)))))

(defn send-start-query [conn token query]
  (log/debugf "Sending start query with token %d, query: %s" token query)
  (send-query conn token (parse-query :START query)))

(defn send-continue-query [conn token]
  (log/debugf "Sending continue query with token %d" token)
  (send-query conn token (parse-query :CONTINUE)))

(defn send-stop-query [conn token]
  (log/debugf "Sending stop query with token %d" token)
  (send-query conn token (parse-query :STOP)))
