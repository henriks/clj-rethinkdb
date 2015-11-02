(ns rethinkdb.net
  (:require [cheshire.core :as cheshire]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log]
            [rethinkdb.query-builder :refer [parse-query]]
            [rethinkdb.types :as types]
            [rethinkdb.response :refer [parse-response]]
            [rethinkdb.utils :refer [str->bytes int->bytes bytes->int pp-bytes sub-bytes]]
            [clj-tcp.client :as tcp])
  (:import [java.io Closeable]))

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

(defn ^String read-init-response [ in]
  (-> in
   tcp/read!
   String.
   (clojure.string/replace  #"\W*$" "")))

(defn read-response* [result]
  (let [recvd-token (bytes->int (sub-bytes result 0 7) 8)
        length (bytes->int (sub-bytes result 8 11) 4)
        json   (String. (sub-bytes result 12 (+ length 12)))]
      [recvd-token json]))

(defn write-query [channel [token json]]
  (send-int channel token 8)
  (send-int channel (count json) 4)
  (send-str channel json))

(defn connection-errors [error-ch]
  {:connection-errors
    (async/go-loop []
    (when-let [[e v] (async/<! error-ch)]
      (log/error e "Network error occured: ")
      (recur)))})

(defn send-query* [conn token query]
  (let [{:keys [channel]} @conn]
    (write-query channel [token query])
    (let [[recvd-token json]
      (read-response* (tcp/read! channel))]
    (assert (= recvd-token token) "Must not receive response with different token")
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
