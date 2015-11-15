(ns rethinkdb.net
  (:require [cheshire.core :as cheshire]
            [clojure.tools.logging :as log]
            [manifold.stream :as s]
            [manifold.bus :as bus]
            [manifold.deferred :as d]
            [rethinkdb.query-builder :refer [parse-query]]
            [rethinkdb.types :as types]
            [rethinkdb.response :refer [parse-response]]
            [rethinkdb.utils :refer [str->bytes int->bytes bytes->int pp-bytes]]
            [gloss.core :as gloss]
            [gloss.io :as io])
  (:import [java.io Closeable]))

(defmacro dbg [x] `(let [x# ~x] (println "dbg:" '~x "=" x#) x#))

(gloss/defcodec query-frame (gloss/compile-frame
                              (gloss/finite-frame
                                (gloss/prefix :int32-le)
                                (gloss/string :utf-8))
                              cheshire/generate-string
                              #(cheshire/parse-string % true)))

(gloss/defcodec id :int64-le)
(gloss/defcodec msg-protocol [id query-frame])

(defn wrap-duplex-stream
  [s]
  (let [out (s/stream)]
    (s/connect
      (s/map #(io/encode msg-protocol %) out)
      s)

    (s/splice
      out
      (io/decode-stream s msg-protocol))))

(declare send-continue-query send-stop-query)

(defn close
  "Clojure proxy for java.io.Closeable's close."
  [^Closeable x]
  (.close x))

(defn read-init-response [resp]
  (-> resp
      String.
      (clojure.string/replace #"\W*$" "")))

(defn handshake [version auth proto client]
  (let [auth-bytes (if (some? auth)
                     (str->bytes auth)
                     (int->bytes 0 4))
        msg-bytes (byte-array (concat
                                (int->bytes version 4)
                                auth-bytes
                                (int->bytes proto 4)))]
    @(s/put! client msg-bytes)
    (read-init-response @(s/take! client))))

(defn setup-routing [conn]
  (let [client (:client @conn)]
    (s/consume
      (fn [data]
        (log/trace "routing" data)
        (let [[recvd-token json-resp] data]
          (let [{sinks :sinks} @conn]
            (if-let [sink (get sinks recvd-token)]
              (s/put! sink json-resp)
              (log/warn "UNKNOWN TOKEN" recvd-token json-resp)))))
      client)))

(defn send-data [client token query]
  (log/trace "sending" token query)
  (s/put! client [token query]))

(defn token-stream [token client db]
  {:pre  [(integer? token) (s/stream? client)]
   :post [(s/source? %)]}

  (log/debug "token" token)

  (let [db-part {:db [(types/tt->int :DB) [db]]}
        continue-query (concat (parse-query :CONTINUE) db-part)
        stop-query (concat (parse-query :STOP) db-part)

        waiting (atom true)

        input (s/buffered-stream 10)
        output (s/stream)

        cleanup (fn [_]
                  (log/debug "closing input")
                  (d/chain
                    (s/close! input)
                    (fn [_] (log/debug "closed?" (s/closed? input) input))))

        complete-atom (fn [response]                        ; submit data, close streams
                        (log/debug "complete-atom" response)
                        (reset! waiting false)
                        (log/debug "waiting is" @waiting)
                        (d/chain
                          (s/put! output (first response))
                          cleanup))

        complete-sequence (fn [response]                    ; submit data, close streams
                            (log/debug "complete-sequence" response)
                            (reset! waiting false)
                            (d/chain
                              (s/put! output response)
                              cleanup))

        partial-sequence (fn [response]                     ; submit data, send continue
                           (log/debug "partial-sequence" response)
                           (reset! waiting true)
                           (d/chain
                             (s/put! output response)
                             (fn [_] (send-data client token continue-query))))]

    (add-watch
      waiting
      :watcher
      (fn [& args]
        (log/debug "state change" args)))

    (s/on-closed
      input
      #(do
        (log/debug "close callback" @waiting)

        (when @waiting (send-data client token stop-query))))

    (s/connect-via
      input
      (fn [{type :t resp :r :as json-resp}]
        (log/debug "got" json-resp)
        (d/chain
          (let [resp (parse-response resp)]
            (condp get type
              #{1} (complete-atom resp)
              #{2} (complete-sequence resp)
              #{3} (partial-sequence resp)
              (log/log :warn "unhandled response" resp)))
          (fn [_] (d/success-deferred true))))
      output)

    (s/splice input (s/throttle 4 output))))

(defn send-query [conn token query]
  {:pre  [(integer? token) (s/stream? (:client @conn))]
   :post [(d/deferred? %)]}

  (let [{:keys [client db]} @conn]
    (let [bind (fn [stream]
                 (swap! (:conn conn) (fn [m] (assoc-in m [:sinks token] stream))))
          unbind (fn []
                   (log/debug "removing sink for token " token)
                   (swap! (:conn conn) (fn [m]
                                         (update-in m [:sinks]
                                                    (fn [sinks]
                                                      (dissoc sinks token))))))

          stream (token-stream token client db)]
      (s/on-closed stream unbind)
      (bind stream)

      (d/chain
        (send-data client token query)
        (fn [success]
          stream)))))

(defn send-start-query [conn token query]
  (log/debugf "Sending start query with token %d, query: %s" token query)
  (send-query conn token (parse-query :START query)))
