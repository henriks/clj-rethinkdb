(ns rethinkdb.net
  (:require [cheshire.core :as cheshire]
            [clojure.tools.logging :as log]
            [manifold.stream :as s]
            [manifold.bus :as b]
            [manifold.deferred :as d]
            [rethinkdb.query-builder :refer [parse-query]]
            [rethinkdb.types :as types]
            [rethinkdb.response :refer [parse-response]]
            [rethinkdb.utils :refer [str->bytes int->bytes bytes->int pp-bytes]]
            [gloss.core :as gloss]
            [gloss.io :as io])
  (:import [java.io Closeable]))

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

(defn setup-bus [conn]
  (let [{client :client bus :bus} @conn]
    (s/consume
      (fn [data]
        (log/trace "publishing" data)
        (let [[recvd-token json-resp] data]
          (if (b/active? bus recvd-token)
            (b/publish! bus recvd-token json-resp)
            (log/warn "UNKNOWN TOKEN" recvd-token json-resp))))
      client)

    (s/on-drained client #(doseq [[_ subs] (b/topic->subscribers bus)]
                           (doseq [sub subs] (s/close! sub))))))

(defn send-data [client token query]
  (log/trace "sending" token query)
  (s/put! client [token query]))

(defn token-stream [input token client db]
  {:pre  [(integer? token) (s/stream? client)]
   :post [(s/source? %)]}

  (log/trace "token" token)

  (let [db-part {:db [(types/tt->int :DB) [db]]}
        continue-query (concat (parse-query :CONTINUE) db-part)
        stop-query (concat (parse-query :STOP) db-part)

        waiting (atom true)

        output (s/stream)

        cleanup (fn [_]
                  (log/trace "closing input")
                  (d/chain
                    (s/close! input)
                    (fn [_] (log/trace "drained?" (s/drained? input)) input)))

        complete-atom (fn [response]                        ; submit data, close streams
                        (log/trace "complete-atom" response)
                        (reset! waiting false)
                        (log/trace "waiting is" @waiting)
                        (d/chain'
                          (s/put! output (first response))
                          cleanup))

        complete-sequence (fn [response]                    ; submit data, close streams
                            (log/trace "complete-sequence" response)
                            (reset! waiting false)
                            (d/chain'
                              (s/put-all! output response)
                              cleanup))

        partial-sequence (fn [response]                     ; submit data, send continue
                           (log/trace "partial-sequence" response)
                           (reset! waiting true)
                           (d/chain'
                             (s/put-all! output response)
                             (fn [_] (send-data client token continue-query))))

        handle-unexpected (fn [type response]
                            (log/log :warn "unhandled response: " response ", type: " type)
                            (reset! waiting false)
                            (cleanup :_))]

    (add-watch
      waiting
      :watcher
      (fn [& args]
        (log/trace "state change" args)))

    (s/on-drained
      input
      #(do
        (log/trace "close callback" @waiting)
        (when @waiting (send-data client token stop-query))))

    (s/connect-via
      input
      (fn [{type :t resp :r :as json-resp}]
        (log/trace "got" json-resp)
        (d/chain
          (let [resp (parse-response resp)]
            (condp get type
              #{1} (complete-atom resp)
              #{2} (complete-sequence resp)
              #{3} (partial-sequence resp)
              (handle-unexpected type resp)))
          (fn [_] (d/success-deferred true))))
      output)

    (s/source-only output)))

(defn send-query [conn token query]
  {:pre  [(integer? token) (s/stream? (:client @conn))]
   :post [(s/stream? %)]}

  (let [{:keys [client db bus]} @conn]
    (let [stream (token-stream (s/buffer 10 (b/subscribe bus token)) token client db)]

      (d/chain
        (send-data client token query)
        (fn [success]
          (when-not success
            (s/close! stream))))

      stream)))

(defn send-start-query [conn token query]
  (log/tracef "Sending start query with token %d, query: %s" token query)
  (send-query conn token (parse-query :START query)))
