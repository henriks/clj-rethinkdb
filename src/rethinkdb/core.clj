(ns rethinkdb.core
  (:require [rethinkdb.net :refer [send-int send-str read-init-response send-stop-query
                                   make-connection-loops read-response]]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async]
            [clj-tcp.client :as tcp]
            [rethinkdb.utils :as utils]
            [clj-tcp.codec :as tcp-utils])
  (:import [clojure.lang IDeref]
           [io.netty.channel ChannelOption]
           [io.netty.buffer ByteBuf]
           [java.io Closeable]))

(defn send-version
  "Sends protocol version to RethinkDB when establishing connection.
  Hard coded to use v3."
  [out]
  (let [v1 1063369270
        v2 1915781601
        v3 1601562686
        v4 1074539808]
    (send-int out v4 4)))

(defn send-protocol
  "Sends protocol type to RethinkDB when establishing connection.
  Hard coded to use JSON protocol."
  [out]
  (let [protobuf 656407617
        json 2120839367]
    (send-int out json 4)))

(defn send-auth-key
  "Sends auth-key to RethinkDB when establishing connection."
  [out auth-key]
  (let [n (count auth-key)]
    (send-int out n 4)
    (send-str out auth-key)))

(defn close
  "Closes RethinkDB database connection, stops all running queries
  and waits for response before returning."
  [conn]
  (let [{:keys [channel waiting out in error]} @conn]
    (doseq [token waiting]
      (send-stop-query conn token))
     (tcp/close-all channel)
     (async/close! out)
     (async/close! in)
     (async/close! error)
    :closed))

(defrecord Connection [conn]
  IDeref
  (deref [_] @conn)
  Closeable
  (close [this] (close this)))

(defmethod print-method Connection
  [r writer]
  (print-method (:conn r) writer))

(defn connection [m]
  (->Connection (atom m)))

(defn ^Connection connect
  "Creates a database connection to a RethinkDB host.
  If db is supplied, it is used in any queries where a db
  is not explicitly set. Default values are used for any parameters
  not provided.

  (connect :host \"dbserver1.local\")"
  [& {:keys [^String host ^int port token auth-key db]
      :or {host "172.17.0.1"
           port 28015
           token 0
           auth-key ""
           db nil}}]
  (try
    (let [ init-ch (async/timeout 5000)
          {:keys [read-ch write-ch error-ch] :as channel}
              (tcp/client host port {:reuse-client false
                                     :channel-options [[ChannelOption/TCP_NODELAY true][ChannelOption/SO_RCVBUF (int 5242880)]]
                                     :decoder (defn copy-bytebuf [read-ch ^ByteBuf buff]
                                                (read-response (.copy buff) read-ch init-ch))})]
      ;; Initialise the connection
      (send-version channel)
      (send-auth-key channel auth-key)
      (send-protocol channel)
      (let [init-response (async/<!! init-ch)]
        (if-not (= init-response "SUCCESS")
          (throw (ex-info init-response {:host host :port port :auth-key auth-key :db db}))))
      ;; Once initialised, create the connection record
      (connection
        (merge
          {:channel channel
           :in read-ch
           :out write-ch
           :error error-ch
           :db db
           :waiting #{}
           :token token}
        (make-connection-loops channel))))
    (catch Exception e
      (log/error e "Error connecting to RethinkDB database")
      (throw (ex-info "Error connecting to RethinkDB database" {:host host :port port :auth-key auth-key :db db} e)))))
