(ns peer.connection
  (:use [peer url config presence rpc]
        plasma.util
        [aleph object udp])
  (:require [logjam.core :as log]
						[lamina.core :as lamina])
  (:import [org.bitlet.weupnp GatewayDiscover PortMappingEntry]
           [java.net InetAddress NetworkInterface]))

(def *connection-timeout* 2000)
(def *cache-keep-ratio* 0.8)

(defn gateway []
  (try
    (.getValidGateway (GatewayDiscover.))
    (catch java.io.IOException _
      nil)))

; Running the discovery every time takes too long...
(def find-gateway (memoize gateway))

(defrecord NetAddress [local public])

(defn local-addr
  []
  (let [ifaces (enumeration-seq (NetworkInterface/getNetworkInterfaces))
        addrs (flatten (map #(enumeration-seq (.getInetAddresses %)) ifaces))
        hosts (map #(.substring (.toString %) 1) addrs)
        ips (filter #(re-find #"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]" %) hosts)
        me (first (remove #{"127.0.0.1"} ips))]
    me))

(defn local-broadcast-addr
  []
  (apply str (concat (re-seq #"[0-9]*\." (local-addr)) ["255"])))

(defn net-address
  [g]
  (if g
    (NetAddress.
      (.getLocalAddress g)
      (.getExternalIPAddress g))
    (let [local (local-addr)]
      (NetAddress. local local))))

(defn setup-port-forward
  "Setup a port forward on the local router using UPNP.  Throws an exception
  if the operation fails.  Proto is either :udp or :tcp, and the service is a
  string label that will be used to refer to the port forward on the router.

    (setup-port-forward 4242 :tcp \"super peer chat\")
  "
  ([port proto service]
   (setup-port-forward (find-gateway) port proto service))
  ([g port proto service]
   (let [entry (PortMappingEntry.)
         {:keys [local-addr public-addr]} (net-address g)
         addr (.getHostAddress local-addr)]
     (if-not (.getSpecificPortMappingEntry g port (.toUpperCase (name proto)) entry)
       (.addPortMapping g port port addr proto service)))))

(defn clear-port-forward
  "Clear a port forward."
  ([port]
   (clear-port-forward port :tcp))
  ([port proto]
   (clear-port-forward (find-gateway) port proto))
  ([gateway port proto]
   (let [proto (cond
                 (string? proto) (.toUpperCase proto)
                 (keyword? proto) (.toUpperCase (name proto)))]
     (.deletePortMapping gateway port proto))))

;(log/repl :con)

(defn- type-channel
  "Returns a channel of incoming messages on chan of only the given type."
  [chan type]
  (lamina/filter*
    (fn [msg] (and (associative? msg)
                   (= type (:type msg))))
    chan))

(defn- matched-response-channel
  "Returns a result-channel that will receive a single response matching
  the request id."
  [chan id]
  (let [res (lamina/result-channel)
        response-chan (lamina/take* 1 (lamina/filter* #(= id (:id %))
                             (type-channel chan :response)))]
    (lamina/run-pipeline
      response-chan
      lamina/read-channel
      (fn [msg]
        (if (:error msg)
          (throw (Exception. (:message (:error msg))))
          (:result msg))))))

(defn- wrapped-stream-channel
  "Given a channel and a stream-id, returns one side of a channel pair
  that can be used to communicate with a matched stream channel on the
  other side.  Allows for multiplexing many streams over one socket
  channel."
  [chan id]
  (let [s-in-chan (lamina/map* #(:msg %)
                               (lamina/filter* #(= id (:id %))
                                  (type-channel chan :stream)))
        wrap-chan (lamina/channel)
        [snd-chan rcv-chan] (lamina/channel-pair)]
    (lamina/on-drained rcv-chan
      (fn []
        (log/format :stream "[%s] stream closed locally" id)
        (lamina/enqueue chan {:type :stream :id id :msg ::closed})))

    (lamina/receive-all s-in-chan
      (fn [msg]
        (if (= ::closed msg)
          (do
            (lamina/close snd-chan)
            (log/format :stream "[%s] stream closed remotely" id))
          (lamina/enqueue rcv-chan msg))))

    (lamina/siphon (lamina/map* (fn [msg]
                    {:type :stream
                     :id id
                     :msg msg})
                  rcv-chan)
            chan)
    (lamina/receive-all (lamina/fork snd-chan)
      #(log/to :stream "send: " %))
    (lamina/receive-all (lamina/fork rcv-chan)
      #(log/to :stream "recv: " %))

    snd-chan))

(defprotocol IClosable
  (close [this]))

(defprotocol IConnection
  (request
    [con method params]
    "Send a request over this connection. Returns a result-channel
    that will receive a single result message, or an error.")

  (request-channel
    [con]
    "Returns a channel for incoming requests.  The channel will receive
    [ch request] pairs, and the rpc-response or rpc-error enqueued on
    ch will be sent as the response.")

  (send-event
    [con id params]
    "Send an event over this connection.")

  (event-channel
    [con] [con id]
    "Returns a channel for incoming events.  If an ID is passed only incoming
    events with this ID will be enqueued onto the returned channel.")

  (stream
    [con method params]
    "Open a stream channel on this connection.  Returns a channel that can be
    used bi-directionally.")

  (stream-channel
    [con]
    "Returns a channel for incoming stream requests.  The channel will receive
    [ch request] pairs, and the ch can be used as a named bi-direction stream.")

  (on-closed
    [con handler]
    "Register a handler to be called when this connection is closed."))

(defrecord Connection
  [url chan]
  IConnection

  (request
    [this method params]
    (let [id (uuid)
          res (matched-response-channel chan id)]
      (lamina/enqueue chan (rpc-request id method params))
      res))

  (request-channel
    [this]
    (lamina/map* (fn [request] [chan request])
                 (type-channel chan :request)))

  (send-event
    [this id params]
    (lamina/enqueue chan (rpc-event id params)))

  (event-channel
    [this]
    (type-channel chan :event))

  (event-channel
    [this id]
     (lamina/filter* (fn [req] (= id (:id req)))
                     (type-channel chan :event)))

  (stream
    [this method params]
    (let [id (uuid)
          req {:type :stream-request
               :id id
               :method method
               :params params}]
      (lamina/enqueue chan req)
      (wrapped-stream-channel chan id)))

  (stream-channel
    [this]
    (lamina/map* (fn [s-req]
                   [(wrapped-stream-channel chan (:id s-req)) s-req])
                 (type-channel chan :stream-request)))

  (on-closed
    [this handler]
    (lamina/on-closed chan handler))

  IClosable
  (close
    [this]
    (lamina/close chan)))

(defmulti connection-channel
  "Returns a channel representing a network connection to the peer listening at URL."
  (fn [url] (keyword (:proto (url-map url)))))

(defmethod connection-channel :peer
  [url]
  (let [{:keys [proto host port]} (url-map url)
        client (object-client {:host host :port port})
        chan   (lamina/wait-for-result client *connection-timeout*)]
    chan))

(def BASE-UDP-PORT 10000)

(defn try-with-meta
  "Returns obj with the meta-data m if possible, otherwise just returns
  obj unmodified."
  [obj m]
  (if (isa? (type obj) clojure.lang.IObj)
    (with-meta obj m)
    obj))

(defmethod connection-channel :upeer
  [url]
  (let [in-port (+ BASE-UDP-PORT (rand-int 20000))
        udp-chan @(udp-object-socket {:port in-port})
        {:keys [proto host port]} (url-map url)
        [inner outer] (lamina/channel-pair)]
    (log/to :con "[udp-connection] connecting to:" url)

    (lamina/receive-all (lamina/fork udp-chan)
      (fn [msg] (log/to :con "[udp-con] MSG: " msg "\n\n")))

    (lamina/siphon
      (lamina/map* (fn [obj]
                     (let [msg {:message obj :host host :port port}]
                       (log/to :con "[udp-con] sending msg: " msg)
                       msg))
                   outer)
      udp-chan)

    (lamina/siphon
      (lamina/map* (fn [msg]
                     (log/to :con "[udp-con] received:" msg)
                     (try-with-meta (:message msg)
                                    (dissoc msg :message)))
                   udp-chan)
      outer)

    (lamina/on-closed inner #(do
                               (log/to :con "[udp-con] closed!")
                               (lamina/close udp-chan)))
    inner))

(defn- make-connection
  [url]
  (let [chan (connection-channel url)]
    (Connection. url chan)))

(defprotocol IConnectionCache
  "A general purpose PeerConnection cache."

  (get-connection
    [this url]
    "Returns a connection to the peer listening at URL, using a cached
    connection when available.")

  (register-connection
    [this con] [this ch url]
    "Add a new connection to the cache that will use an existing channel and
    URL.  Used to register connections initiated remotely.
    Returns the Connection.")

  (refresh-connection
    [this con]
    "Updates the usage timestamp on this connection to keep it from being
    removed from the cache.")

  (purge-connections
    [this]
    "Apply the cache policy to the current set of connections, possibly
    removing old or unused connections to make space for new ones.  This is
    called automatically so it should normally not need to be called manually.")

  (remove-connection
    [this con]
    "Remove a connection from the cache.")

  (clear-connections
    [this]
    "Remove all connections from the cache.")

  (connection-count
    [this]
    "Get the current number of connections in the cache."))

(defrecord ConnectionManager
  [connections* flush-fn]

  IConnectionCache

  (get-connection
    [this url]
    (let [con-entry (get @connections* url)
          con (if con-entry
                (:con con-entry)
                (make-connection url))]
      (refresh-connection this con)))

  (remove-connection
    [this con]
    (dosync (alter connections* dissoc (:url con))))

  (register-connection
    [this con]
    (log/to :con "register-connection: " (:url con))
    (refresh-connection this con)
    (on-closed con #(remove-connection this con))
    con)

  (register-connection
    [this url ch]
    (register-connection this (Connection. url ch)))

  (refresh-connection
    [this con]
    (dosync (alter connections*
                   assoc (:url con) {:last-used (current-time) :con con}))
    (when (>= (connection-count this) (config :connection-cache-limit))
      (purge-connections this))
    con)

  (clear-connections
    [this]
    (dosync
      (doseq [con (map :con (vals @connections*))]
        (close con))
      (ref-set connections* {})))

  (purge-connections
    [this]
    (dosync
      (let [n-to-drop (- (connection-count this)
                         (* (config :connection-cache-limit)
                            *cache-keep-ratio*))]
        (alter connections*
               (fn [conn-map]
                 (let [[to-keep to-drop] (flush-fn (vals conn-map) n-to-drop)]
                   (doseq [con-entry to-drop]
                     (close (:con con-entry)))
                   (zipmap (map #(:url (:con %)) to-keep) to-keep)))))))

  (connection-count
    [this]
    (count @connections*)))

(defn- lru-flush
  "Remove the least recently used connections."
  [connections n-to-drop]
  (let [sorted  (sort-by :last-used connections)
        to-drop (take n-to-drop sorted)
        to-keep (drop n-to-drop sorted)]
    [to-keep to-drop]))

; TODO: support options for the cache size and timeout...
; * make connections asynchronous and call a callback or something
(defn connection-manager
  "Returns a connection cache that can be used to efficiently manage a large
  number of network connections, where the least-recently-used connections
  are dropped as new connections are made."
  []
  (ConnectionManager. (ref {}) lru-flush ))

(defprotocol IConnectionListener
  (on-connect
    [this handler]
    "Register a handler function to be called on each incoming connection.  The
    handler will be passed a Connection."))

(defrecord ConnectionListener
  [server port chan local-addr public-addr public-url]

  IConnectionListener
  (on-connect
    [this handler]
    (lamina/receive-all chan #(when % (handler %))))

  IClosable
  (close
    [this]
    (server)
    (lamina/close chan)))

(defmulti make-listener
  "Create a network socket listener that will call the
    (handler chan client-info)
  for each incoming connection."
  (fn [proto port handler]
    (keyword proto)))

(defmethod make-listener :peer
  [proto port handler]
  (start-object-server handler {:port port}))

(defmethod make-listener :upeer
  [proto port handler]
  (let [known-hosts (ref #{})
        udp-chan @(udp-object-socket {:port port})]
    (log/to :con "[udp listener] listening on port: " port)
    (lamina/receive-all (lamina/fork udp-chan)
                 (fn [msg] (log/to :con "[udp listener] MSG: " msg "\n\n")))

    (log/to :con "[udp listener] setting up receivers...")
    (lamina/receive-all udp-chan
      (fn [msg]
        (log/to :con "[udp listener] top------------------")
        (let [host-key (select-keys msg [:host :port])
              new-host? (boolean
                          (dosync
                            (if ((ensure known-hosts) host-key)
                              false
                              (alter known-hosts conj host-key))))]
          (log/to :con "[udp listener] new?:" new-host? " msg: " msg)
          (when new-host?
            (let [[inner outer] (lamina/channel-pair)]

              (log/to :con "[udp listener] setup incoming")
              ; incoming messages with the same host/port go to the outer channel
              (lamina/siphon
                (lamina/map*
                  (fn [msg] (try-with-meta (:message msg)
                                           (dissoc msg :message)))
                  (lamina/filter*
                    (fn [{:keys [host port]}] (= host-key {:host host :port port}))
                    udp-chan))
                  outer)

              (log/to :con "[udp listener] setup outgoing")
              ; messages enqueued on inner get wrapped as udp "packets" and sent
              ; to the socket channel
              (lamina/siphon
                (lamina/map*
                  (fn [obj]
                    (log/to :con "[udp listener] sending: "
                            (assoc host-key :message obj))
                    (assoc host-key :message obj))
                  outer)
                udp-chan)

              (lamina/enqueue outer (try-with-meta (:message msg)
                                               (dissoc msg :message)))
              (handler inner host-key))))))
    #(lamina/close udp-chan)))

(defn connection-listener
  "Listen on a port for incoming connections, automatically registering them."
  [manager proto port]
  (let [connect-chan (lamina/channel)
        g (find-gateway)
        {:keys [local public]} (net-address g)
        public-url (peer-url (or public local "127.0.0.1") port)
        listener (make-listener
                   proto port
                   (fn [chan client-info]
                     (log/to :con "handling new connection: " client-info)
                     (let [{:keys [host port]} client-info
                           url (url proto host port)
                           con (register-connection manager url chan)]
                       (log/to :con "listener new connection: " url con)
                       (lamina/enqueue connect-chan con))))]
    (ConnectionListener. listener port connect-chan local public public-url)))

