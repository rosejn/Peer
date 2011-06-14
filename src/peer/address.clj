(ns peer.address
  (:use peer.url)
  (import [org.bitlet.weupnp GatewayDiscover PortMappingEntry]
          [java.net InetAddress NetworkInterface]))

(defn gateway* []
  (try
    (let [discoverer (GatewayDiscover.)]
      (.discover discoverer)
      (.getValidGateway discoverer))
    (catch java.io.IOException e
      nil)))

; TODO: This is memoized so we don't wait for a nil gateway after already
; trying once.  There is probably a better way to do this, but for now it works...
(def gateway (memoize gateway*))

(defrecord NetAddress [local public])

(defn local-addr
  []
  (let [ifaces (enumeration-seq (NetworkInterface/getNetworkInterfaces))
        addrs (flatten (map #(enumeration-seq (.getInetAddresses %)) ifaces))
        hosts (map #(apply str (drop 1 (.toString %))) addrs)
        ips (filter #(not (nil? (re-find #"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]" %))) hosts)
        me (first (filter #(not (= "127.0.0.1" %)) ips))]
    me))

(defn local-broadcast-addr
  []
  (apply str (concat (re-seq #"[0-9]*\." (local-addr)) ["255"])))

(defn broadcast-addr
  []
  "255.255.255.255")

(defn addr-info
  ([] (addr-info (gateway)))
  ([g]
   (if g
     (NetAddress.
      (.getLocalAddress g)
      (.getExternalIPAddress g))
     (let [local (local-addr)]
       (NetAddress. local local)))))

(defn public-url
  "Get the public url for the given port number by getting public IP from the
  local router using UPNP."
  [port]
  (peer-url (or (:public (addr-info)) "127.0.0.1") port))

(defn set-port-forward
  "Setup a port forward on the local router using UPNP.  Throws an exception
  if the operation fails.  Proto is either UDP or TCP, and the service is a
  string label that will be used to refer to the port forward on the router."
  ([port service]
   (set-port-forward port "TCP" service))
  ([port proto service]
   (let [entry (PortMappingEntry.)
         g (gateway)
         {:keys [local-addr public-addr]} (addr-info)
         addr (.getHostAddress local-addr)]
     (if-not (.getSpecificPortMappingEntry g port proto entry)
       (.addPortMapping g port port addr proto service)))))

(defn clear-port-forward
  "Clear a port forward."
  ([port]
   (clear-port-forward port :tcp))
  ([port proto]
   (let [g (gateway)
         proto (cond
                 (string? proto) (.toUpperCase proto)
                 (keyword? proto) (.toUpperCase (name proto)))]
     (.deletePortMapping g port proto))))

