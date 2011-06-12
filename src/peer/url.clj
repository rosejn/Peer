(ns peer.net.url
  (:use [peer config]))

(defn url
  ([proto host]
   (str proto "://" host))
  ([proto host port]
   (str proto "://" host ":" port)))

(defn peer-url
  [host port]
  (url (config :protocol) host port))

(defn url-map [url]
  (let [match (re-find #"(.*)://([0-9a-zA-Z-_.]*):([0-9]*)" url)
        [_ proto host port] match]
    {:proto proto
     :host host
     :port (Integer. port)}))

(defn assert-url
  [url]
  (when-not (and (string? url)
                 (.startsWith url "peer://"))
    (throw (Exception.
             (str "Trying to open a peer connection with an invalid URL: " url)))))
