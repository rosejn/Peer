(ns plasma.net.presence
  (:use [plasma config util]
        [plasma.net address]
        [lamina core]
        [aleph udp]))

(def listeners* (ref {}))

(defn presence-channel
  "Returns a channel that will receive all presence messages broadcast
  on the local network."
  ([] (presence-channel (config :presence-port)))
  ([port]
   (if-let [chan (get @listeners* port)]
     chan
     (let [msg-chan @(udp-object-socket {:port port})
           p-chan (filter* (fn [msg]
                             (and (associative? msg)
                                  (= :presence (:type msg))))
                           (map* :message msg-chan))]
       (dosync (alter listeners* assoc port p-chan))
       p-chan))))

(defn- presence-message
  "Create a presence message."
  [id host port]
  (let [presence-ip (broadcast-addr)
        {:keys [presence-port plasma-version]} (config)
        msg {:type :presence
             :plasma-version plasma-version
             :id id
             :host host
             :port port}]
    {:message msg :host presence-ip :port presence-port}))

(defn presence-broadcaster
  "Start periodically broadcasting a presence message.  Returns a function
  that will stop broadcasting when called."
  [id host port period]
  (let [msg (presence-message id host port)
        broadcast-channel @(udp-object-socket {:broadcast true})]
    (periodically period #(enqueue broadcast-channel msg))))

