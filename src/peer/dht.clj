net-map interface:

(defn net-map 
  "Return a peer that listens on the specified local port, and if given it tries
to connect to one or more initial peers from the neighbor-set."
  [listen-port neighbor-set])

; Is it possible to register a callback with the garbage collector, or something
; like that, in order to automatically leave at the appropriate time?
(defn net-map-leave
  "Remove yourself from the P2P network."
  [dht])

(defn nm-assoc
  "Store an object in the DHT."
  [dht key val])

(defn nm-get
  "Retrive an object from the DHT."
  [dht key])
