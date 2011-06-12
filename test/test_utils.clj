(ns test-utils
  (:use [peer peer url connection bootstrap]
        [clojure test stacktrace])
  (:require [lamina.core :as lamina]))

(defn make-peers
  "Create n peers, each with a monotically increasing port number.
  Then run (fun i) with the peer graph bound to initialize each peer,
  and i being the index of the peer being created."
  ([n start-port fun]
   (doall
     (for [i (range n)]
       (let [p (peer {:port (+ start-port i)
                      ;:path (str "db/peer-" i)
                      }
                     )]
         (with-peer-graph p
                          (fun i)
                          p))))))

(defn close-peers
  [peers]
  (doseq [p peers]
    (close p)))

(defn bootstrap-peers
  [peers strap-url]
  (doall
    (for [p peers]
      (bootstrap p strap-url))))

(defn bootstrapped-peers
  [n]
  (let [port (+ 5000 (rand-int 5000))
        strapper (bootstrap-peer {:path "db/strapper" :port port})
        strap-url (plasma-url "localhost" port)
        peers (make-peers n (inc port) identity)]
    (bootstrap-peers peers strap-url)
    [strapper peers]))
