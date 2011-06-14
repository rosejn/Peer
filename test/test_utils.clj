(ns test-utils
  (:use [peer core url connection bootstrap]
        [clojure test stacktrace]
        [plasma graph])
  (:require [lamina.core :as lamina]
            [plasma.construct :as c]))

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
        strap-url (peer-url "localhost" port)
        peers (make-peers n (inc port) identity)]
    (bootstrap-peers peers strap-url)
    [strapper peers]))

(defn test-graph []
  (c/construct*
    (-> (c/nodes [root     ROOT-ID
                net      :net
                music    :music
                synths   :synths
                kick    {:label :kick  :score 0.8}
                hat     {:label :hat   :score 0.3}
                snare   {:label :snare :score 0.4}
                bass    {:label :bass  :score 0.6}
                sessions :sessions
                take-six :take-six
                red-pill :red-pill])
      (c/edges
        [root     net      :net
         root     music    :music
         music    synths   :synths
         synths   bass     {:label :synth :favorite true}
         synths   hat      :synth
         synths   kick     :synth
         synths   snare    :synth
         root     sessions :sessions
         sessions take-six :session
         take-six kick     :synth
         take-six bass     :synth
         sessions red-pill :session
         red-pill hat      :synth
         red-pill snare    :synth
         red-pill kick     :synth]))))

