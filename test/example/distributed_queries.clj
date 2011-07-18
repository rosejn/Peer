(ns example.distributed-queries
  (:use [plasma util graph api construct]
        [peer core config url connection]
        test-utils
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query :as q]))

; People: alice, bob, carlos, dave, eve
; TODO: figure out how to deal with timestamps

(comment defn make-peer-graph
  []
  (clear-graph)
  (construct*
    (-> (nodes [contacts {}
                projects {}
                zoom {:name "Zoom"
                      :genre "minimal"
                      :last-update ...
                      }
                blam {:name "Blam"
                      :genre "jazz"
                      :last-update ...}
                ])
      (edges [ROOT-ID       contacts   :contacts
              ROOT-ID       projects   :projects
							]))))

(def me     (peer {:port 1100}))
(def alice  (peer {:port 1101}))
(def bob    (peer {:port 1102}))
(def carlos (peer {:port 1103}))

(defn setup-projects
  "Setup a basic graph adding two projects to peer p."
  [p proj-a proj-b]
  (construct p
             (->
               (nodes [projects {}
                       a proj-a
                       b proj-b])
               (edges [ROOT-ID  projects :projects
                       projects a        :project
                       projects b        :project]))))

(defn setup-peers
  []
  (setup-projects alice
                  {:name "Zoom" :genre "minimal"}
                  {:name "Doom" :genre "jazz"})
  (setup-projects bob
                  {:name "Whim" :genre "rock"}
                  {:name "Wham" :genre "jazz"})
  (setup-projects carlos
                  {:name "Flip" :genre "minimal"}
                  {:name "Flop" :genre "rock"}))

(defn setup-remotes
  []
  (let [roots (map (fn [p]
                     (let [id (:id (get-node p ROOT-ID))
                           purl (peer-url "localhost" (:port p))]
                       {:id id :proxy purl}))
                   [alice bob carlos])]
    (construct me
               (->
                 (nodes [net (q/path [:net])
                         proxies roots])
                 (edges [net proxies :peer])))))

(defn setup
  []
  (setup-peers)   ; create our imaginary peers
  (setup-remotes) ; add peer roots as proxies to local graph
  )

(defn peers
  []
  (-> (q/path [p [:net :peer]])
    (q/project ['p :id :proxy])))

(defn with-project-info
  [plan]
  (-> plan (q/project ['p :name :genre])))

(defn peer-projects-by-genre
  [genre]
  (-> (q/path [p [:net :peer :projects :project]])
    (q/where (= (:genre 'p) genre))))

; Get peer projects of the genre jazz
;(query me (with-project-info (peer-projects-by-genre "jazz")))

(defn tone
  [p q]
  (query p q))


(comment defn proxy-node-test []
  (dosync (alter config* assoc
                 :peer-port (+ 10000 (rand-int 20000))
                 :presence-port (+ 10000 (rand-int 20000))))
  (let [port     (+ 1000 (rand-int 10000))
        local    (peer {:port port})
        remote   (peer {:port (inc port)})]
    (try
      (reset-peer local)
      (reset-peer remote)
      ; Add a proxy node to the local graph pointing to the root of the remote
      ; graph.
      (let [remote-root (:id (get-node remote ROOT-ID))]
            (log/to :peer "remote-root: " remote-root)
            (construct local
                         (-> (nodes
                               [net (q/path [:net])
                                remote {:id remote-root
                                        :proxy (peer-url "localhost" (:port remote))}])
                           (edges
                             [net remote :peer])))

        ; Now issue a query that will traverse over the network
        ; through the proxy node.
        (let [q (-> (q/path [synth [:net :peer :music :synths :synth]])
                  (q/project ['synth :label]))
              res (query local q)]
          (is (= #{:kick :bass :snare :hat} (set (map :label res))))))
      (finally
        (close local)
        (close remote)))))

