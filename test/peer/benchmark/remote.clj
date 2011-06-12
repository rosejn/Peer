(ns benchmark.remote
  (:use [plasma util graph viz]
        [plasma.net url connection peer]
        [clojure test stacktrace]
        test-utils)
  (:require [logjam.core :as log]
            [plasma.query.core :as q]))

; Shows the combination of data from local and remote piers when project
; on both local and remote nodes.

(defn- link-proxy
  [local remote]
  (let [net (:id (first (query local (q/path [:net]))))
        p-root (get-node remote ROOT-ID)
        p-id (:id p-root)
        p-port (:port remote)]
    (make-edge net
               (make-proxy-node p-id (plasma-url "localhost" p-port))
               :peer)))

(defn remote-peers [n]
  (let [peers (make-peers n (+ 2000 (rand-int 10000))
                (fn [i]
                  (clear-graph)
                  (make-edge ROOT-ID (make-node {:name :net}) :net)
                  (make-edge ROOT-ID (make-node {:value i}) :data)))
        base (first peers)
        neighbors (next peers)]
    (with-peer-graph base
      (doseq [p neighbors]
        (link-proxy base p)))
    peers))

(defn remote-benchmark []
  (let [peers (remote-peers 5)
        base (first peers)]
    (try
      ; Attach fake latencies
      (with-peer-graph base
        (doseq [p (map :id (query base (q/path [:net :peer])))]
          (node-assoc p :latency (rand-int 120))))
      (println "peers: " (query base (q/path [:net :peer])))

      (println "vals: " (query base (-> (q/path [d [:net :peer :data]])
                                      (q/project [d :value]))))
      (println "peer values: " (query base (-> (q/path [p [:net :peer]
                                                        d [p :data]])
                                             (q/project [p :latency]
                                                        [d :value]))))
      (finally
        (close-peers peers)))))

(comment
(def peers (flood-peers))
(def base (first peers))
(close-peers peers)

)
