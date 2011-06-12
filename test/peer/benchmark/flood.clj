(ns benchmark.flood
  (:use [plasma util url viz]
        [plasma.net graph connection peer]
        [clojure test stacktrace]
        test-utils)
  (:require [logjam.core :as log]
            [plasma.query.core :as q]))

; Shows a two tier flood query where the path expression crosses two proxy nodes

(defn- link-proxy
  [local remote]
  (let [net (:id (first (query local (q/path [:net]))))
        p-root (get-node remote ROOT-ID)
        p-id (:id p-root)
        p-port (:port remote)]
    (make-edge net
               (make-proxy-node p-id (plasma-url "localhost" p-port))
               :peer)))

(defn flood-peers []
  (let [peers (make-peers (+ 1 5 25) (+ 2000 (rand-int 10000))
                (fn [i]
                  (clear-graph)
                  (make-edge ROOT-ID (make-node {:name :net}) :net)
                  (make-edge ROOT-ID (make-node {:value i}) :data)))
        base (first peers)
        neighbors (take 5 (drop 1 peers))
        nneighbors (partition 5 (drop 6 peers))]
    (with-peer-graph base
      (doseq [p neighbors]
        (link-proxy base p)))
    (doseq [[p p-peers] (map vector neighbors nneighbors)]
      (with-peer-graph p
        (doseq [pp p-peers]
            (link-proxy p pp))))
    peers))

(defn flood-benchmark []
  (let [peers (flood-peers)
        base (first peers)]
    (try
      (println "peers: " (query base (q/path [:net :peer])))
      (println "proxies: " (query base (-> (q/path [p [:net :peer]])
                                (q/project [p :proxy]))))
      (println "peer values: " (query base (-> (q/path [p [:net :peer]
                                           d [p :data]])
                                (q/project [d :value]))))
      (println "tier two: "
        (sort (map :value
             (query base
                    (->
                      (q/path [d [:net :peer :net :peer :data]])
                      (q/project [d :value]))))))
      (finally
        (close-peers peers)))))

(comment
(def peers (flood-peers))
(def base (first peers))
(close-peers peers)

)
