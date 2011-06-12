(ns benchmark.auto-configure
  (:use
    [plasma config api]
    [plasma.net peer presence])
  (:require [plasma.query.core :as q]
            [logjam.core :as log]))

(config :presence true)

(log/repl :peer)
(println "starting peer...")
(def p (peer))

(println "peer started")

(println "sleeping 10 secs...")
(Thread/sleep 10000)

(let [ps (get-peers p)]
  (println "n-peers: " (count ps))
  (println "peers: \n" ps))

(close p)
