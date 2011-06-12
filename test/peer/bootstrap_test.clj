(ns plasma.net.bootstrap-test
  (:use [plasma graph util api]
        [plasma.net peer url connection bootstrap]
        test-utils
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query.core :as q]))

;(log/file [:peer :bootstrap :con] "peer.log")

(deftest bootstrap-test
  (let [port (+ 5000 (rand-int 5000))
        strapper (bootstrap-peer {:port port})
        strap-url (plasma-url "localhost" port)
        n-peers 10
        peers (make-peers n-peers (inc port)
                (fn [i]
                  (clear-graph)
                  (let [root-id (root-node-id)]
                    (assoc-node root-id :peer-id i)
                    (make-edge root-id (make-node) :net))))]
    (is (= 1 (count (query strapper (q/path [:net]) 200))))
    (try
      (doseq [p peers]
        (bootstrap p strap-url))
      (Thread/sleep 2000)
      (let [all-peers (query strapper (q/path [:net :peer]))
            p-counts (map (comp first #(query % (q/count* (q/path [:net :peer])) 200))
                          peers)]
        (is (= n-peers (count all-peers)))
        (is (every? #(>= % N-BOOTSTRAP-PEERS) p-counts)))
      (finally
        (close strapper)
        (close-peers peers)))))

(comment
  (def strap (bootstrap-peer {:port 2345}))
  (def strap-url (plasma-url "localhost" 2345))

  (def peers (make-peers 2 2223
  (fn [i]
  (clear-graph)
  (let [root-id (root-node-id)]
  (assoc-node root-id :peer-id i)
  (make-edge root-id (make-node) :net)))))

  (bootstrap (second peers) (plasma-url "localhost" 2234))
  (query (first peers) (-> (q/path [peer [:net :peer]])
  (q/project [peer :proxy :id])) {} 500)
)
