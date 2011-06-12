(ns benchmark.cluster
  (:use [plasma util graph viz]
        [plasma.net url connection peer bootstrap route]
        [clojure test stacktrace]
        test-utils)
  (:require [logjam.core :as log]
            [plasma.query.core :as q]))

(defn cluster-ids
  [n-clusters size]
  (let [start-id 100
        step (* size 1000)]
    (for [i (range n-clusters) j (range size)]
      (+ start-id (* step i) j))))

(defn cluster-peers [n-cl cl-size]
  (let [ids (cluster-ids n-cl cl-size)
        strapper (bootstrap-peer {;:path "db/strapper"
                                  :port 1234})
        strap-url (plasma-url "localhost" 1234)
        peers (make-peers (* n-cl cl-size) (+ 2000 (rand-int 10000))
                          (fn [i]
                            (clear-graph)
                            (make-edge ROOT-ID (make-node {:name :net}) :net)
                            (make-edge ROOT-ID (make-node {:value (nth ids i)}) :data)))]
    (doseq [p peers]
      (bootstrap p strap-url))
    [strapper peers]))

(defn peer-vals
  [p]
  (query p (-> (q/path [peer [:net :peer]
                      data [peer :data]])
             (q/order-by data :value)
             (q/project [peer :id] [data :value])) {} 200))

(defn local-val
  [p]
  (:value (first (query p (-> (q/path [d [:data]])
                            (q/project [d :value]))))))

(defn closest-peers
  [p]
  (let [pval (local-val p)
        pvals (peer-vals p)]
    (sort-by #(Math/abs (- pval (:value %)))
             pvals)))

(defn get-peers
  [p]
  (query p (-> (q/path [peer [:net :peer]])
             (q/project [peer :id :proxy]))))

(defn gather-peers
  [p n]
  (let [start-id (:id (first (closest-peers p)))
        start-con (peer-connection p (:proxy (get-node p start-id)))
        new-peers (concat (get-peers start-con) (random-walk-n p n))
        pids (set (map (fn [{id :id url :proxy}] [id url])
                       new-peers))]
    (doseq [{id :id url :proxy} new-peers]
      (when-not (get-node p id)
        (println "adding peer: " (trim-id id))
        (add-peer p id url)))))

(defn trim-peers
  [p n]
  (let [dead-peers (drop n (closest-peers p))]
    (doseq [{id :id} dead-peers]
      (with-peer-graph
        (remove-node id)))))

(defn cluster-distance
  [p c-size]
  (let [myval (local-val p)
        cluster-vals (take c-size (map :value (closest-peers p)))]
    (println (format "%4s: %s" myval (vec cluster-vals)))
    (float
      (/ (reduce (fn [mem v]
              (+ mem (Math/abs (- myval v))))
            0
            cluster-vals)
       c-size))))

(defn print-clusters
  [peers cluster-size]
  (doseq [p peers]
    (let [closest (map :value (closest-peers p))
          [in-cluster out-cluster] (split-at cluster-size closest)]
    (println (format "%4s: %s -- %s"
                     (local-val p)
                     (vec in-cluster)
                     (vec out-cluster))))))

(defn print-peer
  [p]
  (println (format "%s: %s"
                   (local-val p)
                   (vec (map :value (closest-peers p))))))

(defn cluster-benchmark
  [n-clusters cluster-size n-cycles walk-len]
  (let [[strapper peers] (cluster-peers n-clusters cluster-size)]
    (Thread/sleep (* 3 RETRY-PERIOD))
    (try
      (dotimes [i n-cycles]
        (doseq [p peers]
          (future (gather-peers p walk-len)))
        (Thread/sleep 2000)
        (doseq [p peers]
          (future (trim-peers p 6)))
        (println "Distance: "
                 (reduce +
                         (map #(cluster-distance % cluster-size) peers)))
        #_(print-clusters peers cluster-size)
        #_(doseq [p peers]
          (print-peer p))
        (flush))
      (print-clusters peers cluster-size)
      (finally
        (close strapper)
        (close-peers peers)))))

;(cluster-benchmark 4 3 8 12)
