(ns benchmark.d2ht
  (:use plasma.core
        clojure.stacktrace
        [clojure.contrib.probabilities.random-numbers :only (rand-stream)])
  (:require [logjam.core :as log]
            [plasma.query.core :as q]
            [lamina.core :as lamina]
            [clojure.set :as set]
            [clojure.contrib.generic.collection :as gc]
            [clojure.contrib.probabilities.monte-carlo :as mc]
            [clojure.contrib.probabilities.finite-distributions :as dist]))

;D2HT:
;
;* a dht built using gossip
;* maintain neighbor sets for random and nearby peers
;* gossip with oldest peer in each set (reset age after each gossip)
; - delete if no response
;
;* one dimension (ring) for node ID
;* uses 2 sub protocols for maintaining the random and greedy net topologies

(def ADDR-BITS 8)

(def GOSSIP-PERIOD 2000)

(def KPS-PEERS 5)     ; parameter: L
(def KPS-ROUND-DROP 3) ; parameter: G

(def NPS-PEERS 15)     ; parameter: L

; Peers
; * proxy, timestamp, distance
; * each gossip round choose the least-fresh peer
; - if disconnected, remove and choose next peer

(defn d2ht-setup
  [p]
  (construct p
    (-> (nodes [net (q/path [:net])
                kps :kps
                nps :nps])
      (edges [net kps :kps
              net nps :nps]))))

;Kleinberg peer sampling: (KPS)
;* forms a small-world network
;* maintain L neighbors just for KPS
;* each round probabilistically keep the closest KPS neighbors, then remove and
;gossip the rest, G, of them
; - keep neighbors with the probability:
;
;      (/ 1.0 (distance a b))
;
;1) keep query:
;2) drop query: (inverse probability of keep query) 1 - p
;
;b) request their random peers
;  (-> (q/path [p [p-start :net :kps peer]])
;      (choose G)
;      (project ['p :id :proxy]))
;
;c) push peers
;  (-> (link (q/path [(select-gossip-peer-query) :net :kps :peer])
;                  (select-peers-to-drop) :peer)
;            :peer)

(defn with-peer-info
  [q]
  (-> q
    (q/project ['p :id :distance :timestamp :proxy])))

(defn kps-peers
  []
  (q/path [p [:net :kps :peer]]))

(defn add-kps-peer
  [p props]
  (log/to :d2 "adding: " (trim-id (:id props)))
  (let [dist (ring-abs-distance ADDR-BITS (peer-id p) (:id props))
        ts   (current-time)
        props (assoc props :timestamp ts :distance dist)]
    (if (get-node p (:id props))
      (do
        (log/to :d2 "exists... adding edge")
        (with-peer-graph
          p
          (apply assoc-node (:id props) (flatten (seq (dissoc props :id))))
          (make-edge (:id (first (q/path [:net :kps]))) (:id props) :peer)))
      (do
        (log/to :d2 "construct...")
        (construct p
                   (-> (nodes [kps (q/path [:net :kps])
                               new-peer props])
                     (edges [kps new-peer :peer])))))))

(defn add-kps-peers
  [p peers]
  (log/to :d2 "[add-kps-peers] " (count peers))
  (doseq [new-peer peers]
    (add-kps-peer p new-peer)))

(defn oldest-kps-peer
  "Select the least-fresh peer."
  []
  (-> (kps-peers)
    (q/order-by 'p :timestamp :asc)
    (q/limit 1)))

(defn- take-sample
  [n rt]
  (take n (gc/seq (mc/random-stream rt rand-stream))))

(defn harmonic-close-peers
  "Select n peers with a probability proportional to 1/distance.
  (harmonic probability function)"
  [peers n]
  (if (<= (count peers) n)
    peers
    (let [dist-map (reduce (fn [m v] (assoc m (:id v) (/ 1.0 (:distance v)))) {} peers)
          peer-dist (mc/discrete (dist/normalize dist-map))
          keepers (loop [to-keep #{}]
                    (let [tkc (count to-keep)]
                      (if (= n tkc)
                        to-keep
                        (recur (set/union to-keep
                                          (set (take-sample (- n tkc)
                                                            peer-dist)))))))]
      (filter #(keepers (:id %)) peers))))

;(let [test-peers (map (fn [v] {:id v :dist (* 100 v)}) (range 10))]
;  (harmonic-close-peers test-peers 3))

(defn remove-peers
  [p ptype peers]
  (let [type-id (:id (first (query p (q/path [:net ptype]))))]
    (with-peer-graph p
      (doseq [p-id (map :id peers)]
        (remove-edge type-id p-id)))))

(defn kps-exchange-peers
  [p]
  (let [peers (query p (with-peer-info (kps-peers)))
        keepers (harmonic-close-peers peers (- KPS-PEERS KPS-ROUND-DROP))
        keeper-set (set (map :id keepers))
        to-exchange (filter #(not (keeper-set (:id %))) peers)
        to-exchange (map #(select-keys % [:id :proxy]) to-exchange)]
    (log/format :d2 "[%s] exchanging: %s"
                (trim-id (peer-id p))
                (vec (map (comp trim-id :id) to-exchange)))
    to-exchange))

(defn update-timestamp
  [p id]
  (with-peer-graph p (assoc-node id :timestamp (current-time))))

(defn kps-purge
  [p exchanged]
  (let [num-peers (first (query p (q/count* (kps-peers))))]
    (when (> num-peers KPS-PEERS)
      (remove-peers p :kps (take (- num-peers KPS-PEERS) exchanged)))))

(defn conj-peer
  [elems p]
  (conj elems {:id (peer-id p) :proxy (:url p)}))

(defn kps-gossip
  "Perform a single round of kps gossiping."
  [p]
  (when-let [{url :proxy id :id} (first (query p (with-peer-info (oldest-kps-peer))))]
    (log/to :d2 "gossip url: " url)
    (let [partner (peer-connection p url)
          to-exchange (kps-exchange-peers p)
          sending (conj-peer to-exchange p)]
      (log/to :d2 "sending: " sending)
      (let [res-chan (request partner 'kps-exchange [sending])]
        (lamina/on-success res-chan
          (fn [res]
            (log/to :d2 "response: " res)
            (add-kps-peers p res)
            (kps-purge p to-exchange)))
        (lamina/on-error res-chan
          (fn [e]
            (log/to :d2 "ERROR: " e))))
      (update-timestamp p id))))

(defmethod rpc-handler 'kps-exchange
  [p req]
  (log/to :d2 "inside handler..." req)
  (let [new-peers (first (:params req))
        to-exchange (kps-exchange-peers p)]
    (add-kps-peers p new-peers)
    (kps-purge p to-exchange)
    (conj-peer to-exchange p)))

(defn kps-on
  [p period]
  (assoc p :kps-timer (periodically period (partial kps-gossip p))))

(defn kps-off
  [p]
  (if-let [t (:kps-timer p)]
    (t))
  (dissoc p :kps-timer))

;Neighbor peer sampling: (NPS)
;* gossip request to the oldest peers (either NPS or KPS) who are also closest to
;you in distance, and request their neighbors closest to you

(defn all-peers
  []
  (q/path [p [:net #"nps|kps" :peer]]))

(defn oldest-n-peers
  [p n]
  (query p
         (-> (all-peers)
           (q/order-by 'p :distance :asc)
           (q/limit n))))

(defn closest-n-peers-to-q
  "Return the closest n peers to neighbor q."
  [p q n]
  (take n (sort-by
            (fn [b] (ring-abs-distance (:id q) (:id b)))
            (query p (all-peers)))))

(defn nps-peers
  []
  (q/path [p [:net :nps :peer]]))

(defn add-nps-peer
  [p props]
  (let [dist (ring-abs-distance (peer-id p) (:id props))
        ts   (current-time)
        props (assoc props :timestamp ts :distance dist)]
    (construct p
      (-> (nodes [nps (q/path [:net :nps])
                  new-peer props])
        (edges [kps new-peer :peer])))))

(defn add-nps-peers
  [p new-peers]
  (let [cur-peers (query p (with-peer-info (nps-peers)))
        closest-peers (take NPS-PEERS
                            (sort-by
                              (fn [b] (ring-abs-distance (:id p) (:id b)))
                              (concat cur-peers new-peers)))]
  (doseq [new-peer closest-peers :when (not (get-node p (:id new-peer)))]
    (add-nps-peer p new-peer))))

(defn nps-exchange-peers
  [p q]
  (-> (closest-n-peers-to-q p q 5)
    (conj {:id (peer-id p) :proxy (:url p)})))

(defn nps-gossip
  "Perform a round of NPS gossiping."
  [p]
  (let [oldies (oldest-n-peers p 4)
        q-peer (first (harmonic-close-peers oldies 1))
        q-con (peer-connection p (:proxy q-peer))
        to-exchange (nps-exchange-peers p q-peer)
        res-chan (request q-con 'nps-exchange [to-exchange])]
      (lamina/on-success res-chan
        #(add-nps-peers p %))
    (update-timestamp p (:id q-peer))))

(defmethod rpc-handler 'nps-exchange
  [p req]
  (let [new-peers (first (:params req))
        to-exchange (nps-exchange-peers p)]
    (add-nps-peers new-peers)
    to-exchange))

(defn kps-on
  [p period]
  (assoc p :kps-timer (periodically period (partial kps-gossip p))))

(defn kps-off
  [p]
  (if-let [t (:kps-timer p)]
    (t))
  (dissoc p :kps-timer))

#_(defn start
  [n start-delay n-searches]
  (let [s-port 23423
        s-url (plasma-url "localhost" s-port)
        strapper (bootstrap-peer {:port s-port})
        peers (doall (take n (map #(peer {:port %})
                                  (iterate inc (inc s-port)))))]
    (doseq [p peers]
      (d2ht-setup p))
    [strapper peers]))

(defn addp
  "Add peer b to peer a's kps view."
  [a b]
  (add-kps-peer a {:id (peer-id b) :proxy (plasma-url "localhost" (:port b))}))

(defn setup
  []
  (log/file :d2 "d2.log")
  (def peers (doall (take 9 (map #(peer {:port %})
                                 (iterate inc (inc 1234))))))
  (doseq [p peers] (d2ht-setup p))
  (def p1 (nth peers 0))
  (def p2 (nth peers 1))
  (doseq [p (drop 1 peers)] (addp p p1)))

(defn reset
  []
  (doseq [p peers] (close p))
  (setup))

(defn gossip
  []
  (doseq [p peers] (kps-gossip p)))

(defn pc
  []
  (println "peer-counts: " (vec (map #(first (query % (q/count* (all-peers)))) peers))))

(defn ids
  []
  (println "peer-ids: " (vec (map #(trim-id (peer-id %)) peers))))

