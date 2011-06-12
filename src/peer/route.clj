(ns plasma.net.route
  (:use [plasma graph util digest api]
        [plasma.net connection peer url]
        [clojure.contrib.math :only (expt)])
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query.core :as q]))

(defn flood-n
  [])

(defn random-walk-n
  "Starting at peer p, do an n hop random walk, returning {:id :proxy} maps
  from all the peers traversed."
  ([p n] (random-walk-n p n p))
  ([p n start-peer]
   (let [walk-fn (fn [[_ g]]
                   (let [q (->
                             (q/path [peer [:net :peer]])
                             (q/project ['peer :id :proxy])
                             (q/choose 1))
                         res (first (query g q))]
                     [res (peer-connection p (:proxy res))]))]
     (map first
          (take n (drop 1
                        (iterate walk-fn [nil start-peer])))))))

(defn greedy-iter
  [])

(defn id-bits [id n-bits]
  (mod
    (cond
      (string? id) (hex->int (hex (sha1 id)))
      (number? id) id)
    (expt 2 n-bits)))

(defn ring-distance
  "Compute the distance between points a and b on a ring (finite group)
  where values go from zero to 2^n-bits.  Note that this distance is
  only computed in the clockwise (positive) direction."
  [n-bits a b]
  (let [a (id-bits a n-bits)
        b (id-bits b n-bits)
        max-n (expt 2 n-bits)]
    (mod (+ (- b a)
            max-n)
         max-n)))

(defn ring-abs-distance
  "Compute the natural distance between two points a and b in either direction
  on a ring where values go from zero to 2^n-bits."
  [n-bits a b]
  (let [a (id-bits a n-bits)
        b (id-bits b n-bits)
        max-n (expt 2 n-bits)
        dist (Math/abs (- a b))]
    (min dist (- max-n dist))))

(defn kademlia-distance
  "Compute the kademlia distance between two peer-ids hashed into
  an n-bit address space."
  [a b n-bits]
  (let [a (id-bits a n-bits)
        b (id-bits b n-bits)]
    (bit-xor a b)))

(defn to-binary
  "Convert a Number to a binary string."
  [v]
  (.toString (BigInteger/valueOf v) 2))

(defn rand-bits
  [n]
  (apply str (take n (repeatedly (partial rand-int 2)))))

(defn rand-bucket-id
  "Returns a random ID within the range of bucket B."
  [local-id n n-bits]
  (let [id (id-bits local-id n-bits)
        flip (- n-bits (inc n))
        id (bit-flip id flip)
        id (reduce #(if (zero? (rand-int 2))
                      (bit-flip %1 %2)
                      %1)
                   id
                   (range 0 flip))]
    id))

(defn k-bucket
  "Determine which bucket the test-id should reside in relation
  to the local-id while using an n-bit address space."
  [local-id test-id n-bits]
  (let [id (to-binary (id-bits local-id n-bits))
        test-id (to-binary (id-bits test-id n-bits))]
    (count (take-while (fn [[a b]] (= a b))
                       (map list id test-id)))))

; TODO: Ideally we could put this distance calculation in the query
; to limit the number of nodes sent...
(defn closest-peers
  "Returns the closest n peers (proxy node) to the tgt-id:
  {:id <peer-id> :proxy <peer-url>}
  "
  [p n tgt-id n-bits]
  (let [peers (query p (-> (q/path [p [:kad :bucket :peer]])
                         (q/project ['p :id :proxy])))]
    (if-not (nil? (first peers))
      (take n (sort-by #(kademlia-distance tgt-id (:id %) n-bits) peers))
      [])))

(def ALPHA 3)

(defn dht-lookup
  [p tgt-id n-bits]
  (let [root {:id (peer-id p)}]
    (loop [peers [p]
           closest root]
      (let [closest-dist (kademlia-distance tgt-id (:id closest) n-bits)
            cps (flatten (map #(closest-peers % ALPHA tgt-id n-bits) peers))
            cps (map #(assoc % :distance
                             (kademlia-distance tgt-id (:id %) n-bits))
                     cps)
            cps (filter #(< (:distance %) closest-dist) cps)
            sorted (sort-by :distance cps)]
        (if (empty? sorted)
          (assoc closest :distance (kademlia-distance (:id root) (:id closest) n-bits))
          (recur (map (comp (partial peer-connection p) :proxy) sorted)
                 (first sorted)))))))

(defn dht-join
  "Find peers with the addrs that fit in the slots of our peer table.
   The addrs closest to power of 2 distances from our ID

    guid + 2^0, 2^1, 2^2, 2^3, etc...
  "
  [p]
)
