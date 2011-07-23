(ns peer.bootstrap
  (:use [plasma graph util]
        [peer core config connection])
  (:require [plasma.query :as q]
            [lamina.core :as lamina]
            [logjam.core :as log]))

(log/file :bootstrap "boot.log")

(defn- peer-urls
  [p]
  (with-peer-graph p
    (q/query (-> (q/path [peer [:net :peer]])
               (q/project ['peer :url])))))

(defn- have-peer?
  [p url]
  (contains? (set (peer-urls p)) url))

(defn- advertise-handler
  [p con event]
  (when event
    (let [[root-id url] (:params event)]
      (log/to :bootstrap "[advertise-handler] got advertisement:" url root-id)
      (when-not (have-peer? p url)
        (add-peer p root-id url)))
    (log/to :bootstrap "[advertise-handler] bootstrap peer has:"
            (count (get-peers p)) "peers")))

(defn bootstrap-peer
  "Returns a peer that will automatically add new peers to its graph at
  [:net :peer] when they connect."
  ([] (bootstrap-peer {}))
  ([options]
   (let [p (peer options)]
     (with-peer-graph p (clear-graph))
     (setup-peer-graph p)
     (peer-event-handler p :bootstrap-advertise advertise-handler)
     (log/to :bootstrap "[bootstrap-peer] has:" (count (get-peers p)) "peers")
     p)))

(def N-BOOTSTRAP-PEERS 5)
(def RETRY-PERIOD 200)
(def MAX-RETRY-PERIOD (* 50 RETRY-PERIOD))
(def MAX-RETRIES 50)

(defn add-bootstrap-peers
  ([p boot-url n] (add-bootstrap-peers p boot-url n 0))
  ([p boot-url n n-retries]
   (let [con (peer-connection p boot-url)
         new-peers (query con (-> (q/path [peer [:net :peer]])
                                (q/project ['peer :proxy :id])
                                (q/choose N-BOOTSTRAP-PEERS)))]
     (log/to :bootstrap "n: " n "\n"
             "n-retries: " n-retries "\n"
             "new-peers: " (seq new-peers))
     (doseq [{url :proxy id :id} new-peers]
       (when-not (get-node p id)
               (add-peer p id url)))
     (let [n-peers (first (query p (q/count*
                                     (q/path [:net :peer]))))]
       (log/to :bootstrap "n-peers: " n-peers)
       (when (and
               (not= :closed @(:status p))
               (< n-retries MAX-RETRIES)
               (< n-peers N-BOOTSTRAP-PEERS))
         (schedule (min MAX-RETRY-PERIOD (* RETRY-PERIOD (Math/pow n-retries 1.5)))
                   #(add-bootstrap-peers p boot-url
                                         (- N-BOOTSTRAP-PEERS n-peers)
                                         (inc n-retries))))))))

(defn- advertise
  [con root-id url]
  (send-event con :bootstrap-advertise [root-id url]))

(defn- bootstrap*
  [p boot-url]
  (let [booter (peer-connection p boot-url)
        root-id (with-peer-graph p (root-node-id))
        my-url (:url p)]
    (setup-peer-query-handlers p booter)
    (advertise booter root-id my-url)
    (add-bootstrap-peers p boot-url N-BOOTSTRAP-PEERS)))

(defn bootstrap
  [p boot-url]
  (schedule 1 #(bootstrap* p boot-url)))

