(ns plasma.net.heartbeat
  (:use [plasma graph util config api]
        [plasma.net connection peer address]
        jiraph.graph)
  (:require [plasma.query.core :as q]
            [lamina.core :as lamina]
            [logjam.core :as log]))

; Stores a seq of heartbeat timestamps for each peer, keyed by root-id.
(def heartbeats* (atom {}))

(defn- heartbeat-handler
  [peer con event]
  (let [ts (current-time)
        id (first (:params event))]
    (swap! heartbeats* (fn [beats]
                         (update-in beats [id] #(conj % ts))))))

; TODO: abstract this pattern, also used in bootstrap
(defn- heartbeat-connect-handler
  [peer con]
  (lamina/receive-all (event-channel con :heartbeat)
                      (partial heartbeat-handler peer con)))

(defn detect-failures
  [peer]
  (on-connect peer (partial heartbeat-connect-handler peer)))

(defn- send-heartbeat
  [con root-id]
  (send-event con :heartbeat [root-id]))

(defn- do-heartbeat
  [peer n-query]
  (let [root-id (with-graph (:graph peer) (root-node))]
    (doseq [neighbor (query peer n-query)]
      (let [con (get-connection (:manager peer) (:url neighbor))]
        (send-heartbeat con root-id)))))

(defn heartbeat
  "Send a heartbeat message to all neighbors which are chosen
  by executing a \"neighbor-query\" every period milliseconds.
  The returned function can be called to stop heartbeating."
  [peer period n-query]
  (periodically period
    (fn []
      (try
        (do-heartbeat peer n-query)
        (catch Exception e
          (log/to :heartbeat "Error in heartbeat: " e "\n"
                  (with-out-str (.printStackTrace e))))))))

