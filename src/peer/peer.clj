(ns plasma.net.peer
  (:use [plasma config util graph api]
        [plasma.net url connection address presence rpc]
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [jiraph.graph :as jiraph]
            [plasma.query.construct :as c]
            [plasma.query.core :as q]))

(defmacro with-peer-graph [p & body]
  `(let [g# (:graph ~p)]
     (locking g#
     (if (= jiraph/*graph* g#)
       (do ~@body)
       (jiraph/with-graph g# ~@body)))))

(def *manager* nil)
(def DEFAULT-HTL 50)

(defrecord PlasmaPeer
  [manager graph url port listener status options] ; peer-id, port, max-peers
  IPeer
  (peer-id
    [this]
    (with-peer-graph this (root-node-id)))

  IQueryable
  (get-node
    [this id]
    (with-peer-graph this (find-node id)))

  (construct
    [this spec]
    (with-peer-graph this
      (c/construct* spec)))

  (query
    [this q]
    (query this q {}))

  (query
    [this q params]
    (query this q params q/MAX-QUERY-TIME))

  (query
    [this q params timeout]
    (binding [*manager* manager]
      (with-peer-graph this
        (q/query q params timeout))))

  (query-channel
    [this q]
    (query-channel this q {}))

  (query-channel
    [this q params]
    (binding [*manager* manager]
      (with-peer-graph this
        (q/query-channel q params))))

  (recur-query
    [this pred q]
    (recur-query this pred q {}))

  (recur-query
    [this pred q params]
    (let [params (merge (:params q) params)
          iplan (assoc q
                       :type :recur-query
                       :src-url (public-url port)
                       :pred pred
                       :recur-count count
                       :htl DEFAULT-HTL
                       :params params)]
      (recur-query this iplan)))

  (recur-query
    [this q]
     (comment let [q (map-fn q :recur-count dec)
           res-chan (query-channel q (:params q))]
       (lamina/on-closed res-chan
         (fn []
           (let [res (lamina/channel-seq res-chan)]
             ; Send the result back if we hit the end of the recursion
             (if (zero? (:recur-count q))
               (let [src-url (:src-url q)
                     query-id (:id q)
                     con (get-connection manager (:src-url q))]
                 (send-event con query-id res))

               ; or recur if not
               (doseq [n res]
                 (if (proxy-node? n)
                   (peer-recur-query
                 (receive-all res-chan
                              (fn [v]
                                (if (proxy-node? v)
                                  (peer-recur q v)
                                  (recur* q v)))))))))))))

  ; TODO: Support binding to a different parameter than the ROOT-ID
  ; by passing a {:bind 'my-param} map.
  (iter-n-query
    [this n q]
    (iter-n-query this n q {}))

  (iter-n-query
    [this n q params]
     (let [iplan (assoc q
                        :type :iter-n-query
                        :src-url (public-url port)
                        :iter-n n
                        :htl DEFAULT-HTL
                        :iter-params params)]
     (iter-n-query this iplan)))

  (iter-n-query
    [this q]
    (let [final-res (lamina/channel)
           iter-fn (fn iter-fn [q]
                     (log/to :peer "iter-n: " (:iter-n q))
                     (let [plan (update-in q [:iter-n] dec)
                           plan (update-in plan [:htl] dec)
                           res-chan (query-channel this plan (:iter-params plan))]
                       (lamina/on-closed res-chan
                         (fn []
                           (cond
                             (zero? (:iter-n plan))
                             (lamina/siphon res-chan final-res)

                             (zero? (:htl plan))
                             (lamina/enqueue final-res
                                             {:type :error
                                              :msg :htl-reached})

                             :default
                             (let [res (map :id (lamina/channel-seq res-chan))
                                   params (assoc (:iter-params plan) ROOT-ID res)
                                   plan (assoc plan :iter-params params)]
                               (log/to :peer "--------------------\n"
                                       "iter-fn result: "
                                       (seq res)
                                       "\n--------------------------\n")

                               (iter-fn plan)))))))]
       (iter-fn q)
       final-res))

  IConnectionListener
  (on-connect
    [this handler]
    (on-connect listener handler))

  IClosable
  (close
    [this]
    (close listener)
    (when (:internal-manager options)
      (clear-connections manager))
    (reset! status :closed)))

(defn- net-root
  [p]
  (with-peer-graph p
    (:id (first (q/query (q/path [:net]))))))

(defn add-peer
  [p id url]
  (log/to :peer "[add-peer] adding:" url)
  (with-peer-graph p
    (let [prx (make-proxy-node id url)
          net (net-root p)]
      (make-edge net prx :peer))))

(defn get-peers
  [p]
  (query p (-> (q/path [peer [:net :peer]])
             (q/project ['peer :id :proxy]))))

(defn- setup-peer-presence
  [p]
  (let [p-host (local-addr)
        p-port (:port p)
        pchan (lamina/filter* #(not (and (= p-host (:host %))
                                         (= p-port (:port %))))
                             (presence-channel))]
    (lamina/receive-all pchan
      (fn [{:keys [id host port]}]
        (add-peer p id (plasma-url host port))))
    (presence-broadcaster (peer-id p) p-host p-port (config :presence-period))))

(defmulti rpc-handler
  "A general purpose rpc multimethod."
  (fn [peer req] (:method req)))

(defmethod rpc-handler 'get-node
  [peer req]
  (get-node peer (first (:params req))))

(defmethod rpc-handler 'construct
  [peer req]
  (construct peer (first (:params req))))

(defmethod rpc-handler 'query
  [peer req]
  (apply query peer (:params req)))

(defn- request-handler
  [peer [ch req]]
  (when req
    (log/format :peer "request-handler[%s]: %s" (:id req) (:method req))
    (try
      (let [res (rpc-handler peer req)
            res (if (seq? res)
                  (doall res)
                  res)
            rpc-res (rpc-response req res)]
        (lamina/enqueue ch rpc-res))
      #_(catch java.lang.IllegalArgumentException e
          (lamina/enqueue
            ch
            (rpc-error req (format "No handler found for method: %s\n\n%s" (:method req)
                                   (with-out-str (.printStackTrace e))) e)))
      (catch Exception e
        (log/to :peer "error handling request!\n------------------\n"
                (with-out-str (print-cause-trace e)))
        (.printStackTrace e)
        (lamina/enqueue ch
          (rpc-error req (str "Exception occured while handling request:\n" (with-out-str (.printStackTrace e))) e))))))

(defmulti stream-handler
  "A general purpose stream multimethod."
  (fn [peer ch req] (:method req)))

(defmethod stream-handler 'query-channel
  [peer ch req]
  (log/to :peer "[stream-handler] query-channel: " req)
  (let [res-chan (apply query-channel peer (:params req))]
    (lamina/siphon res-chan ch)
    (lamina/on-drained res-chan
      (fn []
        (lamina/close ch)
        (log/to :peer "[stream-handler] query-channel: closed")))))

(defmethod stream-handler 'node-event-channel
  [peer ch req]
  (log/to :peer "[stream-handler] node-event-channel: " req)
  (let [res-chan (with-peer-graph peer
                   (apply node-event-channel (:params req)))]
    (lamina/siphon res-chan ch)
    (lamina/on-drained res-chan
      (fn []
        (lamina/close ch)
        (log/to :peer "[stream-handler] node-event-channel: closed")))))

(defmethod stream-handler 'edge-event-channel
  [peer ch req]
  (log/to :peer "[stream-handler] edge-event-channel: " req)
  (let [res-chan (with-peer-graph peer
                   (apply edge-event-channel (:params req)))]
    (lamina/siphon res-chan ch)
    (lamina/on-drained res-chan
      (fn []
        (lamina/close ch)
        (log/to :peer "[stream-handler] edge-event-channel: closed")))))

(defn- stream-request-handler
  [peer [ch req]]
  (when req
    (log/to :peer "stream-request: " (:id req))
    (try
      (stream-handler peer ch req)
      (catch Exception e
        (log/to :peer "error handling stream request!\n"
                "-------------------------------\n"
                (with-out-str (print-cause-trace e)))))))

(defn handle-peer-connection
  "Hook a connection up to a peer so that it can receive queries."
  [peer con]
  (log/to :peer "handle-peer-connection new-connection: " (:url con))

  (lamina/receive-all (lamina/filter* #(not (nil? %))
                                      (lamina/fork (:chan con)))
    (fn [msg] (log/to :peer "incoming msg:" msg)))

  (lamina/receive-all (request-channel con)
                      (partial request-handler peer))
  (lamina/receive-all (stream-channel con)
                      (partial stream-request-handler peer)))

(defn setup-peer-graph
  [p]
  (with-peer-graph p
    (if (empty? (q/query (q/path [:net])))
      (make-edge ROOT-ID (make-node) :net))))

(defn peer
  "Create a new peer.

  Available options:
    :path => path to persistent peer graph (database)
    :port => specify port number to listen on"
  ([] (peer {}))
  ([options]
   (let [port (get options :port (config :peer-port))
         [manager options] (if (:manager options)
                             [(:manager options) options]
                             [(connection-manager)
                              (assoc options :internal-manager true)])
         g (if-let [path (:path options)]
             (open-graph path)
             (open-graph))
         listener (connection-listener manager (config :protocol) port)
         status (atom :running)
         url (public-url port)
         p (PlasmaPeer. manager g url port listener status options)]
     (setup-peer-graph p)
     (on-connect p (partial handle-peer-connection p))

     (when (config :presence)
       (setup-peer-presence p))
     p)))

; TODO: Make this URL checking generic, maybe hooking into some Java URL class? (Ugh...)
(defn peer-connection
  "Returns a connection to a remote peer reachable by url, using the local peer p's
  connection manager."
  [p url]
  (assert-url url)
  (get-connection (:manager p) url))

(defn peer-get-node
  "Lookup a node by ID on a remote peer, returns a result-channel."
  [con id]
  (request con 'get-node [id]))

(defn peer-construct
  [con spec]
  (request con 'construct [spec]))

(defn peer-query-channel
  ([con q]
   (peer-query-channel con q {}))
  ([con q params]
   (log/to :peer "[peer-query-channel] starting query: " (:id q))
   (let [s-chan (stream con 'query-channel [q params])]
     s-chan)))

(defn peer-node-event-channel
  [con src-id]
  (stream con 'node-event-channel [src-id]))

; TODO: Use query predicate parser to transmit serialized predicates
(defn peer-edge-event-channel
  ([con src-id] (peer-edge-event-channel con src-id nil))
  ([con src-id pred]
   (stream con 'edge-event-channel [src-id pred])))

(defn peer-query
  "Send a query to the given peer.  Returns a constant channel
  that will get the result of the query when it arrives."
  ([con q]
   (peer-query con q {}))
  ([con q params]
   (peer-query con q params q/MAX-QUERY-TIME))
  ([con q params timeout]
   (let [q (q/with-result-project q)
         rchan (query-channel con q params)
         p (promise)]
     (lamina/on-closed rchan
       (fn [] (deliver p (lamina/channel-seq rchan))))
     (await-promise p (+ timeout q/PROMISE-WAIT-TIME)))))

(defn peer-recur-query
  [con q])

(defn peer-iter-n-query
  [con q n])

(defn peer-peer-id
  [con]
  (get-node con ROOT-ID))

(extend plasma.net.connection.Connection
  IQueryable
  {:get-node peer-get-node
   :construct peer-construct
   :query peer-query
   :query-channel peer-query-channel
   :recur-query peer-recur-query
   :iter-n-query peer-iter-n-query}

  IPeer
  {:peer-id peer-peer-id})

(defmethod peer-sender :default
  [url]
  (partial peer-query-channel (get-connection *manager* url)))

