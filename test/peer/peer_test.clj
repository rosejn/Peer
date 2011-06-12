(ns plasma.net.peer-test
  (:use [plasma config util graph api]
        [plasma.net url connection peer]
        [plasma.query construct]
        test-utils
        clojure.contrib.generic.math-functions
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query.core :as q]
            [jiraph.graph :as jiraph]))

(deftest get-node-test
  (let [p (peer {:path "db/p1" :port 1234})]
    (try
      (let [client (get-connection (connection-manager) (plasma-url "localhost" 1234))]
        (dotimes [i 4]
          (let [res-chan (get-node client ROOT-ID)
                res (wait-for res-chan 400)]
            (is (uuid? (:id res)))))
        (close client))
      (finally
        (close p)))))

(defn- reset-peer
  [p]
  (with-peer-graph p
    (clear-graph)
    (test-graph)))

(deftest simple-query-test []
  (let [port (+ 10000 (rand-int 10000))
        p (peer {:path "db/p1" :port port})
        con (get-connection (connection-manager) (plasma-url "localhost" port))
        q (-> (q/path [s [:music :synths :synth]])
            (q/where (> (* 100 (:score 's)) (sqrt 2500))))]
    (try
      (reset-peer p)
      (is (= 2 (count (query con q {} 200))))
      (finally
        (close con)
        (close p)))))

(deftest peer-query-test []
  (dosync (alter config* assoc
                 :peer-port (+ 10000 (rand-int 20000))
                 :presence-port (+ 10000 (rand-int 20000))))
  (let [port (+ 10000 (rand-int 10000))
        local (peer {:path "db/p1" :port port})
        root-id (:id (get-node local ROOT-ID))
        manager (:manager local)]
    (try
      (reset-peer local)
      (let [{:keys [foo bar] :as res} (construct local
                                         (-> (nodes
                                               [foo {:name "foo"}
                                                bar {:name "bar"}])
                                           (edges [ROOT-ID foo :foo
                                                   foo bar :bar])))]
        (is (= bar (:id (first (query local (q/path [:foo :bar])))))))

      (let [con (get-connection manager (plasma-url "localhost" port))]
        (is (uuid? (:id (wait-for (get-node con ROOT-ID) 200))))
        (let [q (-> (q/path [s [:music :synths :synth]])
                  (q/where (>= (:score 's) 0.6)))
              qp (-> q (q/project 's))
              lres (query local q)
              res  (query con q {} 200)
              lchan (lamina/channel-seq (query-channel local qp) 200)
              cchan (lamina/channel-seq
                      (query-channel con qp)
                      100)]
          (is (= lres res lchan cchan))
          (is (= 2 (count res)))
          (is (= #{:bass :kick} (set (map :label (map #(wait-for (get-node con (:id %)) 200)
                                                      res)))))))
      (finally
        (close local)))))

(deftest proxy-node-test []
  (dosync (alter config* assoc
                 :peer-port (+ 10000 (rand-int 20000))
                 :presence-port (+ 10000 (rand-int 20000))))
  (let [port     (+ 1000 (rand-int 10000))
        local    (peer {:port port})
        remote   (peer {:port (inc port)})]
    (try
      (reset-peer local)
      (reset-peer remote)
      ; Add a proxy node to the local graph pointing to the root of the remote
      ; graph.
      (let [remote-root (:id (get-node remote ROOT-ID))]
            (log/to :peer "remote-root: " remote-root)
            (construct local
                         (-> (nodes
                               [net (q/path [:net])
                                remote {:id remote-root
                                        :proxy (plasma-url "localhost" (:port remote))}])
                           (edges
                             [net remote :peer])))

        ; Now issue a query that will traverse over the network
        ; through the proxy node.
        (let [q (-> (q/path [synth [:net :peer :music :synths :synth]])
                  (q/project ['synth :label]))
              res (query local q)]
          (is (= #{:kick :bass :snare :hat} (set (map :label res))))))
      (finally
        (close local)
        (close remote)))))

(deftest many-proxy-node-test
  (dosync (alter config* assoc
                 :peer-port (+ 10000 (rand-int 20000))
                 :presence-port (+ 10000 (rand-int 20000))))
  (let [n-peers 10
        port  (+ 1000 (rand-int 10000))
        local (peer {:port port})
        peers (doall
                (map
                  (fn [n]
                    (let [p (peer {:port (+ port n 1)
                                   :manager (:manager local)})]
                      (with-peer-graph p
                        (clear-graph)
                        (assoc-node ROOT-ID :peer-id n))
                      (construct p
                                 (-> (nodes [root ROOT-ID
                                             net :net
                                             docs :docs
                                             a {:label (str "a-" n) :score 0.1}
                                             b {:label (str "b-" n) :score 0.5}
                                             c {:label (str "c-" n) :score 0.9}])
                                   (edges [root net  :net
                                           root docs :docs
                                           docs a    :doc
                                           docs b    :doc
                                           docs c    :doc])))
                      [p (with-peer-graph p (root-node-id)) n]))
                  (range n-peers)))]
    (try
      (with-peer-graph local
        (clear-graph)
        (let [net (make-node {:label :net})]
          (make-edge ROOT-ID net {:label :net})
          (doseq [[p peer-root n] peers]
            (make-edge net
                       (make-proxy-node peer-root (plasma-url "localhost" (+ port n 1)))
                       :peer))))
      (let [q (-> (q/path [doc [:net :peer :docs :doc]])
                (q/where (> (:score 'doc) 0.5))
                (q/project ['doc :label :score]))
            res (query local q)]
        (is (= n-peers (count res))))
      (finally
        (close local)
        (close-peers (map first peers))))))

(defn node-chain
  "Create a chain of n nodes starting from src, each one connected
  by an edge labeled label.  Returns the id of the last node in the
  chain."
  [src n label]
  (let [chain-ids (doall
                    (take (inc n) (iterate
                              (fn [src-id]
                                (let [n (make-node)]
                                  (make-edge src-id n label)
                                  n))
                              src)))]
    (log/to :peer "----------------------\n"
            "chain-ids: " (seq chain-ids))
    (last chain-ids)))

(deftest iter-n-test
  (let [local (peer)]
    (try
      (let [end-id (with-peer-graph local
                     (clear-graph)
                     (let [root-id (root-node-id)]
                       (log/to :peer "root-id: " root-id)
                       (node-chain root-id 10 :foo)))
            res-chan (iter-n-query local 10 (-> (q/path [f [:foo]])
                                              (q/project 'f)))]
          (is (= end-id
                 (:id (first (lamina/channel-seq res-chan 200))))))
      (finally
        (close local)))))

(deftest connect-test
  (let [local (peer {:port 2342})
        connected (atom [])]
    (try
      (on-connect local (fn [new-con] (swap! connected #(conj % (:url new-con)))))
      (dotimes [i 10]
        (let [con (get-connection (connection-manager) (plasma-url "localhost" 2342))]
          (wait-for (get-node con ROOT-ID) 200)
          (close con)))
      (is (= 10 (count @connected)))
      (finally
        (close local)))))

(deftest connection-handler-test
  (let [p1 (peer {:port 2222})
        p2 (peer {:port 3333})
        res (atom [])]
    (try
      (on-connect (:listener p2) (fn [incoming]
                                   (future
                                     (let [root (wait-for (get-node incoming ROOT-ID) 200)]
                                       (swap! res #(conj % [1 (:id root)]))))))
      (on-connect (:listener p2) (fn [incoming]
                                   (future
                                     (let [root (wait-for (get-node incoming ROOT-ID) 200)]
                                       (swap! res #(conj % [2 (:id root)]))))))
      (let [con (get-connection (connection-manager) (plasma-url "localhost" 3333))]
        (handle-peer-connection p1 con)
        (Thread/sleep 100)
        (let [id (:id (get-node p1 ROOT-ID))
              sres (sort-by first @res)]
          (is (= [1 id] (first sres)))
          (is (= [2 id] (second sres)))))
        (finally
          (close p1)
          (close p2)))))

(deftest peer-graph-event-test
  (let [port (+ 10000 (rand-int 10000))
        local (peer {:path "db/p1" :port port})
        root-id (:id (get-node local ROOT-ID))]
    (try
      (reset-peer local)
      (let [n-id (with-peer-graph local (make-node))
            con (get-connection (connection-manager) (plasma-url "localhost" port))
            n-chan (peer-node-event-channel con n-id)
            e-chan (peer-edge-event-channel con n-id :test)]
        (Thread/sleep 50)
        (with-peer-graph local
          (dotimes [i 10]
            (assoc-node n-id :val i))
          (dotimes [i 10]
            (make-edge n-id (make-node) {:label :test :val i})))
        (let [r1 (lamina/channel-seq n-chan 300)
              r2 (lamina/channel-seq e-chan 300)]
          (is (= 10 (count r1) (count r2)))
          (is (= (range 10)
                 (map (comp :val :props) r1)))
          (is (= (range 10)
                 (map (comp :val :props) r2)))))
      (finally
        (close local)))))

(comment
(def p (peer {:port 1234}))
(def con (get-connection (connection-manager) (plasma-url "localhost" 1234)))
(query p (q/path [:net]))
(query con (q/path [:net]))
(query-channel con (q/path [:net]))
  )
