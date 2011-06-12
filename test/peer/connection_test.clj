(ns plasma.net.connection-test
  (:use clojure.test
        [plasma config util api]
        [plasma.net url connection rpc])
  (:require [logjam.core :as log]
            [lamina.core :as lamina]))

;(log/file :con "con.log")
;(log/repl :con)

(defrecord MockConnection
  [url]
  IConnection
  (request [c m p])
  (request-channel [_])
  (send-event [c id p])
  (event-channel [c])
  (event-channel [c id])
  (stream [c m p])
  (stream-channel [c])

  IClosable
  (close [_]))

(deftest connection-cache-test
  (let [manager (connection-manager)]
    (try
      (dotimes [i 300]
        (refresh-connection manager (MockConnection.
                                    (url "plasma" "plasma.org" i)))
        (is (<= (connection-count manager)
                (config :connection-cache-limit))))
      (finally
        (clear-connections manager)))))

(defn rpc-test
  [proto port]
  (let [manager (connection-manager)
        listener (connection-listener manager proto port)]
    (try
      (on-connect listener
                  (fn [con]
                    (log/to :con "new connection: " con)
                    (let [requests (request-channel con)]
                      (lamina/receive-all requests
                                          (fn [[ch req]]
                                            (log/to :con "got request: " req)
                                            (let [val (* 2 (first (:params req)))
                                                  res (rpc-response req val)]
                                              (lamina/enqueue ch res)))))))

      (let [client (get-connection manager (url proto "localhost" port))]
        (dotimes [i 20]
          (let [res-chan (request client 'foo [i])
                res (lamina/wait-for-result res-chan 100)]
            (is (= (* 2 i) res))))
        (is (zero? (count (:chan client))))
        (close client))
      (finally
        (close listener)
        (clear-connections manager)))))

(deftest connection-rpc-test
  (rpc-test "plasma" 1234)
  (rpc-test "uplasma" 1234))

(defn event-test
  [proto port]
  (let [manager (connection-manager)
        listener (connection-listener manager proto port)]
    (try
      (let [events (atom [])]
        (on-connect listener
          (fn [con]
            (lamina/receive-all (event-channel con)
              (fn [event]
                (when event
                  (swap! events conj (first (:params event))))))))
        (let [client (get-connection manager (url proto "localhost" port))]
          (dotimes [i 20]
            (send-event client 'foo [i :a :b :c]))
          (close client))
        (Thread/sleep 100)
        (is (= @events (range 20))))
      (finally
        (close listener)
        (clear-connections manager)))))

(deftest connection-event-test
  (event-test "plasma" 1234)
  (event-test "uplasma" 1234))

(defn stream-test
  [proto port]
  (let [manager (connection-manager)
        listener (connection-listener manager proto port)]
    (try
      (on-connect listener
        (fn [con]
          (lamina/receive-all (stream-channel con)
            (fn [[s-chan msg]]
              (lamina/enqueue s-chan (inc (first (:params msg))))
              (lamina/receive-all s-chan
                                  (fn [v]
                                    (lamina/enqueue s-chan (inc v))))))))

        (let [client (get-connection manager (url proto "localhost" port))
              s-chan (stream client 'foo [1])
              res (atom nil)]
          (lamina/receive s-chan #(lamina/enqueue s-chan (inc %)))
          (Thread/sleep 100)
          (lamina/receive s-chan #(lamina/enqueue s-chan (inc %)))
          (Thread/sleep 100)
          (lamina/receive s-chan #(reset! res %))
          (Thread/sleep 100)
          (is (= 6 @res))
          (close client))
      (finally
        (close listener)
        (clear-connections manager)))))

(deftest connection-stream-test
  (stream-test "plasma" 1234)
  (stream-test "uplasma" 1234))

;(use 'aleph.object)
;(use 'lamina.core)
;(def log (atom []))
;(def s (start-object-server
;         (fn [ch _] (receive-all ch #(swap! log conj %)))
;         {:port 1234}))
;
;(def c (wait-for-result (object-client {:host "localhost"
;                                               :port 1234})))
;(enqueue c "testing")
;@log
;
;(defn bad-stuff
;  []
;  (def c (wait-for-result (object-client {:host "localhost"
;                                          :port 1234})))
;  (enqueue c "testing")
;  (close c))
;
;(dotimes [i 100] (bad-stuff))
