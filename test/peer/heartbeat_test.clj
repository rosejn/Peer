(ns peer.heartbeat-test
  (:use [plasma graph util api]
        [peer core url connection bootstrap heartbeat]
        test-utils
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query :as q]))

;(log/file [:peer :con] "peer.log")

(deftest hearbeat-failure-test
  (let [a (peer {:port 1234})
        b (peer {:port 1235})]
    (try
      (detect-failures a)
      (detect-failures b)
      (finally
        (close a)
        (close b)))))

