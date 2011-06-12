(ns plasma.net.heartbeat-test
  (:use [plasma graph util api]
        [plasma.net peer connection bootstrap]
        test-utils
        clojure.test
        clojure.stacktrace)
  (:require [logjam.core :as log]
            [lamina.core :as lamina]
            [plasma.query.core :as q]))

;(log/file [:peer :heart :con] "peer.log")

(deftest test-failure-detection
  )
