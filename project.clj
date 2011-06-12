(defproject peer "0.1.0-SNAPSHOT"
  :description "A peer-to-peer networking toolkit."
  :dependencies [[clojure "1.2.0"]
                 [aleph "0.2.0-alpha1"]
                 [ring/ring-core "0.3.1"]
                 [org.bitlet/weupnp "0.1.2-SNAPSHOT"]
                 [logjam "0.1.0-SNAPSHOT"]]
  :dev-dependencies [[marginalia "0.5.1-SNAPSHOT"]]
  :tasks [marginalia.tasks])
