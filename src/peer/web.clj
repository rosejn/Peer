(ns plasma.web
  (:use [plasma graph config util]
        [aleph formats http tcp]
        [ring.middleware file file-info]
        [clojure.contrib json]
        [clojure stacktrace])
  (:require [logjam.core :as log]
            [lamina.core :as lamina]))

; Remember: must be set in javascript client also.
(def WEB-PORT 4242)

(defn- web-rpc-handler
  [p req]
  (log/to :web "web-rpc-handler: " req)
  (let [res
        (case (:method req)
          "query"
          (with-peer-graph p
            (load-string
              (str
                "(require 'plasma.web)
                 (in-ns 'plasma.web)
                " (first (:params req))))))]
    {:result res
     :error nil
     :id (:id req)}))

(defn- request-handler
  [p ch msg]
  (when msg
    (log/to :web "\nMsg: " msg)
    (try
      (let [request (read-json msg true)
            _ (log/to :web "request: " request)
            res (web-rpc-handler p request)]
        (log/to :web "Result: " res)
        (lamina/enqueue ch (json-str res)))
      (catch Exception e
        (log/to :web "Request Exception: " e)
        (log/to :web "Trace: " (with-out-str (print-stack-trace e)))))))

(defn- dispatch-synchronous
  [request]
  (let [{:keys [request-method query-string uri]} request]
    (comment if (= uri "/")
      (home-view request)
      nil)))

(def sync-app
  (-> dispatch-synchronous
    (wrap-file "public")
    (wrap-file-info)))

(defn- server [p ch request]
  (log/to :web "client connect: " (str request))
  (if (:websocket request)
    (lamina/receive-all ch (partial request-handler p ch))
    (if-let [sync-response (sync-app request)]
      (lamina/enqueue ch sync-response)
      (lamina/enqueue ch {:status 404 :body "Page Not Found"}))))

(defn web-interface
  "Start a web interface for the give peer.
  To specify a custom port pass it in an options map:
    {:port 1234}
  "
  ([p] (web-interface p {:port WEB-PORT :websocket true}))
  ([p options]
   (start-http-server (partial server p) options)))

(comment
(def SPF "<cross-domain-policy><allow-access-from domain='*' to-ports='*'/></cross-domain-policy>\n\n\0")

(defn spf-handler [channel connection-info]
  (receive-all channel (fn [req]
			 (if (= "<policy-file-request/>\0"
				(byte-buffer->string req))
			   (let [f (string->byte-buffer SPF)]
			     (enqueue-and-close channel f))
			   (println (byte-buffer->string req))))))

(defn start-policy-server []
  (start-tcp-server spf-handler
                    {:port 843}))
)
