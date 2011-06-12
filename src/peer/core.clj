(ns peer.core)

(defprotocol IPeer
  (peer-id [this] "Get the UUID for this peer."))

(defprotocol IConnection
  (request
    [con method params]
    "Send a request over this connection. Returns a result-channel
    that will receive a single result message, or an error.")

  (request-channel
    [con]
    "Returns a channel for incoming requests.  The channel will receive
    [ch request] pairs, and the rpc-response or rpc-error enqueued on
    ch will be sent as the response.")

  (send-event
    [con id params]
    "Send an event over this connection.")

  (event-channel
    [con] [con id]
    "Returns a channel for incoming events.  If an ID is passed only incoming
    events with this ID will be enqueued onto the returned channel.")

  (stream
    [con method params]
    "Open a stream channel on this connection.  Returns a channel that can be
    used bi-directionally.")

  (stream-channel
    [con]
    "Returns a channel for incoming stream requests.  The channel will receive
    [ch request] pairs, and the ch can be used as a named bi-direction stream.")

  (on-closed
    [con handler]
    "Register a handler to be called when this connection is closed."))


