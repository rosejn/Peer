(ns peer.util)

(defn uuid
  "Creates a random, immutable UUID object that is comparable using the '=' function."
  [] (str "UUID:" (. java.util.UUID randomUUID)))

(defn uuid? [s]
  (and (string? s)
       (= (seq "UUID:") (take 5 (seq s)))))

(defn trim-id
  "Returns a short version of a uuid."
  [id & [n]]
  (apply str (take (or n 4) (drop 5 id))))

(defn current-time []
  (System/currentTimeMillis))

(defn regexp?
  [obj]
  (= java.util.regex.Pattern (type obj)))

(defn map-fn
  [m key fn & args]
  (assoc m key (apply fn (get m key) args)))

(defn periodically
  "Executes a function every period milliseconds.  Returns a function that can
  be called to terminate the execution.  If true is passed as the argument to
  this function it will terminate immediately rather than waiting for the
  already scheduled tasks to complete."
  [period fun]
  (let [s (Executors/newSingleThreadScheduledExecutor)]
    (.scheduleAtFixedRate s fun (long 0) (long period) TimeUnit/MILLISECONDS)
    (fn [& [now?]]
      (if now?
        (.shutdownNow s)
        (.shutdown s)))))

(defn schedule
  "Schedule a function to run after ms milliseconds.  Returns a function that can
  be called to cancel the scheduled execution."
  [ms fun]
  (let [s (Executors/newSingleThreadScheduledExecutor)]
    (.schedule s fun (long ms) TimeUnit/MILLISECONDS)
    (fn []
        (.shutdownNow s))))

(defn wait-for
  [chan timeout]
  (lamina/wait-for-result chan timeout))

(defn channel-timeout
  "Closes the channel after timeout."
  [ch timeout]
  (schedule timeout
            (fn []
              (log/to :flow "channel-timeout closing...")
              (lamina/close ch))))

(defn await-promise
  "Read a promise waiting for timeout ms for the promise to be delivered.
  Raises an exception if a timeout occurs"
  ([prom timeout]
     (.get (future @prom) timeout TimeUnit/MILLISECONDS)))
