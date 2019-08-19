(ns avisi.crux.bootstrap.active-objects
  (:require [crux.db :as db]
            [crux.tx :as tx]
            [crux.bootstrap :as b]
            [crux.lru :as lru]
            [avisi.crux.active-objects :as ao-tx-log]
            [clojure.core.async :as async]
            [crux.io :as cio]
            [clojure.tools.logging :as log])
  (:import [java.util Date]
           [java.io Closeable]
           [crux.api ICruxAPI]
           [net.java.ao Query EntityStreamCallback]
           [com.atlassian.activeobjects.external ActiveObjects]
           [avisi.crux.tx EventLogEntry]))

(set! *warn-on-reflection* true)

(defn highest-id [^ActiveObjects ao]
  (first
   (map
    (fn [^EventLogEntry tx] (.getID tx))
    (.find ao ^Class EventLogEntry (-> (Query/select)
                                       (.limit 1)
                                       (.order "ID DESC"))))))

(def batch-limit 10000)

(defn- event-log-consumer-main-loop [{:keys [indexer ^ActiveObjects ao running? listeners]}]
  (try
    (loop []
      (when @running?
        (let [start-offset (get-in (db/read-index-meta indexer :crux.tx-log/consumer-state)
                                   [:crux.tx/event-log
                                    :next-offset])
              ended-offset (volatile! nil)
              end-time (volatile! nil)]
          (log/debug "Start streaming from event-log start-offset=" start-offset)
          (.stream ao
                   ^Class EventLogEntry
                   ^Query (-> (Query/select "ID, TOPIC, TIME, BODY, KEY")
                              (.limit batch-limit)
                              (.order "ID ASC")
                              (.where "ID >= ?" (into-array Object [start-offset])))
                   (reify EntityStreamCallback
                     (onRowRead [_ t]
                       (if-not @running?
                         (log/warn "Tried to index event-log entries while the event-log is already closed")
                         ;; Needed because hinting the onRowRead breaks Clojure finding the interface impl
                         (let [entry ^EventLogEntry t]
                           (let [tx-time (Date. ^long (.getTime entry))]
                             (log/debug "reading new entry in event log" {:body (.getBody entry)
                                                                          :key (.getKey entry)
                                                                          :id (.getID entry)
                                                                          :tx-time tx-time})
                             (case (.getTopic entry)
                               "doc" (db/index-doc indexer
                                                   (.getKey entry)
                                                   (ao-tx-log/str->clj (.getBody entry)))
                               "tx" (db/index-tx
                                     indexer
                                     (ao-tx-log/str->clj (.getBody entry))
                                     tx-time
                                     (.getID entry)))
                             (vreset! end-time tx-time)
                             (vreset! ended-offset (.getID entry))))))))
          (log/debug "Done streaming from event-log to-offset=" (or @ended-offset start-offset))
          (when (and @running? (some? @end-time) (some? @ended-offset))
            (let [end-offset (highest-id ao)
                  next-offset (inc (long @ended-offset))
                  lag (- end-offset next-offset)
                  consumer-state {:crux.tx/event-log
                                  {:lag lag
                                   :next-offset next-offset
                                   :time @end-time}}]
              (log/debug "Event log consumer state:" (pr-str consumer-state))
              (db/store-index-meta indexer :crux.tx-log/consumer-state consumer-state)
              (run!
               (fn [[k f]]
                 (try
                   (f consumer-state)
                   (catch Exception e
                     (log/error e "Calling listener failed" {:listener-key k}))))
               listeners)
              (when (and (pos? lag))
                (when (> lag batch-limit)
                 (log/warn "Falling behind" ::event-log "at:" next-offset "end:" end-offset))
                (recur)))))))
    (catch Exception e
      (log/error e "Unexpected failure while indexing"))))

(defn start-event-log-consumer! ^Closeable [indexer tx-log]
  (log/info "Starting event-log-consumer")

  (when-not (db/read-index-meta indexer :crux.tx-log/consumer-state)
    (db/store-index-meta
     indexer
     :crux.tx-log/consumer-state {:crux.tx/event-log {:lag 0
                                                      :next-offset 0
                                                      :time nil}}))

  (let [stop-ch (async/chan 1)
        input-ch  (:input-ch tx-log)
        running? (atom true)]
    (async/go-loop []
      (let [[_ ch] (async/alts! [stop-ch input-ch (async/timeout 30000)])]
        (if (= ch stop-ch)
          (log/info "Stopped event listener")
          (do
            (log/debug "trigger event log consumer main loop")
            (async/<! (async/thread (event-log-consumer-main-loop {:indexer indexer
                                                                   :listeners @(:listeners tx-log)
                                                                   :ao (:ao tx-log)
                                                                   :running? running?})))
            (recur)))))
    (async/put! input-ch 1)
    (log/info "Started event-log-consumer")
    (reify Closeable
      (close [_]
        (reset! running? false)
       ;; Put twice so it will block on the second stop message
        (async/close! input-ch)
        (async/>!! stop-ch :stop)
        (async/>!! stop-ch :stop)))))

(defn start-ao-node ^ICruxAPI [{:keys [ao db-dir doc-cache-size] :as options
                                :or {doc-cache-size (:doc-cache-size b/default-options)}}]
  (log/debugf "Starting crux with db-dir=%s " db-dir)
  (let [input-ch (async/chan (async/sliding-buffer 1))
        kv-store (b/start-kv-store options)
        object-store (lru/new-cached-object-store kv-store doc-cache-size)
        tx-log (ao-tx-log/map->ActiveObjectsTxLog {:ao ao
                                                   :input-ch input-ch
                                                   :listeners (atom {})})
        indexer (tx/->KvIndexer kv-store tx-log object-store)
        event-log-consumer (start-event-log-consumer! indexer tx-log)]
    (b/map->CruxNode {:kv-store kv-store
                      :tx-log tx-log
                      :object-store object-store
                      :indexer indexer
                      :event-log-consumer event-log-consumer
                      :options options
                      :close-fn (fn []
                                  (doseq [c [event-log-consumer tx-log kv-store object-store]]
                                    (cio/try-close c)))})))

