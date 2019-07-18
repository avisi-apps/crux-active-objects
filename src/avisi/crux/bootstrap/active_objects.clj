(ns avisi.crux.bootstrap.active-objects
  (:require [crux.db :as db]
            [crux.tx :as tx]
            [crux.bootstrap :as b]
            [crux.lru :as lru]
            [avisi.crux.active-objects :as ao-tx-log]
            [clojure.core.async :as async]
            [crux.io :as cio]
            [clojure.tools.logging :as log])
  (:import
   [java.util Date]
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

(defn- event-log-consumer-main-loop [{:keys [indexer batch-size ^ActiveObjects ao running?]
                                      :or {batch-size 100}}]
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
                              (.limit 10000)
                              (.order "ID ASC")
                              (.where "ID >= ?" (into-array Object [start-offset])))
                   (reify EntityStreamCallback
                     (onRowRead [_ t]
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
                           (vreset! ended-offset (.getID entry)))))))
          (log/debug "Done streaming from event-log to-offset=" (or @ended-offset start-offset))
          (when (and (some? @end-time) (some? @ended-offset))
            (let [end-offset (highest-id ao)
                  next-offset (inc (long @ended-offset))
                  lag (- end-offset next-offset)
                  consumer-state {:crux.tx/event-log
                                  {:lag lag
                                   :next-offset next-offset
                                   :time @end-time}}]
              (log/debug "Event log consumer state:" (pr-str consumer-state))
              (db/store-index-meta indexer :crux.tx-log/consumer-state consumer-state)
              (when (pos? lag)
                (log/warn "Falling behind" ::event-log "at:" next-offset "end:" end-offset)
                (recur)))))))
    (catch Exception e
      (log/error e "Unexpected failure while indexing"))))

(defn start-event-log-consumer! ^Closeable [indexer ao input-ch]
  (log/info "Starting event-log-consumer")

  (when-not (db/read-index-meta indexer :crux.tx-log/consumer-state)
    (db/store-index-meta
     indexer
     :crux.tx-log/consumer-state {:crux.tx/event-log {:lag 0
                                                      :next-offset 0
                                                      :time nil}}))

  (let [stop-ch (async/chan 1)
        running? (atom true)]
    (async/go-loop []
      (let [[_ ch] (async/alts! [stop-ch input-ch (async/timeout 30000)])]
        (if (= ch stop-ch)
          (log/info "Stopped event listener")
          (do
            (log/debug "trigger event log consumer main loop")
            (async/<! (async/thread (event-log-consumer-main-loop {:indexer indexer
                                                                   :ao ao
                                                                   :running? running?})))
            (recur)))))
    (async/put! input-ch 1)
    (log/info "Started event-log-consumer")
    (reify Closeable
      (close [_]
        (reset! running? false)
        (async/>!! stop-ch :stop)))))

(defn start-ao-node ^ICruxAPI [{:keys [ao db-dir doc-cache-size] :as options
                                :or {doc-cache-size (:doc-cache-size b/default-options)}}]
  (log/debugf "Starting crux with db-dir=%s " db-dir)
  (let [input-ch (async/chan (async/sliding-buffer 1))
        kv-store (b/start-kv-store options)
        object-store (lru/->CachedObjectStore (lru/new-cache doc-cache-size)
                                              (b/start-object-store {:kv kv-store} options))
        tx-log (ao-tx-log/map->ActiveObjectsTxLog {:ao ao
                                                   :input-ch input-ch})
        indexer (tx/->KvIndexer kv-store tx-log object-store)
        event-log-consumer (start-event-log-consumer! indexer ao input-ch)]
    (b/map->CruxNode {:kv-store kv-store
                      :tx-log tx-log
                      :object-store object-store
                      :indexer indexer
                      :event-log-consumer event-log-consumer
                      :options options
                      :close-fn (fn []
                                  (doseq [c [tx-log event-log-consumer kv-store]]
                                    (cio/try-close c)))})))
