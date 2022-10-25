(ns avisi.crux.active-objects
  (:require [crux.db :as db]
            [clojure.spec.alpha :as s]
            [crux.system :as sys]
            [crux.codec :as c]
            [crux.tx :as tx]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [crux.lru :as lru]
            [crux.document-store :as ds]
            [crux.io :as cio])
  (:import [com.atlassian.activeobjects.external ActiveObjects]
           [java.util Date Map]
           [net.java.ao Query]
           [avisi.crux.tx EventLogEntry]
           [crux.api ICursor]
           [crux.codec Id]
           [java.io Closeable]
           [java.lang AutoCloseable]))

(s/def :atlassian.active-objects/instance #(instance? ActiveObjects %))

(defn ->active-objects-config
  {::sys/args {:instance :atlassian.active-objects/instance}}
  [{:keys [instance] :as opts}]
  instance)

;; Do not increase over 500 or oracle DB's will run into trouble with a limit on IN statements
(def batch-size 500)

(s/def ::ao #(instance? ActiveObjects %))
(s/def ::event-log-entry #(instance? EventLogEntry %))
(s/def ::key (s/nilable (s/and string? #(<= (count %) 100))))
(s/def ::body any?)
(s/def ::existing-entries (s/nilable (s/coll-of ::event-log-entry)))

(defn crux-ids->strs [tx-events]
  (mapv (fn [tx-event]
          (mapv
            (fn [x]
              (cond-> x
                (instance? Id x) str)) tx-event)) tx-events))

(defn- now ^Date []
  (Date.))

(defn clj->str [x]
  (pr-str x))

(defn str->clj [^String x]
  (try
    (edn/read-string x)
    (catch Exception e
      (throw (ex-info "Failed to parse string to edn" {:failed-string x} e)))))

(defn create! [^ActiveObjects ao ^Class clz fields]
  (.create ao ^Class clz ^Map fields))

(defn get-existing-event-log-entries [^ActiveObjects ao content-hashes]
  (when (seq content-hashes)
    (seq (.find ao ^Class EventLogEntry
           (-> (Query/select "ID, BODY, KEY")
             (.where (str "KEY IN (" (str/join ", " (repeat (count content-hashes) "?")) ")")
               (object-array content-hashes)))))))

(defn event-log-entry->crux-tx [^EventLogEntry e]
  {:crux.tx/tx-time (Date. ^long (.getTime e))
   :crux.tx/tx-id (.getID e)
   :crux.tx.event/tx-events
   (try
     (->> (str->clj (.getBody e))
          (mapv (fn [tx-event]
                  (mapv #(cond-> %
                                 (and (string? %) (c/hex-id? %)) (-> c/hex->id-buffer c/new-id)) tx-event))))
     (catch Exception error
       (log/error error "Failed to read event log entry" {:body (.getBody e)
                                                          :id (.getID e)})
       []))})

(defn tx-seq
  ([^ActiveObjects ao start-offset]
   (if-let [ret (seq (keep
                         event-log-entry->crux-tx
                         (seq
                           (.find ao
                             EventLogEntry
                             (-> (Query/select "ID, TOPIC, TIME, BODY, KEY")
                               (.limit batch-size)
                               (.order "ID ASC")
                               (.where "ID > ? AND TOPIC = ?" (into-array Object [start-offset "tx"])))))))]
     (concat ret (lazy-seq (tx-seq ao (inc (:crux.tx/tx-id (last ret))))))
     (concat))))

(defn get-docs-by-event-keys
  ([^ActiveObjects ao event-keys]
   (get-docs-by-event-keys ao event-keys true))
  ([^ActiveObjects ao event-keys include-compacted?]
   (when (seq event-keys)
     (seq (.find ao ^Class EventLogEntry
            (-> (Query/select "ID, BODY, KEY")
              (.where (cond-> (str "KEY IN (" (str/join ", " (repeat (count event-keys) "?")) ")")
                        (not include-compacted?) (str " AND (COMPACTED = 0 OR COMPACTED IS NULL)"))
                (object-array event-keys))))))))

(defn docs-by-event-key [^ActiveObjects ao k]
  (seq
    (.find ao ^Class EventLogEntry
      (-> (Query/select "ID")
        (.where "KEY = ? AND (COMPACTED = 0 OR COMPACTED IS NULL)" (into-array [k]))))))

(defn insert-event! [^ActiveObjects ao event-key v topic]
  (log/debug "Save event log entry!" {:topic topic
                                      :event-key event-key
                                      :v v})
  (let [t ^long (.getTime (now))
        payload ^Map (cond-> {"BODY" (clj->str v)
                              "TOPIC" (name topic)
                              "TIME" t
                              "COMPACTED" 0}
                       event-key (assoc "KEY" (str event-key)))]
    (create! ao EventLogEntry payload)))

(defn doc-exists? [^ActiveObjects ao k]
  (not-empty (docs-by-event-key ao k)))

(defn update-doc! [^ActiveObjects ao k doc]
  (->> (docs-by-event-key ao k)
    (run! (fn [^EventLogEntry entry]
            (doto entry
              (.setBody (clj->str doc))
              (.save ))))))

(defn evict-doc! [^ActiveObjects ao k tombstone]
  (->> (docs-by-event-key ao k)
    (run! (fn [^EventLogEntry entry]
            (doto entry
              (.setBody (clj->str tombstone))
              (.setCompacted 1)
              (.save))))))

(defn highest-id [^ActiveObjects ao]
  (first
    (map
      (fn [^EventLogEntry tx] (.getID tx))
      (.find ao ^Class EventLogEntry (-> (Query/select "ID")
                                       (.limit 1)
                                       (.order "ID DESC"))))))

(defrecord ActiveObjectsDocumentStore [^ActiveObjects ao]
  db/DocumentStore
  (submit-docs [this id-and-docs]
    (doseq [[id doc] id-and-docs
            :let [id (str id)]]
      (if (c/evicted-doc? doc)
        (do
          (insert-event! ao id doc "docs")
          (evict-doc! ao id doc))
        (if-not (doc-exists? ao id)
          (insert-event! ao id doc "docs")
          (update-doc! ao id doc)))))
  (fetch-docs [this ids]
    (->>
      (for [id-batch (partition-all batch-size ids)
            row  (get-docs-by-event-keys ao (map (comp str c/new-id) id-batch) false)]
        row)
      (keep (fn [^EventLogEntry entry]
              (try
                [(-> (.getKey entry) c/hex->id-buffer c/new-id)
                 (str->clj (.getBody entry))]
                (catch Exception e
                  (log/error e "Failed to read event log entry" {:body (.getBody entry)
                                                                 :id (.getID entry)})
                  nil))))
      (into {}))))

(defn ->document-store {::sys/deps {:active-objects :atlassian/active-objects}
                        ::sys/args {:doc-cache-size ds/doc-cache-size-opt}}
  [{:keys [doc-cache-size active-objects]}]
  (->> (->ActiveObjectsDocumentStore active-objects)
    (ds/->CachedDocumentStore (lru/new-cache doc-cache-size))))

(defrecord ActiveObjectsTxLog [^ActiveObjects ao ^Closeable tx-consumer]
  db/TxLog
  (submit-tx [this tx-events]
    (let [m ^EventLogEntry (insert-event! ao nil (crux-ids->strs tx-events) ::tx)]
      (when m
        (delay {:crux.tx/tx-id (.getID m)
                :crux.tx/tx-time (Date. ^long (.getTime m))}))))
  (open-tx-log ^ICursor [this after-tx-id]
    (cio/->cursor
      (fn [])
      (tx-seq ao (or after-tx-id 0))))
  (latest-submitted-tx [this]
    {:crux.tx/tx-id (highest-id ao)})
  Closeable
  (close [_]
    (cio/try-close tx-consumer)))

(defn ->ingest-only-tx-log {::sys/deps {:active-objects :atlassian/active-objects}}
  [{:keys [active-objects]}]
  (map->ActiveObjectsTxLog {:ao active-objects}))

(defn ->tx-log {::sys/deps (merge
                             (::sys/deps (meta #'tx/->polling-tx-consumer))
                             (::sys/deps (meta #'->ingest-only-tx-log)))
                ::sys/args (merge (::sys/args (meta #'tx/->polling-tx-consumer))
                                  (::sys/args (meta #'->ingest-only-tx-log)))}
  [opts]
  (let [tx-log (->ingest-only-tx-log opts)]
    (-> tx-log
      (assoc :tx-consumer (tx/->polling-tx-consumer opts
                            (fn [after-tx-id]
                              (db/open-tx-log tx-log after-tx-id)))))))
