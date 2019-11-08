(ns avisi.crux.active-objects
  (:require [crux.db :as db]
            [clojure.spec.alpha :as s]
            [crux.codec :as c]
            [crux.tx :as tx]
            [ghostwheel.core :refer [>defn => | <- ?]]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [clojure.string :as str])
  (:import [com.atlassian.activeobjects.external ActiveObjects]
           [java.util Date Map]
           [java.io Closeable]
           [net.java.ao Query]
           [avisi.crux.tx EventLogEntry]))

(def batch-size 1000)

(s/def ::ao #(instance? ActiveObjects %))
(s/def ::event-log-entry #(instance? EventLogEntry %))
(s/def ::key (s/nilable (s/and string? #(<= (count %) 100))))
(s/def ::body any?)
(s/def ::existing-entries (s/nilable (s/coll-of ::event-log-entry)))

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

(>defn save-event-log-entry! ^EventLogEntry
       [^ActiveObjects ao topic k v existing-entries]
       [::ao #{::tx ::doc} ::key ::body ::existing-entries => ::event-log-entry]
       (log/debug "Save event log entry!" {:topic topic
                                           :k k
                                           :v v})
  (let [body (clj->str v)
        t ^long (.getTime (now))
        payload ^Map (cond-> {"BODY" (clj->str v)
                              "TOPIC" (name topic)
                              "TIME" t}
                             k (assoc "KEY" k))]
    (when existing-entries
      (run!
       (fn [^EventLogEntry existing]
         (when (not= (.getBody ^EventLogEntry existing) body)
           (doto existing
             (.setBody body)
             (.save))))
       existing-entries))
    (when (or (nil? existing-entries)
              (tx/evicted-doc? v))
      (create! ao EventLogEntry payload))))

(defn event-log-entry->crux-tx [^EventLogEntry e]
  {:crux.tx/tx-time (Date. ^long (.getTime e))
   :crux.tx/tx-id (.getID e)
   :crux.api/tx-ops (str->clj (.getBody e))})

(defn tx-seq
  ([ao] (tx-seq ao 0))
  ([^ActiveObjects ao start-offset]
   (when-let [ret (seq (map
                        event-log-entry->crux-tx
                        (seq
                         (.find ao
                                EventLogEntry
                                (-> (Query/select "ID, TOPIC, TIME, BODY, KEY")
                                    (.limit batch-size)
                                    (.order "ID ASC")
                                    (.where "ID >= ? AND TOPIC = ?" (into-array Object [start-offset "tx"])))))))]
     (concat ret (lazy-seq (tx-seq ao (inc (:crux.tx/tx-id (last ret)))))))))

(defprotocol TxListener
  (add-listener! [this key f])
  (remove-listener! [this key]))

(defrecord ActiveObjectsTxLog [^ActiveObjects ao listeners]
  db/TxLog
  (submit-doc [this content-hash doc]
    (save-event-log-entry! ao ::doc (.toString content-hash) doc (get-existing-event-log-entries ao [content-hash])))
  (submit-tx [this tx-ops]
    (s/assert :crux.api/tx-ops tx-ops)
    (let [docs (crux.tx/tx-ops->docs tx-ops)
          docs-with-hashes (reduce (fn [m doc]
                                     (conj m [(str (c/new-id doc)) doc]))
                                   []
                                   docs)
          existing (group-by #(.getKey ^EventLogEntry %)
                             (get-existing-event-log-entries ao (mapv first docs-with-hashes)))]
      (run! (fn [[hash doc]]
              (save-event-log-entry! ao ::doc (.toString hash) doc (get existing hash)))
            docs-with-hashes))
    (let [m ^EventLogEntry (save-event-log-entry! ao ::tx nil (tx/tx-ops->tx-events tx-ops) nil)]
      (when m
        (delay {:crux.tx/tx-id (.getID m)
                :crux.tx/tx-time (Date. ^long (.getTime m))}))))
  (new-tx-log-context [this]
    (reify Closeable
      (close [this])))
  (tx-log [this tx-log-context from-tx-id]
    (tx-seq ao (or from-tx-id 0)))
  TxListener
  (add-listener! [this k f]
    (swap! listeners assoc k f)
    nil)
  (remove-listener! [this k]
    (swap! listeners dissoc k)
    nil)
  Closeable
  (close [this]
    (.flushAll ao)))
