(ns avisi.crux.fixtures
  (:require [avisi.crux.active-objects :as active-objects]
            [crux.io :as cio]
            [crux.api :as crux])
  (:import [net.java.ao EntityManager]
           [com.atlassian.activeobjects.external ActiveObjects]
           [com.atlassian.activeobjects.test TestActiveObjects]
           [avisi.crux.tx EventLogEntry]
           [net.java.ao.atlassian TablePrefix AtlassianFieldNameConverter AtlassianUniqueNameConverter AtlassianSequenceNameConverter AtlassianIndexNameConverter AtlassianTableNameConverter]
           [net.java.ao.builder EntityManagerBuilder]
           [crux.api ICruxAPI]
           [java.time Duration]))

(def ^:dynamic ^ICruxAPI *api*)
(def ^:dynamic *opts* [])

(defn submit+await-tx
  ([tx-ops] (submit+await-tx *api* tx-ops))
  ([api tx-ops]
   (let [tx (crux/submit-tx api tx-ops)]
     (crux/await-tx api tx (Duration/ofSeconds 5))
     tx)))

(defn with-ao-node [f]
  (let [h2-dir (str (cio/create-tmpdir "h2-db"))
        manager ^EntityManager (->
                                (EntityManagerBuilder/url (str "jdbc:h2:" h2-dir))
                                (.username "")
                                (.password "")
                                (.none)
                                (.fieldNameConverter (AtlassianFieldNameConverter.))
                                (.uniqueNameConverter (AtlassianUniqueNameConverter.))
                                (.sequenceNameConverter (AtlassianSequenceNameConverter.))
                                (.indexNameConverter (AtlassianIndexNameConverter.))
                                (.tableNameConverter (AtlassianTableNameConverter.
                                                      (reify TablePrefix
                                                        (prepend [this s]
                                                          (str "AO_" s)))))
                                (.build))
        ao ^ActiveObjects (TestActiveObjects. manager)]
    (.migrate ^ActiveObjects ao (into-array Class [EventLogEntry]))
    (try
      (with-open [standalone-node (crux/start-node
                                    {:atlassian/active-objects {:crux/module `active-objects/->active-objects-config
                                                                :instance ao}
                                     :crux/tx-log {:crux/module `active-objects/->tx-log
                                                   :active-objects :atlassian/active-objects}
                                     :crux/document-store {:crux/module `active-objects/->document-store
                                                           :active-objects :atlassian/active-objects}})]
        (binding [*api* standalone-node]
          (f)))
      (finally
        ;; Otherwise we get errors about reading a deleted file sometimes
        (Thread/sleep 100)
        (cio/delete-dir h2-dir)))))
