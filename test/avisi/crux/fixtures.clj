(ns avisi.crux.fixtures
  (:require [avisi.crux.bootstrap.active-objects :as b]
            [crux.io :as cio])
  (:import [net.java.ao EntityManager]
           [com.atlassian.activeobjects.external ActiveObjects]
           [com.atlassian.activeobjects.test TestActiveObjects]
           [avisi.crux.tx EventLogEntry]
           [net.java.ao.atlassian TablePrefix AtlassianFieldNameConverter AtlassianUniqueNameConverter AtlassianSequenceNameConverter AtlassianIndexNameConverter AtlassianTableNameConverter]
           [net.java.ao.builder EntityManagerBuilder]
           [crux.api ICruxAPI]))

(def ^:dynamic ^ICruxAPI *api* nil)

(defn with-ao-node [f]
  (let [h2-dir (str (cio/create-tmpdir "h2-db"))
        db-dir (str (cio/create-tmpdir "kv-store"))
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
      (with-open [standalone-node (b/start-ao-node {:ao ao
                                                    :kv-backend "crux.kv.memdb.MemKv"
                                                    :db-dir db-dir})]
        (binding [*api* standalone-node]
          (f)))
      (finally
        (cio/delete-dir db-dir)
        (cio/delete-dir h2-dir)))))
