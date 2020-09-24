(ns avisi.crux.active-objects-test
  (:require [avisi.crux.fixtures :refer [*api* with-ao-node] :as fix]
            [crux.codec :as c]
            [clojure.test :as t]
            [crux.db :as db]
            [crux.api :as api]
            [clojure.tools.logging :as log])
  (:import [java.time Duration]
           [crux.api ICruxAPI]))

(t/use-fixtures :each with-ao-node)

(t/deftest test-happy-path-jdbc-event-log
  (let [doc {:crux.db/id :origin-man :name "Adam"}
        submitted-tx (log/spy (.submitTx *api* [[:crux.tx/put doc]]))]
    (.awaitTx ^ICruxAPI *api* submitted-tx (Duration/ofSeconds 2))
    (t/is (.entity (.db *api*) :origin-man))
    (t/testing "Tx log"
      (with-open [tx-log-iterator (.openTxLog *api* 0 false)]
        (t/is (= [{:crux.tx/tx-id 2,
                   :crux.tx/tx-time (:crux.tx/tx-time submitted-tx)
                   :crux.tx.event/tx-events
                   [[:crux.tx/put
                     (c/new-id (:crux.db/id doc))
                     (c/new-id doc)]]}]
                (iterator-seq tx-log-iterator)))))))

(t/deftest test-docs-retention
  (let [doc-store (:document-store *api*)

        doc {:crux.db/id :some-id, :a :b}
        doc-hash (c/new-id doc)

        _ (fix/submit+await-tx [[:crux.tx/put doc]])

        docs (db/fetch-docs doc-store #{doc-hash})]

    (t/is (= 1 (count docs)))
    (t/is (= doc (get docs doc-hash)))

    (t/testing "Compaction"
      (db/submit-docs doc-store [[doc-hash :some-val]])
      (t/is (= :some-val
              (-> (db/fetch-docs doc-store #{doc-hash})
                (get doc-hash)))))

    (t/testing "Eviction"
      (db/submit-docs doc-store [[doc-hash {:crux.db/id :some-id, :crux.db/evicted? true}]])
      (t/is (nil? (-> (db/fetch-docs doc-store #{doc-hash})
                    (get doc-hash)))))

    (t/testing "Resurrect Document"
      (fix/submit+await-tx [[:crux.tx/put doc]])

      (t/is (= doc
              (-> (db/fetch-docs doc-store #{doc-hash})
                (get doc-hash)))))))

(t/deftest test-micro-bench
  (when (Boolean/parseBoolean (System/getenv "CRUX_JDBC_PERFORMANCE"))
    (let [n 1000
          last-tx (atom nil)]
      (time
        (dotimes [n n]
          (reset! last-tx (.submitTx *api* [[:crux.tx/put {:crux.db/id (keyword (str n))}]]))))

      (time
        (.awaitTx *api* last-tx nil))))
  (t/is true))

(t/deftest test-project-star-bug-1016
  (fix/submit+await-tx [[:crux.tx/put {:crux.db/id :put
                                       :crux.db/fn '(fn [ctx doc]
                                                      [[:crux.tx/put doc]])}]])
  (fix/submit+await-tx [[:crux.tx/fn :put {:crux.db/id :foo, :foo :bar}]])

  (let [db (api/db *api*)]

    (t/is (= #{[{:crux.db/id :foo, :foo :bar}]}
            (api/q db
              '{:find [(eql/project ?e [*])]
                :where [[?e :crux.db/id :foo]]})))

    (t/is (= {:crux.db/id :foo, :foo :bar}
            (api/entity db :foo)))

    (t/is (= #{[{:crux.db/id :foo, :foo :bar}]}
            (api/q db
              '{:find [(eql/project ?e [*])]
                :where [[?e :crux.db/id :foo]]})))))
