(ns avisi.crux.bootstrap.active-objects-test
  (:require [clojure.test :as t]
            [avisi.crux.fixtures :refer [*api* with-ao-node]]
            [crux.codec :as c])
  (:import [java.time Duration]
           [java.util Date]
           [clojure.lang LazySeq]))

(t/use-fixtures :each with-ao-node)

(t/deftest test-document-bug-123
  (let [version-1-submitted-tx (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 1}]])]
    (t/is (true? (.hasSubmittedTxUpdatedEntity *api* version-1-submitted-tx :ivan))))

  (let [version-2-submitted-tx (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan" :version 2}]])]
    (t/is (true? (.hasSubmittedTxUpdatedEntity *api* version-2-submitted-tx :ivan))))

  (let [history (.history *api* :ivan)]
    (t/is (= 2 (count history)))
    (t/is (= [{:crux.db/id :ivan :name "Ivan" :version 2}
              {:crux.db/id :ivan :name "Ivan" :version 1}]
             (for [content-hash (map :crux.db/content-hash history)]
               (.document *api* content-hash))))))

(t/deftest test-can-use-api-to-access-crux
  (t/testing "status"
    (t/is (= {:crux.index/index-version 4}
             (dissoc (.status *api*)
                     :crux.zk/zk-active?
                     :crux.kv/kv-backend
                     :crux.kv/estimate-num-keys
                     :crux.tx-log/consumer-state :crux.kv/size
                     :crux.version/version :crux.version/revision))))

  (t/testing "empty db"
    (t/is (.db *api*)))

  (t/testing "syncing empty db"
    (t/is (nil? (.sync *api* (Duration/ofSeconds 10)))))

  (t/testing "transaction"
    (let [valid-time (Date.)
          {:keys [crux.tx/tx-time
                  crux.tx/tx-id]
           :as submitted-tx} (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan"} valid-time]])]
      (t/is (true? (.hasSubmittedTxUpdatedEntity *api* submitted-tx :ivan)))
      (t/is (= tx-time (.sync *api* (:crux.tx/tx-time submitted-tx) nil)))

      (t/testing "query"
        (t/is (= #{[:ivan]} (.q (.db *api*)
                                '{:find [e]
                                  :where [[e :name "Ivan"]]})))
        (t/is (= #{} (.q (.db *api* #inst "1999") '{:find  [e]
                                                    :where [[e :name "Ivan"]]})))

        (t/testing "query string"
          (t/is (= #{[:ivan]} (.q (.db *api*)
                                  "{:find [e] :where [[e :name \"Ivan\"]]}"))))

        (t/testing "query vector"
          (t/is (= #{[:ivan]} (.q (.db *api*) '[:find e
                                                :where [e :name "Ivan"]]))))

        (t/testing "malformed query"
          (t/is (thrown-with-msg? Exception
                                  #"(status 400|Spec assertion failed)"
                                  (.q (.db *api*) '{:find [e]}))))

        (t/testing "query with streaming result"
          (let [db (.db *api*)]
            (with-open [snapshot (.newSnapshot db)]
              (let [result (.q db snapshot '{:find [e]
                                             :where [[e :name "Ivan"]]})]
                (t/is (instance? LazySeq result))
                (t/is (not (realized? result)))
                (t/is (= '([:ivan]) result))
                (t/is (realized? result))))))

        (t/testing "query returning full results"
          (let [db (.db *api*)]
            (with-open [snapshot (.newSnapshot db)]
              (let [result (.q db snapshot '{:find [e]
                                             :where [[e :name "Ivan"]]
                                             :full-results? true})]
                (t/is (instance? LazySeq result))
                (t/is (not (realized? result)))
                (t/is (= '([{:crux.query/var e, :crux.query/value :ivan, :crux.query/doc {:crux.db/id :ivan, :name "Ivan"}}]) result))
                (t/is (realized? result))))))

        (t/testing "entity"
          (t/is (= {:crux.db/id :ivan :name "Ivan"} (.entity (.db *api*) :ivan)))
          (t/is (nil? (.entity (.db *api* #inst "1999") :ivan))))

        (t/testing "entity-tx, document and history"
          (let [entity-tx (.entityTx (.db *api*) :ivan)]
            (t/is (= (merge submitted-tx
                            {:crux.db/id (str (c/new-id :ivan))
                             :crux.db/content-hash (str (c/new-id {:crux.db/id :ivan :name "Ivan"}))
                             :crux.db/valid-time valid-time})
                     entity-tx))
            (t/is (= {:crux.db/id :ivan :name "Ivan"} (.document *api* (:crux.db/content-hash entity-tx))))
            (t/is (= [entity-tx] (.history *api* :ivan)))
            (t/is (= [entity-tx] (.historyRange *api* :ivan #inst "1990" #inst "1990" (:crux.tx/tx-time submitted-tx) (:crux.tx/tx-time submitted-tx))))

            (t/is (nil? (.document *api* (c/new-id :does-not-exist))))
            (t/is (nil? (.entityTx (.db *api* #inst "1999") :ivan)))))

        (t/testing "tx-log"
          (with-open [ctx (.newTxLogContext *api*)]
            (let [result (.txLog *api* ctx nil false)]
              (t/is (instance? LazySeq result))
              (t/is (not (realized? result)))
              (t/is (= [(assoc submitted-tx
                               :crux.api/tx-ops [[:crux.tx/put (c/new-id :ivan) (c/new-id {:crux.db/id :ivan :name "Ivan"}) valid-time]])]
                       result))
              (t/is (realized? result))))

          (t/testing "with documents"
            (with-open [ctx (.newTxLogContext *api*)]
              (let [result (.txLog *api* ctx nil true)]
                (t/is (instance? LazySeq result))
                (t/is (not (realized? result)))
                (t/is (= [(assoc submitted-tx
                                 :crux.api/tx-ops [[:crux.tx/put (c/new-id :ivan) {:crux.db/id :ivan :name "Ivan"} valid-time]])]
                         result))
                (t/is (realized? result)))))

          (t/testing "from tx id"
            (with-open [ctx (.newTxLogContext *api*)]
              (let [result (.txLog *api* ctx (inc tx-id) false)]
                (t/is (instance? LazySeq result))
                (t/is (not (realized? result)))
                (t/is (empty? result))
                (t/is (realized? result))))))

        (t/testing "statistics"
          (let [stats (.attributeStats *api*)]
            (t/is (= 1 (:name stats))))

          (t/testing "updated"
            (let [valid-time (Date.)
                  {:keys [crux.tx/tx-time
                          crux.tx/tx-id]
                   :as submitted-tx} (.submitTx *api* [[:crux.tx/put {:crux.db/id :ivan :name "Ivan2"} valid-time]])]
              (t/is (true? (.hasSubmittedTxUpdatedEntity *api* submitted-tx :ivan)))
              (t/is (= tx-time (.sync *api* (:crux.tx/tx-time submitted-tx) nil)))
              (t/is (= tx-time (.sync *api* nil))))

            (let [stats (.attributeStats *api*)]
              (t/is (= 2 (:name stats)))))

          (t/testing "reflect evicted documents"
            (let [valid-time (Date.)
                  {:keys [crux.tx/tx-time
                          crux.tx/tx-id]
                   :as submitted-tx} (.submitTx *api* [[:crux.tx/evict :ivan]])]
              (t/is (.sync *api* tx-time nil))

              ;; actual removal of the document happens asynchronously after
              ;; the transaction has been processed so waiting on the
              ;; submitted transaction time is not enough
              (while (.entity (.db *api*) :ivan)
                (assert (< (- (.getTime (Date.)) (.getTime valid-time)) 4000))
                (Thread/sleep 500))

              ;; Not sure what this is exactly
              (let [stats (.attributeStats *api*)]
                  (t/is (= 0 (:name stats)))))))))))
