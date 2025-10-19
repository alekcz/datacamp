(ns datacamp.schema-evolution-test
  "Tests for schema evolution scenarios with backup and restore"
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.test-helpers :refer :all]))

(deftest test-cardinality-one-to-many-evolution
  (testing "Evolve attribute from cardinality/one to cardinality/many"
    (with-test-dir test-dir
      (let [initial-schema [{:db/ident :person/name
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}
                            {:db/ident :person/email
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one
                             :db/unique :db.unique/identity}
                            {:db/ident :person/hobby
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}]
            source-config {:store {:backend :mem :id "schema-evo-source"}
                          :schema-flexibility :write}]

        (d/create-database source-config)
        (let [source-conn (d/connect source-config)]
          (try
            (d/transact source-conn {:tx-data initial-schema})
            (d/transact source-conn {:tx-data [{:person/name "Alice"
                                                :person/email "alice@example.com"
                                                :person/hobby "reading"}]})

            ;; Evolve: hobby becomes cardinality/many
            (d/transact source-conn {:tx-data [{:db/ident :person/hobby
                                                :db/cardinality :db.cardinality/many}]})

            (let [alice-eid (ffirst (d/q '[:find ?e :where [?e :person/email "alice@example.com"]]
                                         @source-conn))]
              (d/transact source-conn {:tx-data [[:db/add alice-eid :person/hobby "painting"]
                                                [:db/add alice-eid :person/hobby "cooking"]]})

              (let [hobbies (d/q '[:find [?h ...] :in $ ?e :where [?e :person/hobby ?h]]
                                @source-conn alice-eid)]
                (is (= 3 (count hobbies)) "Should have 3 hobbies after evolution")))

            ;; Backup and restore
            (let [backup-result (backup/backup-to-directory source-conn {:path test-dir}
                                                           :database-id "schema-evo")]
              (is (:success backup-result))

              (let [target-config {:store {:backend :mem :id "schema-evo-target"}
                                  :schema-flexibility :write}]
                (d/create-database target-config)
                (let [target-conn (d/connect target-config)]
                  (try
                    (let [restore-result (backup/restore-from-directory
                                         target-conn {:path test-dir}
                                         (:backup-id backup-result)
                                         :database-id "schema-evo")]
                      (is (:success restore-result))

                      (let [alice-eid (ffirst (d/q '[:find ?e :where [?e :person/email "alice@example.com"]]
                                                   @target-conn))
                            hobbies (d/q '[:find [?h ...] :in $ ?e :where [?e :person/hobby ?h]]
                                        @target-conn alice-eid)]
                        (is (= 3 (count hobbies)) "Should have 3 hobbies after restore")
                        (is (contains? (set hobbies) "reading"))
                        (is (contains? (set hobbies) "painting"))
                        (is (contains? (set hobbies) "cooking"))))

                    (finally
                      (d/release target-conn)
                      (d/delete-database target-config))))))

            (finally
              (d/release source-conn)
              (d/delete-database source-config))))))))
