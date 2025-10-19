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

(deftest test-unique-constraint-preserved-through-backup-restore
  (testing "Unique constraints are preserved from backup regardless of target schema"
    (with-test-dir test-dir
      (let [;; Source schema with unique constraint
            unique-schema [{:db/ident :product/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :product/sku
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}]
            source-config {:store {:backend :mem :id "unique-preserved-source"}
                          :schema-flexibility :write}]

        ;; Create source database with unique constraint
        (d/create-database source-config)
        (let [source-conn (d/connect source-config)]
          (try
            (d/transact source-conn {:tx-data unique-schema})

            ;; Add products with unique SKUs
            (d/transact source-conn {:tx-data [{:product/name "Laptop" :product/sku "LAP-001"}
                                              {:product/name "Mouse" :product/sku "MSE-001"}]})

            (is (= 2 (count (d/q '[:find ?e :where [?e :product/name]] @source-conn)))
                "Should have 2 products in source")

            ;; Backup the data (schema includes unique constraint)
            (let [backup-result (backup/backup-to-directory source-conn {:path test-dir}
                                                           :database-id "unique-preserved")]
              (is (:success backup-result) "Backup should succeed")

              ;; Restore into empty target database
              ;; The backup's schema (with unique constraint) will be restored
              (let [target-config {:store {:backend :mem :id "unique-preserved-target"}
                                  :schema-flexibility :write}]
                (d/create-database target-config)
                (let [target-conn (d/connect target-config)]
                  (try
                    ;; Restore - schema with unique constraint will be included
                    (let [restore-result (backup/restore-from-directory
                                         target-conn {:path test-dir}
                                         (:backup-id backup-result)
                                         :database-id "unique-preserved")]
                      (is (:success restore-result) "Restore should succeed")

                      ;; Verify original data restored
                      (is (= 2 (count (d/q '[:find ?e :where [?e :product/name]] @target-conn)))
                          "Should have 2 products after restore")

                      ;; Verify the unique constraint from backup is now active
                      ;; Check schema attribute
                      (let [sku-attr (d/entity @target-conn :product/sku)]
                        (is (= :db.unique/identity (:db/unique sku-attr))
                            "Unique constraint should be preserved from backup")

                        (is (= :db.type/string (:db/valueType sku-attr))
                            "Value type should be preserved")

                        (is (= :db.cardinality/one (:db/cardinality sku-attr))
                            "Cardinality should be preserved")))

                    (finally
                      (d/release target-conn)
                      (d/delete-database target-config))))))

            (finally
              (d/release source-conn)
              (d/delete-database source-config))))))))

(deftest test-remove-unique-value-constraint
  (testing "Remove unique/value constraint by retransacting schema without it"
    (with-test-dir test-dir
      (let [;; Initial schema with unique/value constraint
            unique-schema [{:db/ident :product/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :product/category
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/value}  ; unique/value means values must be unique
                          {:db/ident :product/price
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]
            ;; Relaxed schema without unique constraint
            relaxed-schema [{:db/ident :product/category
                            :db/valueType :db.type/string
                            :db/cardinality :db.cardinality/one}]
            source-config {:store {:backend :mem :id "remove-unique-source"}
                          :schema-flexibility :write}]

        ;; Create source database with unique constraint
        (d/create-database source-config)
        (let [source-conn (d/connect source-config)]
          (try
            (d/transact source-conn {:tx-data unique-schema})

            ;; Add products with unique categories
            (d/transact source-conn {:tx-data [{:product/name "Gaming Laptop"
                                               :product/category "Electronics"
                                               :product/price 1500}
                                              {:product/name "Office Chair"
                                               :product/category "Furniture"
                                               :product/price 300}]})

            ;; Verify unique constraint is enforced
            (let [category-attr (d/entity @source-conn :product/category)]
              (is (= :db.unique/value (:db/unique category-attr))
                  "Should have unique/value constraint initially"))

            ;; Try to add duplicate category - should fail
            (is (thrown? Exception
                        (d/transact source-conn {:tx-data [{:product/name "Wireless Mouse"
                                                           :product/category "Electronics"  ; Duplicate!
                                                           :product/price 50}]}))
                "Should reject duplicate category with unique/value constraint")

            (is (= 2 (count (d/q '[:find ?e :where [?e :product/name]] @source-conn)))
                "Should still have only 2 products after rejected transaction")

            ;; EVOLVE: Remove unique constraint by retracting the :db/unique attribute
            (let [category-attr-id (:db/id (d/entity @source-conn :product/category))]
              (d/transact source-conn {:tx-data [[:db/retract category-attr-id :db/unique :db.unique/value]]}))

            ;; Verify unique constraint is gone
            (let [category-attr (d/entity @source-conn :product/category)]
              (is (nil? (:db/unique category-attr))
                  "Unique constraint should be removed after retract"))

            ;; Now we CAN add duplicate categories!
            (d/transact source-conn {:tx-data [{:product/name "Wireless Mouse"
                                               :product/category "Electronics"  ; Now allowed!
                                               :product/price 50}
                                              {:product/name "Gaming Monitor"
                                               :product/category "Electronics"  ; Another duplicate!
                                               :product/price 400}]})

            ;; Verify duplicates were added
            (let [electronics-products (d/q '[:find ?name
                                             :where
                                             [?e :product/category "Electronics"]
                                             [?e :product/name ?name]]
                                           @source-conn)]
              (is (= 3 (count electronics-products))
                  "Should have 3 Electronics products after removing unique constraint"))

            ;; Backup the evolved schema (without unique constraint)
            (let [backup-result (backup/backup-to-directory source-conn {:path test-dir}
                                                           :database-id "remove-unique")]
              (is (:success backup-result) "Backup should succeed")

              ;; Restore into fresh database
              (let [target-config {:store {:backend :mem :id "remove-unique-target"}
                                  :schema-flexibility :write}]
                (d/create-database target-config)
                (let [target-conn (d/connect target-config)]
                  (try
                    (let [restore-result (backup/restore-from-directory
                                         target-conn {:path test-dir}
                                         (:backup-id backup-result)
                                         :database-id "remove-unique")]
                      (is (:success restore-result) "Restore should succeed")

                      ;; Verify all 4 products restored
                      (is (= 4 (count (d/q '[:find ?e :where [?e :product/name]] @target-conn)))
                          "Should have 4 products after restore")

                      ;; Verify duplicate categories exist
                      (let [electronics-products (d/q '[:find ?name
                                                       :where
                                                       [?e :product/category "Electronics"]
                                                       [?e :product/name ?name]]
                                                     @target-conn)]
                        (is (= 3 (count electronics-products))
                            "Should have 3 Electronics products after restore")
                        (is (contains? (set (map first electronics-products)) "Gaming Laptop"))
                        (is (contains? (set (map first electronics-products)) "Wireless Mouse"))
                        (is (contains? (set (map first electronics-products)) "Gaming Monitor")))

                      ;; Verify unique constraint is NOT present in restored database
                      (let [category-attr (d/entity @target-conn :product/category)]
                        (is (nil? (:db/unique category-attr))
                            "Unique constraint should not be restored"))

                      ;; Verify we can still add more duplicates after restore
                      (d/transact target-conn {:tx-data [{:product/name "Budget Laptop"
                                                         :product/category "Electronics"
                                                         :product/price 800}]})

                      (is (= 4 (count (d/q '[:find ?e
                                             :where [?e :product/category "Electronics"]]
                                           @target-conn)))
                          "Should be able to add more duplicates after restore"))

                    (finally
                      (d/release target-conn)
                      (d/delete-database target-config))))))

            (finally
              (d/release source-conn)
              (d/delete-database source-config))))))))
