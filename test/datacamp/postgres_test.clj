(ns datacamp.postgres-test
  "Tests for Postgres backend backup operations"
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.test-helpers :refer :all]
            ;; Load Datahike JDBC backend
            [datahike-jdbc.core]))

;; NOTE: These tests require a running PostgreSQL instance
;; Connection settings match docker-compose.yml configuration

(def postgres-config
  {:store {:backend :jdbc
           :dbtype "postgresql"
           :host "localhost"
           :port 5432
           :user "postgres"
           :password "postgres"
           :dbname "datahike_test"}
   :keep-history? true
   :schema-flexibility :write})

(defn postgres-available?
  "Check if PostgreSQL is available for testing"
  []
  (try
    (let [test-config (assoc postgres-config
                            :store (assoc (:store postgres-config)
                                        :id (str "connectivity-test-"
                                               (java.util.UUID/randomUUID))))]
      ;; Try to create or connect to test if it exists
      (if (d/database-exists? test-config)
        (d/delete-database test-config))
      (d/create-database test-config)
      (d/delete-database test-config)
      true)
    (catch Exception e
      (println "PostgreSQL not available:" (.getMessage e))
      false)))

(deftest ^:postgres test-postgres-basic-backup
  (when (postgres-available?)
    (println "\n=== Running: test-postgres-basic-backup ===")
    (testing "Basic backup with Postgres backend"
      (with-test-dir test-dir
        (let [db-id (str "pg-test-" (java.util.UUID/randomUUID))
              config (assoc postgres-config
                           :store (assoc (:store postgres-config) :id db-id))
              conn (create-test-db config)]
          (try
            (populate-test-db conn :user-count 20 :post-count 40)

            (let [result (backup/backup-to-directory conn {:path test-dir}
                                                    :database-id db-id)]
              (assert-backup-successful result)
              (assert-backup-valid (:path result))

              ;; Verify datom count
              (let [manifest (datacamp.metadata/read-edn-from-file
                             (str (:path result) "/manifest.edn"))
                    actual-datoms (count (d/datoms @conn :eavt))]
                (is (= actual-datoms (:stats/datom-count manifest))
                    "Datom count should match")))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:postgres test-postgres-large-dataset
  (when (postgres-available?)
    (println "\n=== Running: test-postgres-large-dataset ===")
    (testing "Backup large dataset from Postgres"
      (with-test-dir test-dir
        (let [db-id (str "pg-large-" (java.util.UUID/randomUUID))
              config (assoc postgres-config
                           :store (assoc (:store postgres-config) :id db-id))
              conn (create-test-db config)]
          (try
            (populate-test-db conn :user-count 200 :post-count 1000)

            (let [result (backup/backup-to-directory conn {:path test-dir}
                                                    :database-id db-id
                                                    :chunk-size (* 8 1024 1024))]
              (assert-backup-successful result)
              (is (>= (:chunk-count result) 1) "Should have at least one chunk")
              (assert-backup-valid (:path result)))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:postgres test-postgres-transactions-during-backup
  (when (postgres-available?)
    (println "\n=== Running: test-postgres-transactions-during-backup ===")
    (testing "Backup consistency with concurrent transactions"
      (with-test-dir test-dir
        (let [db-id (str "pg-concurrent-" (java.util.UUID/randomUUID))
              config (assoc postgres-config
                           :store (assoc (:store postgres-config) :id db-id))
              conn (create-test-db config)]
          (try
            (populate-test-db conn :user-count 50 :post-count 100)

            ;; Start backup
            (let [backup-future (future
                                 (backup/backup-to-directory conn {:path test-dir}
                                                            :database-id db-id))

                  ;; Add more data while backup is running
                  _ (Thread/sleep 100) ; Let backup start
                  _ (d/transact conn [{:user/name "Concurrent User"
                                      :user/email "concurrent@example.com"
                                      :user/age 25}])

                  result @backup-future]

              (assert-backup-successful result)
              (assert-backup-valid (:path result)))

              ;; Backup should be point-in-time consistent
              ;; (may or may not include concurrent transaction))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:postgres test-postgres-history-queries
  (when (postgres-available?)
    (println "\n=== Running: test-postgres-history-queries ===")
    (testing "Backup preserves transaction history"
      (with-test-dir test-dir
        (let [db-id (str "pg-history-" (java.util.UUID/randomUUID))
              config (assoc postgres-config
                           :store (assoc (:store postgres-config) :id db-id))
              conn (create-test-db config)]
          (try
            ;; Create data with history
            (d/transact conn [{:db/id -1
                              :user/name "Original Name"
                              :user/email "user@example.com"
                              :user/age 30}])

            (let [user-eid (:db/id (d/entity @conn [:user/email "user@example.com"]))]
              ;; Update the user
              (d/transact conn [[:db/add user-eid :user/name "Updated Name"]])
              (d/transact conn [[:db/add user-eid :user/age 31]]))

            ;; Backup
            (let [result (backup/backup-to-directory conn {:path test-dir}
                                                    :database-id db-id)]
              (assert-backup-successful result)
              (assert-backup-valid (:path result))

              ;; Verify history is included in backup
              (let [manifest (datacamp.metadata/read-edn-from-file
                             (str (:path result) "/manifest.edn"))]
                (is (pos? (:stats/datom-count manifest))
                    "Should include all historical datoms")))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:postgres test-postgres-connection-pool
  (when (postgres-available?)
    (println "\n=== Running: test-postgres-connection-pool ===")
    (testing "Multiple connections during backup"
      (with-test-dir test-dir
        (let [db-id (str "pg-pool-" (java.util.UUID/randomUUID))
              config (assoc postgres-config
                           :store (assoc (:store postgres-config) :id db-id))
              conn (create-test-db config)]
          (try
            (populate-test-db conn :user-count 30 :post-count 60)

            ;; Create multiple backups concurrently (testing connection handling)
            (let [results (doall
                          (map deref
                               (repeatedly 3
                                          #(future
                                            (backup/backup-to-directory
                                             conn {:path test-dir}
                                             :database-id (str db-id "-" (rand-int 1000)))))))]

              (is (= 3 (count results)) "Should complete all backups")
              (is (every? :success results) "All backups should succeed"))

            (finally
              (d/release conn)
              (cleanup-test-db config))))))))

(deftest ^:postgres test-postgres-backup-restore-roundtrip
  (when (postgres-available?)
    (println "\n=== Running: test-postgres-backup-restore-roundtrip ===")
    (testing "Backup and restore roundtrip with Postgres"
      (with-test-dir test-dir
        (let [source-id (str "pg-source-" (java.util.UUID/randomUUID))
              target-id (str "pg-target-" (java.util.UUID/randomUUID))
              source-config (assoc postgres-config
                                  :store (assoc (:store postgres-config) :id source-id))
              target-config (assoc postgres-config
                                  :store (assoc (:store postgres-config) :id target-id))
              source-conn (create-test-db source-config)]
          (try
            ;; Populate source database
            (populate-test-db source-conn :user-count 25 :post-count 50)

            ;; Backup
            (let [result (backup/backup-to-directory source-conn {:path test-dir}
                                                    :database-id source-id)]
              (assert-backup-successful result)

              ;; TODO: Restore functionality will be implemented in Phase 2
              ;; For now, just verify the backup is valid
              (assert-backup-valid (:path result)))

            (finally
              (d/release source-conn)
              (cleanup-test-db source-config)
              (cleanup-test-db target-config))))))))

;; Run tests
(comment
  ;; To run these tests, make sure PostgreSQL is running and configured:
  ;; lein test :only datacamp.postgres-test
  (run-tests))
