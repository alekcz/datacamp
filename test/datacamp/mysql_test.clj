(ns datacamp.mysql-test
  "Tests for MySQL backend backup operations"
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.test-helpers :refer :all]
            ;; Load Datahike JDBC backend
            [datahike-jdbc.core]))

;; NOTE: These tests require a running MySQL instance
;; Connection settings match docker-compose.yml configuration

(def mysql-config
  {:store {:backend :jdbc
           :dbtype "mysql"
           :host "localhost"
           :port 3306
           :user "root"
           :password "password"
           :dbname "datahike_test"}
   :keep-history? true
   :schema-flexibility :write})

(defn mysql-available?
  "Check if MySQL is available for testing"
  []
  (try
    (let [test-config (assoc mysql-config
                            :store (assoc (:store mysql-config)
                                        :id (str "connectivity-test-"
                                               (java.util.UUID/randomUUID))))]
      ;; Try to create or connect to test if it exists
      (if (d/database-exists? test-config)
        (d/delete-database test-config))
      (d/create-database test-config)
      (d/delete-database test-config)
      true)
    (catch Exception e
      (println "MySQL not available:" (.getMessage e))
      false)))

(deftest ^:mysql test-mysql-basic-backup
  (when (mysql-available?)
    (testing "Basic backup with MySQL backend"
      (println "\n=== Running: test-mysql-basic-backup ===")
      (with-test-dir test-dir
        (let [db-id (str "mysql-test-" (java.util.UUID/randomUUID))
              config (assoc mysql-config
                           :store (assoc (:store mysql-config) :id db-id))
              conn (create-test-db config)]
          (try
            (println "  Populating test database (20 users, 40 posts)...")
            (populate-test-db conn :user-count 20 :post-count 40)

            (println "  Performing backup...")
            (let [result (backup/backup-to-directory conn {:path test-dir}
                                                    :database-id db-id)]
              (println "  Validating backup...")
              (assert-backup-successful result)
              (assert-backup-valid (:path result))

              ;; Verify datom count
              (let [manifest (datacamp.metadata/read-edn-from-file
                             (str (:path result) "/manifest.edn"))
                    actual-datoms (count (d/datoms @conn :eavt))]
                (is (= actual-datoms (:stats/datom-count manifest))
                    "Datom count should match")))
            (println "  ✓ Test completed successfully")

            (finally
              (cleanup-test-db config))))))))

(deftest ^:mysql test-mysql-large-dataset
  (when (mysql-available?)
    (testing "Backup large dataset from MySQL"
      (println "\n=== Running: test-mysql-large-dataset ===")
      (with-test-dir test-dir
        (let [db-id (str "mysql-large-" (java.util.UUID/randomUUID))
              config (assoc mysql-config
                           :store (assoc (:store mysql-config) :id db-id))
              conn (create-test-db config)]
          (try
            (populate-test-db conn :user-count 150 :post-count 750)

            (let [result (backup/backup-to-directory conn {:path test-dir}
                                                    :database-id db-id
                                                    :chunk-size (* 32 1024 1024))]
              (assert-backup-successful result)
              (is (>= (:chunk-count result) 1) "Should have chunks")
              (assert-backup-valid (:path result)))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:mysql test-mysql-utf8-support
  (when (mysql-available?)
    (println "\n=== Running: test-mysql-utf8-support ===")
    (testing "MySQL UTF-8 character support"
      (with-test-dir test-dir
        (let [db-id (str "mysql-utf8-" (java.util.UUID/randomUUID))
              config (assoc mysql-config
                           :store (assoc (:store mysql-config) :id db-id))
              conn (create-test-db config)]
          (try
            ;; Insert data with various UTF-8 characters
            (d/transact conn [{:user/name "José García"
                              :user/email "jose@example.com"
                              :user/age 30}
                             {:user/name "李明"
                              :user/email "li@example.com"
                              :user/age 25}
                             {:user/name "Müller"
                              :user/email "muller@example.com"
                              :user/age 35}
                             {:post/title "Emoji Test 😀 🎉 🚀"
                              :post/content "Testing emojis: 👍 ❤️ 🌟"
                              :post/created-at (java.util.Date.)}])

            (let [result (backup/backup-to-directory conn {:path test-dir}
                                                    :database-id db-id)]
              (assert-backup-successful result)
              (assert-backup-valid (:path result)))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:mysql test-mysql-transactions
  (when (mysql-available?)
    (println "\n=== Running: test-mysql-transactions ===")
    (testing "MySQL transaction handling during backup"
      (with-test-dir test-dir
        (let [db-id (str "mysql-tx-" (java.util.UUID/randomUUID))
              config (assoc mysql-config
                           :store (assoc (:store mysql-config) :id db-id))
              conn (create-test-db config)]
          (try
            (populate-test-db conn :user-count 30 :post-count 60)

            ;; Perform backup with concurrent writes
            (let [backup-future (future
                                 (backup/backup-to-directory conn {:path test-dir}
                                                            :database-id db-id))
                  _ (Thread/sleep 50)
                  _ (d/transact conn [{:user/name "Concurrent User"
                                      :user/email "concurrent@mysql.com"
                                      :user/age 28}])

                  result (deref backup-future 30000 {:success false :error "Timeout after 30 seconds"})]

              (when (:error result)
                (println "  ⚠ Test timeout or error:" (:error result)))
              (assert-backup-successful result)
              (assert-backup-valid (:path result)))

            (finally
              (try (d/release conn) (catch Exception _))
              (try (cleanup-test-db config) (catch Exception _)))))))))

(deftest ^:mysql test-mysql-performance
  (when (mysql-available?)
    (println "\n=== Running: test-mysql-performance ===")
    (testing "MySQL backup performance"
      (with-test-dir test-dir
        (let [db-id (str "mysql-perf-" (java.util.UUID/randomUUID))
              config (assoc mysql-config
                           :store (assoc (:store mysql-config) :id db-id))
              conn (create-test-db config)]
          (try
            (populate-test-db conn :user-count 100 :post-count 400)

            (let [result (assert-performance
                         "MySQL backup (100 users, 400 posts)"
                         #(backup/backup-to-directory conn {:path test-dir}
                                                     :database-id db-id)
                         15000)] ; 15 seconds max

              (assert-backup-successful result))

            (finally
              (cleanup-test-db config))))))))

;; Run tests
(comment
  ;; To run these tests, make sure MySQL is running and configured:
  ;; lein test :only datacamp.mysql-test
  (run-tests))
