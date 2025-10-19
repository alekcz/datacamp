(ns datacamp.redis-test
  "Tests for Redis backend backup operations"
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.test-helpers :refer :all]
            ;; Load Datahike Redis backend
            ;; konserve has redis so we skip this for now.
            ;; [datahike-redis.core]
            ))

;; NOTE: These tests require a running Redis instance
;; Connection settings match docker-compose.yml configuration

(def redis-config
  {:store {:backend :redis
           :uri "redis://localhost:6379"}})

(defn redis-available?
  "Check if Redis is available for testing"
  []
  (try
    (throw 
      (ex-info 
        "konserve-redis has a bug so datahike-redis won't work" 
        {:datahike-redis {:version "0.1.7"}
         :konserve-redis {:version "0.1.13"}}))
    (let [test-config (assoc redis-config
                            :store (assoc (:store redis-config)
                                        :id (str "connectivity-test-"
                                               (java.util.UUID/randomUUID))))]
      (d/create-database test-config)
      (d/delete-database test-config)
      true)
    (catch Exception e
      (println "Redis not available:" (.getMessage e))
      false)))

(deftest ^:redis test-redis-basic-backup
  (when (redis-available?)
    (println "\n=== Running: test-redis-basic-backup ===")
    (testing "Basic backup with Redis backend"
      (with-test-dir test-dir
        (let [db-id (str "redis-test-" (java.util.UUID/randomUUID))
              config (assoc redis-config
                           :store (assoc (:store redis-config) :id db-id))
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

(deftest ^:redis test-redis-memory-efficiency
  (when (redis-available?)
    (println "\n=== Running: test-redis-memory-efficiency ===")
    (testing "Redis backup with memory-efficient streaming"
      (with-test-dir test-dir
        (let [db-id (str "redis-memory-" (java.util.UUID/randomUUID))
              config (assoc redis-config
                           :store (assoc (:store redis-config) :id db-id))
              conn (create-test-db config)]
          (try
            ;; Redis keeps everything in memory, so test with moderate dataset
            (populate-test-db conn :user-count 100 :post-count 300)

            (let [result (backup/backup-to-directory conn {:path test-dir}
                                                    :database-id db-id
                                                    :chunk-size (* 16 1024 1024))]
              (assert-backup-successful result)
              (assert-backup-valid (:path result))

              ;; Should create multiple chunks for streaming
              (is (>= (:chunk-count result) 1) "Should have at least one chunk"))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:redis test-redis-fast-operations
  (when (redis-available?)
    (println "\n=== Running: test-redis-fast-operations ===")
    (testing "Redis backup speed (in-memory backend)"
      (with-test-dir test-dir
        (let [db-id (str "redis-fast-" (java.util.UUID/randomUUID))
              config (assoc redis-config
                           :store (assoc (:store redis-config) :id db-id))
              conn (create-test-db config)]
          (try
            (populate-test-db conn :user-count 50 :post-count 150)

            ;; Redis backups should be fast due to in-memory nature
            (let [result (assert-performance
                         "Redis backup (50 users, 150 posts)"
                         #(backup/backup-to-directory conn {:path test-dir}
                                                     :database-id db-id)
                         5000)] ; Should be faster than disk-based backends

              (assert-backup-successful result))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:redis test-redis-persistence
  (when (redis-available?)
    (println "\n=== Running: test-redis-persistence ===")
    (testing "Redis data persistence through backup"
      (with-test-dir test-dir
        (let [db-id (str "redis-persist-" (java.util.UUID/randomUUID))
              config (assoc redis-config
                           :store (assoc (:store redis-config) :id db-id))
              conn (create-test-db config)]
          (try
            ;; Add data
            (d/transact conn [{:user/name "Redis User"
                              :user/email "redis@example.com"
                              :user/age 30}])

            ;; Backup
            (let [result (backup/backup-to-directory conn {:path test-dir}
                                                    :database-id db-id)]
              (assert-backup-successful result)

              ;; Verify backup preserves data correctly
              (let [manifest (datacamp.metadata/read-edn-from-file
                             (str (:path result) "/manifest.edn"))]
                (is (pos? (:stats/datom-count manifest))
                    "Should contain persisted datoms")))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:redis test-redis-concurrent-access
  (when (redis-available?)
    (println "\n=== Running: test-redis-concurrent-access ===")
    (testing "Redis concurrent backup operations"
      (with-test-dir test-dir
        (let [db-id-1 (str "redis-conc-1-" (java.util.UUID/randomUUID))
              db-id-2 (str "redis-conc-2-" (java.util.UUID/randomUUID))
              config-1 (assoc redis-config
                             :store (assoc (:store redis-config) :id db-id-1))
              config-2 (assoc redis-config
                             :store (assoc (:store redis-config) :id db-id-2))
              conn-1 (create-test-db config-1)
              conn-2 (create-test-db config-2)]
          (try
            (populate-test-db conn-1 :user-count 20 :post-count 40)
            (populate-test-db conn-2 :user-count 25 :post-count 50)

            ;; Concurrent backups
            (let [result-1 (future (backup/backup-to-directory conn-1 {:path test-dir}
                                                              :database-id db-id-1))
                  result-2 (future (backup/backup-to-directory conn-2 {:path test-dir}
                                                              :database-id db-id-2))
                  r1 @result-1
                  r2 @result-2]

              (assert-backup-successful r1)
              (assert-backup-successful r2)
              (is (not= (:backup-id r1) (:backup-id r2))
                  "Should have different backup IDs"))

            (finally
              (cleanup-test-db config-1)
              (cleanup-test-db config-2))))))))

(deftest ^:redis test-redis-key-expiration
  (when (redis-available?)
    (println "\n=== Running: test-redis-key-expiration ===")
    (testing "Redis backup with non-expiring keys"
      (with-test-dir test-dir
        (let [db-id (str "redis-expire-" (java.util.UUID/randomUUID))
              config (assoc redis-config
                           :store (assoc (:store redis-config) :id db-id))
              conn (create-test-db config)]
          (try
            (populate-test-db conn :user-count 10 :post-count 20)

            ;; Backup immediately (Datahike keys shouldn't expire)
            (let [result-1 (backup/backup-to-directory conn {:path test-dir}
                                                      :database-id (str db-id "-1"))]
              (assert-backup-successful result-1)

              ;; Wait a bit and backup again - data should still be there
              (Thread/sleep 1000)

              (let [result-2 (backup/backup-to-directory conn {:path test-dir}
                                                        :database-id (str db-id "-2"))]
                (assert-backup-successful result-2)

                ;; Both backups should have same datom count
                (is (= (:datom-count result-1) (:datom-count result-2))
                    "Data should persist (no expiration)")))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:redis test-redis-data-types
  (when (redis-available?)
    (println "\n=== Running: test-redis-data-types ===")
    (testing "Redis backup with various data types"
      (with-test-dir test-dir
        (let [db-id (str "redis-types-" (java.util.UUID/randomUUID))
              config (assoc redis-config
                           :store (assoc (:store redis-config) :id db-id))
              conn (create-test-db config)]
          (try
            ;; Insert various data types
            (d/transact conn [{:user/name "Type Test"
                              :user/email "types@example.com"
                              :user/age 42
                              :user/tags ["tag1" "tag2" "tag3"]}
                             {:post/title "Rich Data"
                              :post/content "Testing nested structures"
                              :post/created-at (java.util.Date.)}])

            (let [result (backup/backup-to-directory conn {:path test-dir}
                                                    :database-id db-id)]
              (assert-backup-successful result)
              (assert-backup-valid (:path result)))

            (finally
              (cleanup-test-db config))))))))

(deftest ^:redis test-redis-cleanup
  (when (redis-available?)
    (println "\n=== Running: test-redis-cleanup ===")
    (testing "Redis cleanup after backup"
      (with-test-dir test-dir
        (let [db-id (str "redis-cleanup-" (java.util.UUID/randomUUID))
              config (assoc redis-config
                           :store (assoc (:store redis-config) :id db-id))
              conn (create-test-db config)]
          (try
            (populate-test-db conn :user-count 15 :post-count 30)

            (let [result (backup/backup-to-directory conn {:path test-dir}
                                                    :database-id db-id)]
              (assert-backup-successful result)

              ;; Original Redis database should still be accessible
              (is (pos? (count (d/datoms @conn :eavt)))
                  "Original database should remain intact"))

            (finally
              (cleanup-test-db config))))))))

;; Run tests
(comment
  ;; To run these tests, make sure Redis is running and configured:
  ;; lein test :only datacamp.redis-test
  (run-tests))
