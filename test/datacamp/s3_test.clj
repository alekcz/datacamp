(ns datacamp.s3-test
  "S3 tests for S3 (LocalStack) backup and restore"
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.s3 :as s3]
            [datacamp.test-helpers :refer :all]))

;; Configuration for local LocalStack (see docker-compose.yml)
(def localstack-endpoint "http://localhost:4566")
(def test-bucket "datacamp-test")

(def s3-config
  {:bucket test-bucket
   :region "us-east-1"
   :endpoint localstack-endpoint
   ;; Use LocalStack credentials explicitly - don't fall back to AWS env vars
   :access-key-id "test"
   :secret-access-key "test123"})

(defn s3-available?
  []
  (try
    ;; Try to create bucket
    (let [client (s3/create-s3-client s3-config)]
      (s3/ensure-bucket client test-bucket)
      true)
    (catch Exception e
      (println "S3 (LocalStack) not available:" (.getMessage e))
      false)))

(defn cleanup-s3-test-data
  "Clean up test data from S3 bucket"
  [db-id]
  (try
    (let [client (s3/create-s3-client s3-config)
          prefix (str "datahike-backups/" db-id "/")
          objects (s3/list-objects client test-bucket prefix)]
      (doseq [obj objects]
        (s3/delete-object client test-bucket (:key obj))))
    (catch Exception e
      (println "Warning: Failed to clean up S3 test data:" (.getMessage e)))))

(deftest ^:s3 test-s3-backup-and-restore-roundtrip
  (when (s3-available?)
    (testing "Backup to S3 and restore roundtrip using LocalStack"
      (let [db-id (str "s3-it-" (guaranteed-unique-uuid))]
        ;; Clean up any existing test data for this db-id
        (cleanup-s3-test-data db-id)

        (with-test-db source-config {:store {:backend :mem :id "s3-source"}
                                     :schema-flexibility :write}
          (let [source-conn (d/connect source-config)
                source-datom-count (do
                                     ;; Populate source DB
                                     (populate-test-db source-conn :user-count 20 :post-count 40)
                                     (count (d/datoms @source-conn :eavt)))]

            ;; Backup to S3 (LocalStack)
            (let [result (backup/backup-to-s3 source-conn s3-config
                                              :database-id db-id)]
              (assert-backup-successful result)
              (is (= source-datom-count (:datom-count result))
                  "Backup should capture all datoms")

              ;; Restore into a fresh empty mem DB (no schema, it comes from backup)
              (with-empty-db target-config {:store {:backend :mem :id "s3-target"}
                                            :schema-flexibility :write}
                (let [target-conn (d/connect target-config)
                      restore-result (backup/restore-from-s3 target-conn s3-config (:backup-id result)
                                                              :database-id db-id)
                      target-datom-count (count (d/datoms @target-conn :eavt))]

                  ;; Verify restore succeeded
                  (is (:success restore-result) "Restore should succeed")

                  ;; Verify datom counts match (basic sanity check)
                  (is (= source-datom-count target-datom-count)
                      "Restored database should have same number of datoms as source")

                  ;; Verify we have the expected entities
                  (let [user-count (count (d/q '[:find ?e :where [?e :user/name]] @target-conn))
                        post-count (count (d/q '[:find ?e :where [?e :post/title]] @target-conn))]
                    (is (= 20 user-count) "Should have 20 users")
                    (is (= 40 post-count) "Should have 40 posts"))))

              ;; Clean up test data after test
              (cleanup-s3-test-data db-id))))))))
