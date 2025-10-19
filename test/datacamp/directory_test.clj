(ns datacamp.directory-test
  "Tests for directory-based backup operations"
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.directory :as dir]
            [datacamp.metadata :as meta]
            [datacamp.test-helpers :refer :all]))

(deftest test-basic-directory-backup
  (testing "Basic directory backup with memory backend"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-db"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 10 :post-count 20)
              result (backup/backup-to-directory conn
                                                 {:path test-dir}
                                                 :database-id "test-db")]

          (assert-backup-successful result)
          (is (some? (:path result)) "Should have backup path")
          (assert-backup-valid (:path result))

          ;; Verify backup contents
          (let [manifest (meta/read-edn-from-file
                         (str (:path result) "/manifest.edn"))]
            (is (= :full (:backup/type manifest)))
            (is (true? (:backup/completed manifest)))
            (is (pos? (:stats/datom-count manifest)))
            (is (pos? (:stats/chunk-count manifest)))))))))

(deftest test-directory-backup-with-file-backend
  (testing "Directory backup with file backend"
    (with-test-dir test-dir
      (with-test-dir db-dir
        (let [config {:store {:backend :file :path db-dir}}
              conn (create-test-db config)
              _ (populate-test-db conn :user-count 25 :post-count 50)
              result (backup/backup-to-directory conn
                                                {:path test-dir}
                                                :database-id "file-test")]

          (assert-backup-successful result)
          (assert-backup-valid (:path result))

          ;; Verify datom count matches
          (let [manifest (meta/read-edn-from-file
                         (str (:path result) "/manifest.edn"))
                actual-datoms (count (d/datoms @conn :eavt))]
            (is (= actual-datoms (:stats/datom-count manifest)))))))))

(deftest test-list-directory-backups
  (testing "Listing backups in directory"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-db"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 5 :post-count 10)

              ;; Create multiple backups
              result1 (backup/backup-to-directory conn {:path test-dir}
                                                 :database-id "multi-test")
              result2 (backup/backup-to-directory conn {:path test-dir}
                                                 :database-id "multi-test")
              result3 (backup/backup-to-directory conn {:path test-dir}
                                                 :database-id "multi-test")

              ;; List backups
              backups (backup/list-backups-in-directory {:path test-dir}
                                                       "multi-test")]

          (is (= 3 (count backups)) "Should have 3 backups")
          (is (every? :completed? backups) "All backups should be complete")
          (is (every? #(= :full (:type %)) backups) "All should be full backups")
          (is (= #{(:backup-id result1) (:backup-id result2) (:backup-id result3)}
                 (set (map :backup-id backups)))
              "Should find all backup IDs"))))))

(deftest test-verify-directory-backup
  (testing "Verifying directory backup integrity"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-db"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 10 :post-count 20)
              result (backup/backup-to-directory conn {:path test-dir}
                                                :database-id "verify-test")

              ;; Verify backup
              verification (backup/verify-backup-in-directory
                           {:path test-dir}
                           (:backup-id result)
                           :database-id "verify-test")]

          (is (true? (:success verification)) "Verification should succeed")
          (is (true? (:all-chunks-present verification)) "All chunks should be present")
          (is (pos? (:chunk-count verification)) "Should have chunks"))))))

(deftest test-verify-corrupted-backup
  (testing "Detecting corrupted/incomplete backups"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-db"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 10 :post-count 20)
              result (backup/backup-to-directory conn {:path test-dir}
                                                :database-id "corrupt-test")

              ;; Delete a chunk to simulate corruption
              chunks-dir (str (:path result) "/chunks")
              chunk-files (dir/list-files chunks-dir :pattern #"\.fressian\.gz$")
              _ (when (seq chunk-files)
                  (dir/delete-file (:path (first chunk-files))))

              ;; Verify should fail
              verification (backup/verify-backup-in-directory
                           {:path test-dir}
                           (:backup-id result)
                           :database-id "corrupt-test")]

          (is (false? (:success verification)) "Verification should fail")
          (is (false? (:all-chunks-present verification)) "Chunks should be missing")
          (is (seq (:missing-chunks verification)) "Should report missing chunks"))))))

(deftest test-cleanup-incomplete-backups
  (testing "Cleaning up incomplete backups"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-db"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 5 :post-count 10)

              ;; Create a complete backup
              complete-result (backup/backup-to-directory conn {:path test-dir}
                                                         :database-id "cleanup-test")

              ;; Create an incomplete backup (simulate by not marking complete)
              incomplete-id (str (guaranteed-unique-uuid))
              incomplete-path (dir/get-backup-path test-dir "cleanup-test" incomplete-id)
              _ (dir/ensure-directory incomplete-path)
              _ (meta/write-edn-to-file
                 (str incomplete-path "/manifest.edn")
                 {:backup/id incomplete-id
                  :backup/type :full
                  :backup/created-at (java.util.Date. (- (System/currentTimeMillis)
                                                        (* 48 60 60 1000))) ; 48h ago
                  :backup/completed false
                  :stats/datom-count 0})

              ;; Cleanup old incomplete backups
              cleanup-result (backup/cleanup-incomplete-in-directory
                             {:path test-dir}
                             "cleanup-test"
                             :older-than-hours 24)]

          (is (= 1 (:cleaned-count cleanup-result)) "Should clean 1 incomplete backup")
          (is (= [incomplete-id] (map str (:backup-ids cleanup-result))))

          ;; Verify incomplete backup is gone
          (is (not (dir/file-exists? incomplete-path)) "Incomplete backup should be deleted")

          ;; Verify complete backup still exists
          (is (dir/file-exists? (:path complete-result)) "Complete backup should remain"))))))

(deftest test-backup-with-custom-chunk-size
  (testing "Backup with custom chunk size"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-db"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 50 :post-count 100)

              ;; Very small chunk size to force multiple chunks
              result (backup/backup-to-directory conn
                                                {:path test-dir}
                                                :database-id "chunk-test"
                                                :chunk-size (* 10 1024))] ; 10KB - small enough to split data

          (assert-backup-successful result)
          (is (> (:chunk-count result) 1) "Should create multiple chunks with small size")

          ;; Verify chunks exist
          (let [chunk-count (count-backup-chunks (:path result))]
            (is (= (:chunk-count result) chunk-count) "Chunk count should match")))))))

(deftest test-backup-empty-database
  (testing "Backup of empty database"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-db"}}
        (let [conn (d/connect config)
              result (backup/backup-to-directory conn {:path test-dir}
                                                :database-id "empty-test")]

          (assert-backup-successful result)
          ;; Empty DB will still have schema datoms
          (is (pos? (:datom-count result)) "Should have schema datoms"))))))

(deftest test-backup-large-values
  (testing "Backup with large string values"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-db"}}
        (let [conn (d/connect config)
              large-content (apply str (repeat 10000 "Large content test. "))
              _ (d/transact conn [{:post/title "Large Post"
                                  :post/content large-content
                                  :post/created-at (java.util.Date.)}])

              result (backup/backup-to-directory conn {:path test-dir}
                                                :database-id "large-test")]

          (assert-backup-successful result)
          (assert-backup-valid (:path result)))))))

(deftest test-concurrent-backups
  (testing "Multiple concurrent backups to different databases"
    (with-test-dir test-dir
      (with-test-db config1 {:store {:backend :mem :id "test-db-1"}}
        (with-test-db config2 {:store {:backend :mem :id "test-db-2"}}
          (let [conn1 (d/connect config1)
                conn2 (d/connect config2)
                _ (populate-test-db conn1 :user-count 10 :post-count 20)
                _ (populate-test-db conn2 :user-count 15 :post-count 30)

                ;; Create backups concurrently
                result1 (future (backup/backup-to-directory conn1 {:path test-dir}
                                                           :database-id "concurrent-1"))
                result2 (future (backup/backup-to-directory conn2 {:path test-dir}
                                                           :database-id "concurrent-2"))

                r1 @result1
                r2 @result2]

            (assert-backup-successful r1)
            (assert-backup-successful r2)
            (is (not= (:backup-id r1) (:backup-id r2)) "Should have different IDs")))))))

(deftest test-backup-performance
  (testing "Backup performance with medium dataset"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test-db"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 100 :post-count 500)

              result (assert-performance
                     "Medium backup (100 users, 500 posts)"
                     #(backup/backup-to-directory conn {:path test-dir}
                                                 :database-id "perf-test")
                     10000)] ; Should complete within 10 seconds

          (assert-backup-successful result))))))

(run-tests)
