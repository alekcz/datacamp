(ns datacamp.integration-test
  "Integration tests covering multiple backends and workflows"
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.test-helpers :refer :all]))

(deftest test-memory-to-directory-workflow
  (testing "Complete workflow: Memory DB -> Directory backup -> Verification"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "integration-test"}}
        (let [conn (d/connect config)]
          ;; Step 1: Populate database
          (populate-test-db conn :user-count 50 :post-count 100)
          (let [original-datom-count (count (d/datoms @conn :eavt))]

            ;; Step 2: Create backup
            (let [backup-result (backup/backup-to-directory
                                conn {:path test-dir}
                                :database-id "integration-test")]
              (is (:success backup-result) "Backup should succeed")

              ;; Step 3: Verify backup
              (let [verification (backup/verify-backup-in-directory
                                 {:path test-dir}
                                 (:backup-id backup-result)
                                 :database-id "integration-test")]
                (is (:all-chunks-present verification) "All chunks should be present"))

              ;; Step 4: List backups
              (let [backups (backup/list-backups-in-directory
                            {:path test-dir}
                            "integration-test")]
                (is (= 1 (count backups)) "Should have one backup")
                (is (= (:backup-id backup-result)
                       (:backup-id (first backups)))
                    "Should find the backup"))

              ;; Step 5: Verify manifest
              (let [manifest (datacamp.metadata/read-edn-from-file
                             (str (:path backup-result) "/manifest.edn"))]
                (is (= original-datom-count (:stats/datom-count manifest))
                    "Manifest should have correct datom count")))))))))

(deftest test-multiple-backends-same-backup-dir
  (testing "Multiple backends backing up to same directory"
    (with-test-dir test-dir
      (with-test-db mem-config {:store {:backend :mem :id "mem-db"}}
        (with-test-dir file-db-dir
          (let [file-config {:store {:backend :file :path file-db-dir}}
                mem-conn (d/connect mem-config)
                file-conn (create-test-db file-config)]

            ;; Populate both databases
            (populate-test-db mem-conn :user-count 20 :post-count 40)
            (populate-test-db file-conn :user-count 30 :post-count 60)

            ;; Backup both to same directory (different database IDs)
            (let [mem-result (backup/backup-to-directory
                             mem-conn {:path test-dir}
                             :database-id "mem-db")
                  file-result (backup/backup-to-directory
                              file-conn {:path test-dir}
                              :database-id "file-db")]

              (is (:success mem-result) "Memory backup should succeed")
              (is (:success file-result) "File backup should succeed")

              ;; Verify both backups exist and are independent
              (let [mem-backups (backup/list-backups-in-directory
                                {:path test-dir} "mem-db")
                    file-backups (backup/list-backups-in-directory
                                 {:path test-dir} "file-db")]
                (is (= 1 (count mem-backups)) "Should have one memory backup")
                (is (= 1 (count file-backups)) "Should have one file backup")
                (is (not= (:datom-count (first mem-backups))
                         (:datom-count (first file-backups)))
                    "Backups should have different sizes")))

            (cleanup-test-db file-config)))))))

(deftest test-backup-rotation-workflow
  (testing "Backup rotation: Create multiple, keep recent, delete old"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "rotation-test"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 10 :post-count 20)

              ;; Create 5 backups
              results (doall (repeatedly 5
                                       #(backup/backup-to-directory
                                        conn {:path test-dir}
                                        :database-id "rotation-test")))]

          ;; All should succeed
          (is (every? :success results) "All backups should succeed")

          ;; List all backups
          (let [backups (backup/list-backups-in-directory
                        {:path test-dir}
                        "rotation-test")]
            (is (= 5 (count backups)) "Should have 5 backups")

            ;; Simulate rotation: keep only 3 most recent
            (let [sorted-backups (sort-by :created-at
                                         #(compare %2 %1)
                                         backups)
                  to-keep (take 3 sorted-backups)
                  to-delete (drop 3 sorted-backups)]

              ;; Delete old backups
              (doseq [backup to-delete]
                (datacamp.directory/cleanup-directory (:path backup)))

              ;; Verify only 3 remain
              (let [remaining (backup/list-backups-in-directory
                              {:path test-dir}
                              "rotation-test")]
                (is (= 3 (count remaining)) "Should have 3 backups after rotation")))))))))

(deftest test-backup-verification-workflow
  (testing "Comprehensive verification workflow"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "verify-workflow"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 25 :post-count 50)

              ;; Create backup
              backup-result (backup/backup-to-directory
                            conn {:path test-dir}
                            :database-id "verify-workflow")]

          ;; Verification 1: Structure check
          (let [structure (verify-backup-structure (:path backup-result))]
            (is (:manifest-exists? structure))
            (is (:checkpoint-exists? structure))
            (is (:chunks-dir-exists? structure))
            (is (:complete-marker-exists? structure)))

          ;; Verification 2: Chunk integrity
          (let [verification (backup/verify-backup-in-directory
                             {:path test-dir}
                             (:backup-id backup-result)
                             :database-id "verify-workflow")]
            (is (:all-chunks-present verification)))

          ;; Verification 3: Manifest accuracy
          (let [manifest (datacamp.metadata/read-edn-from-file
                         (str (:path backup-result) "/manifest.edn"))
                actual-chunk-count (count-backup-chunks (:path backup-result))]
            (is (= (:stats/chunk-count manifest) actual-chunk-count)
                "Manifest chunk count should match actual")))))))

(deftest test-error-recovery-workflow
  (testing "Error recovery and cleanup"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "error-test"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 15 :post-count 30)

              ;; Create a successful backup
              success-result (backup/backup-to-directory
                             conn {:path test-dir}
                             :database-id "error-test")

              ;; Simulate an incomplete backup
              incomplete-id (str (java.util.UUID/randomUUID))
              incomplete-path (datacamp.directory/get-backup-path
                              test-dir "error-test" incomplete-id)
              _ (datacamp.directory/ensure-directory incomplete-path)
              _ (datacamp.metadata/write-edn-to-file
                 (str incomplete-path "/manifest.edn")
                 {:backup/id incomplete-id
                  :backup/type :full
                  :backup/created-at (java.util.Date.
                                     (- (System/currentTimeMillis)
                                        (* 48 60 60 1000))) ; 48h ago
                  :backup/completed false
                  :stats/datom-count 0})]

          ;; List should show both
          (let [all-backups (backup/list-backups-in-directory
                            {:path test-dir}
                            "error-test")]
            (is (= 2 (count all-backups)) "Should show both backups"))

          ;; Cleanup incomplete
          (let [cleanup-result (backup/cleanup-incomplete-in-directory
                               {:path test-dir}
                               "error-test"
                               :older-than-hours 24)]
            (is (= 1 (:cleaned-count cleanup-result))
                "Should clean one incomplete backup"))

          ;; Verify only complete backup remains
          (let [remaining (backup/list-backups-in-directory
                          {:path test-dir}
                          "error-test")]
            (is (= 1 (count remaining)) "Should have one backup")
            (is (:completed? (first remaining)) "Remaining backup should be complete")))))))

(deftest test-concurrent-backup-workflow
  (testing "Concurrent backups from different connections"
    (with-test-dir test-dir
      (let [configs (map (fn [i]
                          {:store {:backend :mem :id (str "concurrent-" i)}})
                        (range 3))
            conns (map (fn [cfg]
                        (let [conn (create-test-db cfg)]
                          (populate-test-db conn :user-count 10 :post-count 20)
                          conn))
                      configs)]

        ;; Backup all concurrently
        (let [futures (map-indexed
                      (fn [i conn]
                        (future
                         (backup/backup-to-directory
                          conn {:path test-dir}
                          :database-id (str "concurrent-" i))))
                      conns)
              results (map deref futures)]

          ;; All should succeed
          (is (every? :success results) "All concurrent backups should succeed")
          (is (= 3 (count (distinct (map :backup-id results))))
              "Should have unique backup IDs"))

        ;; Cleanup
        (doseq [cfg configs]
          (cleanup-test-db cfg))))))

(deftest test-large-dataset-workflow
  (testing "Large dataset backup workflow"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "large-test"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 500 :post-count 2000)

              ;; Backup with smaller chunks to ensure multiple chunks
              result (backup/backup-to-directory
                     conn {:path test-dir}
                     :database-id "large-test"
                     :chunk-size (* 50 1024))] ; 50KB chunks - small enough to split large dataset

          (is (:success result) "Large backup should succeed")
          (is (> (:chunk-count result) 3) "Should create multiple chunks")

          ;; Verify all chunks
          (let [verification (backup/verify-backup-in-directory
                             {:path test-dir}
                             (:backup-id result)
                             :database-id "large-test")]
            (is (:all-chunks-present verification)
                "All chunks should be present for large backup")))))))

;; Run tests
(comment
  (run-tests))
