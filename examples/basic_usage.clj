(ns examples.basic-usage
  "Basic usage examples for the Datacamp library"
  (:require [datahike.api :as d]
            [datacamp.core :as backup]))

;; =============================================================================
;; Example 1: Simple Backup
;; =============================================================================

(comment
  ;; Create or connect to a Datahike database
  (def cfg {:store {:backend :file :path "/tmp/my-database"}})
  (d/create-database cfg)
  (def conn (d/connect cfg))

  ;; Add some data
  (d/transact conn [{:db/id -1
                     :name "Alice"
                     :email "alice@example.com"
                     :age 30}
                    {:db/id -2
                     :name "Bob"
                     :email "bob@example.com"
                     :age 35}
                    {:db/id -3
                     :name "Charlie"
                     :email "charlie@example.com"
                     :age 28}])

  ;; Backup to S3
  (def result
    (backup/backup-to-s3 conn
                         {:bucket "my-datahike-backups"
                          :region "us-east-1"}
                         :database-id "production-db"))

  ;; Check the result
  (println "Backup completed!")
  (println "Backup ID:" (:backup-id result))
  (println "Datoms backed up:" (:datom-count result))
  (println "Total size:" (:total-size-bytes result) "bytes")
  (println "Duration:" (:duration-ms result) "ms")
  (println "S3 Path:" (:s3-path result)))


;; =============================================================================
;; Example 2: Backup with Custom Configuration
;; =============================================================================

(comment
  ;; Backup with custom chunk size and parallelism
  (def result
    (backup/backup-to-s3 conn
                         {:bucket "my-datahike-backups"
                          :region "us-east-1"
                          :prefix "production/datacenter-1/"}
                         :database-id "my-app-db"
                         :chunk-size (* 128 1024 1024)  ; 128MB chunks
                         :compression :gzip
                         :parallel 8)))  ; 8 parallel uploads


;; =============================================================================
;; Example 3: List All Backups
;; =============================================================================

(comment
  ;; List all backups for a database
  (def backups
    (backup/list-backups {:bucket "my-datahike-backups"
                          :region "us-east-1"}
                         "production-db"))

  ;; Print backup information
  (doseq [backup backups]
    (println "----------------------------------------")
    (println "Backup ID:" (:backup-id backup))
    (println "Type:" (:type backup))
    (println "Created:" (:created-at backup))
    (println "Completed:" (:completed? backup))
    (println "Datoms:" (:datom-count backup))
    (println "Size:" (:size-bytes backup) "bytes")))


;; =============================================================================
;; Example 4: Verify Backup Integrity
;; =============================================================================

(comment
  ;; Verify a specific backup
  (def verification
    (backup/verify-backup {:bucket "my-datahike-backups"
                           :region "us-east-1"}
                          #uuid "550e8400-e29b-41d4-a716-446655440000"  ; backup-id
                          :database-id "production-db"))

  (if (:success verification)
    (println "✓ Backup is valid with" (:chunk-count verification) "chunks")
    (println "✗ Backup verification failed:" (:error verification))))


;; =============================================================================
;; Example 5: Cleanup Old/Incomplete Backups
;; =============================================================================

(comment
  ;; Clean up incomplete backups older than 24 hours
  (def cleanup-result
    (backup/cleanup-incomplete {:bucket "my-datahike-backups"
                                :region "us-east-1"}
                               "production-db"
                               :older-than-hours 24))

  (println "Cleaned up" (:cleaned-count cleanup-result) "incomplete backups")
  (println "Backup IDs removed:" (:backup-ids cleanup-result)))


;; =============================================================================
;; Example 6: Using with Different Storage Backends
;; =============================================================================

(comment
  ;; Memory backend (for testing)
  (def mem-conn (d/connect {:store {:backend :mem :id "test-db"}}))
  (d/transact mem-conn [{:db/id -1 :name "Test"}])
  (backup/backup-to-s3 mem-conn
                       {:bucket "test-backups" :region "us-east-1"}
                       :database-id "test-db")

  ;; File backend
  (def file-conn (d/connect {:store {:backend :file :path "/data/mydb"}}))
  (backup/backup-to-s3 file-conn
                       {:bucket "prod-backups" :region "us-east-1"}
                       :database-id "prod-db"))


;; =============================================================================
;; Example 7: S3-Compatible Storage (LocalStack, DigitalOcean Spaces, etc.)
;; =============================================================================

(comment
  ;; Using LocalStack (S3-compatible)
  (backup/backup-to-s3 conn
                       {:bucket "my-backups"
                        :region "us-east-1"
                        :endpoint "localhost:4566"
                        :path-style-access? true}
                       :database-id "app-db")

  ;; Using DigitalOcean Spaces
  (backup/backup-to-s3 conn
                       {:bucket "my-space"
                        :region "nyc3"
                        :endpoint "nyc3.digitaloceanspaces.com"}
                       :database-id "app-db"))


;; =============================================================================
;; Example 8: Error Handling
;; =============================================================================

(comment
  ;; Backup with error handling
  (try
    (def result
      (backup/backup-to-s3 conn
                           {:bucket "my-backups"
                            :region "us-east-1"}
                           :database-id "prod-db"))

    (if (:success result)
      (do
        (println "✓ Backup successful!")
        (println "  Backup ID:" (:backup-id result))
        (println "  Location:" (:s3-path result)))
      (println "✗ Backup failed:" (:error result)))

    (catch Exception e
      (println "✗ Exception during backup:" (.getMessage e))
      (println "  Cause:" (ex-cause e)))))


;; =============================================================================
;; Example 9: Scheduled Backups
;; =============================================================================

(comment
  ;; Create a simple backup scheduler
  (require '[clojure.core.async :as a])

  (defn schedule-backups
    "Schedule regular backups every interval-ms"
    [conn s3-config database-id interval-ms]
    (let [stop-chan (a/chan)]
      (a/go-loop []
        (a/alt!
          stop-chan
          (println "Backup scheduler stopped")

          (a/timeout interval-ms)
          (do
            (println "Starting scheduled backup...")
            (try
              (let [result (backup/backup-to-s3 conn s3-config
                                                :database-id database-id)]
                (if (:success result)
                  (println "✓ Scheduled backup completed:" (:backup-id result))
                  (println "✗ Scheduled backup failed:" (:error result))))
              (catch Exception e
                (println "✗ Scheduled backup error:" (.getMessage e))))
            (recur))))
      stop-chan))

  ;; Run backups every 6 hours
  (def stop-scheduler
    (schedule-backups conn
                      {:bucket "my-backups" :region "us-east-1"}
                      "production-db"
                      (* 6 60 60 1000)))  ; 6 hours in ms

  ;; Stop the scheduler
  (a/close! stop-scheduler))


;; =============================================================================
;; Example 10: Backup with Monitoring
;; =============================================================================

(comment
  (require '[clojure.tools.logging :as log])

  (defn backup-with-monitoring
    "Perform backup with detailed monitoring and logging"
    [conn s3-config database-id]
    (let [start-time (System/currentTimeMillis)]
      (log/info "Starting backup for database:" database-id)

      (try
        (let [result (backup/backup-to-s3 conn s3-config
                                          :database-id database-id)
              duration-seconds (/ (:duration-ms result) 1000.0)
              throughput-mbps (/ (:total-size-bytes result)
                                duration-seconds
                                1024.0 1024.0)]

          (if (:success result)
            (do
              (log/info "Backup completed successfully"
                       {:backup-id (:backup-id result)
                        :datom-count (:datom-count result)
                        :size-mb (/ (:total-size-bytes result) 1024.0 1024.0)
                        :duration-seconds duration-seconds
                        :throughput-mbps throughput-mbps})

              ;; Send metrics to monitoring system
              (send-metrics {:operation "backup"
                           :status "success"
                           :database-id database-id
                           :duration-ms (:duration-ms result)
                           :size-bytes (:total-size-bytes result)})

              result)

            (do
              (log/error "Backup failed" {:error (:error result)})
              (send-alert {:level "error"
                         :message "Backup failed"
                         :database-id database-id
                         :error (:error result)})
              result)))

        (catch Exception e
          (log/error e "Backup exception")
          (send-alert {:level "critical"
                     :message "Backup exception"
                     :database-id database-id
                     :exception (.getMessage e)})
          (throw e)))))

  ;; Mock functions for the example
  (defn send-metrics [m] (println "Metrics:" m))
  (defn send-alert [m] (println "Alert:" m)))
