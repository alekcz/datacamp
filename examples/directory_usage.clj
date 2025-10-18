(ns examples.directory-usage
  "Examples for using Datacamp with local directory storage"
  (:require [datahike.api :as d]
            [datacamp.core :as backup]))

;; =============================================================================
;; Example 1: Simple Directory Backup
;; =============================================================================

(comment
  ;; Connect to your Datahike database
  (def conn (d/connect {:store {:backend :file :path "/data/mydb"}}))

  ;; Backup to local directory
  (def result
    (backup/backup-to-directory conn
                                {:path "/backups"}
                                :database-id "production-db"))

  ;; Check the result
  (println "Backup ID:" (:backup-id result))
  (println "Datoms backed up:" (:datom-count result))
  (println "Total size:" (:total-size-bytes result) "bytes")
  (println "Location:" (:path result)))


;; =============================================================================
;; Example 2: Backup with Custom Configuration
;; =============================================================================

(comment
  ;; Backup with custom chunk size
  (def result
    (backup/backup-to-directory conn
                                {:path "/backups"}
                                :database-id "my-app-db"
                                :chunk-size (* 128 1024 1024)  ; 128MB chunks
                                :compression :gzip)))


;; =============================================================================
;; Example 3: List All Backups in Directory
;; =============================================================================

(comment
  ;; List all backups for a database
  (def backups
    (backup/list-backups-in-directory {:path "/backups"}
                                      "production-db"))

  ;; Print backup information
  (doseq [backup backups]
    (println "----------------------------------------")
    (println "Backup ID:" (:backup-id backup))
    (println "Type:" (:type backup))
    (println "Created:" (:created-at backup))
    (println "Completed:" (:completed? backup))
    (println "Datoms:" (:datom-count backup))
    (println "Size:" (:size-bytes backup) "bytes")
    (println "Path:" (:path backup))))


;; =============================================================================
;; Example 4: Verify Directory Backup
;; =============================================================================

(comment
  ;; Verify a specific backup
  (def verification
    (backup/verify-backup-in-directory
     {:path "/backups"}
     #uuid "550e8400-e29b-41d4-a716-446655440000"  ; backup-id
     :database-id "production-db"))

  (if (:success verification)
    (println "✓ Backup is valid with" (:chunk-count verification) "chunks")
    (println "✗ Backup verification failed:" (:error verification))))


;; =============================================================================
;; Example 5: Cleanup Incomplete Directory Backups
;; =============================================================================

(comment
  ;; Clean up incomplete backups older than 24 hours
  (def cleanup-result
    (backup/cleanup-incomplete-in-directory {:path "/backups"}
                                            "production-db"
                                            :older-than-hours 24))

  (println "Cleaned up" (:cleaned-count cleanup-result) "incomplete backups")
  (println "Backup IDs removed:" (:backup-ids cleanup-result)))


;; =============================================================================
;; Example 6: Directory Structure
;; =============================================================================

(comment
  ;; After backing up, the directory structure looks like this:
  ;;
  ;; /backups/
  ;; ├── production-db/
  ;; │   ├── {backup-id-1}/
  ;; │   │   ├── manifest.edn
  ;; │   │   ├── checkpoint.edn
  ;; │   │   ├── chunks/
  ;; │   │   │   ├── datoms-0.fressian.gz
  ;; │   │   │   ├── datoms-1.fressian.gz
  ;; │   │   │   └── ...
  ;; │   │   └── complete.marker
  ;; │   ├── {backup-id-2}/
  ;; │   │   └── ...
  ;; └── another-db/
  ;;     └── ...

  ;; You can inspect the manifest directly
  (require '[clojure.edn :as edn])
  (def manifest
    (edn/read-string
     (slurp "/backups/production-db/{backup-id}/manifest.edn")))

  (clojure.pprint/pprint manifest))


;; =============================================================================
;; Example 7: Scheduled Directory Backups
;; =============================================================================

(comment
  (require '[clojure.core.async :as a])

  (defn schedule-directory-backups
    "Schedule regular backups to a directory"
    [conn dir-config database-id interval-ms]
    (let [stop-chan (a/chan)]
      (a/go-loop []
        (a/alt!
          stop-chan
          (println "Directory backup scheduler stopped")

          (a/timeout interval-ms)
          (do
            (println "Starting scheduled directory backup...")
            (try
              (let [result (backup/backup-to-directory conn dir-config
                                                      :database-id database-id)]
                (if (:success result)
                  (println "✓ Backup completed:" (:backup-id result))
                  (println "✗ Backup failed:" (:error result))))
              (catch Exception e
                (println "✗ Backup error:" (.getMessage e))))
            (recur))))
      stop-chan))

  ;; Run daily backups
  (def stop-scheduler
    (schedule-directory-backups conn
                                {:path "/backups"}
                                "production-db"
                                (* 24 60 60 1000)))  ; 24 hours

  ;; Stop the scheduler
  (a/close! stop-scheduler))


;; =============================================================================
;; Example 8: Backup Rotation with Directory
;; =============================================================================

(comment
  (defn rotate-directory-backups
    "Keep only the N most recent backups"
    [dir-config database-id keep-count]
    (let [backups (backup/list-backups-in-directory dir-config database-id)
          completed (filter :completed? backups)
          sorted (sort-by :created-at #(compare %2 %1) completed)
          to-keep (take keep-count sorted)
          to-delete (drop keep-count sorted)]

      (println "Backup Rotation:")
      (println "  Keeping:" (count to-keep) "backups")
      (println "  Deleting:" (count to-delete) "backups")

      ;; Delete old backups
      (doseq [{:keys [path backup-id]} to-delete]
        (println "  Deleting:" backup-id)
        (require '[datacamp.directory :as dir])
        (dir/cleanup-directory path))

      {:kept (count to-keep)
       :deleted (count to-delete)}))

  ;; Keep only last 7 backups
  (rotate-directory-backups {:path "/backups"} "production-db" 7))


;; =============================================================================
;; Example 9: Compare S3 vs Directory Backup
;; =============================================================================

(comment
  ;; Backup to both S3 and local directory for redundancy

  (defn dual-backup
    "Backup to both S3 and local directory"
    [conn database-id]
    (println "Starting dual backup...")

    ;; Backup to S3
    (println "\n1. Backing up to S3...")
    (let [s3-result (backup/backup-to-s3 conn
                                         {:bucket "my-backups"
                                          :region "us-east-1"}
                                         :database-id database-id)]
      (if (:success s3-result)
        (println "  ✓ S3 backup completed:" (:backup-id s3-result))
        (println "  ✗ S3 backup failed:" (:error s3-result))))

    ;; Backup to local directory
    (println "\n2. Backing up to local directory...")
    (let [dir-result (backup/backup-to-directory conn
                                                 {:path "/backups"}
                                                 :database-id database-id)]
      (if (:success dir-result)
        (println "  ✓ Directory backup completed:" (:backup-id dir-result))
        (println "  ✗ Directory backup failed:" (:error dir-result))))

    (println "\n✓ Dual backup completed"))

  ;; Usage
  (dual-backup conn "production-db"))


;; =============================================================================
;; Example 10: Incremental Directory Snapshots
;; =============================================================================

(comment
  ;; Take quick snapshots for testing/development

  (defn quick-snapshot
    "Take a quick backup snapshot to a timestamped directory"
    [conn]
    (let [timestamp (.format (java.text.SimpleDateFormat. "yyyy-MM-dd-HHmmss")
                            (java.util.Date.))
          snapshot-dir (str "/tmp/db-snapshots/" timestamp)]

      (println "Creating snapshot to" snapshot-dir)

      (let [result (backup/backup-to-directory conn
                                               {:path "/tmp/db-snapshots"}
                                               :database-id timestamp
                                               :chunk-size (* 32 1024 1024))]  ; Smaller chunks
        (if (:success result)
          (do
            (println "✓ Snapshot created:" (:path result))
            (println "  Size:" (datacamp.utils/format-bytes (:total-size-bytes result)))
            result)
          (println "✗ Snapshot failed:" (:error result))))))

  ;; Create a snapshot
  (quick-snapshot conn))


;; =============================================================================
;; Example 11: Network-Attached Storage (NAS) Backup
;; =============================================================================

(comment
  ;; Backup to a mounted network drive

  (defn backup-to-nas
    "Backup to a network-attached storage device"
    [conn database-id]
    (let [nas-path "/Volumes/NAS/datahike-backups"  ; macOS
          ;; or "/mnt/nas/datahike-backups"         ; Linux
          ;; or "Z:/datahike-backups"                ; Windows
          ]

      (println "Backing up to NAS:" nas-path)

      (let [result (backup/backup-to-directory conn
                                               {:path nas-path}
                                               :database-id database-id)]
        (if (:success result)
          (do
            (println "✓ NAS backup completed")
            (println "  Location:" (:path result))
            (println "  Size:" (datacamp.utils/format-bytes (:total-size-bytes result))))
          (println "✗ NAS backup failed:" (:error result)))

        result)))

  ;; Backup to NAS
  (backup-to-nas conn "production-db"))


;; =============================================================================
;; Example 12: Directory Backup with Pre/Post Hooks
;; =============================================================================

(comment
  (defn backup-with-verification
    "Backup to directory and immediately verify"
    [conn dir-config database-id]
    (println "Starting backup with verification...")

    ;; Backup
    (let [backup-result (backup/backup-to-directory conn dir-config
                                                    :database-id database-id)]
      (if (:success backup-result)
        (do
          (println "✓ Backup completed:" (:backup-id backup-result))

          ;; Verify
          (println "Verifying backup...")
          (let [verify-result (backup/verify-backup-in-directory
                              dir-config
                              (:backup-id backup-result)
                              :database-id database-id)]
            (if (:all-chunks-present verify-result)
              (do
                (println "✓ Verification passed")
                {:status :success
                 :backup backup-result
                 :verification verify-result})
              (do
                (println "✗ Verification failed")
                {:status :verification-failed
                 :backup backup-result
                 :verification verify-result}))))

        (do
          (println "✗ Backup failed:" (:error backup-result))
          {:status :backup-failed
           :backup backup-result}))))

  ;; Usage
  (backup-with-verification conn {:path "/backups"} "production-db"))
