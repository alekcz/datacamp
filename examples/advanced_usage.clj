(ns examples.advanced-usage
  "Advanced usage examples for the Datacamp library"
  (:require [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.metadata :as meta]
            [datacamp.utils :as utils]
            [clojure.pprint :as pp]))

;; =============================================================================
;; Example 1: Inspecting Backup Metadata
;; =============================================================================

(comment
  (require '[datacamp.s3 :as s3])

  ;; Create S3 client
  (def s3-client (s3/create-s3-client {:bucket "my-backups"
                                       :region "us-east-1"}))

  ;; Read and inspect manifest
  (def manifest
    (meta/read-edn-from-s3 s3-client
                          "my-backups"
                          "production-db/backup-id/manifest.edn"))

  (println "=== BACKUP MANIFEST ===")
  (pp/pprint manifest)

  ;; Access specific fields
  (println "\nBackup Statistics:")
  (println "  Datom count:" (:stats/datom-count manifest))
  (println "  Chunk count:" (:stats/chunk-count manifest))
  (println "  Total size:" (utils/format-bytes (:stats/size-bytes manifest)))
  (println "  Duration:" (get-in manifest [:timing :duration-seconds]) "seconds")

  ;; List all chunks
  (println "\nChunks:")
  (doseq [chunk (:chunks manifest)]
    (println (format "  Chunk %d: %d datoms, %s"
                    (:chunk/id chunk)
                    (:chunk/datom-count chunk)
                    (utils/format-bytes (:chunk/size-bytes chunk))))))


;; =============================================================================
;; Example 2: Comparing Backups
;; =============================================================================

(comment
  (defn compare-backups
    "Compare two backups and show differences"
    [s3-config database-id backup-id-1 backup-id-2]
    (let [backups (backup/list-backups s3-config database-id)
          b1 (first (filter #(= (:backup-id %) backup-id-1) backups))
          b2 (first (filter #(= (:backup-id %) backup-id-2) backups))]

      (println "=== BACKUP COMPARISON ===")
      (println "\nBackup 1:" backup-id-1)
      (println "  Created:" (:created-at b1))
      (println "  Datoms:" (:datom-count b1))
      (println "  Size:" (utils/format-bytes (:size-bytes b1)))

      (println "\nBackup 2:" backup-id-2)
      (println "  Created:" (:created-at b2))
      (println "  Datoms:" (:datom-count b2))
      (println "  Size:" (utils/format-bytes (:size-bytes b2)))

      (println "\nDifference:")
      (println "  Datom delta:" (- (:datom-count b2) (:datom-count b1)))
      (println "  Size delta:" (utils/format-bytes
                                 (- (:size-bytes b2) (:size-bytes b1))))
      (println "  Growth:" (format "%.2f%%"
                                  (* 100.0 (/ (- (:datom-count b2) (:datom-count b1))
                                             (:datom-count b1)))))))

  ;; Usage
  (compare-backups {:bucket "my-backups" :region "us-east-1"}
                   "production-db"
                   #uuid "backup-1-uuid"
                   #uuid "backup-2-uuid"))


;; =============================================================================
;; Example 3: Backup Health Check
;; =============================================================================

(comment
  (defn health-check
    "Check the health of backups for a database"
    [s3-config database-id]
    (let [backups (backup/list-backups s3-config database-id)
          latest-backup (first (sort-by :created-at #(compare %2 %1) backups))
          completed-backups (filter :completed? backups)
          incomplete-backups (filter (complement :completed?) backups)]

      (println "=== BACKUP HEALTH CHECK ===")
      (println "Database:" database-id)
      (println "\nBackup Count:")
      (println "  Total:" (count backups))
      (println "  Completed:" (count completed-backups))
      (println "  Incomplete:" (count incomplete-backups))

      (when latest-backup
        (let [age-hours (utils/hours-since (:created-at latest-backup))]
          (println "\nLatest Backup:")
          (println "  ID:" (:backup-id latest-backup))
          (println "  Created:" (:created-at latest-backup))
          (println "  Age:" (format "%.1f hours" age-hours))
          (println "  Status:" (if (:completed? latest-backup) "✓ Complete" "✗ Incomplete"))

          ;; Health status
          (println "\nHealth Status:")
          (cond
            (> age-hours 48)
            (println "  ⚠ WARNING: Last backup is more than 48 hours old")

            (> age-hours 24)
            (println "  ⚠ NOTICE: Last backup is more than 24 hours old")

            :else
            (println "  ✓ OK: Recent backup available"))))

      ;; Verify latest backup
      (when (and latest-backup (:completed? latest-backup))
        (println "\nVerifying latest backup...")
        (let [verification (backup/verify-backup s3-config
                                                (:backup-id latest-backup)
                                                :database-id database-id)]
          (if (:all-chunks-present verification)
            (println "  ✓ All chunks present and accounted for")
            (println "  ✗ Missing chunks:" (:missing-chunks verification)))))

      ;; Recommendations
      (println "\nRecommendations:")
      (when (> (count incomplete-backups) 0)
        (println "  - Clean up" (count incomplete-backups) "incomplete backups"))
      (when (and latest-backup (> (utils/hours-since (:created-at latest-backup)) 24))
        (println "  - Create a fresh backup"))
      (when (< (count completed-backups) 3)
        (println "  - Maintain at least 3 backup copies"))))

  ;; Usage
  (health-check {:bucket "my-backups" :region "us-east-1"} "production-db"))


;; =============================================================================
;; Example 4: Backup Rotation Policy
;; =============================================================================

(comment
  (defn apply-backup-rotation
    "Apply backup rotation policy: keep last N backups, delete older ones"
    [s3-config database-id keep-count]
    (let [backups (backup/list-backups s3-config database-id)
          completed (filter :completed? backups)
          sorted (sort-by :created-at #(compare %2 %1) completed)
          to-keep (take keep-count sorted)
          to-delete (drop keep-count sorted)]

      (println "Backup Rotation Policy")
      (println "  Total backups:" (count completed))
      (println "  Keeping:" (count to-keep))
      (println "  Deleting:" (count to-delete))

      (when (seq to-delete)
        (println "\nBackups to delete:")
        (doseq [backup to-delete]
          (println (format "  - %s (created: %s, age: %.1f hours)"
                          (:backup-id backup)
                          (:created-at backup)
                          (utils/hours-since (:created-at backup)))))

        ;; Note: Actual deletion would require additional implementation
        (println "\nNote: Implement deletion logic with s3/delete-object"))

      {:kept (count to-keep)
       :deleted (count to-delete)
       :to-delete-ids (map :backup-id to-delete)}))

  ;; Keep last 7 backups
  (apply-backup-rotation {:bucket "my-backups" :region "us-east-1"}
                        "production-db"
                        7))


;; =============================================================================
;; Example 5: Backup Size Analysis
;; =============================================================================

(comment
  (defn analyze-backup-sizes
    "Analyze backup sizes over time"
    [s3-config database-id]
    (let [backups (backup/list-backups s3-config database-id)
          completed (filter :completed? backups)
          sorted (sort-by :created-at completed)]

      (println "=== BACKUP SIZE ANALYSIS ===")
      (println "Database:" database-id)
      (println "\nBackups over time:")

      (doseq [backup sorted]
        (println (format "%s | %8d datoms | %12s | %s"
                        (:created-at backup)
                        (:datom-count backup)
                        (utils/format-bytes (:size-bytes backup))
                        (if (:completed? backup) "✓" "✗"))))

      (when (seq completed)
        (let [total-size (reduce + (map :size-bytes completed))
              avg-size (/ total-size (count completed))
              min-size (apply min (map :size-bytes completed))
              max-size (apply max (map :size-bytes completed))]

          (println "\nStatistics:")
          (println "  Total storage:" (utils/format-bytes total-size))
          (println "  Average size:" (utils/format-bytes avg-size))
          (println "  Min size:" (utils/format-bytes min-size))
          (println "  Max size:" (utils/format-bytes max-size))
          (println "  Growth ratio:" (format "%.2f"
                                             (/ max-size (double min-size))))))))

  ;; Usage
  (analyze-backup-sizes {:bucket "my-backups" :region "us-east-1"}
                       "production-db"))


;; =============================================================================
;; Example 6: Multi-Database Backup
;; =============================================================================

(comment
  (defn backup-multiple-databases
    "Backup multiple databases to S3"
    [databases s3-config]
    (let [results (atom [])]
      (println "=== MULTI-DATABASE BACKUP ===")
      (println "Backing up" (count databases) "databases...\n")

      (doseq [{:keys [conn database-id description]} databases]
        (println "Backing up:" description)
        (try
          (let [start-time (System/currentTimeMillis)
                result (backup/backup-to-s3 conn s3-config
                                            :database-id database-id)
                duration (- (System/currentTimeMillis) start-time)]

            (if (:success result)
              (do
                (println "  ✓ Success -" (:datom-count result) "datoms in"
                        (format "%.2fs" (/ duration 1000.0)))
                (swap! results conj (assoc result :status :success)))
              (do
                (println "  ✗ Failed -" (:error result))
                (swap! results conj (assoc result :status :failed)))))

          (catch Exception e
            (println "  ✗ Exception -" (.getMessage e))
            (swap! results conj {:status :exception
                                :database-id database-id
                                :error (.getMessage e)}))))

      (println "\n=== SUMMARY ===")
      (let [successful (filter #(= :success (:status %)) @results)
            failed (filter #(not= :success (:status %)) @results)]
        (println "Successful:" (count successful))
        (println "Failed:" (count failed))
        (println "Total datoms backed up:"
                (reduce + 0 (map :datom-count successful)))
        (println "Total size:"
                (utils/format-bytes
                 (reduce + 0 (map :total-size-bytes successful)))))

      @results))

  ;; Usage
  (def db1-conn (d/connect {:store {:backend :file :path "/data/db1"}}))
  (def db2-conn (d/connect {:store {:backend :file :path "/data/db2"}}))
  (def db3-conn (d/connect {:store {:backend :file :path "/data/db3"}}))

  (backup-multiple-databases
   [{:conn db1-conn :database-id "users-db" :description "User Database"}
    {:conn db2-conn :database-id "orders-db" :description "Orders Database"}
    {:conn db3-conn :database-id "inventory-db" :description "Inventory Database"}]
   {:bucket "my-backups" :region "us-east-1"}))


;; =============================================================================
;; Example 7: Backup with Pre and Post Hooks
;; =============================================================================

(comment
  (defn backup-with-hooks
    "Backup with pre and post processing hooks"
    [conn s3-config database-id
     & {:keys [pre-backup-fn post-backup-fn on-error-fn]
        :or {pre-backup-fn identity
             post-backup-fn identity
             on-error-fn identity}}]

    (println "Starting backup with hooks...")

    ;; Pre-backup hook
    (try
      (println "Running pre-backup hook...")
      (pre-backup-fn conn database-id)

      ;; Perform backup
      (let [result (backup/backup-to-s3 conn s3-config
                                        :database-id database-id)]

        (if (:success result)
          (do
            ;; Post-backup hook
            (println "Running post-backup hook...")
            (post-backup-fn result database-id)
            result)

          (do
            ;; Error hook
            (println "Running error hook...")
            (on-error-fn (:error result) database-id)
            result)))

      (catch Exception e
        (println "Exception during backup:")
        (on-error-fn e database-id)
        (throw e))))

  ;; Usage with hooks
  (backup-with-hooks
   conn
   {:bucket "my-backups" :region "us-east-1"}
   "production-db"

   :pre-backup-fn
   (fn [conn db-id]
     (println "Pre-backup: Taking database snapshot...")
     (println "Pre-backup: Current datom count:" (d/datoms @conn :eavt))
     ;; Could do things like:
     ;; - Flush write-ahead logs
     ;; - Create consistency marker
     ;; - Notify monitoring system
     )

   :post-backup-fn
   (fn [result db-id]
     (println "Post-backup: Backup completed successfully")
     (println "Post-backup: Backup ID:" (:backup-id result))
     ;; Could do things like:
     ;; - Update backup registry
     ;; - Send success notification
     ;; - Trigger downstream processes
     ;; - Update metrics dashboard
     )

   :on-error-fn
   (fn [error db-id]
     (println "Error: Backup failed for" db-id)
     (println "Error details:" error)
     ;; Could do things like:
     ;; - Send alert to PagerDuty
     ;; - Log to error tracking service
     ;; - Attempt recovery
     ;; - Rollback partial changes
     )))
