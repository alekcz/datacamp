(ns examples.gc-quickstart
  "Quick start guide for using the optimized GC with your 500GB database"
  (:require [datahike.api :as d]
            [datacamp.gc :as gc]
            [datacamp.konserve-extensions :as ext]
            [clojure.core.async :refer [<?!!]]
            [taoensso.timbre :as log]))

;; =============================================================================
;; Step 1: Setup for Your 500GB Production Database
;; =============================================================================

(defn setup-optimized-gc
  "Setup optimized GC for your large production database"
  [conn]
  (let [db @conn
        store-config (:store (:config db))
        store (:store db)]

    ;; Auto-extend the store with batch operations
    (println "Extending store with batch operations...")
    (ext/auto-extend-store! store store-config)

    ;; Verify batch operations are available
    (if (ext/supports-batch? store)
      (println "✓ Batch operations enabled")
      (println "⚠ Batch operations not available, will use parallel deletes"))

    store))

;; =============================================================================
;; Step 2: Run GC with Resumable Support
;; =============================================================================

(defn run-production-gc
  "Run GC on your 500GB database with full resumable support

   Expected timeline:
   - Mark phase: ~6 hours (resumable every 30 seconds)
   - Sweep phase: 2-4 hours (vs 2+ days before)
   - Total: 8-10 hours"
  [conn & {:keys [retention-days dry-run]
           :or {retention-days 7
                dry-run false}}]

  (println "\n=== Starting Production GC ===")
  (println "Database size: 500GB")
  (println "Retention policy:" retention-days "days")
  (println "Mode:" (if dry-run "DRY RUN" "LIVE"))

  ;; Setup batch operations
  (setup-optimized-gc conn)

  (let [db @conn
        gc-id (str "prod-gc-" (System/currentTimeMillis))
        retention-date (java.util.Date.
                        (- (System/currentTimeMillis)
                           (* retention-days 24 60 60 1000)))

        ;; Get backend-optimized settings
        store-config (:store (:config db))
        optimized-settings (gc/optimize-for-backend store-config)]

    (println "\nOptimized settings for" (:backend store-config) "backend:")
    (println "  Batch size:" (:batch-size optimized-settings))
    (println "  Parallel batches:" (:parallel-batches optimized-settings))
    (println "  GC ID:" gc-id)
    (println "  Retention date:" retention-date)

    ;; Check for interrupted GC first
    (let [status (<?!! (gc/get-gc-status db))]
      (when (= (:status status) :in-progress)
        (println "\n⚠ Found interrupted GC:" (:gc-id status))
        (println "  Started:" (:started-at status))
        (println "  Progress: Visited" (:visited-count status) "commits")
        (println "  Resuming...")

        ;; Resume and return
        (return (<?!! (gc/resume-gc! db (:gc-id status)
                                    :batch-size (:batch-size optimized-settings)
                                    :parallel-batches (:parallel-batches optimized-settings))))))

    ;; Start new GC
    (println "\nPhase 1: Mark (resumable)")
    (println "  This will take ~6 hours for 500GB")
    (println "  Checkpoints saved every 30 seconds")
    (println "  Can safely interrupt with Ctrl+C and resume later")

    (let [start-time (System/currentTimeMillis)

          ;; Run GC with monitoring
          gc-future (future
                     (<?!! (gc/gc-storage-optimized!
                           db
                           :remove-before retention-date
                           :resume-gc-id gc-id
                           :batch-size (:batch-size optimized-settings)
                           :parallel-batches (:parallel-batches optimized-settings)
                           :checkpoint-interval 100
                           :dry-run dry-run)))

          ;; Monitor progress
          monitor-future (future
                          (loop []
                            (Thread/sleep 60000) ; Check every minute
                            (when-not (realized? gc-future)
                              (let [status (<?!! (gc/get-gc-status db))]
                                (when (= (:status status) :in-progress)
                                  (let [elapsed (/ (- (System/currentTimeMillis) start-time) 1000.0)]
                                    (println (format "\n[%.1f min] Progress:" (/ elapsed 60)))
                                    (println "  Visited commits:" (:visited-count status))
                                    (println "  Reachable items:" (:reachable-count status))
                                    (println "  Branches:" (:completed-branches status) "/"
                                            (+ (:completed-branches status) (:pending-branches status)))))
                                (recur)))))]

      ;; Wait for completion
      (let [result @gc-future
            duration (- (System/currentTimeMillis) start-time)]

        (future-cancel monitor-future)

        (println "\n=== GC Complete ===")
        (println "Total duration:" (/ duration 1000.0 60.0) "minutes")
        (if dry-run
          (do
            (println "DRY RUN Results:")
            (println "  Would keep:" (:reachable-count result) "items")
            (println "  Would delete:" (:would-delete-count result) "items"))
          (do
            (println "Results:")
            (println "  Kept:" (:reachable-count result) "items")
            (println "  Deleted:" (:deleted-count result) "items")
            (println "  Deletion rate:"
                    (int (/ (:deleted-count result) (/ duration 1000.0)))
                    "items/second")))

        result))))

;; =============================================================================
;; Step 3: Resume Interrupted GC
;; =============================================================================

(defn resume-interrupted-gc
  "Resume a GC that was interrupted (e.g., by server restart)"
  [conn]

  (println "\n=== Checking for Interrupted GC ===")

  (let [db @conn
        status (<?!! (gc/get-gc-status db))]

    (if (= (:status status) :in-progress)
      (do
        (println "Found interrupted GC!")
        (println "  GC ID:" (:gc-id status))
        (println "  Started:" (:started-at status))
        (println "  Last checkpoint:" (:last-checkpoint status))
        (println "  Progress:")
        (println "    Visited commits:" (:visited-count status))
        (println "    Reachable items:" (:reachable-count status))
        (println "    Completed branches:" (:completed-branches status))
        (println "    Pending branches:" (:pending-branches status))

        (println "\nResuming GC...")

        (let [start-time (System/currentTimeMillis)
              store-config (:store (:config db))
              optimized-settings (gc/optimize-for-backend store-config)

              result (<?!! (gc/resume-gc!
                           db
                           (:gc-id status)
                           :batch-size (:batch-size optimized-settings)
                           :parallel-batches (:parallel-batches optimized-settings)))

              duration (- (System/currentTimeMillis) start-time)]

          (println "\nGC resumed and completed!")
          (println "  Resume duration:" (/ duration 1000.0 60.0) "minutes")
          (println "  Total deleted:" (:deleted-count result) "items")

          result))

      (println "No interrupted GC found. Use run-production-gc to start a new GC."))))

;; =============================================================================
;; Usage Examples
;; =============================================================================

(comment
  ;; Connect to your production database
  (def conn (d/connect {:store {:backend :jdbc
                                :dbtype "postgresql"
                                :host "localhost"
                                :port 5432
                                :dbname "production_db"
                                :user "user"
                                :password "password"}}))

  ;; Option 1: Dry run first (recommended for production)
  (run-production-gc conn
                     :retention-days 30
                     :dry-run true)

  ;; Option 2: Run actual GC
  (run-production-gc conn
                     :retention-days 30
                     :dry-run false)

  ;; Option 3: Resume if interrupted
  (resume-interrupted-gc conn)

  ;; Check status at any time
  (let [status (<?!! (gc/get-gc-status @conn))]
    (println "Current GC status:" status)))

;; =============================================================================
;; Command Line Interface
;; =============================================================================

(defn -main
  "Command line interface for production GC

   Usage:
     clojure -M:run -m examples.gc-quickstart run [--dry-run] [--retention-days N]
     clojure -M:run -m examples.gc-quickstart resume
     clojure -M:run -m examples.gc-quickstart status"
  [& args]

  (let [[cmd & opts] args
        ;; Your production database config
        ;; TODO: Load from environment or config file
        conn (d/connect {:store {:backend :jdbc
                                :dbtype "postgresql"
                                :host (System/getenv "DB_HOST")
                                :port (Integer/parseInt (or (System/getenv "DB_PORT") "5432"))
                                :dbname (System/getenv "DB_NAME")
                                :user (System/getenv "DB_USER")
                                :password (System/getenv "DB_PASSWORD")}})]

    (case cmd
      "run"
      (let [dry-run (some #{"--dry-run"} opts)
            retention-days (if-let [idx (.indexOf opts "--retention-days")]
                           (Integer/parseInt (nth opts (inc idx)))
                           7)]
        (run-production-gc conn
                          :retention-days retention-days
                          :dry-run dry-run))

      "resume"
      (resume-interrupted-gc conn)

      "status"
      (let [db @conn
            status (<?!! (gc/get-gc-status db))]
        (if (= (:status status) :in-progress)
          (do
            (println "GC in progress:")
            (println "  ID:" (:gc-id status))
            (println "  Started:" (:started-at status))
            (println "  Visited:" (:visited-count status))
            (println "  Reachable:" (:reachable-count status)))
          (println "No GC in progress")))

      ;; Default
      (do
        (println "Usage:")
        (println "  run [--dry-run] [--retention-days N]  - Start new GC")
        (println "  resume                                - Resume interrupted GC")
        (println "  status                                - Check GC status")
        (System/exit 1)))))