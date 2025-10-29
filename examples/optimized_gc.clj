(ns examples.optimized-gc
  "Examples of using the optimized GC with resumable marking and batch deletion"
  (:require [datahike.api :as d]
            [datacamp.gc :as gc]
            [taoensso.timbre :as log]
            [clojure.core.async :refer [<?!!]]))

;; =============================================================================
;; Basic Usage Examples
;; =============================================================================

(defn example-basic-gc
  "Basic example of running optimized GC"
  []
  (println "\n=== Basic Optimized GC Example ===\n")

  (let [config {:store {:backend :file
                       :path "/tmp/test-gc-db"}}]

    ;; Setup database with some data
    (d/delete-database config)
    (d/create-database config)
    (def conn (d/connect config))

    ;; Create some data and history
    (println "Creating test data...")
    (dotimes [i 10]
      @(d/transact conn [{:test/id i :test/value (str "value-" i)}]))

    ;; Run optimized GC
    (println "\nRunning optimized GC...")
    (let [db @conn
          result (<?!! (gc/gc-storage-optimized! db
                                                 :batch-size 100
                                                 :parallel-batches 2))]
      (println "GC Result:")
      (println "  Reachable items:" (:reachable-count result))
      (println "  Deleted items:" (:deleted-count result))
      (println "  Duration:" (:duration-ms result) "ms"))))

(defn example-resumable-gc
  "Example showing how GC can be interrupted and resumed"
  []
  (println "\n=== Resumable GC Example ===\n")

  (let [config {:store {:backend :file
                       :path "/tmp/resumable-gc-db"}}
        gc-id "test-gc-123"]

    (d/delete-database config)
    (d/create-database config)
    (def conn (d/connect config))

    ;; Create significant data
    (println "Creating test data...")
    (dotimes [i 50]
      @(d/transact conn [{:test/id i :test/data (vec (range 100))}]))

    ;; Start GC with specific ID
    (println "\nStarting GC with ID:" gc-id)
    (let [db @conn]

      ;; Simulate interruption - start GC in background
      (future
        (try
          (<?!! (gc/gc-storage-optimized! db
                                          :resume-gc-id gc-id
                                          :checkpoint-interval 10))
          (catch Exception e
            (println "GC interrupted:" (.getMessage e)))))

      ;; Check status while running
      (Thread/sleep 100)
      (let [status (<?!! (gc/get-gc-status db))]
        (println "\nGC Status during execution:")
        (println "  Status:" (:status status))
        (println "  Visited commits:" (:visited-count status))
        (println "  Reachable items:" (:reachable-count status)))

      ;; Simulate interruption
      (println "\nSimulating interruption...")
      (Thread/sleep 200)

      ;; Resume GC
      (println "\nResuming GC...")
      (let [result (<?!! (gc/resume-gc! db gc-id))]
        (println "GC completed after resume:")
        (println "  Result:" result)))))

(defn example-large-db-gc
  "Example optimized for large database"
  []
  (println "\n=== Large Database GC Example ===\n")

  (let [config {:store {:backend :file
                       :path "/tmp/large-gc-db"}}]

    (d/delete-database config)
    (d/create-database config)
    (def conn (d/connect config))

    ;; Simulate large database with many transactions
    (println "Creating large dataset...")
    (let [batch-size 100]
      (dotimes [batch 10]
        (let [entities (for [i (range batch-size)]
                        {:entity/id (+ (* batch batch-size) i)
                         :entity/batch batch
                         :entity/data (str "data-" i)
                         :entity/vector (vec (range 10))
                         :entity/timestamp (java.util.Date.)})]
          @(d/transact conn entities))
        (when (zero? (mod batch 5))
          (println "  Created" (* (inc batch) batch-size) "entities"))))

    ;; Get optimized settings for backend
    (let [optimized-settings (gc/optimize-for-backend (:store config))
          _ (println "\nUsing optimized settings for" (:backend (:store config)) "backend:")
          _ (println "  Batch size:" (:batch-size optimized-settings))
          _ (println "  Parallel batches:" (:parallel-batches optimized-settings))]

      ;; Run GC with optimized settings
      (println "\nRunning optimized GC...")
      (let [db @conn
            start-time (System/currentTimeMillis)
            result (<?!! (gc/gc-storage-optimized!
                         db
                         :batch-size (:batch-size optimized-settings)
                         :parallel-batches (:parallel-batches optimized-settings)
                         :checkpoint-interval 50))]

        (println "\nGC completed:")
        (println "  Reachable:" (:reachable-count result))
        (println "  Deleted:" (:deleted-count result))
        (println "  Total time:" (/ (:duration-ms result) 1000.0) "seconds")
        (println "  Throughput:" (int (/ (:deleted-count result)
                                        (/ (:duration-ms result) 1000.0)))
                "deletes/second")))))

(defn example-gc-with-retention
  "Example showing GC with time-based retention"
  []
  (println "\n=== GC with Retention Policy Example ===\n")

  (let [config {:store {:backend :file
                       :path "/tmp/retention-gc-db"}}]

    (d/delete-database config)
    (d/create-database config)
    (def conn (d/connect config))

    ;; Create data over time
    (println "Creating temporal data...")

    ;; Old data (will be GC'd)
    @(d/transact conn [{:event/type :old :event/time "2024-01-01"}])
    (Thread/sleep 100)

    ;; Recent data (will be kept)
    @(d/transact conn [{:event/type :recent :event/time "2024-12-01"}])
    @(d/transact conn [{:event/type :current :event/time "2024-12-15"}])

    ;; Set retention date (keep only last 7 days for this example)
    (let [retention-date (java.util.Date. (- (System/currentTimeMillis)
                                            (* 7 24 60 60 1000)))]

      (println "\nRunning GC with retention date:" retention-date)
      (println "(keeping only commits from last 7 days)")

      (let [db @conn
            result (<?!! (gc/gc-storage-optimized!
                         db
                         :remove-before retention-date
                         :batch-size 100))]

        (println "\nGC with retention completed:")
        (println "  Reachable:" (:reachable-count result))
        (println "  Deleted:" (:deleted-count result))))))

(defn example-dry-run-gc
  "Example showing dry-run mode to preview what would be deleted"
  []
  (println "\n=== Dry Run GC Example ===\n")

  (let [config {:store {:backend :file
                       :path "/tmp/dry-run-gc-db"}}]

    (d/delete-database config)
    (d/create-database config)
    (def conn (d/connect config))

    ;; Create test data
    (println "Creating test data...")
    (dotimes [i 20]
      @(d/transact conn [{:item/id i :item/status (if (even? i) :active :inactive)}]))

    ;; First, dry run to see what would be deleted
    (println "\nRunning GC in dry-run mode...")
    (let [db @conn
          dry-result (<?!! (gc/gc-storage-optimized!
                           db
                           :dry-run true))]

      (println "Dry run result:")
      (println "  Would keep:" (:reachable-count dry-result) "items")
      (println "  Would delete:" (:would-delete-count dry-result) "items")

      ;; Now run actual GC
      (println "\nRunning actual GC...")
      (let [actual-result (<?!! (gc/gc-storage-optimized!
                                db
                                :batch-size 50))]

        (println "Actual result:")
        (println "  Kept:" (:reachable-count actual-result) "items")
        (println "  Deleted:" (:deleted-count actual-result) "items")))))

(defn example-monitor-gc-progress
  "Example showing how to monitor GC progress"
  []
  (println "\n=== Monitor GC Progress Example ===\n")

  (let [config {:store {:backend :file
                       :path "/tmp/monitor-gc-db"}}
        gc-id (str (java.util.UUID/randomUUID))]

    (d/delete-database config)
    (d/create-database config)
    (def conn (d/connect config))

    ;; Create substantial data
    (println "Creating test data...")
    (dotimes [batch 5]
      (let [entities (for [i (range 20)]
                      {:batch/id batch :item/id (+ (* batch 20) i)})]
        @(d/transact conn entities)))

    (let [db @conn]

      ;; Start GC in background
      (println "\nStarting GC in background with ID:" gc-id)
      (let [gc-future (future
                       (<?!! (gc/gc-storage-optimized!
                             db
                             :resume-gc-id gc-id
                             :checkpoint-interval 5
                             :batch-size 10)))]

        ;; Monitor progress
        (println "\nMonitoring GC progress...")
        (loop [checks 0]
          (when (< checks 10)
            (Thread/sleep 200)
            (let [status (<?!! (gc/get-gc-status db))]
              (when (= (:status status) :in-progress)
                (println (format "  Check %d - Visited: %d, Reachable: %d, Branches: %d/%d"
                               (inc checks)
                               (:visited-count status)
                               (:reachable-count status)
                               (:completed-branches status)
                               (+ (:completed-branches status) (:pending-branches status))))
                (recur (inc checks))))))

        ;; Wait for completion
        (let [result @gc-future]
          (println "\nGC completed:")
          (println "  Final result:" result))))))

;; =============================================================================
;; Production Patterns
;; =============================================================================

(defn production-gc-pattern
  "Production-ready GC pattern with monitoring and error handling"
  [conn & {:keys [retention-days batch-size max-duration-ms]
           :or {retention-days 7
                batch-size 1000
                max-duration-ms (* 60 60 1000)}}] ; 1 hour default

  (println "\n=== Production GC Pattern ===\n")

  (let [db @conn
        gc-id (str "prod-gc-" (System/currentTimeMillis))
        retention-date (java.util.Date. (- (System/currentTimeMillis)
                                          (* retention-days 24 60 60 1000)))
        start-time (System/currentTimeMillis)]

    (log/info "Starting production GC" gc-id
             "with retention:" retention-days "days"
             "batch-size:" batch-size)

    (try
      ;; First, check if there's an interrupted GC
      (let [existing-status (<?!! (gc/get-gc-status db))]
        (when (= (:status existing-status) :in-progress)
          (log/warn "Found existing GC in progress:" (:gc-id existing-status))
          (println "Resuming existing GC:" (:gc-id existing-status))

          ;; Resume existing GC
          (let [result (<?!! (gc/resume-gc! db (:gc-id existing-status)
                                           :batch-size batch-size))]
            (log/info "Resumed GC completed:" result)
            (return result))))

      ;; Start new GC
      (let [;; Get optimized settings
            backend-settings (gc/optimize-for-backend (:store (:config db)))
            final-batch-size (or batch-size (:batch-size backend-settings))

            ;; Run with timeout monitoring
            gc-future (future
                       (<?!! (gc/gc-storage-optimized!
                             db
                             :remove-before retention-date
                             :resume-gc-id gc-id
                             :batch-size final-batch-size
                             :parallel-batches (:parallel-batches backend-settings)
                             :checkpoint-interval 100)))]

        ;; Monitor with timeout
        (loop [elapsed 0]
          (if (> elapsed max-duration-ms)
            ;; Timeout - GC taking too long
            (do
              (log/error "GC timeout after" (/ elapsed 1000) "seconds")
              (println "ERROR: GC timed out. Can be resumed with ID:" gc-id)
              {:status :timeout
               :gc-id gc-id
               :elapsed-ms elapsed})

            ;; Check if complete
            (if (realized? gc-future)
              ;; Completed
              (let [result @gc-future
                    duration (- (System/currentTimeMillis) start-time)]
                (log/info "GC completed successfully in" (/ duration 1000) "seconds")
                (println "GC completed successfully:")
                (println "  Reachable:" (:reachable-count result))
                (println "  Deleted:" (:deleted-count result))
                (println "  Duration:" (/ duration 1000.0) "seconds")
                result)

              ;; Still running - wait and check again
              (do
                (Thread/sleep 5000)
                (let [status (<?!! (gc/get-gc-status db))]
                  (when (= (:status status) :in-progress)
                    (println (format "GC progress - Visited: %d, Branches: %d/%d"
                                   (:visited-count status)
                                   (:completed-branches status)
                                   (+ (:completed-branches status)
                                      (:pending-branches status)))))
                  (recur (+ elapsed 5000))))))))

      (catch Exception e
        (log/error e "GC failed")
        (println "ERROR: GC failed:" (.getMessage e))
        {:status :error
         :gc-id gc-id
         :error (.getMessage e)}))))

;; =============================================================================
;; CLI Interface
;; =============================================================================

(defn gc-cli
  "Command-line interface for GC operations"
  [& args]
  (let [[cmd & opts] args]
    (case cmd
      "run"
      (let [config {:store {:backend :file :path (or (first opts) "/tmp/db")}}
            conn (d/connect config)]
        (production-gc-pattern conn))

      "status"
      (let [config {:store {:backend :file :path (or (first opts) "/tmp/db")}}
            conn (d/connect config)
            status (<?!! (gc/get-gc-status @conn))]
        (println "GC Status:" status))

      "resume"
      (let [gc-id (first opts)
            config {:store {:backend :file :path (or (second opts) "/tmp/db")}}
            conn (d/connect config)]
        (if gc-id
          (let [result (<?!! (gc/resume-gc! @conn gc-id))]
            (println "Resume result:" result))
          (println "ERROR: GC ID required for resume")))

      ;; Default
      (println "Usage: gc-cli [run|status|resume <gc-id>] [db-path]"))))

;; =============================================================================
;; Running Examples
;; =============================================================================

(comment
  ;; Run individual examples
  (example-basic-gc)
  (example-resumable-gc)
  (example-large-db-gc)
  (example-gc-with-retention)
  (example-dry-run-gc)
  (example-monitor-gc-progress)

  ;; Production pattern
  (let [config {:store {:backend :file :path "/tmp/prod-db"}}]
    (d/create-database config)
    (def prod-conn (d/connect config))
    (production-gc-pattern prod-conn
                          :retention-days 30
                          :batch-size 5000))

  ;; CLI usage
  (gc-cli "run" "/tmp/my-db")
  (gc-cli "status" "/tmp/my-db")
  (gc-cli "resume" "test-gc-123" "/tmp/my-db"))