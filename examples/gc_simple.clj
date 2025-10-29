(ns examples.gc-simple
  "Simple examples using the datacamp.core/gc! function"
  (:require [datahike.api :as d]
            [datacamp.core :as datacamp]))

;; =============================================================================
;; Simple Usage Examples
;; =============================================================================

(defn example-basic
  "Most basic usage - safe dry run by default"
  []
  (let [config {:store {:backend :file :path "/tmp/test-db"}}
        _ (d/delete-database config)
        _ (d/create-database config)
        conn (d/connect config)]

    ;; Add some test data
    (dotimes [i 10]
      @(d/transact conn [{:test/id i :test/value (str "value-" i)}]))

    ;; Safe dry run (default)
    (println "\n1. Dry run (default - safe):")
    (let [result (datacamp/gc! conn)]
      (println "  Would keep:" (:reachable-count result) "items")
      (println "  Would delete:" (:would-delete-count result) "items"))

    ;; Actual GC
    (println "\n2. Actual GC:")
    (let [result (datacamp/gc! conn :dry-run false)]
      (println "  Kept:" (:reachable-count result) "items")
      (println "  Deleted:" (:deleted-count result) "items"))))

(defn example-with-retention
  "GC with retention policy"
  []
  (let [config {:store {:backend :file :path "/tmp/retention-db"}}
        _ (d/delete-database config)
        _ (d/create-database config)
        conn (d/connect config)]

    ;; Add test data
    (dotimes [i 5]
      @(d/transact conn [{:data/id i}])
      (Thread/sleep 10))

    ;; Dry run with 30 day retention
    (println "\nGC with 30 day retention (dry run):")
    (let [result (datacamp/gc! conn :retention-days 30)]
      (println "  Would keep recent data from last 30 days")
      (println "  Would keep:" (:reachable-count result) "items")
      (println "  Would delete:" (:would-delete-count result) "items"))

    ;; Actual GC with retention
    (println "\nActual GC with 7 day retention:")
    (let [result (datacamp/gc! conn :dry-run false :retention-days 7)]
      (println "  Kept:" (:reachable-count result) "items")
      (println "  Deleted:" (:deleted-count result) "items"))))

(defn example-check-status
  "Check GC status"
  []
  (let [config {:store {:backend :file :path "/tmp/status-db"}}
        _ (d/delete-database config)
        _ (d/create-database config)
        conn (d/connect config)]

    ;; Check status - no GC running
    (println "\nChecking GC status:")
    (let [status (datacamp/gc-status conn)]
      (println "  Status:" (:status status)))

    ;; Start a GC in background
    (future
      (datacamp/gc! conn :dry-run false))

    ;; Check status while running
    (Thread/sleep 100)
    (let [status (datacamp/gc-status conn)]
      (when (= (:status status) :in-progress)
        (println "\nGC in progress:")
        (println "  GC ID:" (:gc-id status))
        (println "  Visited:" (:visited-count status) "commits")
        (println "  Reachable:" (:reachable-count status) "items")))))

(defn example-auto-resume
  "Demonstrate automatic resume functionality"
  []
  (let [config {:store {:backend :file :path "/tmp/resume-db"}}
        _ (d/delete-database config)
        _ (d/create-database config)
        conn (d/connect config)]

    ;; Add substantial data
    (println "\nCreating test data...")
    (dotimes [batch 5]
      (let [entities (for [i (range 20)]
                      {:batch/id batch :item/id (+ (* batch 20) i)})]
        @(d/transact conn entities)))

    ;; Start GC in background
    (println "Starting GC...")
    (let [gc-future (future
                     (datacamp/gc! conn :dry-run false :checkpoint-interval 10))]

      ;; Simulate interruption after a moment
      (Thread/sleep 200)
      (future-cancel gc-future)
      (println "GC interrupted!")

      ;; Check status
      (let [status (datacamp/gc-status conn)]
        (when (= (:status status) :in-progress)
          (println "Interrupted GC found:")
          (println "  GC ID:" (:gc-id status))
          (println "  Progress:" (:visited-count status) "commits visited")))

      ;; Call gc! again - it will auto-resume
      (println "\nCalling gc! again (will auto-resume)...")
      (let [result (datacamp/gc! conn :dry-run false)]
        (println "GC completed!")
        (println "  Resumed?:" (:resumed? result))
        (println "  Deleted:" (:deleted-count result) "items")))))

;; =============================================================================
;; Production Pattern
;; =============================================================================

(defn production-example
  "Production-ready pattern using datacamp.core/gc!"
  [conn]

  ;; 1. Always start with dry run
  (println "\n=== Production GC Pattern ===")
  (println "\nStep 1: Dry run to preview")
  (let [dry-result (datacamp/gc! conn :retention-days 30)]
    (println "Dry run results:")
    (println "  Would keep:" (:reachable-count dry-result))
    (println "  Would delete:" (:would-delete-count dry-result))

    ;; 2. Confirm before proceeding
    (when (> (:would-delete-count dry-result) 0)
      (println "\nStep 2: Actual GC")
      (print "Proceed with deletion? (yes/no): ")
      (flush)

      (when (= "yes" (read-line))
        (println "Running actual GC...")
        (let [result (datacamp/gc! conn
                                  :dry-run false
                                  :retention-days 30)]
          (println "GC completed:")
          (println "  Deleted:" (:deleted-count result) "items")
          (println "  Duration:" (/ (:duration-ms result) 1000.0) "seconds")

          ;; 3. Verify status
          (let [status (datacamp/gc-status conn)]
            (println "Final status:" (:status status))))))))

;; =============================================================================
;; Command Line Interface
;; =============================================================================

(defn -main
  "Simple CLI for GC operations

  Usage:
    clojure -M -m examples.gc-simple dry-run [db-path]
    clojure -M -m examples.gc-simple run [db-path]
    clojure -M -m examples.gc-simple status [db-path]"
  [& args]
  (let [[cmd db-path] args
        db-path (or db-path "/tmp/cli-db")
        config {:store {:backend :file :path db-path}}
        _ (d/create-database config)
        conn (d/connect config)]

    (case cmd
      "dry-run"
      (do
        (println "Running GC dry run on" db-path)
        (let [result (datacamp/gc! conn)]
          (println "Would keep:" (:reachable-count result))
          (println "Would delete:" (:would-delete-count result))))

      "run"
      (do
        (println "Running actual GC on" db-path)
        (let [result (datacamp/gc! conn :dry-run false)]
          (println "Deleted:" (:deleted-count result) "items")
          (println "Duration:" (/ (:duration-ms result) 1000.0) "seconds")))

      "status"
      (let [status (datacamp/gc-status conn)]
        (if (= (:status status) :in-progress)
          (do
            (println "GC in progress:")
            (println "  ID:" (:gc-id status))
            (println "  Visited:" (:visited-count status))
            (println "  Reachable:" (:reachable-count status)))
          (println "No GC in progress")))

      ;; Default
      (do
        (println "Usage:")
        (println "  dry-run [db-path]  - Preview what would be deleted")
        (println "  run [db-path]      - Run actual GC")
        (println "  status [db-path]   - Check GC status")))))

;; =============================================================================
;; Run Examples
;; =============================================================================

(comment
  ;; Basic examples
  (example-basic)
  (example-with-retention)
  (example-check-status)
  (example-auto-resume)

  ;; Production pattern with your database
  (let [config {:store {:backend :jdbc
                       :dbtype "postgresql"
                       :host "localhost"
                       :dbname "mydb"}}
        conn (d/connect config)]
    (production-example conn))

  ;; CLI
  (-main "dry-run")
  (-main "run")
  (-main "status"))