(ns examples.live-migration
  "Example usage of the live migration feature in datacamp"
  (:require [datahike.api :as d]
            [datacamp.migration :as migrate]
            [datacamp.core :as backup]
            [taoensso.timbre :as log]))

;; =============================================================================
;; Live Migration Example
;; =============================================================================

(defn example-basic-migration
  "Basic example of migrating from file to PostgreSQL database"
  []
  (println "\n=== Basic Live Migration Example ===\n")

  ;; Source database configuration (file-based)
  (let [source-config {:store {:backend :file
                               :path "/tmp/source-db"}}
        ;; Target database configuration (PostgreSQL)
        target-config {:store {:backend :jdbc
                              :dbtype "postgresql"
                              :host "localhost"
                              :port 5432
                              :dbname "target_db"
                              :user "myuser"
                              :password "mypassword"}}]

    ;; Connect to source database
    (d/create-database source-config)
    (def source-conn (d/connect source-config))

    ;; Add some initial data
    (println "Adding initial data to source database...")
    @(d/transact source-conn
                [{:db/id -1 :user/name "Alice" :user/email "alice@example.com"}
                 {:db/id -2 :user/name "Bob" :user/email "bob@example.com"}
                 {:db/id -3 :post/title "Hello World" :post/author -1}
                 {:db/id -4 :post/title "Clojure Rocks" :post/author -2}])

    ;; Start live migration
    (println "Starting live migration...")
    (def router
      (migrate/live-migrate
       source-conn
       target-config
       :database-id "production-db"
       :backup-dir "./migration-backups"
       :progress-fn (fn [progress]
                     (println "Migration progress:" (:stage progress)))
       :complete-callback (fn [state]
                           (println "Migration completed!" (:migration/id state)))))

    ;; Continue using the database during migration
    ;; The router function handles transaction routing
    (println "Adding data during migration...")
    (router [{:db/id -5 :user/name "Charlie" :user/email "charlie@example.com"}])
    (router [{:db/id -6 :post/title "Migration in Progress" :post/author -5}])

    ;; When ready, finalize the migration
    (println "Finalizing migration...")
    (let [result (router)]
      (println "Migration status:" (:status result))
      (def target-conn (:target-conn result)))

    ;; Now you can use the target connection
    (println "Querying target database:")
    (println "Users:" (d/q '[:find ?name ?email
                            :where
                            [?e :user/name ?name]
                            [?e :user/email ?email]]
                          @target-conn))))

(defn example-migration-with-verification
  "Example showing migration with transaction verification"
  []
  (println "\n=== Migration with Verification Example ===\n")

  (let [source-config {:store {:backend :mem :id "source-mem"}}
        target-config {:store {:backend :file :path "/tmp/verified-target"}}
        verified-txs (atom [])
        tx-count (atom 0)]

    ;; Setup source database
    (d/create-database source-config)
    (def source-conn (d/connect source-config))

    ;; Initial data
    @(d/transact source-conn
                [{:schema/cardinality :db.cardinality/one
                  :db/ident :order/id}
                 {:schema/cardinality :db.cardinality/one
                  :db/ident :order/total}
                 {:schema/cardinality :db.cardinality/many
                  :db/ident :order/items}])

    @(d/transact source-conn
                [{:order/id "ORD-001"
                  :order/total 99.99
                  :order/items ["item1" "item2"]}
                 {:order/id "ORD-002"
                  :order/total 149.99
                  :order/items ["item3" "item4" "item5"]}])

    ;; Start migration with verification
    (println "Starting migration with transaction verification...")
    (def router
      (migrate/live-migrate
       source-conn
       target-config
       :database-id "verified-db"
       :backup-dir "./verified-backups"
       :verify-transactions true
       :progress-fn (fn [progress]
                     (when (= (:stage progress) :applying-transaction)
                       (swap! verified-txs conj (:tx-id progress)))
                     (println "Progress:" (:stage progress)
                             (when (:progress progress)
                               (format "(%.1f%%)" (* 100 (:progress progress))))))
       :complete-callback (fn [state]
                           (println "Verified" (count @verified-txs) "transactions"))))

    ;; Simulate continuous order processing during migration
    (println "Processing orders during migration...")
    (dotimes [i 5]
      (let [order-id (str "ORD-" (format "%03d" (+ 3 i)))
            total (* 50.0 (inc i))]
        (router [{:order/id order-id
                 :order/total total
                 :order/items [(str "item" (* 10 i))]}])
        (swap! tx-count inc)
        (println "Processed order" order-id)))

    ;; Finalize
    (println "Finalizing verified migration...")
    (let [result (router)
          target-conn (:target-conn result)]

      ;; Verify all orders made it
      (let [orders (d/q '[:find ?id ?total
                         :where
                         [?e :order/id ?id]
                         [?e :order/total ?total]]
                       @target-conn)]
        (println "Total orders in target:" (count orders))
        (println "Orders added during migration:" @tx-count)
        (doseq [[id total] (sort orders)]
          (println (format "  %s: $%.2f" id total))))))

(defn example-migration-continuation
  "Example showing how to continue a migration with the same ID"
  []
  (println "\n=== Migration Continuation Example ===\n")

  (let [source-config {:store {:backend :mem :id "continuation-source"}}
        target-config {:store {:backend :file :path "/tmp/continuation-target"}}
        backup-dir "./continuation-backups"
        database-id "continuation-db"
        ;; Use a specific migration ID
        migration-id "my-migration-2024"]

    ;; Setup source
    (d/create-database source-config)
    (def source-conn (d/connect source-config))

    ;; Add initial data
    @(d/transact source-conn
                [{:db/id -1 :task/id "TASK-001" :task/status :pending}
                 {:db/id -2 :task/id "TASK-002" :task/status :active}])

    ;; Start migration with specific ID
    (println "Starting migration with ID:" migration-id)
    (def router-attempt1
      (migrate/live-migrate
       source-conn
       target-config
       :migration-id migration-id  ; Specific ID
       :database-id database-id
       :backup-dir backup-dir))

    ;; Add some transactions
    (router-attempt1 [{:task/id "TASK-003" :task/status :pending}])

    (println "Migration started, but not finalized...")
    (println "Simulating interruption/restart...\n")

    ;; Later... try to start migration with SAME ID
    ;; This will continue the existing migration
    (println "Attempting to start migration with same ID...")
    (def router-attempt2
      (migrate/live-migrate
       source-conn
       target-config
       :migration-id migration-id  ; Same ID - will continue
       :database-id database-id
       :backup-dir backup-dir))

    (println "Migration continued with same ID!")

    ;; Can continue adding transactions
    (router-attempt2 [{:task/id "TASK-004" :task/status :completed}])

    ;; Finalize
    (let [result (router-attempt2)]
      (println "Migration finalized:" (:status result))

      ;; Verify all tasks made it
      (let [target-conn (d/connect target-config)
            tasks (d/q '[:find ?id ?status
                        :where
                        [?e :task/id ?id]
                        [?e :task/status ?status]]
                      @target-conn)]
        (println "Tasks in target:")
        (doseq [[id status] (sort tasks)]
          (println (format "  %s: %s" id status)))))))

(defn example-migration-recovery
  "Example showing how to recover from an interrupted migration"
  []
  (println "\n=== Migration Recovery Example ===\n")

  (let [source-config {:store {:backend :mem :id "recovery-source"}}
        target-config {:store {:backend :file :path "/tmp/recovery-target"}}
        backup-dir "./recovery-backups"
        database-id "recovery-db"]

    ;; Setup source
    (d/create-database source-config)
    (def source-conn (d/connect source-config))

    ;; Add data
    @(d/transact source-conn
                [{:db/id -1 :product/sku "PROD-001" :product/price 29.99}
                 {:db/id -2 :product/sku "PROD-002" :product/price 49.99}])

    ;; Start migration
    (println "Starting migration...")
    (def router
      (migrate/live-migrate
       source-conn
       target-config
       :database-id database-id
       :backup-dir backup-dir))

    ;; Add some transactions
    (println "Adding transactions...")
    (router [{:product/sku "PROD-003" :product/price 39.99}])
    (router [{:product/sku "PROD-004" :product/price 59.99}])

    ;; Simulate interruption (don't finalize)
    (println "Simulating interruption - migration not finalized")
    (println "Migration left in incomplete state\n")

    ;; Later... attempt recovery
    (println "Attempting recovery of interrupted migration...")
    (let [recovery-result (migrate/recover-migration
                          backup-dir
                          database-id
                          :progress-fn (fn [p]
                                        (println "Recovery:" (:stage p))))]

      (if (fn? recovery-result)
        (do
          (println "Migration recovered successfully!")
          ;; Can continue adding transactions
          (recovery-result [{:product/sku "PROD-005" :product/price 69.99}])

          ;; Finalize the recovered migration
          (let [final-result (recovery-result)]
            (println "Recovery completed:" (:status final-result))

            ;; Verify data in target
            (let [target-conn (d/connect target-config)
                  products (d/q '[:find ?sku ?price
                                 :where
                                 [?e :product/sku ?sku]
                                 [?e :product/price ?price]]
                               @target-conn)]
              (println "Products in target after recovery:")
              (doseq [[sku price] (sort products)]
                (println (format "  %s: $%.2f" sku price))))))
        (println "No migration to recover or already completed")))))

(defn example-migration-status-monitoring
  "Example showing how to monitor migration status"
  []
  (println "\n=== Migration Status Monitoring Example ===\n")

  (let [source-config {:store {:backend :mem :id "monitor-source"}}
        target-config {:store {:backend :file :path "/tmp/monitor-target"}}
        backup-dir "./monitor-backups"
        database-id "monitor-db"
        migration-id (atom nil)]

    ;; Setup
    (d/create-database source-config)
    (def source-conn (d/connect source-config))

    ;; Initial data
    @(d/transact source-conn
                [{:metric/name "cpu_usage" :metric/value 45.2}
                 {:metric/name "memory_usage" :metric/value 67.8}])

    ;; Start migration with ID capture
    (println "Starting migration with monitoring...")
    (def router
      (migrate/live-migrate
       source-conn
       target-config
       :database-id database-id
       :backup-dir backup-dir
       :progress-fn (fn [progress]
                     (when (:migration-id progress)
                       (reset! migration-id (:migration-id progress)))
                     (when (= (:stage progress) :ready)
                       (println "Migration ready for finalization")))))

    ;; Monitor status periodically
    (println "Monitoring migration status...")
    (dotimes [i 3]
      (Thread/sleep 100)
      (when @migration-id
        (let [status (migrate/get-migration-status
                     backup-dir database-id @migration-id)]
          (println (format "Check %d - State: %s, Stats: %s"
                          (inc i)
                          (:state status)
                          (:stats status)))))

      ;; Add some data
      (router [{:metric/name (str "metric_" i) :metric/value (rand 100)}]))

    ;; Finalize
    (println "\nFinalizing migration...")
    (router)

    ;; Final status check
    (when @migration-id
      (let [final-status (migrate/get-migration-status
                         backup-dir database-id @migration-id)]
        (println "Final status:")
        (println "  State:" (:state final-status))
        (println "  Started:" (:started-at final-status))
        (println "  Completed:" (:completed-at final-status))
        (println "  Statistics:" (:stats final-status))))))

(defn example-archive-old-migrations
  "Example showing how to archive old completed migrations"
  []
  (println "\n=== Archive Old Migrations Example ===\n")

  (let [backup-dir "./archive-backups"
        database-id "archive-db"]

    ;; List all migrations first
    (println "Current migrations:")
    (let [all-migrations (migrate/list-migrations backup-dir database-id)]
      (doseq [m all-migrations]
        (println (format "  %s: %s (started: %s)"
                        (:migration-id m)
                        (:state m)
                        (:started-at m)))))

    ;; Archive old completed migrations
    (println "\nArchiving old migrations...")
    (let [archive-result (migrate/archive-completed-migrations
                         backup-dir
                         database-id
                         :older-than-hours 168)] ; Archive migrations older than 1 week

      (println "Archive result:")
      (println "  Archived count:" (:archived-count archive-result))
      (println "  Migration IDs:" (:migration-ids archive-result))

      (if (> (:archived-count archive-result) 0)
        (println "Successfully archived old migrations (kept as backups)")
        (println "No old migrations found to archive")))))

;; =============================================================================
;; Production-Ready Migration Pattern
;; =============================================================================

(defn production-migration-pattern
  "Production-ready pattern for live database migration"
  [source-conn target-config & {:keys [database-id backup-dir max-downtime-ms]
                                 :or {database-id "production"
                                      backup-dir "./prod-backups"
                                      max-downtime-ms 1000}}]

  (println "\n=== Production Migration Pattern ===\n")

  (let [start-time (System/currentTimeMillis)
        errors (atom [])
        stats (atom {:transactions-routed 0
                    :transactions-failed 0})

        ;; Enhanced progress tracking
        progress-handler (fn [progress]
                          (log/info "Migration progress" progress)
                          (case (:stage progress)
                            :failed (swap! errors conj (:error progress))
                            nil))

        ;; Enhanced completion handler
        completion-handler (fn [state]
                            (let [duration (- (System/currentTimeMillis) start-time)]
                              (log/info "Migration completed in" duration "ms")
                              (log/info "Final state:" state)))

        ;; Start migration
        router (migrate/live-migrate
               source-conn
               target-config
               :database-id database-id
               :backup-dir backup-dir
               :progress-fn progress-handler
               :complete-callback completion-handler
               :verify-transactions true)]

    ;; Wrap router with monitoring
    (fn monitored-router
      ([]
       ;; Finalization with timing check
       (let [finalize-start (System/currentTimeMillis)]
         (log/info "Starting finalization...")
         (let [result (router)]
           (let [finalize-time (- (System/currentTimeMillis) finalize-start)]
             (if (> finalize-time max-downtime-ms)
               (log/warn "Finalization took" finalize-time "ms, exceeded max" max-downtime-ms "ms")
               (log/info "Finalization completed in" finalize-time "ms"))
             result))))

      ([tx-data]
       ;; Transaction routing with error handling
       (try
         (let [result (router tx-data)]
           (swap! stats update :transactions-routed inc)
           result)
         (catch Exception e
           (swap! stats update :transactions-failed inc)
           (swap! errors conj (.getMessage e))
           (log/error e "Transaction routing failed")
           (throw e)))))))

;; =============================================================================
;; Running Examples
;; =============================================================================

(comment
  ;; Run individual examples
  (example-basic-migration)
  (example-migration-with-verification)
  (example-migration-continuation)
  (example-migration-recovery)
  (example-migration-status-monitoring)
  (example-archive-old-migrations)

  ;; Production pattern example
  (let [source-config {:store {:backend :mem :id "prod-source"}}
        target-config {:store {:backend :file :path "/tmp/prod-target"}}]
    (d/create-database source-config)
    (def prod-conn (d/connect source-config))

    ;; Get production router
    (def prod-router
      (production-migration-pattern
       prod-conn
       target-config
       :database-id "production-db"
       :backup-dir "./production-backups"
       :max-downtime-ms 500))

    ;; Use it for transactions
    (prod-router [{:db/id -1 :entity/type "production"}])

    ;; Finalize when ready
    (prod-router)))