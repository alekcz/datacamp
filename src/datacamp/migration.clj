(ns datacamp.migration
  "Live migration functionality for Datahike databases with zero downtime"
  (:require [datahike.api :as d]
            [taoensso.timbre :as log]
            [datacamp.metadata :as meta]
            [datacamp.directory :as dir]
            [datacamp.utils :as utils]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit]
           [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

;; Forward declaration for mutual recursion
(declare recover-migration)

;; =============================================================================
;; Migration State Management
;; =============================================================================

(defn create-migration-manifest
  "Create a manifest for tracking migration state"
  [{:keys [migration-id source-config target-config database-id started-at]}]
  {:migration/id migration-id
   :migration/version "1.0.0"
   :migration/database-id database-id
   :migration/source-config source-config
   :migration/target-config target-config
   :migration/state :initializing  ; :initializing :backup :catching-up :finalizing :completed :failed
   :migration/started-at started-at
   :migration/completed-at nil
   :migration/initial-backup-id nil
   :migration/transaction-log-path nil
   :migration/last-applied-tx nil
   :migration/stats {:transactions-captured 0
                     :transactions-applied 0
                     :transactions-pending 0
                     :errors []}})

(defn update-migration-state
  "Update migration state in the backup directory"
  [base-dir database-id migration-id updates]
  (let [migration-path (str base-dir "/" database-id "/migrations/" migration-id)
        manifest-path (str migration-path "/migration-manifest.edn")
        _ (dir/ensure-directory migration-path)
        current-manifest (if (dir/file-exists? manifest-path)
                          (meta/read-edn-from-file manifest-path)
                          {})
        updated-manifest (merge current-manifest updates)]
    (meta/write-edn-to-file manifest-path updated-manifest)
    updated-manifest))

(defn read-migration-state
  "Read migration state from disk"
  [base-dir database-id migration-id]
  (let [manifest-path (str base-dir "/" database-id "/migrations/" migration-id "/migration-manifest.edn")]
    (when (dir/file-exists? manifest-path)
      (meta/read-edn-from-file manifest-path))))

(defn find-migration-by-id
  "Find a migration by its ID"
  [base-dir database-id migration-id]
  (read-migration-state base-dir database-id migration-id))

(defn find-active-migration
  "Find any active migration for a database"
  [base-dir database-id]
  (let [migrations-dir (str base-dir "/" database-id "/migrations")]
    (when (dir/file-exists? migrations-dir)
      (let [migration-dirs (.listFiles (io/file migrations-dir))
            active-migrations (keep (fn [dir]
                                     (when (.isDirectory dir)
                                       (let [migration-id (.getName dir)
                                             state (read-migration-state base-dir database-id migration-id)]
                                         (when (and state
                                                  (not= (:migration/state state) :completed)
                                                  (not= (:migration/state state) :failed)
                                                  (not= (:migration/state state) :archived))
                                           state))))
                                   migration-dirs)]
        (first active-migrations)))))

;; =============================================================================
;; Transaction Capture and Replay
;; =============================================================================

(defrecord TransactionCapture [queue listener-key source-conn file-writer captured-count verified-count])

(defn write-transaction-to-log
  "Write a transaction to the EDN log file"
  [writer tx-data tx-id]
  ;; Convert Datoms to plain maps for serialization
  (let [tx-maps (mapv (fn [datom]
                        (if (map? datom)
                          datom
                          {:e (:e datom)
                           :a (:a datom)
                           :v (:v datom)
                           :t (:tx datom)
                           :added (:added datom)}))
                      tx-data)]
    ;; Synchronize writes to prevent concurrent writes from interleaving
    (locking writer
      (.write writer (pr-str {:tx/id tx-id
                              :tx/data tx-maps
                              :tx/timestamp (utils/current-timestamp)}))
      (.write writer "\n")
      (.flush writer))))

(defn start-transaction-capture
  "Start capturing transactions from source database"
  [source-conn log-file-path]
  (let [queue (LinkedBlockingQueue. 10000)
        captured-count (atom 0)
        verified-count (atom 0)
        file-writer (io/writer log-file-path :append true)

        ;; Listen to transactions on source
        listener-key (d/listen source-conn ::migration-capture
                              (fn [{:keys [tx-data db-after]}]
                                (try
                                  ;; Extract the transaction data
                                  (let [tx-id (d/q '[:find ?tx .
                                                    :in $ ?tx-data
                                                    :where
                                                    [?tx :db/txInstant]
                                                    [(some #{?tx} ?tx-data)]]
                                                  db-after tx-data)
                                        tx-datoms (vec (filter #(not= (:e %) tx-id) tx-data))]

                                    ;; Add to queue for processing
                                    (.offer queue {:tx-id tx-id
                                                  :tx-datoms tx-datoms
                                                  :timestamp (System/currentTimeMillis)})
                                    (swap! captured-count inc)

                                    ;; Write to persistent log
                                    (write-transaction-to-log file-writer tx-datoms tx-id))
                                  (catch Exception e
                                    (log/error e "Error capturing transaction")))))]

    (log/info "Started transaction capture, listening for changes...")

    (->TransactionCapture queue listener-key source-conn
                         file-writer captured-count verified-count)))

(defn stop-transaction-capture
  "Stop capturing transactions and flush any pending writes"
  [capture]
  (when capture
    (d/unlisten (:source-conn capture) (:listener-key capture))
    ;; Flush any remaining items in the queue before closing
    (Thread/sleep 100) ;; Give listener a moment to finish
    (.flush (:file-writer capture))
    (.close (:file-writer capture))
    (log/info "Stopped transaction capture. Captured" @(:captured-count capture) "transactions")))

(defn read-transaction-log
  "Read transactions from the log file"
  [log-file-path]
  (when (dir/file-exists? log-file-path)
    (with-open [reader (io/reader log-file-path)]
      (doall
        (map edn/read-string (line-seq reader))))))

(defn apply-captured-transactions
  "Apply captured transactions to target database"
  [target-conn transactions progress-fn]
  (let [total (count transactions)
        applied (atom 0)]
    (doseq [transaction transactions]
      (let [tx-id (:tx/id transaction)
            tx-data (:tx/data transaction)]
        (try
          (when progress-fn
            (progress-fn {:stage :applying-transaction
                         :tx-id tx-id
                         :progress (/ @applied total)}))

          ;; Convert transaction data to proper format and transact
          ;; tx-data is already in map format from write-transaction-to-log
          (let [tx-maps (mapv (fn [datom]
                               (if (and (map? datom) (contains? datom :e))
                                 ;; It's a datom map from our log format
                                 {:db/id (:e datom)
                                  (keyword (namespace (:a datom)) (name (:a datom))) (:v datom)}
                                 ;; It's already a proper transaction map
                                 datom))
                             tx-data)]
            (d/transact target-conn tx-maps))

        (swap! applied inc)

        (catch Exception e
          (log/error e "Failed to apply transaction" tx-id)))))

    {:total total :applied @applied}))

;; =============================================================================
;; Transaction Router
;; =============================================================================

(defn create-transaction-router
  "Create a function that routes transactions based on migration state"
  [source-conn target-conn migration-state-atom capture progress-fn complete-callback]
  (let [router-active (AtomicBoolean. true)
        pending-txs (atom [])]

    (fn transaction-router
      ([]
       ;; Called with no args - check migration status and switch if needed
       (let [state @migration-state-atom]
         (case (:migration/state state)
           :completed
           (do
             (log/info "Migration completed, routing to target database")
             (.set router-active false)
             (when complete-callback
               (complete-callback state))
             :switched-to-target)

           :failed
           (do
             (log/error "Migration failed, continuing with source database")
             :migration-failed)

           ;; Still migrating
           :still-migrating)))

      ([tx-data]
       ;; Called with transaction data - route to appropriate database
       (if (.get router-active)
         (try
           ;; During migration, write to source and track
           (let [result (d/transact source-conn tx-data)]
             (swap! pending-txs conj {:tx-data tx-data
                                     :result result
                                     :timestamp (System/currentTimeMillis)})
             ;; Also capture transaction directly from the result to ensure we don't miss any
             (when (and capture (:file-writer capture))
               (try
                 (let [tx-report result
                       ;; Find the tx entity id from the datoms
                       tx-datoms (:tx-data tx-report)
                       tx-id (some #(when (= (:a %) :db/txInstant) (:e %)) tx-datoms)
                       ;; Filter out the transaction metadata datoms
                       user-datoms (vec (filter #(not= (:e %) tx-id) tx-datoms))]
                   (when (seq user-datoms)
                     (write-transaction-to-log (:file-writer capture) user-datoms tx-id)
                     (swap! (:captured-count capture) inc)))
                 (catch Exception e
                   (log/error e "Failed to capture transaction from router"))))
             result)
           (catch Exception e
             (log/error e "Failed to transact to source database")
             (throw e)))

         ;; After migration, write to target
         (try
           (d/transact target-conn tx-data)
           (catch Exception e
             (log/error e "Failed to transact to target database")
             (throw e))))))))

;; =============================================================================
;; Live Migration
;; =============================================================================

(defn live-migrate
  "Perform live migration from source to target database configuration.

  Parameters:
  - source-conn: Current database connection
  - target-config: Configuration for target database
  - opts: Migration options
    :migration-id - Specific migration ID to use (optional, generates one if not provided)
    :database-id - Database identifier (default: \"default-db\")
    :backup-dir - Directory for backups and migration state (default: \"./backups\")
    :progress-fn - Function called with progress updates
    :complete-callback - Function called when migration completes
    :verify-transactions - Verify each transaction was captured (default: true)

  Returns a transaction router function that should be used for all database writes"
  [source-conn target-config & {:keys [migration-id database-id backup-dir progress-fn
                                       complete-callback verify-transactions]
                                :or {database-id "default-db"
                                     backup-dir "./backups"
                                     verify-transactions true}}]

  (let [migration-id (or migration-id (utils/generate-backup-id))
        migration-state (atom nil)
        started-at (utils/current-timestamp)]

    (log/info "Starting/continuing live migration" migration-id "for database" database-id)

    (try
      ;; Check if this specific migration already exists
      (if-let [existing (find-migration-by-id backup-dir database-id migration-id)]
        (do
          ;; Migration with this ID exists - continue it based on state
          (log/info "Found existing migration" migration-id "in state" (:migration/state existing))
          (case (:migration/state existing)
            :completed
            (do
              (log/info "Migration already completed, connecting to target")
              (let [target-conn (d/connect target-config)]
                (fn completed-router
                  ([] {:status :already-completed
                       :target-conn target-conn
                       :migration-id migration-id})
                  ([tx-data] (d/transact target-conn tx-data)))))

            :archived
            (do
              (log/info "Migration was archived, treating as completed")
              (let [target-conn (d/connect target-config)]
                (fn archived-router
                  ([] {:status :archived
                       :target-conn target-conn
                       :migration-id migration-id})
                  ([tx-data] (d/transact target-conn tx-data)))))

            :failed
            (throw (ex-info "Previous migration with this ID failed"
                         {:migration-id migration-id
                          :previous-state existing}))

            ;; Continue the in-progress migration
            (do
              (log/info "Continuing migration from state" (:migration/state existing))
              (recover-migration backup-dir database-id
                               :progress-fn progress-fn
                               :complete-callback complete-callback))))

        ;; No existing migration with this ID - check for other active migrations
        (do
          (when-let [other-active (find-active-migration backup-dir database-id)]
            (throw (ex-info "Another migration is already in progress"
                         {:active-migration (:migration/id other-active)
                          :requested-migration migration-id})))

          ;; Initialize migration state
          (let [source-config (-> source-conn deref :config)
            initial-state (create-migration-manifest
                          {:migration-id migration-id
                           :source-config source-config
                           :target-config target-config
                           :database-id database-id
                           :started-at started-at})]

        (reset! migration-state initial-state)
        (update-migration-state backup-dir database-id migration-id initial-state)

        (when progress-fn
          (progress-fn {:stage :initialized
                       :migration-id migration-id}))

        ;; Step 1: Start capturing transactions
        (log/info "Step 1: Starting transaction capture...")
        (let [log-path (str backup-dir "/" database-id "/migrations/"
                           migration-id "/transactions.edn")
              _ (dir/ensure-directory (str backup-dir "/" database-id "/migrations/" migration-id))
              capture (start-transaction-capture source-conn log-path)]

          (swap! migration-state assoc
                 :migration/state :backup
                 :migration/transaction-log-path log-path)
          (update-migration-state backup-dir database-id migration-id @migration-state)

          ;; Step 2: Create initial backup
          (log/info "Step 2: Creating initial backup...")
          (when progress-fn
            (progress-fn {:stage :creating-backup}))

          ;; Dynamically resolve backup functions to avoid circular dependency
          (let [backup-ns (requiring-resolve 'datacamp.core/backup-to-directory)
                restore-ns (requiring-resolve 'datacamp.core/restore-from-directory)
                backup-result (backup-ns
                              source-conn
                              {:path backup-dir}
                              :database-id database-id)]

            (when-not (:success backup-result)
              (throw (ex-info "Initial backup failed" backup-result)))

            (swap! migration-state assoc
                   :migration/initial-backup-id (:backup-id backup-result)
                   :migration/state :restore)
            (update-migration-state backup-dir database-id migration-id @migration-state)

            ;; Step 3: Create and restore to target database
            (log/info "Step 3: Restoring to target database...")
            (when progress-fn
              (progress-fn {:stage :restoring-to-target}))

            ;; Create database if it doesn't exist
            (try
              (d/create-database target-config)
              (catch Exception e
                (if (re-find #"already exists" (.getMessage e))
                  (log/info "Target database already exists, will connect to it")
                  (throw e))))

            (let [target-conn (d/connect target-config)
                  restore-result (restore-ns
                                 target-conn
                                 {:path backup-dir}
                                 (:backup-id backup-result)
                                 :database-id database-id
                                 :progress-fn (when progress-fn
                                               (fn [p]
                                                 (progress-fn (assoc p :stage :restore-progress)))))]

              (when-not (:success restore-result)
                (throw (ex-info "Restore to target failed" restore-result)))

              ;; Step 4: Apply captured transactions (catch up)
              (log/info "Step 4: Catching up with captured transactions...")
              (swap! migration-state assoc :migration/state :catching-up)
              (update-migration-state backup-dir database-id migration-id @migration-state)

              (when progress-fn
                (progress-fn {:stage :catching-up
                             :captured-count @(:captured-count capture)}))

              ;; Read and apply transactions from log
              (let [transactions (read-transaction-log log-path)
                    apply-result (apply-captured-transactions
                                 target-conn transactions progress-fn)]

                (log/info "Applied" (:applied apply-result) "of"
                         (:total apply-result) "captured transactions")

                (swap! migration-state update :migration/stats merge
                       {:transactions-captured (:total apply-result)
                        :transactions-applied (:applied apply-result)})
                (update-migration-state backup-dir database-id migration-id @migration-state))

              ;; Step 5: Create router function
              (log/info "Step 5: Creating transaction router...")
              (let [router (create-transaction-router
                           source-conn target-conn migration-state
                           capture progress-fn complete-callback)]

                ;; Step 6: Mark as ready for finalization
                (swap! migration-state assoc
                       :migration/state :ready-to-finalize
                       :migration/target-conn-info {:config target-config})
                (update-migration-state backup-dir database-id migration-id @migration-state)

                (when progress-fn
                  (progress-fn {:stage :ready
                               :migration-id migration-id
                               :message "Migration ready. Call router with no args to finalize"}))

                (log/info "Migration prepared successfully. Use router function for transactions.")
                (log/info "Call (router) with no arguments to finalize migration.")

                ;; Return router function with migration control
                (fn migration-router
                  ([]
                   ;; No args - finalize migration
                   (log/info "Finalizing migration...")
                   (stop-transaction-capture capture)

                   ;; Apply any remaining transactions
                   (let [final-txs (read-transaction-log log-path)
                         already-applied (:transactions-applied (:migration/stats @migration-state))
                         remaining (drop already-applied final-txs)]
                     (when (seq remaining)
                       (log/info "Applying" (count remaining) "final transactions...")
                       (apply-captured-transactions target-conn remaining progress-fn)))

                   ;; Mark migration as complete
                   (swap! migration-state assoc
                          :migration/state :completed
                          :migration/completed-at (utils/current-timestamp))
                   (update-migration-state backup-dir database-id migration-id @migration-state)

                   (when complete-callback
                     (complete-callback @migration-state))

                   (log/info "Migration completed successfully!")
                   {:status :completed
                    :target-conn target-conn
                    :migration-id migration-id})

                  ([tx-data]
                   ;; With transaction data - route appropriately
                   (router tx-data)))))))))) ;; Close: fn, let, let, let, do, if-let

      (catch Exception e
        (log/error e "Migration failed")
        (when migration-state
          (swap! migration-state assoc
                 :migration/state :failed
                 :migration/error (.getMessage e))
          (update-migration-state backup-dir database-id migration-id @migration-state))
        (throw e))))) ;; Close: try, let (line 255), defn

;; =============================================================================
;; Recovery Functions
;; =============================================================================

(defn recover-migration
  "Recover an interrupted migration and continue from where it left off"
  [backup-dir database-id & {:keys [progress-fn complete-callback]}]
  (if-let [migration-state (find-active-migration backup-dir database-id)]
    (let [migration-id (:migration/id migration-state)]
      (log/info "Recovering migration" migration-id "in state" (:migration/state migration-state))

      (case (:migration/state migration-state)
        :completed
        (do
          (log/info "Migration already completed, connecting to target")
          (let [target-config (:migration/target-config migration-state)
                target-conn (d/connect target-config)]
            {:status :already-completed
             :target-conn target-conn
             :migration-id migration-id}))

        :failed
        (do
          (log/error "Previous migration failed, manual intervention required")
          {:status :failed
           :migration-id migration-id
           :error "Previous migration failed"})

        ;; Resume migration
        (let [source-config (:migration/source-config migration-state)
              target-config (:migration/target-config migration-state)
              source-conn (d/connect source-config)]

          ;; Continue based on state
          (case (:migration/state migration-state)
            :backup
            (do
              (log/info "Resuming from backup stage...")
              (live-migrate source-conn target-config
                           :database-id database-id
                           :backup-dir backup-dir
                           :progress-fn progress-fn
                           :complete-callback complete-callback))

            (:restore :catching-up :ready-to-finalize)
            (do
              (log/info "Resuming from" (:migration/state migration-state) "stage...")
              ;; Reconnect to target and continue
              (let [target-conn (d/connect target-config)
                    log-path (:migration/transaction-log-path migration-state)
                    capture (when log-path
                             (start-transaction-capture source-conn log-path))
                    base-router (create-transaction-router source-conn target-conn
                                                          (atom migration-state) capture
                                                          progress-fn complete-callback)]

                ;; Apply any unapplied transactions
                (when (and log-path (= (:migration/state migration-state) :catching-up))
                  (let [transactions (read-transaction-log log-path)
                        already-applied (get-in migration-state [:migration/stats :transactions-applied] 0)
                        remaining (drop already-applied transactions)]
                    (when (seq remaining)
                      (log/info "Applying" (count remaining) "remaining transactions...")
                      (apply-captured-transactions target-conn remaining progress-fn))))

                ;; Return router for continued use - wrap it to handle finalization
                ;; Wrap the router to handle finalization like live-migrate does
                (fn recovery-router
                  ([]
                   ;; No args - finalize migration
                   (log/info "Finalizing recovered migration...")
                   (when capture
                     (stop-transaction-capture capture))

                   ;; Apply any remaining transactions
                   (when log-path
                     (let [final-txs (read-transaction-log log-path)
                           already-applied (get-in migration-state [:migration/stats :transactions-applied] 0)
                           remaining (drop already-applied final-txs)]
                       (when (seq remaining)
                         (log/info "Applying" (count remaining) "final transactions...")
                         (apply-captured-transactions target-conn remaining progress-fn))))

                   ;; Update and save migration state
                   (let [updated-state (assoc migration-state
                                             :migration/state :completed
                                             :migration/completed-at (utils/current-timestamp))]
                     (update-migration-state backup-dir database-id migration-id updated-state)

                     (when complete-callback
                       (complete-callback updated-state))

                     (log/info "Migration completed successfully!")
                     {:status :completed
                      :target-conn target-conn
                      :migration-id migration-id}))

                  ([tx-data]
                   ;; With transaction data - delegate to base router
                   (base-router tx-data)))))

            ;; Unknown state
            (throw (ex-info "Unknown migration state" {:state (:migration/state migration-state)}))))))

    (do
      (log/info "No active migration found")
      {:status :no-migration})))

;; =============================================================================
;; Utility Functions
;; =============================================================================

(defn get-migration-status
  "Get the current status of a migration"
  [backup-dir database-id migration-id]
  (if-let [state (read-migration-state backup-dir database-id migration-id)]
    {:status :found
     :migration-id migration-id
     :state (:migration/state state)
     :started-at (:migration/started-at state)
     :completed-at (:migration/completed-at state)
     :archived-at (:migration/archived-at state)
     :stats (:migration/stats state)}
    {:status :not-found}))

(defn list-migrations
  "List all migrations for a database, including archived ones"
  [backup-dir database-id & {:keys [include-archived] :or {include-archived true}}]
  (let [migrations-dir (str backup-dir "/" database-id "/migrations")]
    (if (dir/file-exists? migrations-dir)
      (let [migration-dirs (.listFiles (io/file migrations-dir))
            migrations (keep (fn [dir]
                             (when (.isDirectory dir)
                               (let [migration-id (.getName dir)
                                     state (read-migration-state backup-dir database-id migration-id)]
                                 (when (and state
                                           (or include-archived
                                               (not= (:migration/state state) :archived)))
                                   {:migration-id migration-id
                                    :state (:migration/state state)
                                    :started-at (:migration/started-at state)
                                    :completed-at (:migration/completed-at state)
                                    :archived-at (:migration/archived-at state)
                                    :backup-id (:migration/initial-backup-id state)}))))
                           migration-dirs)]
        (sort-by :started-at #(compare %2 %1) migrations))
      [])))

(defn archive-completed-migrations
  "Archive completed migrations older than specified hours.
  Marks them as archived but keeps the data as it serves as a backup."
  [backup-dir database-id & {:keys [older-than-hours] :or {older-than-hours 168}}] ; 1 week default
  (let [migrations-dir (str backup-dir "/" database-id "/migrations")]
    (when (dir/file-exists? migrations-dir)
      (let [migration-dirs (.listFiles (io/file migrations-dir))
            archived (atom [])]
        (doseq [dir migration-dirs]
          (when (.isDirectory dir)
            (let [migration-id (.getName dir)
                  state (read-migration-state backup-dir database-id migration-id)]
              (when (and state
                        (= (:migration/state state) :completed)
                        (:migration/completed-at state)
                        (> (utils/hours-since (:migration/completed-at state))
                           older-than-hours))
                ;; Mark as archived instead of deleting
                (let [archived-state (assoc state
                                           :migration/state :archived
                                           :migration/archived-at (utils/current-timestamp)
                                           :migration/archived-reason "Auto-archived after completion")]
                  (update-migration-state backup-dir database-id migration-id archived-state))
                (swap! archived conj migration-id)
                (log/info "Archived migration" migration-id)))))
        {:archived-count (count @archived)
         :migration-ids @archived}))))

(defn cleanup-completed-migrations
  "Deprecated: Use archive-completed-migrations instead.
  This function now archives migrations instead of deleting them."
  [backup-dir database-id & opts]
  (log/warn "cleanup-completed-migrations is deprecated. Use archive-completed-migrations instead.")
  (apply archive-completed-migrations backup-dir database-id opts))