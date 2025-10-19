(ns datacamp.migration-test
  "Integration tests for live migration functionality"
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.migration :as migrate]
            [datacamp.test-helpers :refer :all]
            [datacamp.complex-test :as complex-test]
            [datacamp.directory :as directory]
            [datacamp.metadata :as metadata]
            [taoensso.timbre :as log]))

(deftest test-basic-live-migration
  (testing "Basic live migration from memory to file database"
    (with-test-dir test-dir
      (with-test-dir target-dir
        (with-test-db source-config {:store {:backend :mem :id "source-db"}}
          (let [source-conn (d/connect source-config)
                target-config {:store {:backend :file :path target-dir}}
                progress-calls (atom [])
                complete-called (atom false)]

            ;; Populate source database
            (populate-test-db source-conn :user-count 10 :post-count 20)
            (let [initial-datom-count (count (d/datoms @source-conn :eavt))]

              ;; Start live migration
              (let [router (migrate/live-migrate
                           source-conn target-config
                           :database-id "test-migration"
                           :backup-dir test-dir
                           :progress-fn (fn [progress]
                                         (swap! progress-calls conj progress))
                           :complete-callback (fn [state]
                                              (reset! complete-called true)))]

                ;; Verify migration started
                (is (fn? router) "Should return a router function")
                (is (> (count @progress-calls) 0) "Should have progress updates")

                ;; Add some transactions during migration
                (router [{:db/id -1 :user/name "NewUser1"}])
                (router [{:db/id -2 :user/name "NewUser2"}])
                (router [{:db/id -3 :post/title "NewPost1" :post/author [:user/name "NewUser1"]}])

                ;; Wait a bit for transactions to be captured
                (Thread/sleep 100)

                ;; Finalize migration
                (let [result (router)]
                  (is (= :completed (:status result)) "Migration should complete")
                  (is @complete-called "Complete callback should be called")

                  ;; Verify target database has all data
                  (let [target-conn (:target-conn result)
                        target-datom-count (count (d/datoms @target-conn :eavt))
                        users (d/q '[:find [?name ...]
                                    :where [_ :user/name ?name]]
                                  @target-conn)
                        posts (d/q '[:find [?title ...]
                                    :where [_ :post/title ?title]]
                                  @target-conn)]

                    ;; Should have original data plus new transactions
                    (is (>= target-datom-count initial-datom-count)
                        "Target should have at least initial datoms")
                    (is (contains? (set users) "NewUser1") "Should have new user 1")
                    (is (contains? (set users) "NewUser2") "Should have new user 2")
                    (is (contains? (set posts) "NewPost1") "Should have new post"))))))

          ;; Cleanup
          (cleanup-test-db source-config))))))

(deftest test-migration-with-continuous-writes
  (testing "Migration with continuous writes during the process"
    (with-test-dir test-dir
      (with-test-dir target-dir
        (with-test-db source-config {:store {:backend :mem :id "continuous-source"}}
          (let [source-conn (d/connect source-config)
                target-config {:store {:backend :file :path target-dir}}
                write-thread-active (atom true)
                writes-completed (atom 0)]

            ;; Populate initial data
            (populate-test-db source-conn :user-count 5 :post-count 10)

            ;; Start background writer thread
            (let [writer-thread (future
                                 (while @write-thread-active
                                   (try
                                     @(d/transact source-conn
                                                 [{:db/id -1
                                                   :user/name (str "BackgroundUser" @writes-completed)}])
                                     (swap! writes-completed inc)
                                     (Thread/sleep 50)
                                     (catch Exception e
                                       (log/warn "Write failed:" (.getMessage e))))))]

              (try
                ;; Start migration
                (let [router (migrate/live-migrate
                             source-conn target-config
                             :database-id "continuous-test"
                             :backup-dir test-dir)]

                  ;; Let some writes happen during migration
                  (Thread/sleep 500)

                  ;; Now use router for new writes
                  (router [{:db/id -1 :user/name "RouterUser1"}])
                  (router [{:db/id -2 :user/name "RouterUser2"}])

                  ;; Stop background writer
                  (reset! write-thread-active false)
                  @writer-thread

                  ;; Finalize migration
                  (let [result (router)
                        target-conn (:target-conn result)]

                    ;; Verify all users made it to target
                    (let [all-users (d/q '[:find [?name ...]
                                          :where [_ :user/name ?name]]
                                        @target-conn)
                          background-users (filter #(re-find #"BackgroundUser" %) all-users)
                          router-users (filter #(re-find #"RouterUser" %) all-users)]

                      (is (> (count background-users) 0) "Should have background users")
                      (is (= 2 (count router-users)) "Should have both router users")
                      (log/info "Migrated" (count background-users) "background writes"))))

                (finally
                  (reset! write-thread-active false))))

            ;; Cleanup
            (cleanup-test-db source-config)))))))

(deftest test-migration-recovery
  (testing "Recovery of interrupted migration"
    (with-test-dir test-dir
      (with-test-dir target-dir
        (with-test-db source-config {:store {:backend :mem :id "recovery-source"}}
          (let [source-conn (d/connect source-config)
                target-config {:store {:backend :file :path target-dir}}]

            ;; Populate source
            (populate-test-db source-conn :user-count 10 :post-count 20)

            ;; Start migration but simulate interruption
            (let [migration-id (atom nil)
                  router (migrate/live-migrate
                         source-conn target-config
                         :database-id "recovery-test"
                         :backup-dir test-dir
                         :progress-fn (fn [progress]
                                       (when (= (:stage progress) :initialized)
                                         (reset! migration-id (:migration-id progress)))))]

              ;; Add some transactions
              (router [{:db/id -1 :user/name "BeforeInterrupt1"}])
              (router [{:db/id -2 :user/name "BeforeInterrupt2"}])

              ;; Simulate interruption (don't finalize)
              ;; Now attempt recovery
              (let [recovery-result (migrate/recover-migration
                                    test-dir "recovery-test"
                                    :progress-fn (fn [p] (log/info "Recovery progress:" p)))]

                (is (fn? recovery-result) "Should return router function on recovery")

                ;; Add more transactions after recovery
                (recovery-result [{:db/id -3 :user/name "AfterRecovery1"}])

                ;; Finalize
                (let [final-result (recovery-result)]

                  ;; Use the target connection from the result
                  (let [target-conn (:target-conn final-result)
                        users (d/q '[:find [?name ...]
                                    :where [_ :user/name ?name]]
                                  @target-conn)]
                    (is (contains? (set users) "BeforeInterrupt1") "Should have pre-interrupt user 1")
                    (is (contains? (set users) "BeforeInterrupt2") "Should have pre-interrupt user 2")
                    (is (contains? (set users) "AfterRecovery1") "Should have post-recovery user"))))))

            ;; Cleanup
            (cleanup-test-db source-config))))))

(deftest test-migration-continuation-with-same-id
  (testing "Continuing migration with the same ID"
    (with-test-dir test-dir
      (with-test-dir target-dir
        (with-test-db source-config {:store {:backend :mem :id "continuation-source"}}
          (let [source-conn (d/connect source-config)
                target-config {:store {:backend :file :path target-dir}}
                specific-migration-id "test-migration-123"]

            ;; Populate source
            (populate-test-db source-conn :user-count 5 :post-count 10)

            ;; Start migration with specific ID
            (let [router1 (migrate/live-migrate
                          source-conn target-config
                          :migration-id specific-migration-id
                          :database-id "continuation-test"
                          :backup-dir test-dir)]

              ;; Add some transactions
              (router1 [{:db/id -1 :user/name "FirstAttemptUser"}])

              ;; Don't finalize - simulate interruption

              ;; Try to start another migration with the SAME ID
              ;; This should continue the existing migration
              (let [router2 (migrate/live-migrate
                            source-conn target-config
                            :migration-id specific-migration-id
                            :database-id "continuation-test"
                            :backup-dir test-dir)]

                ;; Should be able to continue using the router
                (router2 [{:db/id -2 :user/name "SecondAttemptUser"}])

                ;; Finalize
                (let [result (router2)
                      target-conn (d/connect target-config)]

                  ;; Verify both users made it
                  (let [users (d/q '[:find [?name ...]
                                    :where [_ :user/name ?name]]
                                  @target-conn)]
                    (is (contains? (set users) "FirstAttemptUser") "Should have user from first attempt")
                    (is (contains? (set users) "SecondAttemptUser") "Should have user from second attempt"))))))

            ;; Cleanup
            (cleanup-test-db source-config))))))

(deftest test-completed-migration-reuse
  (testing "Using same migration ID for completed migration"
    (with-test-dir test-dir
      (with-test-dir target-dir
        (with-test-db source-config {:store {:backend :mem :id "reuse-source"}}
          (let [source-conn (d/connect source-config)
                target-config {:store {:backend :file :path target-dir}}
                specific-migration-id "completed-migration-456"]

            ;; Populate source
            (populate-test-db source-conn :user-count 3 :post-count 5)

            ;; Complete a migration
            (let [router1 (migrate/live-migrate
                          source-conn target-config
                          :migration-id specific-migration-id
                          :database-id "reuse-test"
                          :backup-dir test-dir)]

              (router1 [{:db/id -1 :user/name "OriginalUser"}])

              ;; Finalize the migration
              (let [result1 (router1)]
                (is (= :completed (:status result1)) "First migration should complete"))

              ;; Now try to use the same migration ID again
              ;; Should return a router that routes to target
              (let [router2 (migrate/live-migrate
                            source-conn target-config
                            :migration-id specific-migration-id
                            :database-id "reuse-test"
                            :backup-dir test-dir)]

                ;; Should be able to write through the router
                (router2 [{:db/id -2 :user/name "AfterCompletedUser"}])

                ;; Check the status
                (let [status (router2)]
                  (is (= :already-completed (:status status)) "Should indicate already completed")

                  ;; Verify target has both users
                  (let [target-conn (:target-conn status)
                        users (d/q '[:find [?name ...]
                                    :where [_ :user/name ?name]]
                                  @target-conn)]
                    (is (contains? (set users) "OriginalUser") "Should have original user")
                    (is (contains? (set users) "AfterCompletedUser") "Should have new user"))))))

            ;; Cleanup
            (cleanup-test-db source-config))))))

(deftest test-migration-status-tracking
  (testing "Migration status tracking and queries"
    (with-test-dir test-dir
      (with-test-dir target-dir
        (with-test-db source-config {:store {:backend :mem :id "status-source"}}
          (let [source-conn (d/connect source-config)
                target-config {:store {:backend :file :path target-dir}}
                migration-id (atom nil)]

            ;; Populate source
            (populate-test-db source-conn :user-count 5 :post-count 10)

            ;; Start migration
            (let [router (migrate/live-migrate
                         source-conn target-config
                         :database-id "status-test"
                         :backup-dir test-dir
                         :progress-fn (fn [progress]
                                       (when (:migration-id progress)
                                         (reset! migration-id (:migration-id progress)))))]

              ;; Check status while running
              (let [status (migrate/get-migration-status
                           test-dir "status-test" @migration-id)]
                (is (= :found (:status status)) "Should find migration")
                (is (some? (:state status)) "Should have state")
                (is (some? (:started-at status)) "Should have start time"))

              ;; Finalize
              (router)

              ;; Check status after completion
              (let [final-status (migrate/get-migration-status
                                 test-dir "status-test" @migration-id)]
                (is (= :found (:status final-status)) "Should find migration")
                (is (= :completed (:state final-status)) "Should be completed")
                (is (some? (:completed-at final-status)) "Should have completion time"))))

            ;; Cleanup
            (cleanup-test-db source-config))))))

(deftest test-transaction-verification
  (testing "Transaction capture verification during migration"
    (with-test-dir test-dir
      (with-test-dir target-dir
        (with-test-db source-config {:store {:backend :mem :id "verify-source"}}
          (let [source-conn (d/connect source-config)
                target-config {:store {:backend :file :path target-dir}}
                captured-txs (atom [])]

            ;; Populate source
            (populate-test-db source-conn :user-count 3 :post-count 5)

            ;; Start migration with verification
            (let [router (migrate/live-migrate
                         source-conn target-config
                         :database-id "verify-test"
                         :backup-dir test-dir
                         :verify-transactions true
                         :progress-fn (fn [progress]
                                       (when (= (:stage progress) :applying-transaction)
                                         (swap! captured-txs conj (:tx-id progress)))))]

              ;; Perform specific transactions
              (let [tx1 (router [{:db/id -1 :user/name "VerifyUser1"}])
                    tx2 (router [{:db/id -2 :user/name "VerifyUser2"}])
                    tx3 (router [{:db/id -3 :post/title "VerifyPost"}])]

                ;; Wait for capture
                (Thread/sleep 100)

                ;; Finalize and verify
                (let [result (router)
                      target-conn (:target-conn result)]

                  ;; Check all transactions made it
                  (let [users (d/q '[:find [?name ...]
                                    :where [_ :user/name ?name]]
                                  @target-conn)
                        posts (d/q '[:find [?title ...]
                                    :where [_ :post/title ?title]]
                                  @target-conn)]

                    (is (contains? (set users) "VerifyUser1") "User 1 should be migrated")
                    (is (contains? (set users) "VerifyUser2") "User 2 should be migrated")
                    (is (contains? (set posts) "VerifyPost") "Post should be migrated"))))))

            ;; Cleanup
            (cleanup-test-db source-config))))))

(deftest test-migration-archiving
  (testing "Archiving of old completed migrations"
    (with-test-dir test-dir
      ;; Create mock completed migrations
      (let [database-id "archive-test"
            migrations-dir (str test-dir "/" database-id "/migrations")]

        ;; Create several completed migration states
        (doseq [i (range 3)]
          (let [migration-id (str "old-migration-" i)
                migration-path (str migrations-dir "/" migration-id)
                old-date (java.util.Date. (- (System/currentTimeMillis)
                                            (* (+ 200 (* i 24)) 60 60 1000)))] ; 200+ hours ago

            (directory/ensure-directory migration-path)
            (metadata/write-edn-to-file
             (str migration-path "/migration-manifest.edn")
             {:migration/id migration-id
              :migration/state :completed
              :migration/completed-at old-date})))

        ;; Create one recent completed migration (should not be archived)
        (let [recent-id "recent-migration"
              recent-path (str migrations-dir "/" recent-id)]
          (directory/ensure-directory recent-path)
          (metadata/write-edn-to-file
           (str recent-path "/migration-manifest.edn")
           {:migration/id recent-id
            :migration/state :completed
            :migration/completed-at (java.util.Date.)}))

        ;; Run archive
        (let [archive-result (migrate/archive-completed-migrations
                             test-dir database-id
                             :older-than-hours 168)] ; 1 week

          (is (= 3 (:archived-count archive-result)) "Should archive 3 old migrations")
          (is (not (contains? (set (:migration-ids archive-result)) "recent-migration"))
              "Should not archive recent migration")

          ;; Verify migrations are archived, not deleted
          (doseq [migration-id (:migration-ids archive-result)]
            (let [state (migrate/get-migration-status test-dir database-id migration-id)]
              (is (= :found (:status state)) "Migration should still exist")
              (is (= :archived (:state state)) "Migration should be marked as archived")
              (is (some? (:archived-at state)) "Should have archive timestamp")))

          ;; Verify recent migration is still completed
          (let [recent-state (migrate/get-migration-status test-dir database-id "recent-migration")]
            (is (= :completed (:state recent-state)) "Recent migration should still be completed")))))))

(deftest test-concurrent-transaction-handling
  (testing "Handling concurrent transactions during migration"
    (with-test-dir test-dir
      (with-test-dir target-dir
        (with-test-db source-config {:store {:backend :mem :id "concurrent-source"}}
          (let [source-conn (d/connect source-config)
                target-config {:store {:backend :file :path target-dir}}
                tx-count (atom 0)]

            ;; Populate source
            (populate-test-db source-conn :user-count 5 :post-count 10)

            ;; Start migration
            (let [router (migrate/live-migrate
                         source-conn target-config
                         :database-id "concurrent-test"
                         :backup-dir test-dir)]

              ;; Concurrent transactions
              (let [futures (doall
                            (for [i (range 10)]
                              (future
                                (router [{:db/id (- (inc i))
                                         :user/name (str "ConcurrentUser" i)}])
                                (swap! tx-count inc))))]

                ;; Wait for all to complete
                (doseq [f futures] @f)

                ;; Give transaction capture a moment to process
                (Thread/sleep 500)

                ;; Finalize
                (let [result (router)
                      target-conn (:target-conn result)]

                  ;; Verify all concurrent users made it
                  (let [all-users (d/q '[:find [?name ...]
                                        :where
                                        [_ :user/name ?name]]
                                      @target-conn)
                        concurrent-users (filter #(.startsWith % "ConcurrentUser") all-users)]

                    (is (= 10 (count concurrent-users)) "All concurrent users should be migrated"))))))

            ;; Cleanup
            (cleanup-test-db source-config))))))

(deftest test-different-backend-migrations
  (testing "Migration between different backend types"
    (with-test-dir test-dir
      (with-test-dir source-dir
        (with-test-dir target-dir
          ;; Test file -> memory migration
          (let [file-config {:store {:backend :file :path source-dir}}
                mem-config {:store {:backend :mem :id "target-mem"}}
                file-conn (create-test-db file-config)]

            (try
              ;; Populate file database
              (populate-test-db file-conn :user-count 8 :post-count 15)

              ;; Migrate file -> memory
              (let [router (migrate/live-migrate
                           file-conn mem-config
                           :database-id "file-to-mem"
                           :backup-dir test-dir)]

                ;; Add transactions
                (router [{:db/id -1 :user/name "FileToMemUser"}])

                ;; Finalize
                (let [result (router)
                      target-conn (:target-conn result)]

                  ;; Verify migration
                  (let [users (d/q '[:find [?name ...]
                                    :where [_ :user/name ?name]]
                                  @target-conn)]
                    (is (contains? (set users) "FileToMemUser") "Should have migrated user")
                    (is (>= (count users) 8) "Should have original users"))))

              (finally
                (cleanup-test-db file-config)))))))))

(deftest test-list-migrations
  (testing "Listing all migrations including archived"
    (with-test-dir test-dir
      (let [database-id "list-test"
            migrations-dir (str test-dir "/" database-id "/migrations")]

        ;; Create various migration states
        ;; 1. Active migration
        (let [active-path (str migrations-dir "/active-migration")]
          (directory/ensure-directory active-path)
          (metadata/write-edn-to-file
           (str active-path "/migration-manifest.edn")
           {:migration/id "active-migration"
            :migration/state :catching-up
            :migration/started-at (java.util.Date.)
            :migration/initial-backup-id "backup-active"}))

        ;; 2. Completed migration
        (let [completed-path (str migrations-dir "/completed-migration")]
          (directory/ensure-directory completed-path)
          (metadata/write-edn-to-file
           (str completed-path "/migration-manifest.edn")
           {:migration/id "completed-migration"
            :migration/state :completed
            :migration/started-at (java.util.Date. (- (System/currentTimeMillis) (* 2 60 60 1000)))
            :migration/completed-at (java.util.Date. (- (System/currentTimeMillis) (* 1 60 60 1000)))
            :migration/initial-backup-id "backup-completed"}))

        ;; 3. Archived migration
        (let [archived-path (str migrations-dir "/archived-migration")]
          (directory/ensure-directory archived-path)
          (metadata/write-edn-to-file
           (str archived-path "/migration-manifest.edn")
           {:migration/id "archived-migration"
            :migration/state :archived
            :migration/started-at (java.util.Date. (- (System/currentTimeMillis) (* 48 60 60 1000)))
            :migration/completed-at (java.util.Date. (- (System/currentTimeMillis) (* 47 60 60 1000)))
            :migration/archived-at (java.util.Date. (- (System/currentTimeMillis) (* 24 60 60 1000)))
            :migration/initial-backup-id "backup-archived"}))

        ;; Test listing with archived
        (let [all-migrations (migrate/list-migrations test-dir database-id
                                                     :include-archived true)]
          (is (= 3 (count all-migrations)) "Should list all migrations including archived")
          (is (= #{"active-migration" "completed-migration" "archived-migration"}
                 (set (map :migration-id all-migrations)))
              "Should have all migration IDs"))

        ;; Test listing without archived
        (let [non-archived (migrate/list-migrations test-dir database-id
                                                   :include-archived false)]
          (is (= 2 (count non-archived)) "Should list only non-archived migrations")
          (is (= #{"active-migration" "completed-migration"}
                 (set (map :migration-id non-archived)))
              "Should not include archived migration"))))))

(deftest test-complex-migration-with-continuous-writes
  (testing "Complex migration with 120k entities and continuous writes"
    (with-test-dir test-dir
      (with-test-dir target-dir
        (with-test-dir reference-dir
          (with-test-db source-config {:store {:backend :mem :id "complex-source"}
                                       :schema-flexibility :write
                                       :keep-history? true}
            (with-test-db reference-config {:store {:backend :mem :id "complex-reference"}
                                           :schema-flexibility :write
                                           :keep-history? true}
              (let [source-conn (d/connect source-config)
                    reference-conn (d/connect reference-config)
                    target-config {:store {:backend :file :path target-dir}
                                  :schema-flexibility :write
                                  :keep-history? true}

                    ;; Helper to generate complex entities
                    generate-batch (fn [batch-num size]
                                    (vec (for [i (range size)]
                                          {:entity/id (java.util.UUID/randomUUID)
                                           :entity/signature (java.util.UUID/randomUUID)
                                           :entity/type :event
                                           :entity/created-at (java.util.Date.)
                                           :event/type (rand-nth [:device/reading :audit/login
                                                                 :order/placed :order/shipped])
                                           :event/at (java.util.Date.)
                                           :event/data (str "{\"batch\":" batch-num
                                                          ",\"index\":" i
                                                          ",\"temperature\":" (rand)
                                                          ",\"humidity\":" (rand-int 100)
                                                          ",\"pressure\":" (+ 900 (rand-int 200)) "}")})))]

                ;; Install complex schema in both databases
                (log/info "Installing complex schema...")
                @(d/transact source-conn complex-test/complex-schema)
                @(d/transact reference-conn complex-test/complex-schema)

                ;; Generate and add initial 20k complex entities
                (log/info "Creating initial 20,000 complex entities...")
                (let [initial-batch (generate-batch 0 20000)]
                  @(d/transact source-conn initial-batch)
                  @(d/transact reference-conn initial-batch))

                (log/info "Initial data loaded. Starting migration...")

                ;; Start migration
                (let [migration-start (System/currentTimeMillis)
                      write-thread-active (atom true)
                      batches-written (atom [])
                      router (migrate/live-migrate
                             source-conn target-config
                             :migration-id "complex-migration-120k"
                             :database-id "complex-test"
                             :backup-dir test-dir
                             :progress-fn (fn [progress]
                                           (when (#{:initialized :creating-backup :restoring-to-target
                                                   :catching-up :ready} (:stage progress))
                                             (log/info "Migration stage:" (:stage progress)))))]

                  ;; Start continuous write thread - 10k entities per second for 10 seconds
                  (let [writer-thread
                        (future
                          (try
                            (dotimes [second 10]
                              (when @write-thread-active
                                (let [batch-num (inc second)
                                      batch (generate-batch batch-num 10000)
                                      elapsed (/ (- (System/currentTimeMillis) migration-start) 1000.0)]

                                  (log/info (format "Second %.1f: Writing batch %d (10k entities)..."
                                                   elapsed batch-num))

                                  ;; Write to both source (through router) and reference
                                  (router batch)
                                  @(d/transact reference-conn batch)

                                  (swap! batches-written conj batch-num)
                                  (Thread/sleep 1000))))

                            (log/info "Continuous writes completed. Total batches:" @batches-written)
                            (catch Exception e
                              (log/error e "Writer thread error"))))]

                    (try
                      ;; Wait for writes to complete
                      @writer-thread
                      (reset! write-thread-active false)

                      ;; Add a few more transactions through the router
                      (log/info "Adding final verification batch...")
                      (let [final-batch (generate-batch 99 1000)]
                        (router final-batch)
                        @(d/transact reference-conn final-batch))

                      ;; Finalize migration
                      (log/info "Finalizing migration...")
                      (let [result (router)
                            target-conn (:target-conn result)
                            finalize-time (- (System/currentTimeMillis) migration-start)]

                        (is (= :completed (:status result)) "Migration should complete")
                        (log/info (format "Migration completed in %.1f seconds" (/ finalize-time 1000.0)))

                        ;; Compare migrated database with reference database
                        (log/info "Comparing migrated vs reference databases...")
                        (Thread/sleep 2000) ; Let everything settle

                        (let [migrated-db @target-conn
                              reference-db @reference-conn

                              ;; Count total entities
                              migrated-count (count (d/q '[:find ?e
                                                          :where [?e :entity/type :event]]
                                                        migrated-db))
                              reference-count (count (d/q '[:find ?e
                                                          :where [?e :entity/type :event]]
                                                         reference-db))]

                          (log/info "Entity counts:")
                          (log/info "  Migrated:" migrated-count)
                          (log/info "  Reference:" reference-count)
                          (log/info "  Expected: ~121,000 (20k + 10x10k + 1k)")

                          (is (= migrated-count reference-count)
                              "Migrated and reference should have same entity count")
                          (is (>= migrated-count 121000) "Should have at least 121k entities")

                          ;; Verify batch markers in data
                          (doseq [batch-num (concat [0 99] @batches-written)]
                            (let [migrated-batch (d/q '[:find (count ?e)
                                                       :in $ ?batch-str
                                                       :where
                                                       [?e :event/data ?data]
                                                       [(.contains ^String ?data ?batch-str)]]
                                                     migrated-db
                                                     (str "\"batch\":" batch-num))
                                  reference-batch (d/q '[:find (count ?e)
                                                        :in $ ?batch-str
                                                        :where
                                                        [?e :event/data ?data]
                                                        [(.contains ^String ?data ?batch-str)]]
                                                      reference-db
                                                      (str "\"batch\":" batch-num))]

                              (is (= migrated-batch reference-batch)
                                  (format "Batch %d should match in both databases" batch-num))))

                          ;; Verify no data loss - sample some random entities
                          (let [sample-ids (take 100 (d/q '[:find [?id ...]
                                                           :where [?e :entity/id ?id]]
                                                         reference-db))]
                            (doseq [id sample-ids]
                              (let [in-migrated (d/q '[:find ?e .
                                                      :in $ ?id
                                                      :where [?e :entity/id ?id]]
                                                    migrated-db id)]
                                (is (some? in-migrated)
                                    (format "Entity %s should exist in migrated db" id)))))))

                      (finally
                        (reset! write-thread-active false)))))))))))))

(deftest test-migration-error-handling
  (testing "Migration error handling with various injected failures"
    (with-test-dir test-dir
      (with-test-dir target-dir
        (with-test-db source-config {:store {:backend :mem :id "error-source"}
                                     :schema-flexibility :write}
          (let [source-conn (d/connect source-config)
                target-config {:store {:backend :file :path target-dir}
                              :schema-flexibility :write}

                ;; Error injection control
                error-mode (atom nil)
                error-count (atom 0)

                ;; Wrapper to inject errors during transaction routing
                create-error-prone-router (fn [original-router]
                                           (fn error-router
                                             ([]
                                              ;; Finalization - may fail based on mode
                                              (case @error-mode
                                                :fail-finalization
                                                (throw (ex-info "Injected finalization error"
                                                               {:injected true :mode @error-mode}))

                                                ;; Normal finalization
                                                (original-router)))

                                             ([tx-data]
                                              ;; Transaction routing - may fail based on mode
                                              (case @error-mode
                                                :fail-every-third
                                                (do
                                                  (swap! error-count inc)
                                                  (if (zero? (mod @error-count 3))
                                                    (throw (ex-info "Injected transaction error"
                                                                 {:injected true
                                                                  :count @error-count
                                                                  :mode @error-mode}))
                                                    (original-router tx-data)))

                                                :fail-after-5
                                                (do
                                                  (swap! error-count inc)
                                                  (if (> @error-count 5)
                                                    (throw (ex-info "Injected after-5 error"
                                                                 {:injected true
                                                                  :count @error-count
                                                                  :mode @error-mode}))
                                                    (original-router tx-data)))

                                                ;; Normal routing
                                                (original-router tx-data)))))

                ;; Helper to test recovery after error
                test-error-recovery (fn [mode description initial-data transactions]
                                      (log/info (format "\n=== Testing: %s ===" description))

                                      ;; Setup initial data
                                      (d/transact source-conn complex-test/complex-schema)
                                      (d/transact source-conn initial-data)

                                      ;; Start migration
                                      (let [migration-id (str "error-test-" (name mode))
                                            errors-caught (atom [])
                                            successful-txs (atom 0)]

                                        ;; First attempt with errors
                                        (reset! error-mode mode)
                                        (reset! error-count 0)

                                        (let [router-1 (migrate/live-migrate
                                                       source-conn target-config
                                                       :migration-id migration-id
                                                       :database-id "error-test"
                                                       :backup-dir test-dir)
                                              error-router (create-error-prone-router router-1)]

                                          ;; Try transactions, catching errors
                                          (doseq [tx transactions]
                                            (try
                                              (error-router tx)
                                              (swap! successful-txs inc)
                                              (catch Exception e
                                                (swap! errors-caught conj e)
                                                (log/warn "Expected error caught:" (.getMessage e)))))

                                          (log/info "First attempt results:")
                                          (log/info "  Successful transactions:" @successful-txs)
                                          (log/info "  Errors caught:" (count @errors-caught))

                                          ;; Verify errors were caught as expected
                                          (case mode
                                            :fail-every-third
                                            (is (> (count @errors-caught) 0)
                                                "Should have caught some errors")

                                            :fail-after-5
                                            (is (>= @successful-txs 5)
                                                "Should succeed for first 5 transactions")

                                            nil))

                                        ;; Now recover/continue without errors
                                        (reset! error-mode nil)
                                        (reset! error-count 0)
                                        (log/info "Attempting recovery without errors...")

                                        (let [router-2 (migrate/live-migrate
                                                       source-conn target-config
                                                       :migration-id migration-id  ; Same ID - should continue
                                                       :database-id "error-test"
                                                       :backup-dir test-dir)]

                                          ;; Complete remaining transactions
                                          (doseq [tx transactions]
                                            (router-2 tx))

                                          ;; Finalize
                                          (let [result (router-2)]
                                            (is (= :completed (:status result))
                                                (format "%s: Should complete after recovery" description))

                                            ;; Verify data integrity
                                            (let [target-conn (:target-conn result)
                                                  final-count (count (d/q '[:find ?e
                                                                           :where [?e :entity/type _]]
                                                                         @target-conn))]
                                              (log/info "Final entity count after recovery:" final-count)
                                              (is (pos? final-count)
                                                  "Should have entities after recovery"))))))]

            ;; Test 1: Errors during transaction routing (every third fails)
            (test-error-recovery
             :fail-every-third
             "Intermittent transaction failures (every 3rd fails)"
             [{:entity/id (java.util.UUID/randomUUID)
               :entity/type :company
               :company/name "Test Company"}]
             (for [i (range 15)]
               [{:entity/id (java.util.UUID/randomUUID)
                 :entity/type :event
                 :event/type :test
                 :event/at (java.util.Date.)
                 :event/data (str "Event " i)}]))

            ;; Test 2: Errors after certain number of transactions
            (test-error-recovery
             :fail-after-5
             "Failures after 5 successful transactions"
             [{:entity/id (java.util.UUID/randomUUID)
               :entity/type :product
               :product/sku "TEST-001"
               :product/name "Test Product"}]
             (for [i (range 10)]
               [{:entity/id (java.util.UUID/randomUUID)
                 :entity/type :event
                 :event/type :product-view
                 :event/at (java.util.Date.)
                 :event/data (str "View " i)}]))

            ;; Test 3: Network-like intermittent failures with retries
            (log/info "\n=== Testing: Network-like intermittent failures ===")
            (let [migration-id "network-failure-test"
                  network-fail-count (atom 0)
                  max-network-fails 3]

              ;; Setup data
              @(d/transact source-conn [{:entity/id (java.util.UUID/randomUUID)
                                        :entity/type :test
                                        :test/name "Network Test"}])

              ;; Start migration
              (let [router (migrate/live-migrate
                           source-conn target-config
                           :migration-id migration-id
                           :database-id "network-test"
                           :backup-dir test-dir)]

                ;; Simulate network failures with retries
                (dotimes [attempt 5]
                  (let [tx-data [{:entity/id (java.util.UUID/randomUUID)
                                 :entity/type :event
                                 :event/type :network-test
                                 :event/at (java.util.Date.)
                                 :event/data (str "Attempt " attempt)}]]

                    ;; Retry logic
                    (loop [retry 0]
                      (when (< retry 3)
                        (let [succeeded (try
                                         ;; Simulate random network failure
                                         (when (and (< @network-fail-count max-network-fails)
                                                   (< (rand) 0.5))
                                           (swap! network-fail-count inc)
                                           (throw (ex-info "Simulated network timeout"
                                                        {:type :network-timeout
                                                         :retry retry})))

                                         ;; If no failure, process transaction
                                         (router tx-data)
                                         (log/info (format "Transaction %d succeeded on retry %d"
                                                          attempt retry))
                                         true

                                         (catch Exception e
                                           (log/warn (format "Transaction %d failed on retry %d: %s"
                                                           attempt retry (.getMessage e)))
                                           (Thread/sleep (* 100 (Math/pow 2 retry))) ; Exponential backoff
                                           false))]
                          (when (not succeeded)
                            (if (< retry 2)
                              (recur (inc retry))
                              (log/error (format "Transaction %d failed after all retries" attempt)))))))))

                ;; Finalize
                (let [result (router)]
                  (is (= :completed (:status result))
                      "Should complete despite network failures")
                  (log/info "Migration completed despite" @network-fail-count "network failures"))))

            ;; Test 4: Recovery after migration state corruption
            (log/info "\n=== Testing: State corruption recovery ===")
            (let [migration-id "corruption-recovery-test"
                  migration-path (str test-dir "/corruption-test/migrations/" migration-id)]

              ;; Create corrupted state
              (directory/ensure-directory migration-path)
              (spit (str migration-path "/migration-manifest.edn")
                   "{:migration/state :catching-up :migration/id CORRUPTED")  ; Invalid EDN

              ;; Attempt to read corrupted state (should fail gracefully)
              (is (thrown? Exception
                          (migrate/get-migration-status test-dir "corruption-test" migration-id))
                  "Should fail to read corrupted state")

              ;; Clean up corrupted state
              (io/delete-file (str migration-path "/migration-manifest.edn"))

              ;; Start fresh migration with same ID
              (let [router (migrate/live-migrate
                           source-conn target-config
                           :migration-id migration-id
                           :database-id "corruption-test"
                           :backup-dir test-dir)]

                (router [{:entity/id (java.util.UUID/randomUUID)
                         :entity/type :recovery-test}])

                (let [result (router)]
                  (is (= :completed (:status result))
                      "Should complete after corruption recovery"))))

            ;; Test 5: Concurrent migration attempts (should block)
            (log/info "\n=== Testing: Concurrent migration blocking ===")
            (let [migration-id-1 "concurrent-1"
                  migration-id-2 "concurrent-2"]

              ;; Start first migration
              (let [router-1 (migrate/live-migrate
                             source-conn target-config
                             :migration-id migration-id-1
                             :database-id "concurrent-test"
                             :backup-dir test-dir)]

                ;; Attempt second migration while first is active (should fail)
                (is (thrown-with-msg? Exception #"already in progress"
                                     (migrate/live-migrate
                                      source-conn target-config
                                      :migration-id migration-id-2
                                      :database-id "concurrent-test"
                                      :backup-dir test-dir))
                    "Second migration should be blocked")

                ;; Complete first migration
                (router-1 [{:entity/id (java.util.UUID/randomUUID)
                           :entity/type :concurrent-test}])
                (router-1)

                ;; Now second migration should work
                (let [router-2 (migrate/live-migrate
                               source-conn target-config
                               :migration-id migration-id-2
                               :database-id "concurrent-test"
                               :backup-dir test-dir)]

                  (router-2 [{:entity/id (java.util.UUID/randomUUID)
                             :entity/type :concurrent-test-2}])

                  (let [result (router-2)]
                    (is (= :completed (:status result))
                        "Second migration should complete after first")))))

            ;; Cleanup
            (cleanup-test-db source-config)))))))

;; Run tests
(comment
  (run-tests))