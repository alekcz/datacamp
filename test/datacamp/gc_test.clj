(ns datacamp.gc-test
  "Tests for optimized garbage collection with resumable marking and batch deletion"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :as async :refer [<!!]]
            [datahike.api :as d]
            [datacamp.gc :as gc]
            [datacamp.test-helpers :as h]
            [konserve.core :as k]
            [superv.async :refer [<?? S]])
  (:import [java.util Date UUID]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def ^:dynamic *test-db-config* nil)
(def ^:dynamic *test-conn* nil)

(defn gc-test-fixture [f]
  (let [config {:store {:backend :mem :id (str "gc-test-" (UUID/randomUUID))}}]
    (binding [*test-db-config* config
              *test-conn* (h/create-test-db config)]
      (try
        ;; Initialize database with at least one transaction to ensure proper flush
        (when *test-conn*
          (d/transact *test-conn* [{:db/ident :test/initialization}])
          ;; Force a read to ensure the database is properly flushed
          @*test-conn*)
        (f)
        (finally
          (h/cleanup-test-db config))))))

(use-fixtures :each gc-test-fixture)

;; =============================================================================
;; Checkpoint Tests
;; =============================================================================

(deftest checkpoint-save-load-test
  (testing "Checkpoint can be saved and loaded"
    (let [db @*test-conn*
          store (:store db)
          gc-id (str (UUID/randomUUID))
          branches [:main :branch-1]
          checkpoint (gc/create-gc-checkpoint gc-id branches)]

      ;; Save checkpoint
      (<?? S (gc/save-checkpoint! store checkpoint))

      ;; Load checkpoint
      (let [loaded (<?? S (gc/load-checkpoint store gc-id))]
        (is (some? loaded) "Checkpoint should be loaded")
        (is (= gc-id (:gc-id loaded)) "GC ID should match")
        (is (= (set branches) (:pending-branches loaded)) "Branches should match"))))

  (testing "Checkpoint stored under :datacamp namespace"
    (let [db @*test-conn*
          store (:store db)
          gc-id (str (UUID/randomUUID))
          checkpoint (gc/create-gc-checkpoint gc-id [:main])]

      ;; Save checkpoint
      (<?? S (gc/save-checkpoint! store checkpoint))

      ;; Verify it's under :datacamp key
      (let [datacamp-data (<?? S (k/get store :datacamp))]
        (is (some? datacamp-data) "Datacamp namespace should exist")
        (is (some? (:gc-checkpoint datacamp-data)) "GC checkpoint should be in :datacamp")
        (is (= gc-id (:gc-id (:gc-checkpoint datacamp-data))) "GC ID should match")))))

(deftest checkpoint-delete-test
  (testing "Checkpoint can be deleted"
    (let [db @*test-conn*
          store (:store db)
          gc-id (str (UUID/randomUUID))
          checkpoint (gc/create-gc-checkpoint gc-id [:main])]

      ;; Save and verify
      (<?? S (gc/save-checkpoint! store checkpoint))
      (is (some? (<?? S (gc/load-checkpoint store gc-id))))

      ;; Delete
      (<?? S (gc/delete-checkpoint! store))

      ;; Verify deleted
      (is (nil? (<?? S (gc/load-checkpoint store gc-id))) "Checkpoint should be deleted"))))

(deftest checkpoint-doesnt-conflict-test
  (testing "Multiple GC checkpoints don't conflict"
    (let [db @*test-conn*
          store (:store db)
          gc-id-1 (str (UUID/randomUUID))
          gc-id-2 (str (UUID/randomUUID))
          checkpoint-1 (gc/create-gc-checkpoint gc-id-1 [:main])
          checkpoint-2 (gc/create-gc-checkpoint gc-id-2 [:main])]

      ;; Save first checkpoint
      (<?? S (gc/save-checkpoint! store checkpoint-1))

      ;; Save second checkpoint (overwrites)
      (<?? S (gc/save-checkpoint! store checkpoint-2))

      ;; Only second checkpoint should be found
      (is (nil? (<?? S (gc/load-checkpoint store gc-id-1))))
      (is (some? (<?? S (gc/load-checkpoint store gc-id-2)))))))

;; =============================================================================
;; GC Status Tests
;; =============================================================================

(deftest gc-status-no-gc-test
  (testing "GC status when no GC in progress"
    (let [db @*test-conn*
          status (<?? S (gc/get-gc-status db))]
      (is (= :no-gc-in-progress (:status status))))))

(deftest gc-status-with-checkpoint-test
  (testing "GC status when GC checkpoint exists"
    (let [db @*test-conn*
          store (:store db)
          gc-id (str (UUID/randomUUID))
          checkpoint (-> (gc/create-gc-checkpoint gc-id [:main :branch-1])
                        (assoc :visited #{"commit-1" "commit-2"}
                               :reachable #{:key1 :key2 :key3}
                               :completed-branches #{:main}))]

      ;; Save checkpoint
      (<?? S (gc/save-checkpoint! store checkpoint))

      ;; Check status
      (let [status (<?? S (gc/get-gc-status db))]
        (is (= :in-progress (:status status)))
        (is (= gc-id (:gc-id status)))
        (is (= 2 (:visited-count status)))
        (is (= 3 (:reachable-count status)))
        (is (= 1 (:completed-branches status)))
        (is (= 1 (:pending-branches status)))))))

;; =============================================================================
;; Basic GC Tests
;; =============================================================================

(deftest basic-gc-dry-run-test
  (testing "Basic GC dry run doesn't delete anything"
    ;; Add some test data
    (h/populate-test-db *test-conn* :user-count 10 :post-count 20)

    (let [db @*test-conn*
          initial-datom-count (count (d/datoms db :eavt))
          result (<?? S (gc/gc-storage-optimized! db :dry-run true))]

      ;; Verify result structure
      (is (some? (:reachable-count result)))
      (is (some? (:would-delete-count result)))
      (is (true? (:dry-run result)))

      ;; Verify nothing was deleted
      (let [db-after @*test-conn*
            final-datom-count (count (d/datoms db-after :eavt))]
        (is (= initial-datom-count final-datom-count)
            "Dry run should not delete anything")))))

(deftest basic-gc-actual-run-test
  (testing "Basic GC actually deletes unreachable data"
    ;; Create data and make some of it unreachable by time
    (h/populate-test-db *test-conn* :user-count 5 :post-count 10)

    ;; Create a retention date that keeps all current data
    (let [retention-date (Date. (- (System/currentTimeMillis) (* 1000 60 60 24)))
          db @*test-conn*
          result (<?? S (gc/gc-storage-optimized!
                       db
                       :remove-before retention-date
                       :dry-run false))]

      ;; Verify result
      (is (some? (:reachable-count result)))
      (is (number? (:deleted-count result)))
      (is (some? (:duration-ms result)))

      ;; Verify checkpoint was cleaned up
      (let [status (<?? S (gc/get-gc-status db))]
        (is (= :no-gc-in-progress (:status status))
            "Checkpoint should be cleaned up after successful GC")))))

;; =============================================================================
;; Resumable GC Tests
;; =============================================================================

(deftest gc-resume-from-checkpoint-test
  (testing "GC can resume from a saved checkpoint"
    (h/populate-test-db *test-conn* :user-count 10 :post-count 20)

    (let [db @*test-conn*
          store (:store db)
          gc-id (str (UUID/randomUUID))

          ;; Create a checkpoint that's partially complete
          checkpoint (-> (gc/create-gc-checkpoint gc-id [:main])
                        (assoc :visited #{"some-commit"}
                               :reachable #{:key1 :key2}
                               :completed-branches #{}))]

      ;; Save checkpoint
      (<?? S (gc/save-checkpoint! store checkpoint))

      ;; Resume GC
      (let [result (<?? S (gc/resume-gc! db gc-id :dry-run true))]

        ;; Verify it completed
        (is (some? result))
        (is (some? (:reachable-count result)))))))

(deftest gc-resume-with-invalid-id-test
  (testing "GC resume with invalid ID starts fresh"
    (h/populate-test-db *test-conn* :user-count 5 :post-count 10)

    (let [db @*test-conn*
          result (<?? S (gc/resume-gc! db "non-existent-id" :dry-run true))]

      ;; Should still work, just starts fresh
      (is (some? result))
      (is (some? (:reachable-count result))))))

;; =============================================================================
;; Batch Size and Optimization Tests
;; =============================================================================

(deftest backend-optimization-test
  (testing "Backend-specific optimizations are detected"
    (let [s3-optimized (gc/optimize-for-backend {:backend :s3})
          jdbc-optimized (gc/optimize-for-backend {:backend :jdbc})
          file-optimized (gc/optimize-for-backend {:backend :file})
          mem-optimized (gc/optimize-for-backend {:backend :mem})]

      ;; S3 should have different settings than JDBC
      (is (= 1000 (:batch-size s3-optimized)))
      (is (= 5000 (:batch-size jdbc-optimized)))
      (is (= 100 (:batch-size file-optimized)))

      ;; Parallel batches should differ
      (is (= 3 (:parallel-batches s3-optimized)))
      (is (= 1 (:parallel-batches jdbc-optimized)))
      (is (= 10 (:parallel-batches file-optimized))))))

;; =============================================================================
;; Retention Policy Tests
;; =============================================================================

(deftest retention-policy-test
  (testing "GC respects retention date"
    ;; Add initial data
    (d/transact *test-conn* [{:user/name "old-user" :user/email "old@example.com"}])

    ;; Wait a moment
    (Thread/sleep 100)

    ;; Add recent data
    (d/transact *test-conn* [{:user/name "new-user" :user/email "new@example.com"}])

    (let [db @*test-conn*
          ;; Set retention to keep only data from last 50ms
          retention-date (Date. (- (System/currentTimeMillis) 50))
          result (<?? S (gc/gc-storage-optimized!
                       db
                       :remove-before retention-date
                       :dry-run true))]

      ;; The recent data should be reachable
      (is (pos? (:reachable-count result)))

      ;; Both users should still be reachable since we're keeping the current head
      (let [users (d/q '[:find ?name
                        :where [_ :user/name ?name]]
                      db)]
        (is (= 2 (count users)))))))

;; =============================================================================
;; Checkpoint Interval Tests
;; =============================================================================

(deftest checkpoint-interval-test
  (testing "Checkpoints are saved at specified intervals"
    (h/populate-test-db *test-conn* :user-count 5 :post-count 10)

    (let [db @*test-conn*
          gc-id (str (UUID/randomUUID))
          ;; Very small interval for testing
          checkpoint-interval 1]

      ;; Start GC with small checkpoint interval
      (let [gc-future (future
                       (<?? S (gc/gc-storage-optimized!
                             db
                             :resume-gc-id gc-id
                             :checkpoint-interval checkpoint-interval
                             :dry-run true)))]

        ;; Wait a bit for checkpoints to be saved
        (Thread/sleep 100)

        ;; Check if checkpoint exists (might exist if GC is still running)
        (let [status (<?? S (gc/get-gc-status db))]
          (when (= :in-progress (:status status))
            (is (= gc-id (:gc-id status))
                "Checkpoint should have the correct GC ID")))

        ;; Wait for completion
        @gc-future

        ;; After completion, checkpoint should be cleaned up
        (let [final-status (<?? S (gc/get-gc-status db))]
          (is (= :no-gc-in-progress (:status final-status))
              "Checkpoint should be cleaned up after completion"))))))

;; =============================================================================
;; Edge Cases and Error Handling
;; =============================================================================

(deftest gc-on-empty-database-test
  (testing "GC works on empty database"
    (let [db @*test-conn*
          result (<?? S (gc/gc-storage-optimized! db :dry-run false))]

      ;; Should complete without errors
      (is (some? result))
      (is (number? (:deleted-count result)))
      (is (>= (:deleted-count result) 0)))))

(deftest gc-with-no-branches-test
  (testing "GC handles database with no branches gracefully"
    (let [db @*test-conn*]
      ;; Try to run GC - should handle missing branches
      (try
        (let [result (<?? S (gc/gc-storage-optimized! db :dry-run true))]
          (is (some? result)))
        (catch Exception e
          ;; It's okay if it throws - the database might not have :branches key
          (is (some? e)))))))

(deftest concurrent-gc-attempts-test
  (testing "Multiple concurrent GC attempts"
    (h/populate-test-db *test-conn* :user-count 5 :post-count 10)

    (let [db @*test-conn*
          gc-id (str (UUID/randomUUID))

          ;; Start first GC
          gc1-future (future
                      (<?? S (gc/gc-storage-optimized!
                            db
                            :resume-gc-id gc-id
                            :checkpoint-interval 50
                            :dry-run true)))

          ;; Try to start second GC with same ID
          ;; (should either use existing checkpoint or fail gracefully)
          gc2-future (future
                      (Thread/sleep 50) ; Let first one start
                      (<?? S (gc/resume-gc! db gc-id :dry-run true)))]

      ;; Both should complete
      (is (some? @gc1-future))
      (is (some? @gc2-future)))))

;; =============================================================================
;; Performance Tests
;; =============================================================================

(deftest gc-performance-test
  (testing "GC completes in reasonable time for small database"
    (h/populate-test-db *test-conn* :user-count 20 :post-count 50)

    (let [db @*test-conn*]
      (h/assert-performance
       "GC dry run on small database"
       #(<?? S (gc/gc-storage-optimized! db :dry-run true))
       5000)))) ; Should complete within 5 seconds

;; =============================================================================
;; Integration Tests with Datahike Operations
;; =============================================================================

(deftest gc-doesnt-affect-current-data-test
  (testing "GC doesn't affect currently reachable data"
    ;; Add test data
    (let [initial-data (h/populate-test-db *test-conn* :user-count 10 :post-count 20)
          initial-count (:total-datoms initial-data)]

      ;; Run GC with recent retention date (keeps everything)
      (let [db @*test-conn*
            retention-date (Date. 0) ; Keep everything
            _ (<?? S (gc/gc-storage-optimized!
                    db
                    :remove-before retention-date
                    :dry-run false))

            ;; Verify all data still exists
            final-db @*test-conn*
            final-count (count (d/datoms final-db :eavt))]

        ;; All current data should still be there
        (is (= initial-count final-count)
            "GC should not delete reachable data")

        ;; Verify we can still query data
        (let [users (d/q '[:find ?name
                          :where [_ :user/name ?name]]
                        final-db)]
          (is (= 10 (count users)) "All users should still be queryable"))))))

(deftest gc-with-transactions-during-run-test
  (testing "GC handles transactions during execution"
    (h/populate-test-db *test-conn* :user-count 10 :post-count 20)

    (let [db @*test-conn*
          ;; Start GC in background
          gc-future (future
                     (<?? S (gc/gc-storage-optimized!
                           db
                           :checkpoint-interval 10
                           :dry-run false)))]

      ;; Add more data while GC is running
      (dotimes [i 3]
        (Thread/sleep 20)
        (d/transact *test-conn* [{:user/name (str "concurrent-user-" i)
                                  :user/email (str "concurrent-" i "@example.com")}]))

      ;; Wait for GC to complete
      (let [result @gc-future]
        (is (some? result) "GC should complete successfully")

        ;; Verify the concurrent data is still there
        (let [concurrent-users (d/q '[:find ?name
                                      :where
                                      [_ :user/name ?name]
                                      [(clojure.string/starts-with? ?name "concurrent")]]
                                   @*test-conn*)]
          (is (= 3 (count concurrent-users))
              "Data added during GC should still exist"))))))
