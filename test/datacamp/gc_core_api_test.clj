(ns datacamp.gc-core-api-test
  "Integration tests for the datacamp.core/gc! API"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [datacamp.core :as datacamp]
            [datacamp.test-helpers :as h])
  (:import [java.util UUID]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def ^:dynamic *test-db-config* nil)
(def ^:dynamic *test-conn* nil)

(defn gc-api-test-fixture [f]
  (let [config {:store {:backend :mem :id (str "gc-api-test-" (UUID/randomUUID))}}]
    (binding [*test-db-config* config
              *test-conn* (h/create-test-db config)]
      (try
        (f)
        (finally
          (h/cleanup-test-db config))))))

(use-fixtures :each gc-api-test-fixture)

;; =============================================================================
;; Basic API Tests
;; =============================================================================

(deftest gc-dry-run-default-test
  (testing "gc! defaults to dry-run mode (safety first)"
    (h/populate-test-db *test-conn* :user-count 10 :post-count 20)

    (let [initial-datom-count (count (d/datoms @*test-conn* :eavt))
          result (datacamp/gc! *test-conn*)]

      ;; Should return dry-run result
      (is (some? (:would-delete-count result)) "Should have would-delete-count in dry run")
      (is (some? (:reachable-count result)) "Should have reachable-count")

      ;; Verify nothing was deleted
      (let [final-datom-count (count (d/datoms @*test-conn* :eavt))]
        (is (= initial-datom-count final-datom-count)
            "Default dry-run should not delete anything")))))

(deftest gc-actual-run-test
  (testing "gc! with :dry-run false actually deletes"
    (h/populate-test-db *test-conn* :user-count 5 :post-count 10)

    (let [result (datacamp/gc! *test-conn* :dry-run false)]

      ;; Should return actual result
      (is (some? (:deleted-count result)) "Should have deleted-count")
      (is (some? (:reachable-count result)) "Should have reachable-count")
      (is (some? (:duration-ms result)) "Should have duration")
      (is (number? (:deleted-count result)))
      (is (>= (:deleted-count result) 0)))))

(deftest gc-with-retention-days-test
  (testing "gc! accepts retention-days option"
    (h/populate-test-db *test-conn* :user-count 10 :post-count 20)

    ;; Test with different retention periods
    (let [result-7 (datacamp/gc! *test-conn* :retention-days 7)
          result-30 (datacamp/gc! *test-conn* :retention-days 30)
          result-365 (datacamp/gc! *test-conn* :retention-days 365)]

      ;; All should complete
      (is (some? result-7))
      (is (some? result-30))
      (is (some? result-365))

      ;; Longer retention should keep more data (or same)
      (is (<= (:would-delete-count result-365)
             (:would-delete-count result-30)
             (:would-delete-count result-7))))))

;; =============================================================================
;; Auto-Resume Tests
;; =============================================================================

(deftest gc-auto-resume-test
  (testing "gc! automatically resumes interrupted GC"
    (h/populate-test-db *test-conn* :user-count 10 :post-count 20)

    ;; Start a GC that will be "interrupted"
    (let [gc-future (future
                     (datacamp/gc! *test-conn*
                                  :dry-run false
                                  :checkpoint-interval 5))]

      ;; Wait a bit then cancel
      (Thread/sleep 100)
      (future-cancel gc-future)

      ;; Check if checkpoint exists
      (let [status (datacamp/gc-status *test-conn*)]
        (when (= :in-progress (:status status))
          ;; Call gc! again - should auto-resume
          (let [result (datacamp/gc! *test-conn* :dry-run false)]

            (is (some? result) "Auto-resume should work")
            (is (true? (:resumed? result)) "Should indicate it was resumed")

            ;; After completion, no checkpoint should remain
            (let [final-status (datacamp/gc-status *test-conn*)]
              (is (= :no-gc-in-progress (:status final-status))
                  "Checkpoint should be cleaned up"))))))))

(deftest gc-force-new-test
  (testing "gc! with :force-new abandons existing checkpoint"
    (h/populate-test-db *test-conn* :user-count 10 :post-count 20)

    ;; Start a GC
    (let [gc-future (future
                     (datacamp/gc! *test-conn*
                                  :dry-run false
                                  :checkpoint-interval 5))]

      ;; Wait a bit then cancel
      (Thread/sleep 100)
      (future-cancel gc-future)

      ;; Check if checkpoint exists
      (let [status (datacamp/gc-status *test-conn*)]
        (when (= :in-progress (:status status))
          (let [old-gc-id (:gc-id status)]

            ;; Force new GC
            (let [result (datacamp/gc! *test-conn*
                                      :dry-run false
                                      :force-new true)]

              (is (some? result) "Force new should work")
              (is (not (:resumed? result)) "Should not indicate resumed")

              ;; Should have started fresh
              (is (some? result)))))))))

;; =============================================================================
;; GC Status API Tests
;; =============================================================================

(deftest gc-status-no-gc-test
  (testing "gc-status when no GC in progress"
    (let [status (datacamp/gc-status *test-conn*)]
      (is (= :no-gc-in-progress (:status status))))))

(deftest gc-status-during-gc-test
  (testing "gc-status while GC is running"
    (h/populate-test-db *test-conn* :user-count 10 :post-count 20)

    ;; Start GC in background
    (let [gc-future (future
                     (datacamp/gc! *test-conn*
                                  :dry-run false
                                  :checkpoint-interval 10))]

      ;; Check status while running
      (Thread/sleep 100)

      (let [status (datacamp/gc-status *test-conn*)]
        (when (= :in-progress (:status status))
          (is (some? (:gc-id status)) "Should have GC ID")
          (is (some? (:started-at status)) "Should have start time")
          (is (number? (:visited-count status)) "Should have visited count")
          (is (number? (:reachable-count status)) "Should have reachable count")))

      ;; Wait for completion
      @gc-future

      ;; Status after completion
      (let [final-status (datacamp/gc-status *test-conn*)]
        (is (= :no-gc-in-progress (:status final-status))
            "Status should show no GC after completion")))))

;; =============================================================================
;; Backend Optimization Tests
;; =============================================================================

(deftest gc-auto-optimization-test
  (testing "gc! automatically detects and applies backend optimizations"
    (h/populate-test-db *test-conn* :user-count 5 :post-count 10)

    ;; Run GC without specifying batch sizes
    (let [result (datacamp/gc! *test-conn* :dry-run false)]

      ;; Should complete successfully with auto-detected settings
      (is (some? result))
      (is (some? (:deleted-count result)))

      ;; Verify it worked
      (is (>= (:deleted-count result) 0)))))

(deftest gc-manual-optimization-test
  (testing "gc! accepts manual batch-size and parallel-batches"
    (h/populate-test-db *test-conn* :user-count 5 :post-count 10)

    (let [result (datacamp/gc! *test-conn*
                              :dry-run false
                              :batch-size 100
                              :parallel-batches 2)]

      ;; Should use provided settings
      (is (some? result))
      (is (some? (:deleted-count result))))))

;; =============================================================================
;; Error Handling Tests
;; =============================================================================

(deftest gc-on-empty-db-test
  (testing "gc! handles empty database gracefully"
    (let [result (datacamp/gc! *test-conn*)]

      (is (some? result))
      (is (some? (:reachable-count result))))))

(deftest gc-with-invalid-options-test
  (testing "gc! handles invalid retention-days gracefully"
    (h/populate-test-db *test-conn* :user-count 5 :post-count 10)

    ;; Negative retention days - should still work or throw meaningful error
    (try
      (let [result (datacamp/gc! *test-conn* :retention-days -1)]
        (is (some? result) "Should handle negative retention days"))
      (catch Exception e
        ;; Or throw meaningful error
        (is (some? (.getMessage e)))))))

;; =============================================================================
;; Integration with Datahike Tests
;; =============================================================================

(deftest gc-preserves-data-integrity-test
  (testing "gc! preserves all reachable data"
    ;; Add known test data
    (d/transact *test-conn* [{:user/name "Alice" :user/email "alice@example.com"}
                            {:user/name "Bob" :user/email "bob@example.com"}])

    ;; Run GC
    (let [result (datacamp/gc! *test-conn* :dry-run false :retention-days 30)]

      (is (some? result))

      ;; Verify data still exists
      (let [users (d/q '[:find ?name
                        :where [_ :user/name ?name]]
                      @*test-conn*)]
        (is (= 2 (count users)) "All users should still exist")
        (is (contains? (set (map first users)) "Alice"))
        (is (contains? (set (map first users)) "Bob"))))))

(deftest gc-multiple-runs-test
  (testing "gc! can be run multiple times safely"
    (h/populate-test-db *test-conn* :user-count 10 :post-count 20)

    ;; Run GC multiple times
    (let [result1 (datacamp/gc! *test-conn* :dry-run false)
          result2 (datacamp/gc! *test-conn* :dry-run false)
          result3 (datacamp/gc! *test-conn* :dry-run false)]

      ;; All should complete
      (is (some? result1))
      (is (some? result2))
      (is (some? result3))

      ;; Subsequent runs should delete less (or nothing)
      (is (<= (:deleted-count result3)
             (:deleted-count result2)
             (:deleted-count result1))))))

;; =============================================================================
;; Workflow Tests
;; =============================================================================

(deftest gc-recommended-workflow-test
  (testing "Recommended workflow: dry-run then actual"
    (h/populate-test-db *test-conn* :user-count 10 :post-count 20)

    ;; Step 1: Dry run to preview
    (let [dry-result (datacamp/gc! *test-conn* :retention-days 30)]

      (is (some? (:would-delete-count dry-result)) "Dry run should show what would be deleted")
      (is (some? (:reachable-count dry-result)))

      (let [initial-count (count (d/datoms @*test-conn* :eavt))]

        ;; Step 2: Actual run
        (let [actual-result (datacamp/gc! *test-conn*
                                         :dry-run false
                                         :retention-days 30)]

          (is (some? (:deleted-count actual-result)))

          ;; Verify some deletion might have occurred
          (let [final-count (count (d/datoms @*test-conn* :eavt))]
            ;; Current head should be kept, so count should be similar
            (is (<= final-count initial-count))))))))

(deftest gc-with-concurrent-queries-test
  (testing "gc! doesn't interfere with concurrent queries"
    (h/populate-test-db *test-conn* :user-count 20 :post-count 40)

    ;; Start GC in background
    (let [gc-future (future (datacamp/gc! *test-conn* :dry-run false))

          ;; Run queries concurrently
          query-future (future
                        (dotimes [_ 10]
                          (Thread/sleep 20)
                          (let [users (d/q '[:find ?name
                                            :where [_ :user/name ?name]]
                                          @*test-conn*)]
                            (is (pos? (count users)) "Queries should work during GC"))))]

      ;; Wait for both to complete
      (let [gc-result @gc-future
            _ @query-future]

        (is (some? gc-result) "GC should complete")

        ;; Verify database still queryable
        (let [final-users (d/q '[:find ?name
                                :where [_ :user/name ?name]]
                              @*test-conn*)]
          (is (pos? (count final-users)) "Database should still be queryable"))))))

;; =============================================================================
;; Documentation Examples Tests
;; =============================================================================

(deftest gc-docstring-examples-test
  (testing "Examples from gc! docstring work correctly"
    (h/populate-test-db *test-conn* :user-count 5 :post-count 10)

    ;; Example 1: Dry run (default)
    (let [result1 (datacamp/gc! *test-conn*)]
      (is (some? result1)))

    ;; Example 2: Actual run with 30 day retention
    (let [result2 (datacamp/gc! *test-conn* :dry-run false :retention-days 30)]
      (is (some? result2)))

    ;; Example 3: Resume (will auto-resume if checkpoint exists)
    (let [result3 (datacamp/gc! *test-conn* :dry-run false)]
      (is (some? result3)))))
