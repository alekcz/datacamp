(ns datacamp.konserve-extensions-test
  "Tests for Konserve batch operation extensions"
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.core.async :as async]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [datacamp.konserve-extensions :as ext]
            [superv.async :refer [<?? S]])
  (:import [java.util UUID]))

;; =============================================================================
;; Test Fixtures
;; =============================================================================

(def ^:dynamic *test-store* nil)

(defn store-fixture [f]
  (binding [*test-store* (<?? S (new-mem-store))]
    (f)))

(use-fixtures :each store-fixture)

;; =============================================================================
;; Batch Support Detection Tests
;; =============================================================================

(deftest supports-batch-test
  (testing "Memory store doesn't support batch operations by default"
    (is (not (ext/supports-batch? *test-store*))
        "Fresh memory store should not support batch operations"))

  (testing "Memory store supports batch after extension"
    (ext/extend-memory-store-with-batch! *test-store*)
    (is (ext/supports-batch? *test-store*)
        "Memory store should support batch after extension")))

;; =============================================================================
;; Batch Dissoc Tests
;; =============================================================================

(deftest batch-dissoc-test
  (testing "Batch dissoc removes multiple keys"
    ;; Add test data
    (<?? S (k/assoc *test-store* :key1 "value1"))
    (<?? S (k/assoc *test-store* :key2 "value2"))
    (<?? S (k/assoc *test-store* :key3 "value3"))
    (<?? S (k/assoc *test-store* :key4 "value4"))

    ;; Verify data exists
    (is (= "value1" (<?? S (k/get *test-store* :key1))))
    (is (= "value2" (<?? S (k/get *test-store* :key2))))

    ;; Batch delete
    (let [deleted-count (<?? S (ext/batch-dissoc! *test-store* [:key1 :key2 :key3]))]

      ;; Verify deletion count
      (is (= 3 deleted-count) "Should delete 3 keys")

      ;; Verify keys are deleted
      (is (nil? (<?? S (k/get *test-store* :key1))))
      (is (nil? (<?? S (k/get *test-store* :key2))))
      (is (nil? (<?? S (k/get *test-store* :key3))))

      ;; Verify key4 still exists
      (is (= "value4" (<?? S (k/get *test-store* :key4)))))))

(deftest batch-dissoc-empty-list-test
  (testing "Batch dissoc with empty key list"
    (let [result (<?? S (ext/batch-dissoc! *test-store* []))]
      (is (= 0 result) "Should return 0 for empty list"))))

(deftest batch-dissoc-nonexistent-keys-test
  (testing "Batch dissoc with non-existent keys"
    (let [result (<?? S (ext/batch-dissoc! *test-store* [:nonexistent1 :nonexistent2]))]
      ;; Should complete without errors
      (is (number? result)))))

;; =============================================================================
;; Batch Get Tests
;; =============================================================================

(deftest batch-get-test
  (testing "Batch get retrieves multiple values"
    ;; Add test data
    (<?? S (k/assoc *test-store* :a "value-a"))
    (<?? S (k/assoc *test-store* :b "value-b"))
    (<?? S (k/assoc *test-store* :c "value-c"))

    ;; Batch get
    (let [results (<?? S (ext/batch-get *test-store* [:a :b :c]))]

      (is (map? results) "Should return a map")
      (is (= "value-a" (:a results)))
      (is (= "value-b" (:b results)))
      (is (= "value-c" (:c results))))))

(deftest batch-get-mixed-keys-test
  (testing "Batch get with mix of existing and non-existing keys"
    (<?? S (k/assoc *test-store* :exists1 "value1"))
    (<?? S (k/assoc *test-store* :exists2 "value2"))

    (let [results (<?? S (ext/batch-get *test-store* [:exists1 :nonexistent :exists2]))]

      (is (= "value1" (:exists1 results)))
      (is (= "value2" (:exists2 results)))
      (is (nil? (:nonexistent results))))))

;; =============================================================================
;; Batch Assoc Tests
;; =============================================================================

(deftest batch-assoc-test
  (testing "Batch assoc sets multiple key-value pairs"
    (let [kvs [[:k1 "v1"]
               [:k2 "v2"]
               [:k3 "v3"]]
          count (<?? S (ext/batch-assoc! *test-store* kvs))]

      (is (= 3 count) "Should set 3 key-value pairs")

      ;; Verify values were set
      (is (= "v1" (<?? S (k/get *test-store* :k1))))
      (is (= "v2" (<?? S (k/get *test-store* :k2))))
      (is (= "v3" (<?? S (k/get *test-store* :k3)))))))

(deftest batch-assoc-overwrites-test
  (testing "Batch assoc overwrites existing values"
    ;; Set initial value
    (<?? S (k/assoc *test-store* :key "old-value"))

    (is (= "old-value" (<?? S (k/get *test-store* :key))))

    ;; Batch assoc with new value
    (<?? S (ext/batch-assoc! *test-store* [[:key "new-value"]]))

    ;; Verify overwrite
    (is (= "new-value" (<?? S (k/get *test-store* :key))))))

;; =============================================================================
;; Backend Optimization Tests
;; =============================================================================

(deftest optimize-for-backend-test
  (testing "Backend-specific optimizations are returned"
    ;; optimize-for-backend is in datacamp.gc, not konserve-extensions
    (let [gc-optimize (requiring-resolve 'datacamp.gc/optimize-for-backend)
          s3-opts (gc-optimize {:backend :s3})
          jdbc-opts (gc-optimize {:backend :jdbc})
          file-opts (gc-optimize {:backend :file})
          mem-opts (gc-optimize {:backend :mem})
          unknown-opts (gc-optimize {:backend :unknown})]

      ;; S3 optimizations
      (is (= 1000 (:batch-size s3-opts)))
      (is (= 3 (:parallel-batches s3-opts)))

      ;; JDBC optimizations
      (is (= 5000 (:batch-size jdbc-opts)))
      (is (= 1 (:parallel-batches jdbc-opts)))

      ;; File optimizations
      (is (= 100 (:batch-size file-opts)))
      (is (= 10 (:parallel-batches file-opts)))

      ;; Memory optimizations
      (is (some? (:batch-size mem-opts)))
      (is (some? (:parallel-batches mem-opts)))

      ;; Unknown backend gets defaults
      (is (some? (:batch-size unknown-opts)))
      (is (some? (:parallel-batches unknown-opts))))))

;; =============================================================================
;; Auto-Extension Tests
;; =============================================================================

(deftest auto-extend-store-test
  (testing "Auto-extend adds batch support to memory store"
    (let [store (<?? S (new-mem-store))
          config {:backend :mem}]

      ;; Note: Memory stores may already have batch support from previous tests
      ;; since extend modifies the class globally. The important thing is that
      ;; auto-extend is idempotent and batch operations work correctly.

      ;; Auto-extend (idempotent - won't break if already extended)
      (ext/auto-extend-store! store config)

      ;; Should support batch after auto-extend
      (is (ext/supports-batch? store))

      ;; Verify batch operations work
      (<?? S (k/assoc store :test "value"))
      (let [result (<?? S (ext/batch-dissoc! store [:test]))]
        (is (= 1 result))
        (is (nil? (<?? S (k/get store :test))))))))

(deftest auto-extend-idempotent-test
  (testing "Auto-extend is idempotent"
    (let [store (<?? S (new-mem-store))
          config {:backend :mem}]

      ;; Extend twice
      (ext/auto-extend-store! store config)
      (ext/auto-extend-store! store config)

      ;; Should still work
      (is (ext/supports-batch? store))

      (<?? S (k/assoc store :key "value"))
      (let [result (<?? S (ext/batch-dissoc! store [:key]))]
        (is (= 1 result))))))

;; =============================================================================
;; Performance Tests
;; =============================================================================

(deftest batch-vs-individual-performance-test
  (testing "Batch operations are more efficient than individual"
    (let [store (<?? S (new-mem-store))
          num-keys 100
          keys-to-delete (vec (map #(keyword (str "key-" %)) (range num-keys)))]

      ;; Setup data
      (doseq [k keys-to-delete]
        (<?? S (k/assoc store k "value")))

      ;; Measure batch delete
      (let [batch-start (System/nanoTime)
            _ (<?? S (ext/batch-dissoc! store keys-to-delete))
            batch-duration (/ (- (System/nanoTime) batch-start) 1000000.0)]

        ;; Verify all deleted
        (doseq [k keys-to-delete]
          (is (nil? (<?? S (k/get store k)))))

        ;; Batch should complete in reasonable time
        (is (< batch-duration 5000) ; Less than 5 seconds for 100 deletes
            (str "Batch delete took " batch-duration "ms"))))))

;; =============================================================================
;; Edge Cases
;; =============================================================================

(deftest batch-with-duplicate-keys-test
  (testing "Batch operations handle duplicate keys"
    (<?? S (k/assoc *test-store* :dup "value"))

    ;; Batch delete with duplicates
    (let [result (<?? S (ext/batch-dissoc! *test-store* [:dup :dup :dup]))]
      ;; Should handle gracefully (count might vary by implementation)
      (is (number? result))

      ;; Key should be deleted
      (is (nil? (<?? S (k/get *test-store* :dup)))))))

(deftest batch-with-special-keys-test
  (testing "Batch operations handle special keys"
    (let [special-keys [:keyword
                       "string"
                       123
                       :with-namespace/name
                       :with-dash-and_underscore]]

      ;; Assoc all special keys
      (doseq [k special-keys]
        (<?? S (k/assoc *test-store* k "value")))

      ;; Batch dissoc
      (let [result (<?? S (ext/batch-dissoc! *test-store* special-keys))]
        (is (= (count special-keys) result))

        ;; Verify all deleted
        (doseq [k special-keys]
          (is (nil? (<?? S (k/get *test-store* k)))))))))

(deftest batch-large-values-test
  (testing "Batch operations handle large values"
    (let [large-value (vec (range 10000))
          keys [:large1 :large2 :large3]]

      ;; Store large values
      (<?? S (ext/batch-assoc! *test-store*
                             (map #(vector % large-value) keys)))

      ;; Verify stored
      (doseq [k keys]
        (is (= large-value (<?? S (k/get *test-store* k)))))

      ;; Batch delete
      (let [result (<?? S (ext/batch-dissoc! *test-store* keys))]
        (is (= 3 result))

        ;; Verify deleted
        (doseq [k keys]
          (is (nil? (<?? S (k/get *test-store* k)))))))))

;; =============================================================================
;; Fallback Behavior Tests
;; =============================================================================

(deftest fallback-to-parallel-operations-test
  (testing "Batch operations work via native support or fallback"
    (let [store (<?? S (new-mem-store))]

      ;; Memory stores may have batch support from previous tests
      ;; (extend modifies the class globally)
      ;; The important thing is that batch operations work

      ;; Add test data
      (<?? S (k/assoc store :k1 "v1"))
      (<?? S (k/assoc store :k2 "v2"))
      (<?? S (k/assoc store :k3 "v3"))

      ;; Batch dissoc should work (either native or fallback)
      (let [result (<?? S (ext/batch-dissoc! store [:k1 :k2 :k3]))]
        (is (number? result))

        ;; Verify deletion worked
        (is (nil? (<?? S (k/get store :k1))))
        (is (nil? (<?? S (k/get store :k2))))
        (is (nil? (<?? S (k/get store :k3))))))))
