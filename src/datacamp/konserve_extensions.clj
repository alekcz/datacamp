(ns datacamp.konserve-extensions
  "Extensions to Konserve for batch operations.
   These can be contributed upstream once battle-tested."
  (:require [konserve.core :as k]
            [konserve.protocols :as kp]
            [clojure.core.async :as async]
            [superv.async :refer [go-try S <? <??]]
            [taoensso.timbre :as log])
  (:import [java.util.concurrent Executors TimeUnit Callable]
           [java.io File]))

;; =============================================================================
;; Protocol for Batch Operations
;; =============================================================================

(defprotocol PBatchOperations
  "Protocol for batch operations on stores"
  (-batch-dissoc [this keys] "Delete multiple keys in a single operation")
  (-batch-assoc [this kvs] "Set multiple key-value pairs in a single operation")
  (-batch-get [this keys] "Get multiple values in a single operation"))

;; =============================================================================
;; S3 Store Batch Operations
;; =============================================================================

(defn s3-batch-delete!
  "Batch delete for S3-backed stores.
   S3 allows up to 1000 objects per delete request."
  [s3-client bucket keys]
  (when (seq keys)
    (let [;; S3 has a limit of 1000 objects per request
          batches (partition-all 1000 keys)]
      (doseq [batch batches]
        (let [delete-request {:Bucket bucket
                             :Delete {:Objects (map #(hash-map :Key %) batch)
                                    :Quiet true}}]
          (try
            (.deleteObjects s3-client delete-request)
            (log/debug "Deleted" (count batch) "objects from S3")
            (catch Exception e
              (log/error e "Failed to delete S3 batch"))))))))

(defn extend-s3-store-with-batch!
  "Extend an S3 store with batch operations"
  [store s3-client bucket]
  (extend (class store)
    PBatchOperations
    {:-batch-dissoc
     (fn [_ keys]
       (go-try S
         (s3-batch-delete! s3-client bucket keys)
         (count keys)))

     :-batch-get
     (fn [this keys]
       (go-try S
         ;; For S3, parallel get is often faster than batch
         (let [futures (map #(go-try S
                             (let [result (<? S (k/get this %))]
                               [% (if (and (vector? result) (= 2 (count result)))
                                    (second result)
                                    result)])) keys)]
           (into {} (<?? S (async/map vector futures))))))

     :-batch-assoc
     (fn [this kvs]
       (go-try S
         ;; S3 doesn't have batch put, but we can parallelize
         (let [futures (map (fn [[k v]] (k/assoc this k v)) kvs)]
           (<?? S (async/map (constantly :done) futures))
           (count kvs))))}))

;; =============================================================================
;; PostgreSQL/JDBC Store Batch Operations
;; =============================================================================

(defn jdbc-batch-delete!
  "Batch delete for JDBC-backed stores"
  [conn table-name keys]
  (when (seq keys)
    ;; PostgreSQL can handle large arrays efficiently
    (let [jdbc-execute! (try (require 'clojure.java.jdbc)
                             (resolve 'clojure.java.jdbc/execute!)
                             (catch Exception _ nil))]
      (if jdbc-execute!
        (let [sql (str "DELETE FROM " table-name " WHERE key = ANY(?)")]
          (try
            (jdbc-execute! conn [sql (into-array String keys)])
            (log/debug "Deleted" (count keys) "rows from" table-name)
            (catch Exception e
              (log/error e "Failed to delete JDBC batch"))))
        (log/warn "JDBC not available, skipping batch delete")))))

(defn jdbc-batch-get
  "Batch get for JDBC-backed stores"
  [conn table-name keys]
  (when (seq keys)
    (let [jdbc-query (try (require 'clojure.java.jdbc)
                         (resolve 'clojure.java.jdbc/query)
                         (catch Exception _ nil))]
      (if jdbc-query
        (let [sql (str "SELECT key, value FROM " table-name " WHERE key = ANY(?)")
              results (jdbc-query conn [sql (into-array String keys)])]
          (into {} (map (juxt :key :value) results)))
        {}))))

(defn extend-jdbc-store-with-batch!
  "Extend a JDBC store with batch operations"
  [store conn table-name]
  (extend (class store)
    PBatchOperations
    {:-batch-dissoc
     (fn [_ keys]
       (go-try S
         (jdbc-batch-delete! conn table-name keys)
         (count keys)))

     :-batch-get
     (fn [_ keys]
       (go-try S
         (jdbc-batch-get conn table-name keys)))

     :-batch-assoc
     (fn [this kvs]
       (go-try S
         ;; Use batch insert with ON CONFLICT
         (let [jdbc-execute-batch! (try (require 'clojure.java.jdbc)
                                        (resolve 'clojure.java.jdbc/execute-batch!)
                                        (catch Exception _ nil))]
           (if jdbc-execute-batch!
             (let [sql (str "INSERT INTO " table-name " (key, value) VALUES (?, ?) "
                           "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")]
               (jdbc-execute-batch! conn sql
                                   (map (fn [[k v]] [k v]) kvs))
               (count kvs))
             (do (log/warn "JDBC not available, skipping batch assoc")
                 0)))))}))

;; =============================================================================
;; File Store Batch Operations
;; =============================================================================

(defn file-batch-delete!
  "Batch delete for file-backed stores using thread pool"
  [base-path keys parallelism]
  (when (seq keys)
    (let [executor (Executors/newFixedThreadPool parallelism)]
      (try
        (let [futures (map (fn [key]
                           (.submit executor
                                   ^Callable
                                   (fn []
                                     (let [file (File. base-path (str key))]
                                       (when (.exists file)
                                         (.delete file))))))
                         keys)]
          ;; Wait for all deletions to complete
          (doseq [f futures]
            (.get f))
          (log/debug "Deleted" (count keys) "files"))
        (finally
          (.shutdown executor)
          (.awaitTermination executor 10 TimeUnit/SECONDS))))))

(defn extend-file-store-with-batch!
  "Extend a file store with batch operations"
  [store base-path]
  (extend (class store)
    PBatchOperations
    {:-batch-dissoc
     (fn [_ keys]
       (go-try S
         (file-batch-delete! base-path keys 10)
         (count keys)))

     :-batch-get
     (fn [this keys]
       (go-try S
         ;; Parallel read with thread pool
         (let [executor (Executors/newFixedThreadPool 10)]
           (try
             (let [futures (map #(go-try S
                                 (let [result (<? S (k/get this %))]
                                   [% (if (and (vector? result) (= 2 (count result)))
                                        (second result)
                                        result)])) keys)]
               (into {} (<?? S (async/map vector futures))))
             (finally
               (.shutdown executor))))))

     :-batch-assoc
     (fn [this kvs]
       (go-try S
         ;; Parallel write with thread pool
         (let [executor (Executors/newFixedThreadPool 10)]
           (try
             (let [futures (map (fn [[k v]] (k/assoc this k v)) kvs)]
               (<?? S (async/map (constantly :done) futures))
               (count kvs))
             (finally
               (.shutdown executor))))))}))

;; =============================================================================
;; Memory Store Batch Operations
;; =============================================================================

(defn extend-memory-store-with-batch!
  "Extend a memory store with batch operations"
  [store]
  (extend (class store)
    PBatchOperations
    {:-batch-dissoc
     (fn [this keys]
       (go-try S
         ;; Use konserve's dissoc for each key to ensure proper cleanup
         (doseq [k keys]
           (<? S (k/dissoc this k)))
         (count keys)))

     :-batch-get
     (fn [this keys]
       (go-try S
         ;; For memory store, we need to get each key properly through konserve API
         (let [futures (map #(go-try S
                             (let [result (<? S (k/get this %))]
                               [% (if (and (vector? result) (= 2 (count result)))
                                    (second result)
                                    result)])) keys)]
           (into {} (<?? S (async/map vector futures))))))

     :-batch-assoc
     (fn [this kvs]
       (go-try S
         ;; Use konserve's assoc for each key-value pair to ensure proper serialization
         (doseq [[k v] kvs]
           (<? S (k/assoc this k v)))
         (count kvs)))}))

;; =============================================================================
;; Generic Batch Wrapper
;; =============================================================================

(defn supports-batch?
  "Check if a store supports batch operations"
  [store]
  (satisfies? PBatchOperations store))

(defn batch-dissoc!
  "Delete multiple keys from store.
   Falls back to parallel single operations if batch not supported."
  [store keys & {:keys [parallelism] :or {parallelism 10}}]
  (go-try S
    (if (supports-batch? store)
      ;; Use native batch operation
      (<? S (-batch-dissoc store keys))

      ;; Fall back to parallel single operations
      (let [;; Limit parallelism to avoid overwhelming the store
            batches (partition-all parallelism keys)]
        (loop [remaining batches
               total 0]
          (if (empty? remaining)
            total
            (let [batch (first remaining)
                  futures (map #(k/dissoc store %) batch)]
              (<? S (async/map (constantly :done) futures))
              (recur (rest remaining) (+ total (count batch))))))))))

(defn batch-get
  "Get multiple values from store.
   Falls back to parallel single operations if batch not supported."
  [store keys & {:keys [parallelism] :or {parallelism 10}}]
  (go-try S
    (if (supports-batch? store)
      ;; Use native batch operation
      (<? S (-batch-get store keys))

      ;; Fall back to parallel single operations
      (let [batches (partition-all parallelism keys)]
        (loop [remaining batches
               results {}]
          (if (empty? remaining)
            results
            (let [batch (first remaining)
                  futures (map #(go-try S
                                 (let [result (<? S (k/get store %))]
                                   ;; Handle metadata+value tuple format
                                   [% (if (and (vector? result) (= 2 (count result)))
                                        (second result)  ; Extract value from [metadata value]
                                        result)])) batch)
                  batch-results (<?? S (async/map vector futures))]
              (recur (rest remaining)
                     (into results batch-results)))))))))

(defn batch-assoc!
  "Set multiple key-value pairs in store.
   Falls back to parallel single operations if batch not supported."
  [store kvs & {:keys [parallelism] :or {parallelism 10}}]
  (go-try S
    (if (supports-batch? store)
      ;; Use native batch operation
      (<? S (-batch-assoc store kvs))

      ;; Fall back to parallel single operations
      (let [batches (partition-all parallelism kvs)]
        (loop [remaining batches
               total 0]
          (if (empty? remaining)
            total
            (let [batch (first remaining)
                  futures (map (fn [[k v]] (k/assoc store k v)) batch)]
              (<? S (async/map (constantly :done) futures))
              (recur (rest remaining) (+ total (count batch))))))))))

;; =============================================================================
;; Store Detection and Auto-Extension
;; =============================================================================

(defn auto-extend-store!
  "Automatically extend a store with batch operations based on its type"
  [store store-config]
  (when-not (supports-batch? store)
    (case (:backend store-config)
      :s3 (when-let [s3-config (:s3 store-config)]
            (extend-s3-store-with-batch! store
                                         (:client s3-config)
                                         (:bucket s3-config)))

      :jdbc (when-let [jdbc-config (:jdbc store-config)]
              (extend-jdbc-store-with-batch! store
                                            (:connection jdbc-config)
                                            (:table jdbc-config "datahike")))

      :file (when-let [path (:path store-config)]
              (extend-file-store-with-batch! store path))

      :mem (extend-memory-store-with-batch! store)

      ;; Unknown backend - no extension
      nil))
  store)

;; =============================================================================
;; Usage Example
;; =============================================================================

(comment
  ;; Example: Extend a store with batch operations
  (let [store-config {:backend :file :path "/tmp/test-store"}
        store (k/connect store-config)]

    ;; Auto-extend with batch operations
    (auto-extend-store! store store-config)

    ;; Now you can use batch operations
    (<?!! (batch-dissoc! store ["key1" "key2" "key3"]))
    (<?!! (batch-assoc! store [["key4" "value4"]
                              ["key5" "value5"]]))
    (<?!! (batch-get store ["key4" "key5"]))))