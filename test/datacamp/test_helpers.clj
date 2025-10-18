(ns datacamp.test-helpers
  "Test helpers and utilities for Datacamp tests"
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.directory :as dir]
            [clojure.java.io :as io]
            [zufall.core :refer [rand-german-mammal]])
  (:import [java.io File]
           [java.util UUID]))

;; =============================================================================
;; Test Data Generators
;; =============================================================================

(def test-schema
  [{:db/ident :user/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/email
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :user/age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/rating
    :db/valueType :db.type/double
    :db/cardinality :db.cardinality/one}
   {:db/ident :user/tags
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many}
   {:db/ident :post/title
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :post/content
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :post/author
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :post/created-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}])

(defn generate-test-users
  "Generate test user data"
  [n]
  (mapv (fn [i]
          (let [name (rand-german-mammal)]
            {:user/name name
            :user/email (str name "@example.com")
            :user/age (+ 20 (rand-int 40))
            :user/rating (* 5 (rand))
            :user/tags ["test" (str "tag-" (rand-int 10))]}))
        (range n)))

(defn generate-test-posts
  "Generate test post data"
  [n user-eids]
  (mapv (fn [i]
          {:post/title (str "Post " i)
           :post/content (str "This is the content of post " i ". "
                            (apply str (repeat 50 "Lorem ipsum dolor sit amet. ")))
           :post/author (rand-nth user-eids)
           :post/created-at (java.util.Date.)})
        (range n)))

;; =============================================================================
;; Database Setup and Teardown
;; =============================================================================

(defn create-test-db
  "Create a test database with the given configuration"
  [config]
  (try
    (d/delete-database config)
    (catch Exception _))
  (d/create-database config)
  (let [conn (d/connect config)]
    (d/transact conn test-schema)
    conn))

(defn populate-test-db
  "Populate a test database with sample data"
  [conn & {:keys [user-count post-count]
           :or {user-count 50 post-count 100}}]
  (let [users (generate-test-users user-count)
        {:keys [tempids]} (d/transact conn users)
        user-eids (vals tempids)
        posts (generate-test-posts post-count user-eids)]
    (d/transact conn posts)
    {:user-count user-count
     :post-count post-count
     :total-datoms (count (d/datoms @conn :eavt))}))

(defn cleanup-test-db
  "Clean up a test database"
  [config]
  (try
    (d/delete-database config)
    (catch Exception e
      (println "Warning: Failed to cleanup database:" (.getMessage e)))))

;; =============================================================================
;; Directory Test Helpers
;; =============================================================================

(defn temp-test-dir
  "Create a temporary test directory"
  []
  (let [tmp-dir (io/file (System/getProperty "java.io.tmpdir")
                         (str "datacamp-test-" (UUID/randomUUID)))]
    (.mkdirs tmp-dir)
    (.getAbsolutePath tmp-dir)))

(defn cleanup-test-dir
  "Clean up a test directory"
  [dir-path]
  (when dir-path
    (try
      (dir/cleanup-directory dir-path)
      (catch Exception e
        (println "Warning: Failed to cleanup directory:" (.getMessage e))))))

;; =============================================================================
;; Backup Test Helpers
;; =============================================================================

(defn verify-backup-structure
  "Verify that a backup has the expected structure"
  [backup-path]
  (let [manifest-path (str backup-path "/manifest.edn")
        checkpoint-path (str backup-path "/checkpoint.edn")
        chunks-dir (str backup-path "/chunks")
        complete-marker (str backup-path "/complete.marker")]
    {:manifest-exists? (dir/file-exists? manifest-path)
     :checkpoint-exists? (dir/file-exists? checkpoint-path)
     :chunks-dir-exists? (dir/file-exists? chunks-dir)
     :complete-marker-exists? (dir/file-exists? complete-marker)}))

(defn count-backup-chunks
  "Count the number of chunks in a backup"
  [backup-path]
  (let [chunks-dir (str backup-path "/chunks")
        files (dir/list-files chunks-dir :pattern #"\.fressian\.gz$")]
    (count files)))

(defn backup-size
  "Get the total size of a backup in bytes"
  [backup-path]
  (dir/get-directory-size backup-path))

;; =============================================================================
;; Assertion Helpers
;; =============================================================================

(defn assert-backup-successful
  "Assert that a backup completed successfully"
  [result]
  (is (true? (:success result)) "Backup should succeed")
  (is (some? (:backup-id result)) "Backup should have an ID")
  (is (pos? (:datom-count result)) "Backup should contain datoms")
  (is (pos? (:total-size-bytes result)) "Backup should have size"))

(defn assert-backup-valid
  "Assert that a backup is valid and complete"
  [backup-path]
  (let [structure (verify-backup-structure backup-path)]
    (is (:manifest-exists? structure) "Manifest should exist")
    (is (:checkpoint-exists? structure) "Checkpoint should exist")
    (is (:chunks-dir-exists? structure) "Chunks directory should exist")
    (is (:complete-marker-exists? structure) "Complete marker should exist")
    (is (pos? (count-backup-chunks backup-path)) "Should have chunks")))

(defn assert-dbs-equivalent
  "Assert that two databases have the same datoms"
  [conn1 conn2]
  (let [datoms1 (sort-by (juxt :e :a :v) (d/datoms @conn1 :eavt))
        datoms2 (sort-by (juxt :e :a :v) (d/datoms @conn2 :eavt))]
    (is (= (count datoms1) (count datoms2))
        "Databases should have same number of datoms")
    ;; Compare just the data, not the transaction IDs
    (is (= (map (juxt :e :a :v) datoms1)
           (map (juxt :e :a :v) datoms2))
        "Databases should have equivalent data")))

;; =============================================================================
;; Test Macros
;; =============================================================================

(defmacro with-test-db
  "Execute body with a test database, cleaning up afterwards"
  [config-sym config & body]
  `(let [~config-sym ~config
         conn# (create-test-db ~config-sym)]
     (try
       ~@body
       (finally
         (cleanup-test-db ~config-sym)))))

(defmacro with-test-dir
  "Execute body with a test directory, cleaning up afterwards"
  [dir-sym & body]
  `(let [~dir-sym (temp-test-dir)]
     (try
       ~@body
       (finally
         (cleanup-test-dir ~dir-sym)))))

(defmacro with-test-backup
  "Execute body with a test backup, cleaning up afterwards"
  [backup-result-sym conn dir-config database-id & body]
  `(let [~backup-result-sym (backup/backup-to-directory ~conn ~dir-config
                                                        :database-id ~database-id)]
     (try
       ~@body
       (finally
         (when (:path ~backup-result-sym)
           (dir/cleanup-directory (:path ~backup-result-sym)))))))

;; =============================================================================
;; Performance Helpers
;; =============================================================================

(defn measure-time
  "Measure execution time of a function"
  [f]
  (let [start (System/nanoTime)
        result (f)
        end (System/nanoTime)
        duration-ms (/ (- end start) 1000000.0)]
    {:result result
     :duration-ms duration-ms}))

(defn assert-performance
  "Assert that an operation completes within expected time"
  [description f max-ms]
  (let [{:keys [result duration-ms]} (measure-time f)]
    (is (<= duration-ms max-ms)
        (format "%s should complete within %dms (took %.2fms)"
                description max-ms duration-ms))
    result))
