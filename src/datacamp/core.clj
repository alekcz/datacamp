(ns datacamp.core
  "Public API for Datahike backup and migration operations"
  (:require [datahike.api :as d]
            [taoensso.timbre :as log]
            [datacamp.s3 :as s3]
            [datacamp.directory :as dir]
            [datacamp.serialization :as ser]
            [datacamp.compression :as comp]
            [datacamp.metadata :as meta]
            [datacamp.utils :as utils]))

;; Default configuration
(def default-config
  {:chunk-size (* 64 1024 1024)  ; 64MB
   :compression :gzip
   :parallel 1                  ; Sequential by default (safer for memory)
   :storage-class :standard})

;; Public API

(defn backup-to-s3
  "Create a full backup of database to S3

  Parameters:
  - conn: Datahike connection
  - s3-config: Map with :bucket, :region, and optional :prefix
  - opts: Optional configuration map with:
    - :chunk-size - Size of each chunk in bytes (default: 64MB)
    - :compression - Compression algorithm (default: :gzip)
    - :parallel - Number of chunks to process concurrently (default: 1)
                  Values: 1 (sequential, default), 2-4 (balanced), 8+ (high-performance)
    - :database-id - Custom database identifier

  Returns: Map with backup details including :backup-id

  Example:
  (backup-to-s3 conn
                {:bucket \"my-bucket\" :region \"us-east-1\"}
                :chunk-size (* 512 1024 1024)  ; 512MB chunks
                :parallel 8                     ; 8 concurrent uploads
                :database-id \"production\")"
  [conn s3-config & {:keys [chunk-size compression parallel database-id]
                     :or {chunk-size (* 64 1024 1024)
                          compression :gzip
                          parallel 1
                          database-id "default-db"}}]
  (let [backup-id (utils/generate-backup-id)
        s3-client (s3/create-s3-client s3-config)
        bucket (:bucket s3-config)
        prefix (or (:prefix s3-config) "datahike-backups/")
        backup-path (str prefix database-id "/" backup-id "/")
        started-at (utils/current-timestamp)]

    (log/info "Starting backup" backup-id "for database" database-id)

    (try
      ;; Get database snapshot
      (let [db @conn
            datoms (d/datoms db :eavt) ; Lazy sequence - do NOT realize into memory

            ;; Calculate approximate chunk size (datoms per chunk)
            ;; Original chunk-size is in bytes; estimate ~100 bytes per datom
            datoms-per-chunk (max 1 (quot chunk-size 100))

            ;; Collect stats as we stream through chunks
            stats (atom {:datom-count 0
                        :chunk-count 0
                        :max-eid 0
                        :max-tx 0
                        :completed-chunks #{}})]

        (log/info "Starting streaming backup with chunk size" datoms-per-chunk "datoms")

        ;; Create initial checkpoint (we don't know total chunks yet)
        (let [checkpoint (meta/create-checkpoint
                          {:operation :backup
                           :backup-id backup-id
                           :total-chunks :unknown})]
          (meta/update-checkpoint s3-client bucket
                                 (str backup-path "checkpoint.edn")
                                 checkpoint))

        ;; Stream through datoms and upload chunks with parallelism
        (let [;; Helper function to process a single chunk
              process-chunk (fn [idx chunk]
                              (when (seq chunk) ; Skip empty chunks
                                (let [chunk-vec (vec chunk) ; Realize only THIS chunk
                                      chunk-size (count chunk-vec)

                                      ;; Extract metadata from chunk
                                      min-tx (reduce min Long/MAX_VALUE (map :tx chunk-vec))
                                      max-tx (reduce max 0 (map :tx chunk-vec))
                                      max-eid (reduce max 0 (map :e chunk-vec))]

                                  (log/info "Processing chunk" idx "with" chunk-size "datoms")

                                  (let [chunk-data (ser/serialize-datom-chunk idx chunk-vec)
                                        compressed (comp/compress-chunk chunk-data :algorithm compression)
                                        checksum (utils/sha256 compressed)
                                        chunk-key (str backup-path "chunks/datoms-" idx ".fressian.gz")]

                                    ;; Return all data needed for upload and metadata
                                    {:idx idx
                                     :chunk-size chunk-size
                                     :min-tx min-tx
                                     :max-tx max-tx
                                     :max-eid max-eid
                                     :compressed compressed
                                     :checksum checksum
                                     :chunk-key chunk-key}))))

              ;; Process chunks in parallel batches
              indexed-chunks (map-indexed vector (partition-all datoms-per-chunk datoms))
              chunk-metadata (vec
                             (mapcat
                              (fn [batch]
                                ;; Process batch of chunks in parallel
                                (let [futures (doall
                                              (map (fn [[idx chunk]]
                                                    (future
                                                      (try
                                                        (when-let [processed (process-chunk idx chunk)]
                                                          (log/info "Uploading chunk" (:idx processed))
                                                          ;; Upload to S3
                                                          (let [response (s3/put-object s3-client bucket
                                                                                       (:chunk-key processed)
                                                                                       (:compressed processed)
                                                                                       :content-type "application/octet-stream")]

                                                            ;; Update stats atomically
                                                            (swap! stats (fn [s]
                                                                          (-> s
                                                                              (update :datom-count + (:chunk-size processed))
                                                                              (update :chunk-count inc)
                                                                              (update :max-eid max (:max-eid processed))
                                                                              (update :max-tx max (:max-tx processed)))))

                                                            ;; Update checkpoint atomically
                                                            ;; Note: In parallel mode, completed count may not be sequential
                                                            (swap! stats update :completed-chunks conj (:idx processed))

                                                            ;; Return chunk metadata
                                                            (meta/create-chunk-metadata
                                                             {:chunk-id (:idx processed)
                                                              :tx-range [(:min-tx processed) (:max-tx processed)]
                                                              :max-eid (:max-eid processed)
                                                              :datom-count (:chunk-size processed)
                                                              :size-bytes (alength (:compressed processed))
                                                              :checksum (:checksum processed)
                                                              :s3-key (:chunk-key processed)
                                                              :s3-etag (:ETag response)})))
                                                        (catch Exception e
                                                          (log/error e "Failed to process chunk" idx)
                                                          (throw e)))))
                                                   batch))
                                      ;; Wait for all futures in batch and collect results
                                      results (doall (map deref futures))]
                                  ;; Filter out nils and return results
                                  (filter some? results)))
                              ;; Partition indexed chunks into batches for parallel processing
                              (partition-all parallel indexed-chunks)))]

          ;; Create and upload manifest using collected stats
          (let [completed-at (utils/current-timestamp)
                total-size (reduce + (map :chunk/size-bytes chunk-metadata))
                final-stats @stats
                manifest (meta/create-manifest
                          {:backup-id backup-id
                           :backup-type :full
                           :database-id database-id
                           :datahike-version "0.6.1"
                           :datom-count (:datom-count final-stats)
                           :chunk-count (:chunk-count final-stats)
                           :max-eid (:max-eid final-stats)
                           :max-tx (:max-tx final-stats)
                           :total-size total-size
                           :tx-range [nil nil]
                           :chunks chunk-metadata
                           :started-at started-at
                           :completed-at completed-at})]

            (meta/write-edn-to-s3 s3-client bucket
                                 (str backup-path "manifest.edn")
                                 manifest)

            ;; Create completion marker
            (s3/put-object s3-client bucket
                          (str backup-path "complete.marker")
                          (.getBytes "complete" "UTF-8"))

            (log/info "Backup completed successfully"
                     "- Backup ID:" backup-id
                     "- Datoms:" (:datom-count final-stats)
                     "- Chunks:" (:chunk-count final-stats)
                     "- Size:" (utils/format-bytes total-size)
                     "- Duration:" (format "%.2f seconds"
                                          (/ (- (.getTime completed-at)
                                               (.getTime started-at))
                                            1000.0)))

            {:success true
             :backup-id backup-id
             :database-id database-id
             :datom-count (:datom-count final-stats)
             :chunk-count (:chunk-count final-stats)
             :max-eid (:max-eid final-stats)
             :max-tx (:max-tx final-stats)
             :total-size-bytes total-size
             :duration-ms (- (.getTime completed-at) (.getTime started-at))
             :s3-path (str "s3://" bucket "/" backup-path)})))

      (catch Exception e
        (log/error e "Backup failed for" backup-id)
        {:success false
         :backup-id backup-id
         :error (.getMessage e)}))))

(defn list-backups
  "List available backups in S3 for a database

  Parameters:
  - s3-config: Map with :bucket, :region, and optional :prefix
  - database-id: Database identifier

  Returns: Sequence of backup information maps"
  [s3-config database-id]
  (let [s3-client (s3/create-s3-client s3-config)
        bucket (:bucket s3-config)
        prefix (str (or (:prefix s3-config) "datahike-backups/")
                   database-id "/")]
    (try
      (let [objects (s3/list-objects s3-client bucket prefix)
            manifest-keys (filter #(re-find #"manifest\.edn$" (:key %)) objects)]
        (doall
         (map (fn [{:keys [key last-modified]}]
                (try
                  (let [manifest (meta/read-edn-from-s3 s3-client bucket key)]
                    {:backup-id (:backup/id manifest)
                     :type (:backup/type manifest)
                     :created-at (:backup/created-at manifest)
                     :completed? (:backup/completed manifest)
                     :datom-count (:stats/datom-count manifest)
                     :size-bytes (:stats/size-bytes manifest)
                     :s3-key key})
                  (catch Exception e
                    (log/warn "Failed to read manifest" key ":" (.getMessage e))
                    nil)))
              manifest-keys)))
      (catch Exception e
        (log/error e "Failed to list backups")
        []))))

(defn verify-backup
  "Verify backup integrity by checking all chunks exist

  Parameters:
  - s3-config: Map with :bucket, :region, and optional :prefix
  - backup-id: Backup identifier
  - database-id: Database identifier (default: \"default-db\")

  Returns: Map with verification results"
  [s3-config backup-id & {:keys [database-id] :or {database-id "default-db"}}]
  (let [s3-client (s3/create-s3-client s3-config)
        bucket (:bucket s3-config)
        prefix (or (:prefix s3-config) "datahike-backups/")
        backup-path (str prefix database-id "/" backup-id "/")
        manifest-key (str backup-path "manifest.edn")]
    (try
      (let [manifest (meta/read-edn-from-s3 s3-client bucket manifest-key)
            chunks (:chunks manifest)
            missing-chunks (filter (fn [chunk]
                                    (not (s3/object-exists? s3-client bucket
                                                           (:chunk/s3-key chunk))))
                                  chunks)]
        (if (empty? missing-chunks)
          {:success true
           :backup-id backup-id
           :chunk-count (count chunks)
           :all-chunks-present true}
          {:success false
           :backup-id backup-id
           :chunk-count (count chunks)
           :missing-chunks (map :chunk/id missing-chunks)
           :all-chunks-present false}))
      (catch Exception e
        (log/error e "Verification failed for backup" backup-id)
        {:success false
         :backup-id backup-id
         :error (.getMessage e)}))))

(defn cleanup-incomplete
  "Clean up incomplete backups older than specified hours

  Parameters:
  - s3-config: Map with :bucket, :region, and optional :prefix
  - database-id: Database identifier
  - older-than-hours: Remove incomplete backups older than this (default: 24)

  Returns: Map with cleanup results"
  [s3-config database-id & {:keys [older-than-hours] :or {older-than-hours 24}}]
  (let [s3-client (s3/create-s3-client s3-config)
        bucket (:bucket s3-config)
        prefix (str (or (:prefix s3-config) "datahike-backups/")
                   database-id "/")]
    (log/info "Cleaning up incomplete backups older than" older-than-hours "hours")

    ;; Clean up old multipart uploads
    (s3/cleanup-old-multipart-uploads s3-client bucket prefix older-than-hours)

    ;; Find and clean incomplete backups
    (let [backups (list-backups s3-config database-id)
          incomplete (filter (fn [backup]
                              (and (not (:completed? backup))
                                   (> (utils/hours-since (:created-at backup))
                                      older-than-hours)))
                            backups)]
      (log/info "Found" (count incomplete) "incomplete backups to clean up")
      {:cleaned-count (count incomplete)
       :backup-ids (map :backup-id incomplete)})))

;; =============================================================================
;; Directory-based Backup API
;; =============================================================================

(defn backup-to-directory
  "Create a full backup of database to a local directory

  Parameters:
  - conn: Datahike connection
  - directory-config: Map with :path (base directory for backups)
  - opts: Optional configuration map with:
    - :chunk-size - Size of each chunk in bytes (default: 64MB)
    - :compression - Compression algorithm (default: :gzip)
    - :parallel - Number of chunks to process concurrently (default: 1)
                  Values: 1 (sequential, default), 2-4 (balanced), 8+ (high-performance)
    - :database-id - Custom database identifier (default: \"default-db\")

  Returns: Map with backup details including :backup-id

  Example:
  (backup-to-directory conn
                       {:path \"/backups\"}
                       :chunk-size (* 256 1024 1024)  ; 256MB chunks
                       :parallel 8                     ; 8 concurrent writes
                       :database-id \"production\")"
  [conn directory-config & {:keys [chunk-size compression parallel database-id]
                            :or {chunk-size (* 64 1024 1024)
                                 compression :gzip
                                 parallel 1
                                 database-id "default-db"}}]
  (let [backup-id (utils/generate-backup-id)
        base-dir (:path directory-config)
        backup-path (dir/get-backup-path base-dir database-id backup-id)
        started-at (utils/current-timestamp)]

    (log/info "Starting backup" backup-id "for database" database-id "to" backup-path)

    (try
      ;; Ensure backup directory exists
      (dir/ensure-directory backup-path)

      ;; Get database snapshot
      (let [db @conn
            datoms (d/datoms db :eavt) ; Lazy sequence - do NOT realize into memory

            ;; Calculate approximate chunk size (datoms per chunk)
            datoms-per-chunk (max 1 (quot chunk-size 100))

            ;; Collect stats as we stream through chunks
            stats (atom {:datom-count 0
                        :chunk-count 0
                        :max-eid 0
                        :max-tx 0
                        :completed-chunks #{}})

            chunks-dir (str backup-path "/chunks")]

        (log/info "Starting streaming backup with chunk size" datoms-per-chunk "datoms")

        ;; Create chunks directory
        (dir/ensure-directory chunks-dir)

        ;; Create initial checkpoint (we don't know total chunks yet)
        (let [checkpoint (meta/create-checkpoint
                          {:operation :backup
                           :backup-id backup-id
                           :total-chunks :unknown})]
          (meta/update-checkpoint-file
           (str backup-path "/checkpoint.edn")
           checkpoint))

        ;; Stream through datoms and write chunks with parallelism
        (let [;; Helper function to process a single chunk
              process-chunk (fn [idx chunk]
                              (when (seq chunk) ; Skip empty chunks
                                (let [chunk-vec (vec chunk) ; Realize only THIS chunk
                                      chunk-size (count chunk-vec)

                                      ;; Extract metadata from chunk
                                      min-tx (reduce min Long/MAX_VALUE (map :tx chunk-vec))
                                      max-tx (reduce max 0 (map :tx chunk-vec))
                                      max-eid (reduce max 0 (map :e chunk-vec))]

                                  (log/info "Processing chunk" idx "with" chunk-size "datoms")

                                  (let [chunk-data (ser/serialize-datom-chunk idx chunk-vec)
                                        compressed (comp/compress-chunk chunk-data :algorithm compression)
                                        checksum (utils/sha256 compressed)
                                        chunk-path (str chunks-dir "/datoms-" idx ".fressian.gz")]

                                    ;; Return all data needed for writing and metadata
                                    {:idx idx
                                     :chunk-size chunk-size
                                     :min-tx min-tx
                                     :max-tx max-tx
                                     :max-eid max-eid
                                     :compressed compressed
                                     :checksum checksum
                                     :chunk-path chunk-path}))))

              ;; Process chunks in parallel batches
              indexed-chunks (map-indexed vector (partition-all datoms-per-chunk datoms))
              chunk-metadata (vec
                             (mapcat
                              (fn [batch]
                                ;; Process batch of chunks in parallel
                                (let [futures (doall
                                              (map (fn [[idx chunk]]
                                                    (future
                                                      (try
                                                        (when-let [processed (process-chunk idx chunk)]
                                                          (log/info "Writing chunk" (:idx processed))
                                                          ;; Write to disk
                                                          (let [{:keys [size]} (dir/write-file (:chunk-path processed)
                                                                                              (:compressed processed))]

                                                            ;; Update stats atomically
                                                            (swap! stats (fn [s]
                                                                          (-> s
                                                                              (update :datom-count + (:chunk-size processed))
                                                                              (update :chunk-count inc)
                                                                              (update :max-eid max (:max-eid processed))
                                                                              (update :max-tx max (:max-tx processed)))))

                                                            ;; Update checkpoint atomically
                                                            ;; Note: In parallel mode, completed count may not be sequential
                                                            (swap! stats update :completed-chunks conj (:idx processed))

                                                            ;; Return chunk metadata
                                                            (meta/create-chunk-metadata
                                                             {:chunk-id (:idx processed)
                                                              :tx-range [(:min-tx processed) (:max-tx processed)]
                                                              :max-eid (:max-eid processed)
                                                              :datom-count (:chunk-size processed)
                                                              :size-bytes size
                                                              :checksum (:checksum processed)
                                                              :s3-key (str "chunks/datoms-" (:idx processed) ".fressian.gz")  ; Keep for compatibility
                                                              :s3-etag nil})))
                                                        (catch Exception e
                                                          (log/error e "Failed to process chunk" idx)
                                                          (throw e)))))
                                                   batch))
                                      ;; Wait for all futures in batch and collect results
                                      results (doall (map deref futures))]
                                  ;; Filter out nils and return results
                                  (filter some? results)))
                              ;; Partition indexed chunks into batches for parallel processing
                              (partition-all parallel indexed-chunks)))]

          ;; Create and write manifest using collected stats
          (let [completed-at (utils/current-timestamp)
                total-size (reduce + (map :chunk/size-bytes chunk-metadata))
                final-stats @stats
                manifest (meta/create-manifest
                          {:backup-id backup-id
                           :backup-type :full
                           :database-id database-id
                           :datahike-version "0.6.1"
                           :datom-count (:datom-count final-stats)
                           :chunk-count (:chunk-count final-stats)
                           :max-eid (:max-eid final-stats)
                           :max-tx (:max-tx final-stats)
                           :total-size total-size
                           :tx-range [nil nil]
                           :chunks chunk-metadata
                           :started-at started-at
                           :completed-at completed-at})]

            (meta/write-edn-to-file (str backup-path "/manifest.edn") manifest)

            ;; Create completion marker
            (dir/write-file (str backup-path "/complete.marker")
                          (.getBytes "complete" "UTF-8"))

            (log/info "Backup completed successfully"
                     "- Backup ID:" backup-id
                     "- Datoms:" (:datom-count final-stats)
                     "- Chunks:" (:chunk-count final-stats)
                     "- Size:" (utils/format-bytes total-size)
                     "- Duration:" (format "%.2f seconds"
                                          (/ (- (.getTime completed-at)
                                               (.getTime started-at))
                                            1000.0))
                     "- Location:" backup-path)

            {:success true
             :backup-id backup-id
             :database-id database-id
             :datom-count (:datom-count final-stats)
             :chunk-count (:chunk-count final-stats)
             :max-eid (:max-eid final-stats)
             :max-tx (:max-tx final-stats)
             :total-size-bytes total-size
             :duration-ms (- (.getTime completed-at) (.getTime started-at))
             :path backup-path})))

      (catch Exception e
        (log/error e "Backup failed for" backup-id)
        {:success false
         :backup-id backup-id
         :error (.getMessage e)}))))

(defn list-backups-in-directory
  "List available backups in a local directory for a database

  Parameters:
  - directory-config: Map with :path (base directory for backups)
  - database-id: Database identifier

  Returns: Sequence of backup information maps"
  [directory-config database-id]
  (let [base-dir (:path directory-config)
        db-path (str base-dir "/" database-id)]
    (try
      (let [backup-dirs (dir/list-backups-in-directory base-dir database-id)]
        (doall
         (keep (fn [{:keys [backup-id path]}]
                (try
                  (let [manifest-path (str path "/manifest.edn")
                        manifest (meta/read-edn-from-file manifest-path)]
                    {:backup-id (:backup/id manifest)
                     :type (:backup/type manifest)
                     :created-at (:backup/created-at manifest)
                     :completed? (:backup/completed manifest)
                     :datom-count (:stats/datom-count manifest)
                     :size-bytes (:stats/size-bytes manifest)
                     :path path})
                  (catch Exception e
                    (log/warn "Failed to read manifest" path ":" (.getMessage e))
                    nil)))
              backup-dirs)))
      (catch Exception e
        (log/error e "Failed to list backups")
        []))))

(defn verify-backup-in-directory
  "Verify backup integrity by checking all chunks exist

  Parameters:
  - directory-config: Map with :path (base directory for backups)
  - backup-id: Backup identifier
  - database-id: Database identifier (default: \"default-db\")

  Returns: Map with verification results"
  [directory-config backup-id & {:keys [database-id] :or {database-id "default-db"}}]
  (let [base-dir (:path directory-config)
        backup-path (dir/get-backup-path base-dir database-id backup-id)
        manifest-path (str backup-path "/manifest.edn")]
    (try
      (let [manifest (meta/read-edn-from-file manifest-path)
            chunks (:chunks manifest)
            missing-chunks (filter (fn [chunk]
                                    (not (dir/file-exists?
                                          (str backup-path "/" (:chunk/s3-key chunk)))))
                                  chunks)]
        (if (empty? missing-chunks)
          {:success true
           :backup-id backup-id
           :chunk-count (count chunks)
           :all-chunks-present true}
          {:success false
           :backup-id backup-id
           :chunk-count (count chunks)
           :missing-chunks (map :chunk/id missing-chunks)
           :all-chunks-present false}))
      (catch Exception e
        (log/error e "Verification failed for backup" backup-id)
        {:success false
         :backup-id backup-id
         :error (.getMessage e)}))))

(defn cleanup-incomplete-in-directory
  "Clean up incomplete backups older than specified hours

  Parameters:
  - directory-config: Map with :path (base directory for backups)
  - database-id: Database identifier
  - older-than-hours: Remove incomplete backups older than this (default: 24)

  Returns: Map with cleanup results"
  [directory-config database-id & {:keys [older-than-hours] :or {older-than-hours 24}}]
  (log/info "Cleaning up incomplete backups older than" older-than-hours "hours")

  (let [backups (list-backups-in-directory directory-config database-id)
        incomplete (filter (fn [backup]
                            (and (not (:completed? backup))
                                 (> (utils/hours-since (:created-at backup))
                                    older-than-hours)))
                          backups)]
    (log/info "Found" (count incomplete) "incomplete backups to clean up")

    ;; Delete incomplete backup directories
    (doseq [{:keys [path backup-id]} incomplete]
      (try
        (dir/cleanup-directory path)
        (log/info "Cleaned up incomplete backup:" backup-id)
        (catch Exception e
          (log/error e "Failed to cleanup backup:" backup-id))))

    {:cleaned-count (count incomplete)
     :backup-ids (map :backup-id incomplete)}))

(comment
  ;; Example usage:

  ;; Create a backup
  (require '[datahike.api :as d])
  (def conn (d/connect {:store {:backend :mem :id "test"}}))

  ;; Add some test data
  (d/transact conn [{:db/id -1 :name "Alice"}
                    {:db/id -2 :name "Bob"}])

  ;; Backup to S3
  (def result
    (backup-to-s3 conn
                  {:bucket "my-backups"
                   :region "us-east-1"}
                  :database-id "test-db"))

  ;; List backups
  (list-backups {:bucket "my-backups" :region "us-east-1"} "test-db")

  ;; Verify backup
  (verify-backup {:bucket "my-backups" :region "us-east-1"}
                 (:backup-id result)
                 :database-id "test-db")

  ;; Cleanup old incomplete backups
  (cleanup-incomplete {:bucket "my-backups" :region "us-east-1"}
                      "test-db"
                      :older-than-hours 24))

;; =============================================================================
;; Restore API - Helper Functions
;; =============================================================================

(defn compare-datom-maps-by-tx
  "Compare two datom data maps by transaction order for sorting.
   Ensures :db/txInstant comes first within each transaction."
  [d1 d2]
  (let [tx-cmp (compare (:tx d1) (:tx d2))]
    (if (zero? tx-cmp)
      ;; Same transaction: txInstant attributes must come first
      (let [a1 (:a d1)
            a2 (:a d2)
            tx-inst-1 (= a1 :db/txInstant)
            tx-inst-2 (= a2 :db/txInstant)]
        (cond
          (and tx-inst-1 (not tx-inst-2)) -1
          (and tx-inst-2 (not tx-inst-1)) 1
          :else (let [e-cmp (compare (:e d1) (:e d2))]
                  (if (zero? e-cmp)
                    (compare a1 a2)
                    e-cmp))))
      tx-cmp)))

(defn merge-sorted-chunk-streams
  "K-way merge of sorted chunk streams using a priority queue.
   Each chunk is a lazy sequence. Returns a lazy sequence of datom data maps
   sorted by transaction order. Only keeps O(k) datoms in memory at once
   where k is the number of chunks.

   chunks: sequence of lazy sequences of datom data maps"
  [chunks]
  (let [pq (java.util.PriorityQueue.
            (reify java.util.Comparator
              (compare [_ entry1 entry2]
                ;; Each entry is [datom chunk-idx remaining]
                ;; Compare by the datom (first element)
                (compare-datom-maps-by-tx (first entry1) (first entry2)))))]

    ;; Initialize priority queue with first datom from each non-empty chunk
    (doseq [[chunk-idx chunk-seq] (map-indexed vector chunks)]
      (when-let [first-datom (first chunk-seq)]
        (.offer pq [first-datom chunk-idx (rest chunk-seq)])))

    ;; Lazy sequence that pulls from priority queue
    ((fn step []
       (lazy-seq
        (when-not (.isEmpty pq)
          (let [[datom chunk-idx remaining] (.poll pq)]
            ;; If this chunk has more datoms, add next one to queue
            (when-let [next-datom (first remaining)]
              (.offer pq [next-datom chunk-idx (rest remaining)]))
            ;; Return current datom and continue
            (cons datom (step)))))))))

;; =============================================================================
;; Restore API
;; =============================================================================

(defn restore-from-s3
  "Restore a database from an S3 backup

  Parameters:
  - conn: Datahike connection (should be to an empty database)
  - s3-config: Map with :bucket, :region, and optional :prefix
  - backup-id: The backup identifier to restore from
  - opts: Optional configuration map with:
    - :database-id - Database identifier (default: \"default-db\")
    - :verify-checksums - Verify chunk checksums during restore (default: true)
    - :progress-fn - Function called with progress updates (default: nil)
    - :suppress-error-logging - Don't log errors (useful for expected failures in tests) (default: false)

  Returns: Map with restore details"
  [conn s3-config backup-id & {:keys [database-id verify-checksums progress-fn suppress-error-logging]
                                :or {database-id "default-db"
                                     verify-checksums true
                                     progress-fn nil
                                     suppress-error-logging false}}]
  (let [s3-client (s3/create-s3-client s3-config)
        bucket (:bucket s3-config)
        prefix (or (:prefix s3-config) "datahike-backups/")
        backup-path (str prefix database-id "/" backup-id "/")
        manifest-key (str backup-path "manifest.edn")
        started-at (utils/current-timestamp)]

    (log/info "Starting restore of backup" backup-id "for database" database-id)

    (try
      ;; Read manifest
      (let [manifest (meta/read-edn-from-s3 s3-client bucket manifest-key)
            chunks (:chunks manifest)
            chunk-count (count chunks)
            datom-count (:stats/datom-count manifest)]

        (log/info "Restoring" datom-count "datoms from" chunk-count "chunks")

        (when progress-fn
          (progress-fn {:stage :started
                       :backup-id backup-id
                       :total-chunks chunk-count
                       :total-datoms datom-count}))

        ;; Create lazy sequences for each chunk (don't realize yet)
        (let [chunk-streams
              (map (fn [chunk-meta]
                     (lazy-seq
                      (let [chunk-id (:chunk/id chunk-meta)
                            chunk-key (:chunk/s3-key chunk-meta)
                            expected-checksum (:chunk/checksum chunk-meta)]

                        (log/info "Downloading chunk" chunk-id "of" chunk-count)

                        (when progress-fn
                          (progress-fn {:stage :downloading
                                       :chunk-id chunk-id
                                       :chunk-count chunk-count}))

                        ;; Download chunk
                        (let [response (s3/get-object s3-client bucket chunk-key)
                              compressed-data (utils/response->bytes response)]

                          ;; Verify checksum if requested
                          (when verify-checksums
                            (let [actual-checksum (utils/sha256 compressed-data)]
                              (when (not= expected-checksum actual-checksum)
                                (throw (ex-info "Checksum mismatch for chunk"
                                              {:chunk-id chunk-id
                                               :expected expected-checksum
                                               :actual actual-checksum})))))

                          ;; Decompress and deserialize
                          (let [decompressed (comp/decompress-chunk compressed-data :algorithm :gzip)
                                chunk-data (ser/deserialize-datom-chunk decompressed)
                                datom-vecs (:datoms chunk-data)]

                            (log/info "Processed chunk" chunk-id "with" (count datom-vecs) "datoms")

                            (when progress-fn
                              (progress-fn {:stage :processed
                                           :chunk-id chunk-id
                                           :chunk-count chunk-count
                                           :datoms-in-chunk (count datom-vecs)}))

                            ;; Convert vectors back to datom data maps - returns lazy seq
                            (map ser/vec->datom-data datom-vecs))))))
                   chunks)]

          (log/info "Using k-way merge to stream datoms in transaction order")

          ;; Use k-way merge sort to stream datoms in transaction order
          ;; This only keeps O(k) datoms in memory where k = number of chunks
          (let [tx0 536870912  ; Datahike's initial transaction with built-in schema
                sorted-datom-stream (->> (merge-sorted-chunk-streams chunk-streams)
                                        ;; Remove datoms from tx0 (built-in schema)
                                        (remove (fn [{:keys [tx]}] (= tx tx0))))]

            (when progress-fn
              (progress-fn {:stage :transacting
                           :total-datoms datom-count}))

            ;; Update max-eid and max-tx from manifest (no need to scan!)
            (let [max-eid-in-backup (or (:stats/max-eid manifest)
                                       ;; Fallback for old backups without this field
                                       (reduce max 0 (map :chunk/max-eid chunks)))
                  max-tx-in-backup (or (:stats/max-tx manifest)
                                      ;; Fallback for old backups without this field
                                      (let [tx-ranges (keep :chunk/tx-range chunks)]
                                        (if (seq tx-ranges)
                                          (reduce max 0 (map second tx-ranges))
                                          0)))]
              (log/info "Updating database max-eid to" max-eid-in-backup "and max-tx to" max-tx-in-backup)
              (swap! conn (fn [db]
                           (-> db
                               (assoc :max-eid max-eid-in-backup)
                               (assoc :max-tx max-tx-in-backup)))))

            ;; Stream datoms to load-entities in batches to avoid holding everything in memory
            ;; Process in batches of 10000 datoms
            (when progress-fn
              (progress-fn {:stage :loading-entities
                           :total-datoms datom-count}))

            (log/info "Loading entities directly into database in streaming fashion")

            ;; Stream load-entities in batches
            (let [batch-size 10000
                  loaded-count (atom 0)]
              (doseq [batch (partition-all batch-size sorted-datom-stream)]
                (let [batch-vectors (mapv (fn [{:keys [e a v tx added]}]
                                           [e a v tx added])
                                         batch)]
                  (swap! loaded-count + (count batch-vectors))
                  (log/info "Loading batch of" (count batch-vectors) "datoms, total so far:" @loaded-count)
                  @(d/load-entities conn batch-vectors)))

              ;; All done - report completion
              (let [completed-at (utils/current-timestamp)
                    duration-ms (- (.getTime completed-at) (.getTime started-at))
                    final-datom-count @loaded-count]

                (log/info "Restore completed successfully"
                         "- Backup ID:" backup-id
                         "- Datoms restored:" final-datom-count
                         "- Duration:" (format "%.2f seconds" (/ duration-ms 1000.0)))

                (when progress-fn
                  (progress-fn {:stage :completed
                               :backup-id backup-id
                               :datoms-restored final-datom-count
                               :duration-ms duration-ms}))

                {:success true
                 :backup-id backup-id
                 :database-id database-id
                 :datoms-restored final-datom-count
                 :chunks-processed chunk-count
                 :duration-ms duration-ms})))))

      (catch Exception e
        (when-not suppress-error-logging
          (log/error e "Restore failed for backup" backup-id))

        (when progress-fn
          (progress-fn {:stage :failed
                       :backup-id backup-id
                       :error (.getMessage e)}))

        {:success false
         :backup-id backup-id
         :error (.getMessage e)}))))

(defn restore-from-directory
  "Restore a database from a directory backup

  Parameters:
  - conn: Datahike connection (should be to an empty database)
  - directory-config: Map with :path (base directory for backups)
  - backup-id: The backup identifier to restore from
  - opts: Optional configuration map with:
    - :database-id - Database identifier (default: \"default-db\")
    - :verify-checksums - Verify chunk checksums during restore (default: true)
    - :progress-fn - Function called with progress updates (default: nil)
    - :suppress-error-logging - Don't log errors (useful for expected failures in tests) (default: false)

  Returns: Map with restore details"
  [conn directory-config backup-id & {:keys [database-id verify-checksums progress-fn suppress-error-logging]
                                       :or {database-id "default-db"
                                            verify-checksums true
                                            progress-fn nil
                                            suppress-error-logging false}}]
  (let [base-dir (:path directory-config)
        backup-path (dir/get-backup-path base-dir database-id backup-id)
        manifest-path (str backup-path "/manifest.edn")
        started-at (utils/current-timestamp)]

    (log/info "Starting restore of backup" backup-id "from" backup-path)

    (try
      ;; Read manifest
      (let [manifest (meta/read-edn-from-file manifest-path)
            chunks (:chunks manifest)
            chunk-count (count chunks)
            datom-count (:stats/datom-count manifest)]

        (log/info "Restoring" datom-count "datoms from" chunk-count "chunks")

        (when progress-fn
          (progress-fn {:stage :started
                       :backup-id backup-id
                       :total-chunks chunk-count
                       :total-datoms datom-count}))

        ;; Create lazy sequences for each chunk (don't realize yet)
        (let [chunk-streams
              (map (fn [chunk-meta]
                     (lazy-seq
                      (let [chunk-id (:chunk/id chunk-meta)
                            chunk-key (:chunk/s3-key chunk-meta) ; Path relative to backup dir
                            chunk-path (str backup-path "/" chunk-key)
                            expected-checksum (:chunk/checksum chunk-meta)]

                        (log/info "Reading chunk" chunk-id "of" chunk-count)

                        (when progress-fn
                          (progress-fn {:stage :reading
                                       :chunk-id chunk-id
                                       :chunk-count chunk-count}))

                        ;; Read chunk
                        (let [compressed-data (dir/read-file chunk-path)]

                          ;; Verify checksum if requested
                          (when verify-checksums
                            (let [actual-checksum (utils/sha256 compressed-data)]
                              (when (not= expected-checksum actual-checksum)
                                (throw (ex-info "Checksum mismatch for chunk"
                                              {:chunk-id chunk-id
                                               :expected expected-checksum
                                               :actual actual-checksum})))))

                          ;; Decompress and deserialize
                          (let [decompressed (comp/decompress-chunk compressed-data :algorithm :gzip)
                                chunk-data (ser/deserialize-datom-chunk decompressed)
                                datom-vecs (:datoms chunk-data)]

                            (log/info "Processed chunk" chunk-id "with" (count datom-vecs) "datoms")

                            (when progress-fn
                              (progress-fn {:stage :processed
                                           :chunk-id chunk-id
                                           :chunk-count chunk-count
                                           :datoms-in-chunk (count datom-vecs)}))

                            ;; Convert vectors back to datom data maps - returns lazy seq
                            (map ser/vec->datom-data datom-vecs))))))
                   chunks)]

          (log/info "Using k-way merge to stream datoms in transaction order")

          ;; Use k-way merge sort to stream datoms in transaction order
          ;; This only keeps O(k) datoms in memory where k = number of chunks
          (let [tx0 536870912  ; Datahike's initial transaction with built-in schema
                sorted-datom-stream (->> (merge-sorted-chunk-streams chunk-streams)
                                        ;; Remove datoms from tx0 (built-in schema)
                                        (remove (fn [{:keys [tx]}] (= tx tx0))))]

            (when progress-fn
              (progress-fn {:stage :transacting
                           :total-datoms datom-count}))

            ;; Update max-eid and max-tx from manifest (no need to scan!)
            (let [max-eid-in-backup (or (:stats/max-eid manifest)
                                       ;; Fallback for old backups without this field
                                       (reduce max 0 (map :chunk/max-eid chunks)))
                  max-tx-in-backup (or (:stats/max-tx manifest)
                                      ;; Fallback for old backups without this field
                                      (let [tx-ranges (keep :chunk/tx-range chunks)]
                                        (if (seq tx-ranges)
                                          (reduce max 0 (map second tx-ranges))
                                          0)))]
              (log/info "Updating database max-eid to" max-eid-in-backup "and max-tx to" max-tx-in-backup)
              (swap! conn (fn [db]
                           (-> db
                               (assoc :max-eid max-eid-in-backup)
                               (assoc :max-tx max-tx-in-backup)))))

            ;; Stream datoms to load-entities in batches to avoid holding everything in memory
            ;; Process in batches of 10000 datoms
            (when progress-fn
              (progress-fn {:stage :loading-entities
                           :total-datoms datom-count}))

            (log/info "Loading entities directly into database in streaming fashion")

            ;; Stream load-entities in batches
            (let [batch-size 10000
                  loaded-count (atom 0)]
              (doseq [batch (partition-all batch-size sorted-datom-stream)]
                (let [batch-vectors (mapv (fn [{:keys [e a v tx added]}]
                                           [e a v tx added])
                                         batch)]
                  (swap! loaded-count + (count batch-vectors))
                  (log/info "Loading batch of" (count batch-vectors) "datoms, total so far:" @loaded-count)
                  @(d/load-entities conn batch-vectors)))

              ;; All done - report completion
              (let [completed-at (utils/current-timestamp)
                    duration-ms (- (.getTime completed-at) (.getTime started-at))
                    final-datom-count @loaded-count]

                (log/info "Restore completed successfully"
                         "- Backup ID:" backup-id
                         "- Datoms restored:" final-datom-count
                         "- Duration:" (format "%.2f seconds" (/ duration-ms 1000.0)))

                (when progress-fn
                  (progress-fn {:stage :completed
                               :backup-id backup-id
                               :datoms-restored final-datom-count
                               :duration-ms duration-ms}))

                {:success true
                 :backup-id backup-id
                 :database-id database-id
                 :datoms-restored final-datom-count
                 :chunks-processed chunk-count
                 :duration-ms duration-ms})))))

      (catch Exception e
        (when-not suppress-error-logging
          (log/error e "Restore failed for backup" backup-id))

        (when progress-fn
          (progress-fn {:stage :failed
                       :backup-id backup-id
                       :error (.getMessage e)}))

        {:success false
         :backup-id backup-id
         :error (.getMessage e)}))))

(comment
  ;; Example restore usage:

  ;; Restore from S3
  (require '[datahike.api :as d])

  ;; Create a new empty database for restoration
  (def restore-cfg {:store {:backend :mem :id "restored-db"}})
  (d/create-database restore-cfg)
  (def restore-conn (d/connect restore-cfg))

  ;; Restore from a specific backup
  (def restore-result
    (restore-from-s3 restore-conn
                     {:bucket "my-backups"
                      :region "us-east-1"}
                     "backup-20240115-123456-abc"
                     :database-id "test-db"
                     :progress-fn (fn [progress]
                                   (println "Progress:" progress))))

  ;; Verify the restored data
  (d/q '[:find ?e ?name
         :where [?e :name ?name]]
       @restore-conn)

  ;; Restore from directory
  (def restore-result-dir
    (restore-from-directory restore-conn
                           {:path "/path/to/backups"}
                           "backup-20240115-123456-abc"
                           :database-id "test-db")))

;; =============================================================================
;; Live Migration API
;; =============================================================================

(defn live-migrate
  "Perform zero-downtime live migration between database backends.

  This function enables migrating data from one Datahike database configuration
  to another while the application continues to operate. It handles:
  - Creating an initial backup while capturing new transactions
  - Restoring to the target database
  - Replaying captured transactions to catch up
  - Providing a router function for seamless switchover

  Parameters:
  - source-conn: Current database connection
  - target-config: Configuration for target database
  - opts: Migration options
    :migration-id - Specific migration ID (optional, continues if exists)
    :database-id - Database identifier (default: \"default-db\")
    :backup-dir - Directory for backups and migration state (default: \"./backups\")
    :progress-fn - Function called with progress updates
    :complete-callback - Function called when migration completes
    :verify-transactions - Verify each transaction was captured (default: true)

  Returns:
  A transaction router function that should be used for all database writes.
  Call this function with transaction data to route writes appropriately.
  Call with no arguments to finalize the migration and switch to the target.

  Example:
  ```clojure
  ;; Start migration
  (def router (live-migrate source-conn target-config
                           :database-id \"prod-db\"
                           :backup-dir \"./migrations\"))

  ;; Continue transacting through the router
  (router [{:user/name \"Alice\"}])
  (router [{:user/name \"Bob\"}])

  ;; When ready to switch over (minimal downtime here)
  (let [result (router)]
    ;; result contains :target-conn with the new connection
    (def new-conn (:target-conn result)))
  ```"
  [source-conn target-config & opts]
  (let [migrate-fn (requiring-resolve 'datacamp.migration/live-migrate)]
    (apply migrate-fn source-conn target-config opts)))

(defn recover-migration
  "Recover an interrupted migration and continue from where it left off.

  If a migration was interrupted (e.g., server restart), this function
  can resume it from the last checkpoint.

  Parameters:
  - backup-dir: Directory containing migration state
  - database-id: Database identifier
  - opts: Recovery options
    :progress-fn - Function called with progress updates
    :complete-callback - Function called when migration completes

  Returns:
  - If migration found and resumed: Router function to continue migration
  - If migration completed: Map with :status :already-completed and :target-conn
  - If no migration: Map with :status :no-migration

  Example:
  ```clojure
  ;; Check for and recover any interrupted migration
  (let [result (recover-migration \"./migrations\" \"prod-db\")]
    (if (fn? result)
      ;; Migration resumed, use router
      (do
        (result [{:data \"new-transaction\"}])
        (result))  ; Finalize
      ;; Check status
      (println \"Recovery status:\" (:status result))))
  ```"
  [backup-dir database-id & opts]
  (let [recover-fn (requiring-resolve 'datacamp.migration/recover-migration)]
    (apply recover-fn backup-dir database-id opts)))

(defn get-migration-status
  "Get the current status of a migration.

  Parameters:
  - backup-dir: Directory containing migration state
  - database-id: Database identifier
  - migration-id: Specific migration ID to check

  Returns:
  Map with migration status including:
  - :status - :found or :not-found
  - :state - Current migration state
  - :started-at - When migration started
  - :completed-at - When migration completed (if applicable)
  - :stats - Migration statistics

  Example:
  ```clojure
  (get-migration-status \"./migrations\" \"prod-db\" \"migration-123\")
  ;; => {:status :found
  ;;     :state :catching-up
  ;;     :started-at #inst \"2024-01-15T10:00:00\"
  ;;     :stats {:transactions-captured 150
  ;;             :transactions-applied 120}}
  ```"
  [backup-dir database-id migration-id]
  (let [status-fn (requiring-resolve 'datacamp.migration/get-migration-status)]
    (status-fn backup-dir database-id migration-id)))

(defn archive-completed-migrations
  "Archive completed migrations older than specified hours.
  Migrations are marked as archived but kept as they serve as backups.

  Parameters:
  - backup-dir: Directory containing migration state
  - database-id: Database identifier
  - opts:
    :older-than-hours - Archive migrations older than this (default: 168 / 1 week)

  Returns:
  Map with archive results:
  - :archived-count - Number of migrations archived
  - :migration-ids - IDs of archived migrations

  Example:
  ```clojure
  (archive-completed-migrations \"./migrations\" \"prod-db\"
                                :older-than-hours 24)
  ;; => {:archived-count 3
  ;;     :migration-ids [\"migration-123\" \"migration-456\" \"migration-789\"]}
  ```"
  [backup-dir database-id & opts]
  (let [archive-fn (requiring-resolve 'datacamp.migration/archive-completed-migrations)]
    (apply archive-fn backup-dir database-id opts)))

(defn cleanup-completed-migrations
  "Deprecated: Use archive-completed-migrations instead.
  This function now archives migrations instead of deleting them."
  [backup-dir database-id & opts]
  (let [cleanup-fn (requiring-resolve 'datacamp.migration/cleanup-completed-migrations)]
    (apply cleanup-fn backup-dir database-id opts)))

(defn list-migrations
  "List all migrations for a database.

  Parameters:
  - backup-dir: Directory containing migration state
  - database-id: Database identifier
  - opts:
    :include-archived - Include archived migrations (default: true)

  Returns:
  List of migration summaries sorted by start time (newest first)

  Example:
  ```clojure
  (list-migrations \"./migrations\" \"prod-db\")
  ;; => [{:migration-id \"migration-123\"
  ;;      :state :completed
  ;;      :started-at #inst \"2024-01-15T10:00:00\"
  ;;      :completed-at #inst \"2024-01-15T10:05:00\"
  ;;      :backup-id \"backup-456\"}
  ;;     ...]
  ```"
  [backup-dir database-id & opts]
  (let [list-fn (requiring-resolve 'datacamp.migration/list-migrations)]
    (apply list-fn backup-dir database-id opts)))

;; =============================================================================
;; Garbage Collection
;; =============================================================================

(defn gc!
  "Run optimized garbage collection on the database.

  SAFETY: Defaults to dry-run mode. Set :dry-run false to actually delete.

  Automatically resumes from checkpoint if a previous GC was interrupted.

  Parameters:
  - conn: Datahike connection
  - opts: Configuration options
    - :dry-run - Preview what would be deleted without actually deleting (default: true)
    - :retention-days - Keep commits from last N days (default: 7)
    - :batch-size - Number of keys to delete per batch (default: auto-detected)
    - :parallel-batches - Number of batches to delete in parallel (default: auto)
    - :checkpoint-interval - Commits between checkpoints (default: 100)
    - :force-new - Force start new GC even if one is in progress (default: false)

  Returns:
  Map with GC results:
  - :reachable-count - Number of items kept
  - :deleted-count - Number of items deleted (or would-delete-count if dry-run)
  - :duration-ms - Total time taken
  - :resumed? - true if resumed from checkpoint

  Examples:
  ```clojure
  ;; Dry run to see what would be deleted (default)
  (gc! conn)

  ;; Actually run GC with 30 day retention
  (gc! conn :dry-run false :retention-days 30)

  ;; Resume interrupted GC automatically
  (gc! conn :dry-run false) ; Will resume if checkpoint exists
  ```"
  [conn & {:keys [dry-run retention-days batch-size parallel-batches
                  checkpoint-interval force-new]
           :or {dry-run true
                retention-days 7
                checkpoint-interval 100
                force-new false}
           :as opts}]
  (let [gc-fn (requiring-resolve 'datacamp.gc/gc-storage-optimized!)
        get-status-fn (requiring-resolve 'datacamp.gc/get-status)
        resume-fn (requiring-resolve 'datacamp.gc/resume-gc!)
        optimize-fn (requiring-resolve 'datacamp.gc/optimize-for-backend)

        db @conn
        status @(get-status-fn db)
        store-config (:store (:config db))

        ;; Auto-detect optimal settings if not provided
        optimized (optimize-fn store-config)
        final-batch-size (or batch-size (:batch-size optimized))
        final-parallel (or parallel-batches (:parallel-batches optimized))

        ;; Calculate retention date
        retention-date (java.util.Date.
                        (- (System/currentTimeMillis)
                           (* retention-days 24 60 60 1000)))]

    (log/info "GC requested with" (if dry-run "DRY RUN" "LIVE") "mode")
    (when dry-run
      (log/warn "Running in DRY RUN mode - no data will be deleted"))

    ;; Check for existing GC
    (if (and (= (:status status) :in-progress) (not force-new))
      ;; Resume existing GC
      (do
        (log/info "Resuming interrupted GC" (:gc-id status)
                  "from" (:last-checkpoint status))
        (let [result @(resume-fn db (:gc-id status)
                                :batch-size final-batch-size
                                :parallel-batches final-parallel
                                :dry-run dry-run)]
          (assoc result :resumed? true)))

      ;; Start new GC
      (do
        (when (= (:status status) :in-progress)
          (log/warn "Force starting new GC, abandoning" (:gc-id status)))

        (log/info "Starting new GC with" retention-days "day retention")
        @(gc-fn db
               :remove-before retention-date
               :batch-size final-batch-size
               :parallel-batches final-parallel
               :checkpoint-interval checkpoint-interval
               :dry-run dry-run)))))

(defn gc-status
  "Get the status of an ongoing or interrupted GC operation.

  Parameters:
  - conn: Datahike connection

  Returns:
  Map with GC status:
  - :status - :in-progress or :no-gc-in-progress
  - :gc-id - ID of the ongoing GC
  - :started-at - When GC started
  - :visited-count - Number of commits visited
  - :reachable-count - Number of reachable items found
  - :completed-branches - Branches processed
  - :pending-branches - Branches remaining

  Example:
  ```clojure
  (gc-status conn)
  ;; => {:status :in-progress
  ;;     :gc-id \"gc-2024-12-28\"
  ;;     :visited-count 5000
  ;;     :reachable-count 150000
  ;;     :completed-branches 2
  ;;     :pending-branches 1}
  ```"
  [conn]
  (let [get-status-fn (requiring-resolve 'datacamp.gc/get-gc-status)]
    @(get-status-fn @conn)))
