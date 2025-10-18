(ns datacamp.core
  "Public API for Datahike backup operations"
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
   :parallel 4
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
    - :parallel - Number of parallel uploads (default: 4)
    - :database-id - Custom database identifier

  Returns: Map with backup details including :backup-id"
  [conn s3-config & {:keys [chunk-size compression parallel database-id]
                     :or {chunk-size (* 64 1024 1024)
                          compression :gzip
                          parallel 4
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
            datoms (d/datoms db :eavt)
            datom-vec (vec datoms)
            datom-count (count datom-vec)
            chunks (partition-all (quot chunk-size 100) datom-vec) ; Rough chunking
            chunk-count (count chunks)]

        (log/info "Backing up" datom-count "datoms in" chunk-count "chunks")

        ;; Create checkpoint
        (let [checkpoint (meta/create-checkpoint
                          {:operation :backup
                           :backup-id backup-id
                           :total-chunks chunk-count})]
          (meta/update-checkpoint s3-client bucket
                                 (str backup-path "checkpoint.edn")
                                 checkpoint))

        ;; Upload chunks
        (let [chunk-metadata
              (doall
               (map-indexed
                (fn [idx chunk]
                  (log/info "Uploading chunk" idx "of" chunk-count)
                  (let [chunk-data (ser/serialize-datom-chunk idx chunk)
                        compressed (comp/compress-chunk chunk-data :algorithm compression)
                        checksum (utils/sha256 compressed)
                        chunk-key (str backup-path "chunks/datoms-" idx ".fressian.gz")
                        response (s3/put-object s3-client bucket chunk-key compressed
                                               :content-type "application/octet-stream")]

                    ;; Update checkpoint
                    (meta/update-checkpoint s3-client bucket
                                           (str backup-path "checkpoint.edn")
                                           {:progress/completed (inc idx)
                                            :state/completed-chunks (set (range (inc idx)))})

                    (meta/create-chunk-metadata
                     {:chunk-id idx
                      :tx-range [nil nil] ; TODO: Extract from datoms
                      :datom-count (count chunk)
                      :size-bytes (alength compressed)
                      :checksum checksum
                      :s3-key chunk-key
                      :s3-etag (:ETag response)})))
                chunks))]

          ;; Create and upload manifest
          (let [completed-at (utils/current-timestamp)
                total-size (reduce + (map :chunk/size-bytes chunk-metadata))
                manifest (meta/create-manifest
                          {:backup-id backup-id
                           :backup-type :full
                           :database-id database-id
                           :datahike-version "0.6.1"
                           :datom-count datom-count
                           :chunk-count chunk-count
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
                     "- Datoms:" datom-count
                     "- Size:" (utils/format-bytes total-size)
                     "- Duration:" (format "%.2f seconds"
                                          (/ (- (.getTime completed-at)
                                               (.getTime started-at))
                                            1000.0)))

            {:success true
             :backup-id backup-id
             :database-id database-id
             :datom-count datom-count
             :chunk-count chunk-count
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
    - :database-id - Custom database identifier (default: \"default-db\")

  Returns: Map with backup details including :backup-id"
  [conn directory-config & {:keys [chunk-size compression database-id]
                            :or {chunk-size (* 64 1024 1024)
                                 compression :gzip
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
            datoms (d/datoms db :eavt)
            datom-vec (vec datoms)
            datom-count (count datom-vec)
            chunks (partition-all (quot chunk-size 100) datom-vec) ; Rough chunking
            chunk-count (count chunks)
            chunks-dir (str backup-path "/chunks")]

        (log/info "Backing up" datom-count "datoms in" chunk-count "chunks")

        ;; Create chunks directory
        (dir/ensure-directory chunks-dir)

        ;; Create checkpoint
        (let [checkpoint (meta/create-checkpoint
                          {:operation :backup
                           :backup-id backup-id
                           :total-chunks chunk-count})]
          (meta/update-checkpoint-file
           (str backup-path "/checkpoint.edn")
           checkpoint))

        ;; Write chunks
        (let [chunk-metadata
              (doall
               (map-indexed
                (fn [idx chunk]
                  (log/info "Writing chunk" idx "of" chunk-count)
                  (let [chunk-data (ser/serialize-datom-chunk idx chunk)
                        compressed (comp/compress-chunk chunk-data :algorithm compression)
                        checksum (utils/sha256 compressed)
                        chunk-path (str chunks-dir "/datoms-" idx ".fressian.gz")
                        {:keys [size]} (dir/write-file chunk-path compressed)]

                    ;; Update checkpoint
                    (meta/update-checkpoint-file
                     (str backup-path "/checkpoint.edn")
                     {:progress/completed (inc idx)
                      :state/completed-chunks (set (range (inc idx)))})

                    (meta/create-chunk-metadata
                     {:chunk-id idx
                      :tx-range [nil nil]
                      :datom-count (count chunk)
                      :size-bytes size
                      :checksum checksum
                      :s3-key (str "chunks/datoms-" idx ".fressian.gz")  ; Keep for compatibility
                      :s3-etag nil})))
                chunks))]

          ;; Create and write manifest
          (let [completed-at (utils/current-timestamp)
                total-size (reduce + (map :chunk/size-bytes chunk-metadata))
                manifest (meta/create-manifest
                          {:backup-id backup-id
                           :backup-type :full
                           :database-id database-id
                           :datahike-version "0.6.1"
                           :datom-count datom-count
                           :chunk-count chunk-count
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
                     "- Datoms:" datom-count
                     "- Size:" (utils/format-bytes total-size)
                     "- Duration:" (format "%.2f seconds"
                                          (/ (- (.getTime completed-at)
                                               (.getTime started-at))
                                            1000.0))
                     "- Location:" backup-path)

            {:success true
             :backup-id backup-id
             :database-id database-id
             :datom-count datom-count
             :chunk-count chunk-count
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
