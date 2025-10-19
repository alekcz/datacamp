(ns datacamp.metadata
  "EDN metadata file handling for human-readable backup metadata"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [taoensso.timbre :as log]
            [datacamp.s3 :as s3]
            [datacamp.directory :as dir]
            [datacamp.utils :as utils]))

(defn write-edn-to-s3
  "Write EDN data to S3 with proper formatting"
  [s3-client bucket key data]
  (let [edn-str (with-out-str (pp/pprint data))
        data-bytes (.getBytes edn-str "UTF-8")]
    (s3/put-object s3-client bucket key data-bytes
                  :content-type "application/edn"
                  :metadata {"Content-Format" "edn"
                            "Human-Readable" "true"})))

(defn read-edn-from-s3
  "Read and parse EDN file from S3"
  [s3-client bucket key]
  (let [response (s3/get-object s3-client bucket key)
        bytes (utils/response->bytes response)]
    (when (nil? bytes)
      (throw (IllegalArgumentException. (str "Empty S3 response body for key: " key))))
    (edn/read-string (String. ^bytes bytes "UTF-8"))))

(defn create-manifest
  "Create a human-readable manifest file"
  [{:keys [backup-id backup-type database-id datahike-version
           datom-count chunk-count total-size tx-range chunks
           started-at completed-at]}]
  {:backup/id backup-id
   :backup/type backup-type
   :backup/created-at started-at
   :backup/completed (some? completed-at)
   :backup/version "1.0.0"

   :_comment "This is a Datahike backup manifest file"
   :_debug {:generated-by "datacamp.core"
            :edn-format true
            :inspect-with "Any text editor or Clojure REPL"}

   :database/id database-id
   :database/branch :db
   :datahike/version (or datahike-version "0.6.1")

   :format/version "1.0.0"
   :format/compression :gzip
   :format/encryption nil

   :stats/datom-count datom-count
   :stats/chunk-count chunk-count
   :stats/size-bytes total-size
   :stats/tx-range tx-range

   :chunks chunks

   :timing {:backup-started started-at
           :backup-completed completed-at
           :duration-seconds (when completed-at
                              (/ (- (.getTime completed-at)
                                   (.getTime started-at))
                                1000.0))}})

(defn create-chunk-metadata
  "Create metadata for a single chunk"
  [{:keys [chunk-id tx-range datom-count size-bytes checksum s3-key s3-etag]}]
  {:chunk/id chunk-id
   :chunk/tx-range tx-range
   :chunk/datom-count datom-count
   :chunk/size-bytes size-bytes
   :chunk/checksum checksum
   :chunk/s3-etag s3-etag
   :chunk/s3-key s3-key})

(defn update-checkpoint
  "Update checkpoint file with current progress"
  [s3-client bucket checkpoint-key progress]
  (let [checkpoint (merge
                    {:checkpoint/version "1.0.0"
                     :checkpoint/updated-at (utils/current-timestamp)
                     :_comment "Resume checkpoint - DO NOT EDIT MANUALLY"}
                    progress)]
    (write-edn-to-s3 s3-client bucket checkpoint-key checkpoint)))

(defn create-checkpoint
  "Create initial checkpoint data"
  [{:keys [operation backup-id total-chunks]}]
  {:checkpoint/version "1.0.0"
   :checkpoint/operation operation
   :checkpoint/backup-id backup-id
   :checkpoint/started-at (utils/current-timestamp)
   :checkpoint/updated-at (utils/current-timestamp)

   :progress/total-chunks total-chunks
   :progress/completed 0
   :progress/current-chunk nil

   :state/completed-chunks #{}
   :state/failed-chunks {}
   :state/s3-multipart nil

   :resume/token nil
   :resume/retry-count 0})

(defn read-checkpoint
  "Read checkpoint from S3"
  [s3-client bucket checkpoint-key]
  (try
    (read-edn-from-s3 s3-client bucket checkpoint-key)
    (catch Exception e
      (log/debug "No checkpoint found:" (.getMessage e))
      nil)))

(defn create-config-edn
  "Create database configuration EDN"
  [db-config]
  {:config/version "1.0.0"
   :config/exported-at (utils/current-timestamp)
   :_comment "Database configuration"
   :config db-config})

(defn create-schema-edn
  "Create schema EDN from database"
  [schema-attrs]
  {:schema/version "1.0.0"
   :schema/exported-at (utils/current-timestamp)
   :_comment "Database schema definition"
   :attributes schema-attrs})

;; Directory-based EDN operations

(defn write-edn-to-file
  "Write EDN data to a local file with proper formatting"
  [file-path data]
  (let [edn-str (with-out-str (pp/pprint data))]
    (dir/write-file file-path edn-str)))

(defn read-edn-from-file
  "Read and parse EDN file from local filesystem"
  [file-path]
  (edn/read-string (slurp file-path)))

(defn update-checkpoint-file
  "Update checkpoint file with current progress (directory version)"
  [checkpoint-path progress]
  (let [checkpoint (merge
                    {:checkpoint/version "1.0.0"
                     :checkpoint/updated-at (utils/current-timestamp)
                     :_comment "Resume checkpoint - DO NOT EDIT MANUALLY"}
                    progress)]
    (write-edn-to-file checkpoint-path checkpoint)))

(defn read-checkpoint-file
  "Read checkpoint from local file"
  [checkpoint-path]
  (try
    (read-edn-from-file checkpoint-path)
    (catch Exception e
      (log/debug "No checkpoint found:" (.getMessage e))
      nil)))
