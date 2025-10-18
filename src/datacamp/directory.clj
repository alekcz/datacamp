(ns datacamp.directory
  "Local directory storage for Datahike backups"
  (:require [clojure.java.io :as io]
            [taoensso.timbre :as log]
            [datacamp.utils :as utils])
  (:import [java.io File]))

(defn ensure-directory
  "Ensure a directory exists, creating it if necessary"
  [^String path]
  (let [dir (io/file path)]
    (when-not (.exists dir)
      (.mkdirs dir))
    (when-not (.isDirectory dir)
      (throw (ex-info "Path exists but is not a directory" {:path path})))
    dir))

(defn write-file
  "Write data to a file with retry logic"
  [file-path data & {:keys [content-type]}]
  (utils/retry-with-backoff
   (fn []
     (log/debug "Writing to file:" file-path)
     (let [file (io/file file-path)]
       (ensure-directory (.getParent file))
       (if (bytes? data)
         (with-open [out (io/output-stream file)]
           (.write out ^bytes data))
         (spit file data))
       {:file-path file-path
        :size (.length file)}))
   :max-attempts 3))

(defn read-file
  "Read data from a file with retry logic"
  [file-path]
  (utils/retry-with-backoff
   (fn []
     (log/debug "Reading from file:" file-path)
     (let [file (io/file file-path)]
       (when-not (.exists file)
         (throw (ex-info "File does not exist" {:path file-path})))
       (with-open [in (io/input-stream file)]
         (let [bytes (byte-array (.length file))]
           (.read in bytes)
           bytes))))
   :max-attempts 3))

(defn file-exists?
  "Check if a file exists"
  [file-path]
  (.exists (io/file file-path)))

(defn list-files
  "List files in a directory with optional filter"
  [dir-path & {:keys [pattern]}]
  (let [dir (io/file dir-path)]
    (when (.exists dir)
      (->> (.listFiles dir)
           (filter #(.isFile ^File %))
           (filter (fn [^File f]
                    (if pattern
                      (re-find pattern (.getName f))
                      true)))
           (map (fn [^File f]
                  {:path (.getAbsolutePath f)
                   :name (.getName f)
                   :size (.length f)
                   :last-modified (java.util.Date. (.lastModified f))}))))))

(defn delete-file
  "Delete a file"
  [file-path]
  (log/debug "Deleting file:" file-path)
  (let [file (io/file file-path)]
    (when (.exists file)
      (.delete file))))

(defn get-backup-path
  "Get the full path for a backup"
  [base-dir database-id backup-id]
  (str base-dir "/" database-id "/" backup-id))

(defn list-backups-in-directory
  "List all backup directories for a database"
  [base-dir database-id]
  (let [db-dir (io/file base-dir database-id)]
    (when (.exists db-dir)
      (->> (.listFiles db-dir)
           (filter #(.isDirectory ^File %))
           (map (fn [^File d]
                  {:backup-id (.getName d)
                   :path (.getAbsolutePath d)
                   :last-modified (java.util.Date. (.lastModified d))}))))))

(defn cleanup-directory
  "Remove a directory and all its contents"
  [dir-path]
  (let [dir (io/file dir-path)]
    (when (.exists dir)
      (doseq [file (.listFiles dir)]
        (if (.isDirectory file)
          (cleanup-directory (.getAbsolutePath file))
          (.delete file)))
      (.delete dir))))

(defn get-directory-size
  "Calculate the total size of a directory"
  [dir-path]
  (let [dir (io/file dir-path)]
    (if (.exists dir)
      (->> (file-seq dir)
           (filter #(.isFile ^File %))
           (map #(.length ^File %))
           (reduce + 0))
      0)))

(defn copy-file
  "Copy a file from source to destination"
  [src-path dest-path]
  (ensure-directory (.getParent (io/file dest-path)))
  (io/copy (io/file src-path) (io/file dest-path)))
