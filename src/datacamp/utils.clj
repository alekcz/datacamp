(ns datacamp.utils
  "Utility functions for Datahike backup operations"
  (:require [taoensso.timbre :as log])
  (:import [java.security MessageDigest SecureRandom]
           [java.util Date UUID]
           [java.time LocalDateTime]
           [java.time.format DateTimeFormatter]))

(defn generate-backup-id
  "Generate a unique backup ID in format: YYYYMMDD-HHMMSS-hash (UTC timezone)
   Example: 20251018-195132-a3f9c2"
  []
  (let [now (java.time.ZonedDateTime/now (java.time.ZoneId/of "UTC"))
        formatter (DateTimeFormatter/ofPattern "yyyyMMdd-HHmmss")
        date-str (.format now formatter)
        ;; Generate short random hash (6 chars)
        random-bytes (byte-array 3)
        _ (.nextBytes (SecureRandom.) random-bytes)
        hash (apply str (map #(format "%02x" %) random-bytes))]
    (str date-str "-" hash)))

(defn current-timestamp
  "Get current timestamp"
  []
  (Date.))

(defn hours-since
  "Calculate hours since a given timestamp"
  [timestamp]
  (let [now (.getTime (Date.))
        then (.getTime timestamp)
        diff-ms (- now then)]
    (/ diff-ms 1000.0 60.0 60.0)))

(defn sha256
  "Calculate SHA-256 hash of byte array"
  [^bytes data]
  (let [digest (MessageDigest/getInstance "SHA-256")
        hash-bytes (.digest digest data)]
    (apply str (map #(format "%02x" %) hash-bytes))))

(defn estimate-value-size
  "Estimate serialized size of a value"
  [v]
  (cond
    (nil? v) 1
    (boolean? v) 1
    (number? v) 8
    (string? v) (.length ^String v)
    (keyword? v) (+ 1 (.length (name v)))
    (inst? v) 8
    (uuid? v) 16
    (vector? v) (reduce + 0 (map estimate-value-size v))
    (map? v) (reduce + 0 (mapcat (fn [[k v]] [(estimate-value-size k) (estimate-value-size v)]) v))
    (set? v) (reduce + 0 (map estimate-value-size v))
    :else 64)) ; conservative estimate for complex types

(defn format-bytes
  "Format byte count as human-readable string"
  [bytes]
  (cond
    (< bytes 1024) (str bytes " B")
    (< bytes (* 1024 1024)) (format "%.2f KB" (/ bytes 1024.0))
    (< bytes (* 1024 1024 1024)) (format "%.2f MB" (/ bytes 1024.0 1024.0))
    :else (format "%.2f GB" (/ bytes 1024.0 1024.0 1024.0))))

(defn exponential-backoff
  "Calculate exponential backoff delay in milliseconds"
  [retry-count & {:keys [base-ms max-ms]
                  :or {base-ms 1000 max-ms 16000}}]
  (min max-ms (* base-ms (Math/pow 2 retry-count))))

(defn retry-with-backoff
  "Retry a function with exponential backoff"
  [f & {:keys [max-attempts base-ms]
        :or {max-attempts 5 base-ms 1000}}]
  (loop [attempt 0]
    (let [result (try
                   {:success true :value (f)}
                   (catch Exception e
                     {:success false :error e}))]
      (if (:success result)
        (:value result)
        (if (< attempt (dec max-attempts))
          (do
            (log/warn "Attempt" (inc attempt) "failed, retrying after backoff..."
                      (.getMessage (:error result)))
            (Thread/sleep (exponential-backoff attempt :base-ms base-ms))
            (recur (inc attempt)))
          (throw (:error result)))))))

(defn classify-error
  "Classify an error for appropriate handling"
  [^Exception e]
  (let [msg (.getMessage e)
        class-name (.getName (class e))]
    (cond
      ;; Network/transient errors
      (or (re-find #"(?i)timeout" msg)
          (re-find #"(?i)connection" msg)
          (re-find #"(?i)throttl" msg)
          (re-find #"(?i)429" msg))
      :transient

      ;; Data errors
      (or (re-find #"(?i)serializ" msg)
          (re-find #"(?i)parse" msg)
          (re-find #"(?i)invalid" msg))
      :data

      ;; Resource errors
      (or (re-find #"(?i)memory" msg)
          (re-find #"(?i)space" msg)
          (re-find #"(?i)quota" msg))
      :resource

      ;; Fatal errors
      (or (re-find #"(?i)credential" msg)
          (re-find #"(?i)unauthorized" msg)
          (re-find #"(?i)forbidden" msg))
      :fatal

      :else :unknown)))

(defn get-process-id
  "Get the current process ID"
  []
  (-> (java.lang.management.ManagementFactory/getRuntimeMXBean)
      (.getName)
      (clojure.string/split #"@")
      first))

(defn response->bytes
  "Convert AWS SDK response body to byte array
  The AWS SDK returns the body as a java.io.InputStream"
  [response]
  (when-let [body (:Body response)]
    (with-open [in body]
      (let [baos (java.io.ByteArrayOutputStream.)]
        (clojure.java.io/copy in baos)
        (.toByteArray baos)))))
