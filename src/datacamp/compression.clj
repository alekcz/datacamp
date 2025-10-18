(ns datacamp.compression
  "Compression utilities for backup data"
  (:require [taoensso.timbre :as log])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]
           [java.util.zip GZIPOutputStream GZIPInputStream]))

(defn compress-gzip
  "Compress byte array using GZIP"
  [^bytes data & {:keys [level] :or {level 6}}]
  (let [baos (ByteArrayOutputStream.)]
    (with-open [gzip (GZIPOutputStream. baos)]
      (.write gzip data))
    (.toByteArray baos)))

(defn decompress-gzip
  "Decompress GZIP byte array"
  [^bytes data]
  (let [bais (ByteArrayInputStream. data)
        baos (ByteArrayOutputStream.)]
    (with-open [gzip (GZIPInputStream. bais)]
      (let [buffer (byte-array 8192)]
        (loop []
          (let [n (.read gzip buffer)]
            (when (pos? n)
              (.write baos buffer 0 n)
              (recur))))))
    (.toByteArray baos)))

(defn compress-chunk
  "Compress a serialized chunk"
  [^bytes data & {:keys [algorithm level]
                  :or {algorithm :gzip level 6}}]
  (case algorithm
    :gzip (compress-gzip data :level level)
    :none data
    (throw (ex-info "Unsupported compression algorithm"
                   {:algorithm algorithm}))))

(defn decompress-chunk
  "Decompress a chunk"
  [^bytes data & {:keys [algorithm]
                  :or {algorithm :gzip}}]
  (case algorithm
    :gzip (decompress-gzip data)
    :none data
    (throw (ex-info "Unsupported compression algorithm"
                   {:algorithm algorithm}))))

(defn compression-ratio
  "Calculate compression ratio"
  [original-size compressed-size]
  (if (zero? original-size)
    0.0
    (double (/ compressed-size original-size))))
