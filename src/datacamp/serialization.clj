(ns datacamp.serialization
  "Serialization utilities using Fressian format"
  (:require [clojure.data.fressian :as fress]
            [taoensso.timbre :as log])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]))

(defn datom->vec
  "Convert a Datahike Datom to a vector for serialization"
  [datom]
  ;; Datoms have the structure [e a v tx added]
  [(:e datom)
   (:a datom)
   (:v datom)
   (:tx datom)
   (:added datom)])

(defn vec->datom-data
  "Convert a vector back to datom data map (not a Datom record, just the data)"
  [[e a v tx added]]
  {:e e :a a :v v :tx tx :added added})

(defn serialize-to-bytes
  "Serialize data to bytes using Fressian"
  [data]
  (let [baos (ByteArrayOutputStream.)]
    (with-open [writer (fress/create-writer baos)]
      (fress/write-object writer data))
    (.toByteArray baos)))

(defn deserialize-from-bytes
  "Deserialize data from bytes using Fressian"
  [^bytes data]
  (let [bais (ByteArrayInputStream. data)]
    (with-open [reader (fress/create-reader bais)]
      (fress/read-object reader))))

(defn serialize-datom-chunk
  "Serialize a chunk of datoms to Fressian format"
  [chunk-id datoms]
  (let [chunk-data {:format/type :datom-chunk
                   :format/version "1.0.0"
                   :chunk/id chunk-id
                   ;; Convert datoms to plain vectors for serialization
                   :datoms (mapv datom->vec datoms)}]
    (serialize-to-bytes chunk-data)))

(defn deserialize-datom-chunk
  "Deserialize a chunk of datoms from Fressian format"
  [^bytes data]
  (let [chunk-data (deserialize-from-bytes data)]
    (when (not= (:format/type chunk-data) :datom-chunk)
      (throw (ex-info "Invalid chunk format" {:type (:format/type chunk-data)})))
    chunk-data))

(defn serialize-transaction
  "Serialize a transaction to Fressian format"
  [{:keys [tx-id timestamp datoms metadata]}]
  (serialize-to-bytes
   {:format/type :transaction
    :format/version "1.0.0"
    :tx-id tx-id
    :timestamp timestamp
    ;; Convert datoms to plain vectors for serialization
    :datoms (mapv datom->vec datoms)
    :metadata metadata}))

(defn deserialize-transaction
  "Deserialize a transaction from Fressian format"
  [^bytes data]
  (let [tx-data (deserialize-from-bytes data)]
    (when (not= (:format/type tx-data) :transaction)
      (throw (ex-info "Invalid transaction format" {:type (:format/type tx-data)})))
    tx-data))
