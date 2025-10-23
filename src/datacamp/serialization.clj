(ns datacamp.serialization
  "Serialization utilities supporting Fressian and CBOR formats"
  (:require [clojure.data.fressian :as fress]
            [clj-cbor.core :as cbor]
            [clojure.walk :as walk]
            [taoensso.timbre :as log])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]))

(defn preserve-doubles
  "Mark doubles for preservation during CBOR serialization.
  CBOR may lose precision by converting doubles to floats.
  See: https://github.com/replikativ/datahike/issues/633"
  [data]
  (walk/postwalk
   (fn [x]
     (if (and (number? x) (instance? Double x))
       ;; Tag doubles so they can be restored after deserialization
       {:__type :double :value x}
       x))
   data))

(defn restore-doubles
  "Restore double values that were preserved during CBOR serialization."
  [data]
  (walk/postwalk
   (fn [x]
     (if (and (map? x) (= (:__type x) :double))
       ;; Convert back to double, handling potential float conversion
       (double (:value x))
       x))
   data))

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
  [v]
  (let [[e a v* tx added] v]
    {:e e :a a :v v* :tx tx :added added}))

(defn serialize-to-bytes
  "Serialize data to bytes using specified format (:fressian or :cbor, defaults to :fressian)

  When using CBOR format, doubles are preserved to avoid precision loss during
  float conversion. See: https://github.com/replikativ/datahike/issues/633"
  ([data] (serialize-to-bytes data :fressian))
  ([data format]
   (case format
     :fressian (let [baos (ByteArrayOutputStream.)]
                 (with-open [writer (fress/create-writer baos)]
                   (fress/write-object writer data))
                 (.toByteArray baos))
     :cbor (let [preserved-data (preserve-doubles data)]
             (cbor/encode preserved-data))
     (throw (ex-info "Unsupported serialization format" {:format format})))))

(defn deserialize-from-bytes
  "Deserialize data from bytes using specified format (:fressian or :cbor, defaults to :fressian)

  When using CBOR format, doubles are restored to ensure schema compatibility."
  ([^bytes data] (deserialize-from-bytes data :fressian))
  ([^bytes data format]
   (case format
     :fressian (let [bais (ByteArrayInputStream. data)]
                 (with-open [reader (fress/create-reader bais)]
                   (fress/read-object reader)))
     :cbor (let [decoded (cbor/decode data)]
             (restore-doubles decoded))
     (throw (ex-info "Unsupported deserialization format" {:format format})))))

(defn serialize-datom-chunk
  "Serialize a chunk of datoms to specified format (:fressian or :cbor, defaults to :fressian)

  Datoms should be Datahike datom records/objects with :e, :a, :v, :tx, :added properties."
  ([chunk-id datoms] (serialize-datom-chunk chunk-id datoms :fressian))
  ([chunk-id datoms format]
   (let [chunk-data {:format/type :datom-chunk
                     :format/version "1.0.0"
                     :format/serialization format
                     :chunk/id chunk-id
                     ;; Convert datoms to plain vectors for serialization
                     :datoms (mapv datom->vec datoms)}]
     (serialize-to-bytes chunk-data format))))

(defn deserialize-datom-chunk
  "Deserialize a chunk of datoms from bytes (format detected from metadata)"
  ([^bytes data] (deserialize-datom-chunk data :fressian))
  ([^bytes data format]
   (let [chunk-data (deserialize-from-bytes data format)]
     (when (not= (:format/type chunk-data) :datom-chunk)
       (throw (ex-info "Invalid chunk format" {:type (:format/type chunk-data)})))
     chunk-data)))

(defn serialize-transaction
  "Serialize a transaction to specified format (:fressian or :cbor, defaults to :fressian)"
  ([tx-data] (serialize-transaction tx-data :fressian))
  ([{:keys [tx-id timestamp datoms metadata]} format]
   (serialize-to-bytes
    {:format/type :transaction
     :format/version "1.0.0"
     :format/serialization format
     :tx-id tx-id
     :timestamp timestamp
     ;; Convert datoms to plain vectors for serialization
     :datoms (mapv datom->vec datoms)
     :metadata metadata}
    format)))

(defn deserialize-transaction
  "Deserialize a transaction from bytes (format detected from metadata)"
  ([^bytes data] (deserialize-transaction data :fressian))
  ([^bytes data format]
   (let [tx-data (deserialize-from-bytes data format)]
     (when (not= (:format/type tx-data) :transaction)
       (throw (ex-info "Invalid transaction format" {:type (:format/type tx-data)})))
     tx-data)))
