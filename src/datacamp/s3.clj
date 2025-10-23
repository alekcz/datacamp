(ns datacamp.s3
  "S3 integration layer for Datahike backups"
  (:require [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [taoensso.timbre :as log]
            [datacamp.utils :as utils]))

(defn- parse-endpoint
  "Parse an endpoint string like http://localhost:9000 into an endpoint-override map
  suitable for cognitect.aws.client.api/client."
  [endpoint]
  (when endpoint
    (let [u (java.net.URI. endpoint)
          scheme (.getScheme u)
          host (.getHost u)
          port (.getPort u)
          protocol (if (= scheme "http") :http :https)]
      (cond-> {:protocol protocol :hostname host}
        (pos? port) (assoc :port port)))))

(defn create-s3-client
  "Create an S3 client from configuration. Supports :endpoint like
  \"http://localhost:4566\" for LocalStack and other S3-compatible services.

  Optional keys:
  - :access-key-id, :secret-access-key for static credentials"
  [{:keys [region endpoint access-key-id secret-access-key] :as _config}]
  (let [client-opts (cond-> {:api :s3}
                      region (assoc :region region)
                      endpoint (assoc :endpoint-override (parse-endpoint endpoint))
                      (and access-key-id secret-access-key)
                      (assoc :credentials-provider (credentials/basic-credentials-provider
                                                   {:access-key-id access-key-id
                                                    :secret-access-key secret-access-key})))]
    (aws/client client-opts)))

(defn ensure-bucket
  "Create the bucket if it does not exist. Ignores already-exists errors."
  [s3-client bucket]
  (try
    (aws/invoke s3-client {:op :CreateBucket
                           :request {:Bucket bucket}})
    (catch Exception e
      (let [msg (.getMessage e)]
        (when-not (or (re-find #"BucketAlreadyOwnedByYou" msg)
                      (re-find #"BucketAlreadyExists" msg))
          (throw e)))))
  true)

(defn put-object
  "Upload an object to S3 with retry logic"
  [s3-client bucket key data & {:keys [content-type metadata]}]
  (utils/retry-with-backoff
   (fn []
     (let [request (cond-> {:Bucket bucket
                           :Key key
                           :Body data}
                     content-type (assoc :ContentType content-type)
                     metadata (assoc :Metadata metadata))]
       (log/debug "Uploading to S3:" bucket key)
       (let [response (aws/invoke s3-client {:op :PutObject :request request})]
         ;; Check for errors in the response
         (when-let [error (:cognitect.anomalies/category response)]
           (throw (ex-info (str "S3 PutObject failed: " (or (:message response) error))
                          {:bucket bucket
                           :key key
                           :error response})))
         response)))
   :max-attempts 5))

(defn get-object
  "Download an object from S3 with retry logic"
  [s3-client bucket key]
  (utils/retry-with-backoff
   (fn []
     (log/debug "Downloading from S3:" bucket key)
     (let [response (aws/invoke s3-client {:op :GetObject
                                           :request {:Bucket bucket
                                                    :Key key}})]
       ;; Check for errors in the response
       (when-let [error (:cognitect.anomalies/category response)]
         (throw (ex-info (str "S3 GetObject failed: " (or (:message response) error))
                        {:bucket bucket
                         :key key
                         :error response})))
       response))
   :max-attempts 5))

(defn object-exists?
  "Check if an object exists in S3"
  [s3-client bucket key]
  (try
    (aws/invoke s3-client {:op :HeadObject
                          :request {:Bucket bucket
                                   :Key key}})
    true
    (catch Exception e
      (if (re-find #"404" (.getMessage e))
        false
        (throw e)))))

(defn list-objects
  "List objects in S3 with a prefix"
  [s3-client bucket prefix]
  (let [response (aws/invoke s3-client {:op :ListObjectsV2
                                        :request {:Bucket bucket
                                                 :Prefix prefix}})]
    (when-let [contents (:Contents response)]
      (map (fn [obj] {:key (:Key obj)
                     :size (:Size obj)
                     :last-modified (:LastModified obj)
                     :etag (:ETag obj)})
           contents))))

(defn delete-object
  "Delete an object from S3"
  [s3-client bucket key]
  (log/debug "Deleting from S3:" bucket key)
  (aws/invoke s3-client {:op :DeleteObject
                        :request {:Bucket bucket
                                 :Key key}}))

(defn create-multipart-upload
  "Initiate a multipart upload"
  [s3-client bucket key & {:keys [content-type metadata]}]
  (let [request (cond-> {:Bucket bucket :Key key}
                  content-type (assoc :ContentType content-type)
                  metadata (assoc :Metadata metadata))
        response (aws/invoke s3-client {:op :CreateMultipartUpload
                                       :request request})]
    (:UploadId response)))

(defn upload-part
  "Upload a part in a multipart upload"
  [s3-client bucket key upload-id part-number data]
  (let [response (aws/invoke s3-client {:op :UploadPart
                                       :request {:Bucket bucket
                                                :Key key
                                                :UploadId upload-id
                                                :PartNumber part-number
                                                :Body data}})]
    {:part-number part-number
     :etag (:ETag response)}))

(defn complete-multipart-upload
  "Complete a multipart upload"
  [s3-client bucket key upload-id parts]
  (aws/invoke s3-client {:op :CompleteMultipartUpload
                        :request {:Bucket bucket
                                 :Key key
                                 :UploadId upload-id
                                 :MultipartUpload
                                 {:Parts (map (fn [{:keys [part-number etag]}]
                                               {:PartNumber part-number
                                                :ETag etag})
                                             parts)}}}))

(defn abort-multipart-upload
  "Abort a multipart upload"
  [s3-client bucket key upload-id]
  (log/info "Aborting multipart upload:" key upload-id)
  (aws/invoke s3-client {:op :AbortMultipartUpload
                        :request {:Bucket bucket
                                 :Key key
                                 :UploadId upload-id}}))

(defn list-incomplete-uploads
  "List incomplete multipart uploads"
  [s3-client bucket prefix]
  (let [response (aws/invoke s3-client {:op :ListMultipartUploads
                                       :request {:Bucket bucket
                                                :Prefix prefix}})]
    (when-let [uploads (:Uploads response)]
      (map (fn [upload]
             {:key (:Key upload)
              :upload-id (:UploadId upload)
              :initiated (:Initiated upload)})
           uploads))))

(defn cleanup-old-multipart-uploads
  "Clean up multipart uploads older than specified hours"
  [s3-client bucket prefix hours]
  (let [uploads (list-incomplete-uploads s3-client bucket prefix)
        old-uploads (filter (fn [{:keys [initiated]}]
                             (> (utils/hours-since initiated) hours))
                           uploads)]
    (log/info "Found" (count old-uploads) "old multipart uploads to clean up")
    (doseq [{:keys [key upload-id]} old-uploads]
      (try
        (abort-multipart-upload s3-client bucket key upload-id)
        (log/info "Cleaned up multipart upload:" key)
        (catch Exception e
          (log/error e "Failed to abort multipart upload:" key))))))
