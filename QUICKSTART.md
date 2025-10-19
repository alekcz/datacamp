# Datacamp Quick Start Guide

Get started with Datacamp in 5 minutes!

## Prerequisites

1. A Datahike database
2. For S3 backups: AWS credentials configured (environment variables, `~/.aws/credentials`, or IAM role)
3. For S3 backups: An S3 bucket (or S3-compatible storage)
4. For directory backups: A local directory or mounted network storage

## Installation

Add to your `project.clj`:

```clojure
[datacamp "0.1.0-SNAPSHOT"]
```

Or `deps.edn`:

```clojure
{:deps {datacamp {:mvn/version "0.1.0-SNAPSHOT"}}}
```

## Basic Usage

### Option 1: Backup to S3

```clojure
(require '[datahike.api :as d])
(require '[datacamp.core :as backup])

;; Connect to your Datahike database
(def conn (d/connect {:store {:backend :file :path "/data/mydb"}}))

;; Backup to S3
(def result
  (backup/backup-to-s3 conn
                       {:bucket "my-backups"
                        :region "us-east-1"}
                       :database-id "production-db"))

;; Check the result
(println "Backup ID:" (:backup-id result))
(println "Success:" (:success result))
```

### Option 2: Backup to Local Directory

```clojure
(require '[datahike.api :as d])
(require '[datacamp.core :as backup])

;; Connect to your Datahike database
(def conn (d/connect {:store {:backend :file :path "/data/mydb"}}))

;; Backup to local directory
(def result
  (backup/backup-to-directory conn
                              {:path "/backups"}
                              :database-id "production-db"))

;; Check the result
(println "Backup ID:" (:backup-id result))
(println "Location:" (:path result))
```

That's it! Your database is now backed up.

### Restore from S3

```clojure
(require '[datahike.api :as d])
(require '[datacamp.core :as backup])

(def restore-conn (d/connect {:store {:backend :file :path "/tmp/restore-db"}}))
(backup/restore-from-s3 restore-conn
                        {:bucket "my-backups" :region "us-east-1"}
                        backup-id
                        :database-id "production-db")
```

### Restore from Local Directory

```clojure
(def restore-conn (d/connect {:store {:backend :file :path "/tmp/restore-db"}}))
(backup/restore-from-directory restore-conn
                               {:path "/backups"}
                               backup-id
                               :database-id "production-db")
```

### Live Migration (memory → file example)

```clojure
(require '[datacamp.migration :as migrate])

(def source-conn (d/connect {:store {:backend :mem :id "live-src"}}))
(def target-cfg  {:store {:backend :file :path "/tmp/live-target"}})

;; Start migration and get a router back
(def router (migrate/live-migrate
             source-conn target-cfg
             :database-id "my-db"
             :backup-dir "/backups"
             :progress-fn println))

;; Route new writes during migration
(router [{:user/name "Alice"}])
(router [{:user/name "Bob"}])

;; Finalize (cutover to target)
(def result (router))
```

## What Just Happened?

The library:
1. ✅ Streamed your database datoms in chunks (memory efficient)
2. ✅ Compressed each chunk with GZIP (60-80% size reduction)
3. ✅ Uploaded chunks to S3 in parallel
4. ✅ Created human-readable metadata files (EDN format)
5. ✅ Tracked progress with checkpoints (for future resume capability)

## S3 Structure

Your backup is organized like this:

```
s3://my-backups/
└── datahike-backups/
    └── production-db/
        └── {backup-id}/
            ├── manifest.edn           # Human-readable metadata
            ├── checkpoint.edn         # Progress tracking
            ├── chunks/
            │   ├── datoms-0.fressian.gz
            │   ├── datoms-1.fressian.gz
            │   └── ...
            └── complete.marker        # Completion flag
```

## Directory Structure (for local backups)

```
/backups/
└── production-db/
    └── {backup-id}/
        ├── manifest.edn           # Human-readable metadata
        ├── checkpoint.edn         # Progress tracking
        ├── chunks/
        │   ├── datoms-0.fressian.gz
        │   ├── datoms-1.fressian.gz
        │   └── ...
        └── complete.marker        # Completion flag
```

## Common Operations

### For S3 Backups

#### List All Backups

```clojure
(def backups
  (backup/list-backups {:bucket "my-backups" :region "us-east-1"}
                       "production-db"))

(doseq [b backups]
  (println (:backup-id b) "-" (:created-at b)))
```

#### Verify Backup Integrity

```clojure
(def verification
  (backup/verify-backup {:bucket "my-backups" :region "us-east-1"}
                        backup-id
                        :database-id "production-db"))

(if (:all-chunks-present verification)
  (println "✓ Backup is valid")
  (println "✗ Backup has missing chunks"))
```

#### Clean Up Old Backups

```clojure
;; Remove incomplete backups older than 24 hours
(backup/cleanup-incomplete {:bucket "my-backups" :region "us-east-1"}
                           "production-db"
                           :older-than-hours 24)
```

### For Directory Backups

#### List All Backups

```clojure
(def backups
  (backup/list-backups-in-directory {:path "/backups"}
                                    "production-db"))

(doseq [b backups]
  (println (:backup-id b) "-" (:created-at b)))
```

#### Verify Backup Integrity

```clojure
(def verification
  (backup/verify-backup-in-directory {:path "/backups"}
                                     backup-id
                                     :database-id "production-db"))

(if (:all-chunks-present verification)
  (println "✓ Backup is valid")
  (println "✗ Backup has missing chunks"))
```

#### Clean Up Old Backups

```clojure
;; Remove incomplete backups older than 24 hours
(backup/cleanup-incomplete-in-directory {:path "/backups"}
                           "production-db"
                           :older-than-hours 24)
```

## Configuration Options

### Customize Chunk Size

```clojure
(backup/backup-to-s3 conn s3-config
                     :database-id "my-db"
                     :chunk-size (* 128 1024 1024))  ; 128MB chunks
```

### Adjust Parallelism

```clojure
(backup/backup-to-s3 conn s3-config
                     :database-id "my-db"
                     :parallel 8)  ; 8 parallel uploads
```

### Use S3 Key Prefix

```clojure
(backup/backup-to-s3 conn
                     {:bucket "my-backups"
                      :region "us-east-1"
                      :prefix "production/datacenter-1/"}
                     :database-id "my-db")
```

## S3-Compatible Storage

### MinIO

```clojure
(backup/backup-to-s3 conn
                     {:bucket "my-backups"
                      :region "us-east-1"
                      :endpoint "minio.example.com:9000"
                      :path-style-access? true}
                     :database-id "my-db")
```

### DigitalOcean Spaces

```clojure
(backup/backup-to-s3 conn
                     {:bucket "my-space"
                      :region "nyc3"
                      :endpoint "nyc3.digitaloceanspaces.com"}
                     :database-id "my-db")
```

## Inspecting Backups

You can inspect backup metadata directly with AWS CLI:

```bash
# View manifest
aws s3 cp s3://my-backups/datahike-backups/production-db/{backup-id}/manifest.edn -

# Check checkpoint status
aws s3 cp s3://my-backups/datahike-backups/production-db/{backup-id}/checkpoint.edn -
```

Or from Clojure:

```clojure
(require '[datacamp.metadata :as meta])
(require '[datacamp.s3 :as s3])

(def s3-client (s3/create-s3-client {:bucket "my-backups" :region "us-east-1"}))

(def manifest
  (meta/read-edn-from-s3 s3-client "my-backups"
                        "datahike-backups/production-db/{backup-id}/manifest.edn"))

(clojure.pprint/pprint manifest)
```

## Error Handling

The library includes automatic retry with exponential backoff for:
- Network timeouts
- S3 throttling
- Transient errors

```clojure
(try
  (def result (backup/backup-to-s3 conn s3-config :database-id "my-db"))

  (if (:success result)
    (println "Backup successful:" (:backup-id result))
    (println "Backup failed:" (:error result)))

  (catch Exception e
    (println "Exception:" (.getMessage e))))
```

## Performance

Typical performance characteristics:

- **Memory Usage**: Constant O(chunk-size), typically < 512MB
- **Throughput**:
  - Local operations: > 500 MB/s (disk I/O bound)
  - Network operations: > 50 MB/s (network bound)
- **Compression**: 60-80% size reduction with GZIP

## Example: Complete Workflow (S3)

```clojure
(ns my-app.backup
  (:require [datahike.api :as d]
            [datacamp.core :as backup]))

(defn backup-database [conn database-id]
  (println "Starting backup for" database-id)

  (let [result (backup/backup-to-s3 conn
                                    {:bucket "my-backups"
                                     :region "us-east-1"}
                                    :database-id database-id)]
    (if (:success result)
      (do
        (println "✓ Backup successful!")
        (println "  Backup ID:" (:backup-id result))
        (println "  Datoms:" (:datom-count result))
        (println "  Size:" (:total-size-bytes result) "bytes")
        (println "  Location:" (:s3-path result))

        ;; Verify the backup
        (let [verification (backup/verify-backup
                            {:bucket "my-backups" :region "us-east-1"}
                            (:backup-id result)
                            :database-id database-id)]
          (if (:all-chunks-present verification)
            (println "✓ Verification passed")
            (println "✗ Verification failed")))

        result)

      (do
        (println "✗ Backup failed:" (:error result))
        nil))))

;; Usage
(def conn (d/connect {:store {:backend :file :path "/data/mydb"}}))
(backup-database conn "production-db")
```

## Next Steps

- See [examples/basic_usage.clj](examples/basic_usage.clj) for more examples
- See [examples/advanced_usage.clj](examples/advanced_usage.clj) for advanced patterns
- Read the [full specification](doc/spec.temp.md) for architecture details
- Check [README.md](README.md) for complete documentation

## Getting Help

- Issues: https://github.com/alekcz/datacamp/issues
- Documentation: [doc/spec.md](doc/spec.md)

## What's Next?

Planned enhancements:
- **Incremental backups** - Backup only changes since last backup
- **Encryption** - Encrypt backups at rest and in transit
- **Operational tooling** - Metrics, monitoring, and rotation policies

---

Happy backing up! 🚀
