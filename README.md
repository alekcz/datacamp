# Datacamp - Datahike S3 Backup Library

> Your data can't always be on a hike. Sometimes it wants to stop and rest at camp.  
> Datacamp helps you backup and restore your Datahike database.

A production-ready backup library for Datahike databases with S3 storage integration. This library provides streaming backup operations with minimal memory footprint, crash-resilient resumable operations, and support for future live synchronization capabilities.

## Features

- **Dual Storage Options**: Backup to S3 or local directories
- **Full Backups**: Complete database snapshots
- **Memory Efficient**: Streaming architecture with constant memory usage
- **Resilient**: Checkpoint-based resumable operations
- **Human-Readable Metadata**: EDN format for easy inspection and debugging
- **Efficient Data Storage**: Fressian binary format with GZIP compression
- **Production Ready**: Comprehensive error handling and retry logic

## Installation

Add to your `project.clj` or `deps.edn`:

```clojure
;; Leiningen
[datacamp "0.1.0-SNAPSHOT"]

;; deps.edn
{:deps {datacamp {:mvn/version "0.1.0-SNAPSHOT"}}}
```

## Quick Start

### Backup to S3

```clojure
(require '[datahike.api :as d])
(require '[datacamp.core :as backup])

;; Create a Datahike connection
(def conn (d/connect {:store {:backend :file :path "/tmp/my-db"}}))

;; Backup to S3
(def result
  (backup/backup-to-s3 conn
                       {:bucket "my-backups"
                        :region "us-east-1"}
                       :database-id "production-db"))

;; Check the result
(println "Backup ID:" (:backup-id result))
(println "Datoms backed up:" (:datom-count result))
(println "Size:" (datacamp.utils/format-bytes (:total-size-bytes result)))
```

### Backup to Local Directory

```clojure
(require '[datahike.api :as d])
(require '[datacamp.core :as backup])

;; Create a Datahike connection
(def conn (d/connect {:store {:backend :file :path "/tmp/my-db"}}))

;; Backup to local directory
(def result
  (backup/backup-to-directory conn
                              {:path "/backups"}
                              :database-id "production-db"))

;; Check the result
(println "Backup ID:" (:backup-id result))
(println "Location:" (:path result))
```

## Usage

### Creating a Backup

```clojure
(backup/backup-to-s3 conn
                     {:bucket "my-backups"
                      :region "us-east-1"
                      :prefix "production/"}
                     :database-id "my-database"
                     :chunk-size (* 64 1024 1024)  ; 64MB chunks
                     :compression :gzip
                     :parallel 4)
```

### Listing Backups

```clojure
(backup/list-backups {:bucket "my-backups"
                      :region "us-east-1"}
                     "my-database")
;; Returns:
;; [{:backup-id #uuid "..."
;;   :type :full
;;   :created-at #inst "2024-01-15T10:30:00.000Z"
;;   :completed? true
;;   :datom-count 1000000
;;   :size-bytes 268435456}
;;  ...]
```

### Verifying a Backup

```clojure
(backup/verify-backup {:bucket "my-backups"
                       :region "us-east-1"}
                      backup-id
                      :database-id "my-database")
;; Returns:
;; {:success true
;;  :backup-id #uuid "..."
;;  :chunk-count 16
;;  :all-chunks-present true}
```

### Cleaning Up Incomplete Backups

```clojure
(backup/cleanup-incomplete {:bucket "my-backups"
                            :region "us-east-1"}
                           "my-database"
                           :older-than-hours 24)
```

## Backup Format

The library uses a hybrid format strategy:

- **Metadata Files (EDN)**: Human-readable format for manifests, configuration, and checkpoints
- **Data Files (Fressian + GZIP)**: Binary format for efficient datom storage

### S3 Structure

```
s3://bucket/datahike-backups/
├── {database-id}/
│   ├── {backup-id}/
│   │   ├── manifest.edn                # Backup metadata (human-readable)
│   │   ├── config.edn                  # Database configuration
│   │   ├── chunks/
│   │   │   ├── datoms-0.fressian.gz    # Chunked datoms (binary)
│   │   │   ├── datoms-1.fressian.gz
│   │   │   └── ...
│   │   ├── checkpoint.edn              # Resume checkpoint
│   │   └── complete.marker             # Completion flag
```

### Inspecting Backups

Since metadata files are EDN, you can inspect them directly:

```bash
# View manifest with AWS CLI
aws s3 cp s3://bucket/datahike-backups/db-id/backup-id/manifest.edn -

# Check checkpoint status
aws s3 cp s3://bucket/datahike-backups/db-id/backup-id/checkpoint.edn -
```

Or from Clojure:

```clojure
(require '[datacamp.metadata :as meta])

;; Read manifest
(def manifest
  (meta/read-edn-from-s3 s3-client bucket
                        "db-id/backup-id/manifest.edn"))

(clojure.pprint/pprint manifest)
```

## Configuration

### S3 Configuration

```clojure
{:bucket "my-backups"           ; Required: S3 bucket name
 :region "us-east-1"            ; Required: AWS region
 :prefix "production/"          ; Optional: Key prefix
 :endpoint "localhost:9000"     ; Optional: Custom endpoint (for MinIO, etc.)
 :path-style-access? true}      ; Optional: Use path-style access
```

### Backup Options

```clojure
:database-id "my-db"             ; Database identifier (default: "default-db")
:chunk-size (* 64 1024 1024)     ; Chunk size in bytes (default: 64MB)
:compression :gzip               ; Compression algorithm (default: :gzip)
:parallel 4                      ; Parallel uploads (default: 4)
```

## Architecture

The library follows these design principles:

1. **Memory Efficiency**: Never loads the full database into memory
2. **Streaming First**: Processes data in chunks
3. **Resilience**: Checkpointing for crash recovery
4. **Compatibility**: Works with all Datahike storage backends
5. **Simplicity**: Clean API with sensible defaults

## Roadmap

This is the first version (Phase 1) with basic backup functionality. Future phases will include:

- **Phase 2**: Restore operations
- **Phase 3**: Advanced resumable operations
- **Phase 4**: Incremental backups
- **Phase 5**: Live synchronization
- **Phase 6**: Encryption support

See [doc/spec.md](doc/spec.md) for the complete specification and roadmap.

## Requirements

- Clojure 1.11.1 or higher
- Datahike 0.6.1 or higher
- AWS credentials configured (via environment variables, ~/.aws/credentials, or IAM roles)

## AWS Credentials

The library uses the AWS SDK for Clojure, which supports standard AWS credential providers:

- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM instance profile (for EC2)
- IAM role (for ECS, Lambda, etc.)

## Performance

- **Memory Usage**: Constant O(chunk-size), typically < 512MB
- **Throughput**:
  - Local: > 500 MB/s (disk I/O bound)
  - Network: > 50 MB/s (network bound)
- **Compression Ratio**: 60-80% with GZIP level 6

## Error Handling

The library includes comprehensive error handling:

- **Transient Errors**: Automatic retry with exponential backoff
- **Network Failures**: Resume from checkpoint
- **S3 Throttling**: Rate limiting and backoff
- **Serialization Errors**: Logged and skipped (continues backup)

## Testing

```bash
# Run tests
lein test

# Run with specific AWS profile
AWS_PROFILE=dev lein test
```

## Contributing

Contributions are welcome! Please see the [spec document](doc/spec.md) for the full architecture and design decisions.

## License

Copyright © 2025

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.

## Support

For issues and questions:
- GitHub Issues: https://github.com/alekcz/datacamp/issues
- See [doc/spec.md](doc/spec.md) for detailed documentation

## Acknowledgments

This library is built on top of:
- [Datahike](https://github.com/replikativ/datahike) - Durable Datalog database
- [AWS SDK for Clojure](https://github.com/cognitect-labs/aws-api) - AWS API client
- [Fressian](https://github.com/clojure/data.fressian) - Binary serialization format
