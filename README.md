# Datacamp - Datahike Backup, Restore, and Live Migration

[![Tests](https://github.com/alekcz/datacamp/actions/workflows/test.yml/badge.svg)](https://github.com/alekcz/datacamp/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/alekcz/datacamp/branch/master/graph/badge.svg)](https://codecov.io/gh/alekcz/datacamp)
[![Clojars Project](https://img.shields.io/clojars/v/org.alekcz/datacamp.svg)](https://clojars.org/org.alekcz/datacamp)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue.svg)](https://www.eclipse.org/legal/epl-2.0/)

> Datacamp helps you backup and restore your Datahike database.
> Your data can't always be on a hike. Sometimes it wants to stop and rest at camp.

A production-ready backup and migration library for Datahike databases. It provides streaming backup/restore (S3 and local directory), crash‑resilient resumable operations, and live migration (continuous capture + cutover) between backends.

## Features

- Backup targets: S3 or local directory
- Restore from S3 or local directory
- Live migration: continuous capture + router-based cutover
- Memory efficient: streaming with constant memory usage
- Resilient: checkpointed, resumable operations
- Human-readable metadata (EDN manifests/checkpoints)
- Compact data files (Fressian or CBOR + GZIP)
- Comprehensive error handling and verification helpers

## Installation

Add to your `project.clj` or `deps.edn`:

```clojure
;; Leiningen
[org.alekcz/datacamp "0.1.0-SNAPSHOT"]

;; deps.edn
{:deps {org.alekcz/datacamp {:mvn/version "0.1.0-SNAPSHOT"}}}
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

### Restore from S3

```clojure
(require '[datahike.api :as d])
(require '[datacamp.core :as backup])

;; Connect to a fresh/empty target database
(def restore-conn (d/connect {:store {:backend :file :path "/tmp/restore-db"}}))

;; Restore a specific backup ID
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
;; => {:status :completed, :target-conn <...>, :migration-id "..."}
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
- **Data Files (Fressian or CBOR + GZIP)**: Binary format for efficient datom storage
  - **Fressian** (default): Official Clojure serialization format, stable and well-tested
  - **CBOR**: Compact Binary Object Representation, with automatic double-precision preservation

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
 :endpoint "localhost:4566"     ; Optional: Custom endpoint (for LocalStack, etc.)
 :path-style-access? true}      ; Optional: Use path-style access
```

### Backup Options

```clojure
:database-id "my-db"             ; Database identifier (default: "default-db")
:chunk-size (* 64 1024 1024)     ; Chunk size in bytes (default: 64MB)
:compression :gzip               ; Compression algorithm (default: :gzip)
:parallel 4                      ; Parallel uploads (default: 4)
:serialization :fressian         ; Serialization format: :fressian (default) or :cbor
```

### Serialization Format Selection

The library supports two serialization formats:

#### Fressian (Default)
```clojure
;; Explicit Fressian format
(backup/backup-to-s3 conn
                     {:bucket "my-backups" :region "us-east-1"}
                     :database-id "my-db"
                     :serialization :fressian)  ; or omit for default
```

**When to use Fressian:**
- Default choice for most use cases
- Stable, well-tested Clojure serialization format
- Native support for all Clojure/Datahike data types
- Good compression ratio with GZIP

#### CBOR (Compact Binary Object Representation)
```clojure
;; Using CBOR format
(backup/backup-to-s3 conn
                     {:bucket "my-backups" :region "us-east-1"}
                     :database-id "my-db"
                     :serialization :cbor)
```

**When to use CBOR:**
- Industry-standard binary format (RFC 8949)
- Interoperability with non-Clojure systems
- Automatic double-precision preservation (prevents float conversion issues)
- Slightly more compact in some cases

**Important:** CBOR includes automatic protection against double-to-float precision loss during serialization. This ensures schema compatibility when using `:db.type/double` attributes. See [datahike#633](https://github.com/replikativ/datahike/issues/633) for background on this issue.

#### Restoring with Different Formats

When restoring, specify the same format used during backup:

```clojure
;; Restore CBOR backup
(backup/restore-from-s3 restore-conn
                        {:bucket "my-backups" :region "us-east-1"}
                        backup-id
                        :database-id "my-db"
                        :serialization :cbor)

;; Restore Fressian backup (default)
(backup/restore-from-s3 restore-conn
                        {:bucket "my-backups" :region "us-east-1"}
                        backup-id
                        :database-id "my-db"
                        :serialization :fressian)
```

## Architecture

The library follows these design principles:

1. **Memory Efficiency**: Never loads the full database into memory
2. **Streaming First**: Processes data in chunks
3. **Resilience**: Checkpointing for crash recovery
4. **Compatibility**: Works with all Datahike storage backends
5. **Simplicity**: Clean API with sensible defaults

## Roadmap

Current release includes full backup, restore (S3 and directory), and live migration. Upcoming work:

- Incremental/delta backups
- Advanced resumability and resumable uploads across sessions
- Encryption at rest/in transit
- Cloud‑native operational tooling and metrics

See the full specification in [doc/SPEC.md](doc/SPEC.md) and design rationale in [doc/RATIONALE.md](doc/RATIONALE.md).

## Requirements

- **Java**: 11, 17, or 21 (compiled for Java 11 bytecode compatibility)
- **Clojure**: 1.11.1 or higher
- **Datahike**: 0.6.1 or higher
- **AWS credentials**: Configured via environment variables, ~/.aws/credentials, or IAM roles (for S3 backups)

Datacamp is tested against Java 11, 17, and 21 to ensure broad compatibility. The library is compiled with Java 11 target bytecode, making it compatible with Java 11+ environments.

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

## Documentation

Comprehensive documentation is available in the [`doc/`](doc/) directory:

### For Users

- **[Quick Start Guide](doc/QUICKSTART.md)** - Get up and running in 5 minutes
  - Installation and basic usage
  - Common operations and workflows
  - S3-compatible storage configuration
  - Example code and troubleshooting

### For Developers and Maintainers

- **[Architecture Rationale](doc/RATIONALE.md)** - Deep dive into design decisions
  - Problems Datacamp solves
  - Core design principles and trade-offs
  - Critical design decisions explained
  - Memory management strategy
  - Backup/restore data flow
  - Live migration architecture
  - Error handling and safety mechanisms

- **[Technical Specification](doc/SPEC.md)** - Complete implementation specification
  - Architecture overview and component design
  - Backup format specification
  - Streaming algorithms and memory management
  - API contracts and error handling
  - Performance characteristics
  - Complete enough for alternative language implementations

### For Testing

- **[Testing Guide](doc/TESTING.md)** - Quick reference for running tests
  - Docker Compose setup
  - Babashka task reference
  - Backend-specific test instructions
  - Troubleshooting test failures

- **[Test Strategy](doc/TESTSTRATEGY.md)** - Comprehensive testing documentation
  - Testing philosophy and approach
  - Test coverage by component (all 49 tests explained)
  - Test infrastructure and helpers
  - Writing new tests
  - Debugging strategies

## Testing

Quick test commands (see [doc/TESTING.md](doc/TESTING.md) for full details):

```bash
# Quick tests (no external services)
bb test:quick

# All tests (starts/stops services as needed)
bb test:all

# Code coverage
bb coverage
```

## Contributing

Contributions are welcome! Before contributing:

1. Read the [Architecture Rationale](doc/RATIONALE.md) to understand design decisions
2. Review the [Technical Specification](doc/SPEC.md) for implementation details
3. Check the [Test Strategy](doc/TESTSTRATEGY.md) to understand testing approach
4. Run the full test suite to ensure everything works

## License

Copyright © 2025 Alexander Oloo

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
- See [documentation](doc/) for detailed guides and specifications

## Acknowledgments

This library is built on top of:
- [Datahike](https://github.com/replikativ/datahike) - Durable Datalog database
- [AWS SDK for Clojure](https://github.com/cognitect-labs/aws-api) - AWS API client
- [Fressian](https://github.com/clojure/data.fressian) - Binary serialization format
