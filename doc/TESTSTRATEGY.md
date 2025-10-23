# Datacamp Test Strategy

This document explains the comprehensive testing strategy for Datacamp, detailing what tests exist, why they exist, what they verify, and how they ensure every feature works correctly. This guide will help maintainers understand the test coverage, add new tests appropriately, and debug test failures.

## Table of Contents

1. [Testing Philosophy](#testing-philosophy)
2. [Test Architecture Overview](#test-architecture-overview)
3. [Test Coverage by Component](#test-coverage-by-component)
4. [Test Types and Their Purposes](#test-types-and-their-purposes)
5. [Test Infrastructure](#test-infrastructure)
6. [Running the Tests](#running-the-tests)
7. [Writing New Tests](#writing-new-tests)
8. [Test Patterns and Best Practices](#test-patterns-and-best-practices)
9. [Debugging Test Failures](#debugging-test-failures)
10. [Continuous Integration](#continuous-integration)

---

## Testing Philosophy

### Core Principles

1. **Comprehensive Coverage**: Every public API function has tests
2. **Real Dependencies**: Test against real PostgreSQL, MySQL, Redis, S3 (via LocalStack)
3. **Property-Based Testing**: Use generative testing where appropriate (test.check)
4. **Isolation**: Each test is independent and can run in any order
5. **Fast Feedback**: Quick tests run without external dependencies, comprehensive tests use Docker
6. **Realistic Scenarios**: Test with production-like data volumes and error conditions

### Testing Pyramid

```
        ┌─────────────┐
        │   End-to-   │  ← Integration tests (datacamp.integration-test)
        │     End     │     Full workflows, multiple components
        ├─────────────┤
        │ Integration │  ← Backend tests (postgres, mysql, redis, s3)
        │    Tests    │     Real databases, real S3
        ├─────────────┤
        │    Unit     │  ← Component tests (directory, serialization, etc.)
        │   Tests     │     Fast, no external dependencies
        └─────────────┘
```

**Distribution**:
- Unit tests: ~40% (fast, no dependencies)
- Integration tests: ~50% (backend-specific)
- End-to-end tests: ~10% (full workflows)

### What We Test

1. **Correctness**: Does the function do what it's supposed to?
2. **Edge Cases**: Empty databases, single datoms, huge databases
3. **Error Handling**: Network failures, S3 throttling, corrupt data
4. **Performance**: Acceptable throughput, constant memory usage
5. **Idempotency**: Can operations be safely retried?
6. **Concurrent Safety**: Multiple operations at once
7. **Data Integrity**: Checksums, transaction ordering, schema preservation

### What We Don't Test

1. **Implementation Details**: We test behavior, not private functions
2. **Third-Party Libraries**: We trust Datahike, AWS SDK, etc.
3. **User Interface**: Datacamp is a library, no UI to test

---

## Test Architecture Overview

### Test Namespaces

```
test/datacamp/
├── test_helpers.clj          - Shared test utilities and macros
├── directory_test.clj         - Local directory backup/restore tests
├── postgres_test.clj          - PostgreSQL backend integration tests
├── mysql_test.clj             - MySQL backend integration tests
├── redis_test.clj             - Redis backend integration tests (skipped due to upstream bug)
├── s3_test.clj                - S3 (LocalStack) integration tests
├── migration_test.clj         - Live migration tests (all scenarios)
├── complex_test.clj           - Complex schema and large dataset tests
├── schema_evolution_test.clj  - Schema evolution and compatibility tests
└── integration_test.clj       - End-to-end workflow tests
```

### Test Dependencies

**External Services** (managed by Docker Compose):
- PostgreSQL 15 (port 5432)
- MySQL 8.0 (port 3306)
- Redis 7 (port 6379)
- LocalStack S3 (port 4566)

**Test Data**:
- Small: 10-100 entities (fast tests)
- Medium: 1,000-10,000 entities (realistic tests)
- Large: 20,000-120,000 entities (stress tests)

### Test Execution Modes

1. **Quick Tests** (`bb test:quick`): No external dependencies, runs in ~10s
2. **Backend Tests** (`bb test:postgres`, etc.): Specific backend, runs in ~30s each
3. **Full Suite** (`bb test:all`): Everything, runs in ~5 minutes
4. **Coverage** (`bb coverage`): Full suite with code coverage analysis

---

## Test Coverage by Component

### 1. Directory Tests (`directory_test.clj`)

**Purpose**: Verify local filesystem backup/restore operations.

**Coverage**: 72 assertions across 11 tests

**Tests**:

#### `test-directory-basic-backup`
- **What**: Basic backup to local directory
- **Why**: Core functionality verification
- **Verifies**: Backup succeeds, manifest created, chunks written, datoms match

#### `test-directory-backup-with-file-backend`
- **What**: Backup from file-based Datahike backend
- **Why**: Ensure compatibility with different backends
- **Verifies**: File backend → directory backup works

#### `test-directory-list-backups`
- **What**: List all backups in directory
- **Why**: Users need to discover available backups
- **Verifies**: Correct backup list, metadata accuracy

#### `test-directory-verify-backup`
- **What**: Verify backup integrity
- **Why**: Detect corrupt or incomplete backups
- **Verifies**: All chunks present, checksums match

#### `test-directory-cleanup-incomplete`
- **What**: Clean up incomplete backups older than threshold
- **Why**: Prevent accumulation of failed backups
- **Verifies**: Old incomplete backups deleted, complete backups preserved

#### `test-directory-backup-restore-roundtrip`
- **What**: Backup then restore, compare databases
- **Why**: Ensure data fidelity through full cycle
- **Verifies**: Source and target databases identical

#### `test-directory-custom-chunk-size`
- **What**: Backup with different chunk sizes (32MB, 128MB)
- **Why**: Users may tune performance
- **Verifies**: Custom chunk sizes work correctly

#### `test-directory-empty-database`
- **What**: Backup empty database (0 datoms)
- **Why**: Edge case that must work
- **Verifies**: Empty backup succeeds, restore creates empty database

#### `test-directory-large-values`
- **What**: Backup datoms with large string values
- **Why**: Ensure serialization handles large data
- **Verifies**: Large values preserved correctly

#### `test-directory-concurrent-backups`
- **What**: Multiple simultaneous backups to same directory
- **Why**: Prevent conflicts in production
- **Verifies**: Concurrent backups don't corrupt each other

#### `test-directory-performance-benchmark`
- **What**: Measure backup throughput
- **Why**: Ensure acceptable performance
- **Verifies**: > 1 MB/s throughput, < 2s for 1000 entities

**Key Assertions**:
```clojure
(is (:success result))                    ; Backup succeeded
(is (pos? (:datom-count result)))         ; Has datoms
(is (.exists (io/file manifest-path)))    ; Manifest exists
(is (= source-count target-count))        ; Datom counts match
```

### 2. PostgreSQL Tests (`postgres_test.clj`)

**Purpose**: Verify PostgreSQL backend integration.

**Setup**: Docker container with `datahike_test` database.

**Tests**:

#### `test-postgres-basic-backup`
- **What**: Backup PostgreSQL-backed Datahike database
- **Why**: Most common production backend
- **Verifies**: JDBC connection works, backup succeeds

#### `test-postgres-backup-restore-roundtrip`
- **What**: Full backup → restore cycle with PostgreSQL
- **Why**: Ensure data fidelity with JDBC backend
- **Verifies**: Source and target databases identical

#### `test-postgres-large-dataset`
- **What**: Backup 100 users, 400 posts (~5000 datoms)
- **Why**: Realistic production data volume
- **Verifies**: Large dataset backs up, performance acceptable

#### `test-postgres-transactions-during-backup`
- **What**: Write transactions while backup is in progress
- **Why**: Ensure backup doesn't lock database
- **Verifies**: Concurrent writes succeed, backup captures consistent snapshot

#### `test-postgres-history-queries`
- **What**: Backup/restore database with history enabled
- **Why**: History is common feature
- **Verifies**: Historical queries work after restore

#### `test-postgres-connection-pool`
- **What**: Backup using connection pool
- **Why**: Production uses connection pooling
- **Verifies**: Pooled connections work correctly

**Key Insight**: These tests verify that Datacamp works with Datahike's JDBC backend, which has different performance characteristics than memory/file backends.

### 3. MySQL Tests (`mysql_test.clj`)

**Purpose**: Verify MySQL backend integration.

**Setup**: Docker container with `datahike_test` database.

**Tests**:

#### `test-mysql-basic-backup`
- **What**: Backup MySQL-backed Datahike database
- **Why**: MySQL is alternative to PostgreSQL
- **Verifies**: MySQL JDBC works, backup succeeds

#### `test-mysql-utf8-support`
- **What**: Backup data with UTF-8 characters (emoji, non-ASCII)
- **Why**: MySQL UTF-8 handling can be tricky
- **Verifies**: Unicode data preserved correctly

#### `test-mysql-large-dataset`
- **What**: Backup 100 users, 400 posts
- **Why**: Performance characteristics different from PostgreSQL
- **Verifies**: Large MySQL datasets work

#### `test-mysql-transactions`
- **What**: Concurrent transactions during backup
- **Why**: MySQL transaction isolation
- **Verifies**: ACID properties maintained

#### `test-mysql-performance`
- **What**: Measure MySQL backup throughput
- **Why**: Compare to PostgreSQL performance
- **Verifies**: Acceptable throughput

**Key Insight**: MySQL tests ensure cross-database compatibility and catch MySQL-specific issues (UTF-8 encoding, transaction isolation).

### 4. Redis Tests (`redis_test.clj`)

**Status**: Currently skipped due to upstream `konserve-redis` bug.

**Purpose**: Verify Redis backend integration.

**Future Tests** (when enabled):
- Basic backup/restore
- Performance characteristics (Redis is fast)
- Key expiration handling
- Memory pressure scenarios

**Skip Logic**:
```clojure
(when-not (redis-available?)
  (println "Redis not available: konserve-redis has a bug")
  (is true)) ; Pass test but skip execution
```

### 5. S3 Tests (`s3_test.clj`)

**Purpose**: Verify S3 backup/restore against real S3-compatible storage.

**Setup**: LocalStack container (local S3-compatible service).

**Tests**:

#### `test-s3-backup-and-restore-roundtrip`
- **What**: Full S3 backup → restore cycle
- **Why**: Core S3 functionality
- **Verifies**:
  - S3 upload succeeds
  - Manifest written to S3
  - Chunks uploaded
  - Restore from S3 works
  - Data fidelity maintained

**Configuration**:
```clojure
{:bucket "test-bucket"
 :region "us-east-1"
 :endpoint "http://localhost:4566"
 :path-style-access? true}
```

**Key Insight**: Using LocalStack allows testing real S3 operations without AWS costs or network dependency.

### 6. Migration Tests (`migration_test.clj`)

**Purpose**: Verify live migration with zero downtime.

**Tests**:

#### `test-basic-migration`
- **What**: Migrate memory → file backend
- **Why**: Simplest migration scenario
- **Verifies**:
  - Source remains operational
  - Backup created
  - Target populated
  - Transaction capture works
  - Router routes transactions
  - Finalization succeeds

#### `test-migration-with-continuous-writes`
- **What**: Write transactions during entire migration
- **Why**: Realistic production scenario
- **Verifies**:
  - Writes don't block
  - All transactions captured
  - All transactions replayed
  - Target has all data

#### `test-complex-migration-with-continuous-writes`
- **What**: Migrate 20k initial entities + 100k written during migration
- **Why**: Stress test with large volumes
- **Verifies**:
  - Large migrations complete
  - Memory stays constant
  - Performance acceptable
  - Data integrity maintained

**Timing**:
```clojure
;; Create 20,000 initial entities
;; During migration: Write 10,000 entities/second for 10 seconds
;; Total: 121,000+ entities migrated
;; Time: < 60 seconds
```

#### `test-migration-error-handling`
- **What**: Inject failures during migration
- **Why**: Test resilience
- **Failure Scenarios**:
  - Network interruption during backup
  - Disk full during restore
  - Process crash during transaction capture
  - State corruption
- **Verifies**:
  - Recovery from interruptions
  - Transaction log preserved
  - Can resume from checkpoint
  - No data loss

#### `test-concurrent-migration-blocking`
- **What**: Try to start second migration while first is active
- **Why**: Prevent conflicts
- **Verifies**: Second migration rejected with clear error

**Key Insight**: Migration tests are the most complex, testing the full state machine and transaction capture/replay mechanism.

### 7. Complex Tests (`complex_test.clj`)

**Purpose**: Test complex schemas and large datasets.

**Schema**:
```clojure
{:person/name {:db/cardinality :db.cardinality/one}
 :person/emails {:db/cardinality :db.cardinality/many}  ; Multi-valued
 :person/company {:db/valueType :db.type/ref}           ; Reference
 :person/manager {:db/valueType :db.type/ref}           ; Self-reference
 :company/addresses {:db/cardinality :db.cardinality/many
                     :db/valueType :db.type/ref}        ; Many refs
 :order/line-items {:db/cardinality :db.cardinality/many
                    :db/isComponent true}}               ; Components
```

**Tests**:

#### `test-complex-schema-backup-restore`
- **What**: Backup/restore with complex schema (20k entities, 140k datoms)
- **Why**: Production schemas are complex
- **Verifies**:
  - Multi-valued attributes preserved
  - References preserved
  - Self-references work
  - Components preserved
  - Entity integrity maintained

**Entity Integrity Checks**:
```clojure
;; Verify companies exist
(count (d/q '[:find ?c :where [?c :company/name]] @target-conn))

;; Verify manager relationships
(count (d/q '[:find ?p :where [?p :person/manager ?m]] @target-conn))

;; Verify order line items
(d/q '[:find ?o (count ?li) :where [?o :order/id] [?o :order/line-items ?li]]
     @target-conn)

;; Verify multi-valued emails
(d/q '[:find ?n (count ?e) :where [?p :person/name ?n] [?p :person/emails ?e]]
     @target-conn)
```

#### `test-complex-schema-s3-backup-restore`
- **What**: Same as above but with S3 storage
- **Why**: Ensure S3 path works with complex schemas
- **Verifies**: Complex schemas work with S3

**Key Insight**: These tests catch issues with entity ID remapping, reference preservation, and cardinality handling.

### 8. Schema Evolution Tests (`schema_evolution_test.clj`)

**Purpose**: Verify backup/restore across schema changes.

**Tests**:

#### `test-schema-evolution-backup-restore`
- **What**: Create DB with schema v1, add data, evolve schema to v2, backup, restore
- **Why**: Schemas evolve in production
- **Schema Changes Tested**:
  - Add new attributes
  - Change cardinality (one → many)
  - Add unique constraints
  - Remove unique constraints
- **Verifies**:
  - Schema preserved in backup
  - Evolved schema works after restore
  - Data compatible with new schema
  - Constraints enforced

**Example Evolution**:
```clojure
;; V1: Single email
{:user/email {:db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity}}

;; V2: Multiple emails
{:user/email {:db/cardinality :db.cardinality/many
              :db/unique :db.unique/identity}}
```

**Key Insight**: Tests ensure Datacamp doesn't break during schema evolution, a common production scenario.

### 9. Integration Tests (`integration_test.clj`)

**Purpose**: End-to-end workflows and multi-component tests.

**Tests**:

#### `test-complete-backup-verify-workflow`
- **What**: Backup → verify → restore → verify cycle
- **Why**: Typical production workflow
- **Verifies**: All operations in sequence work

#### `test-multiple-backups-same-directory`
- **What**: Create multiple backups of same database
- **Why**: Backup rotation use case
- **Verifies**: Multiple backups don't interfere

#### `test-backup-rotation`
- **What**: Keep last N backups, delete older
- **Why**: Disk space management
- **Verifies**: Cleanup preserves recent backups

#### `test-concurrent-backup-and-restore`
- **What**: Backup DB1 while restoring DB2
- **Why**: Production may do both simultaneously
- **Verifies**: No conflicts, both succeed

#### `test-large-database-end-to-end`
- **What**: Backup/restore 10k entities
- **Why**: Realistic production size
- **Verifies**: Performance, memory usage, correctness

**Key Insight**: Integration tests ensure components work together correctly, catching integration bugs that unit tests miss.

---

## Test Types and Their Purposes

### 1. Correctness Tests

**Purpose**: Verify operations produce correct results.

**Examples**:
- Backup then restore produces identical database
- List backups returns all backups
- Verify backup reports correct chunk count

**Pattern**:
```clojure
(let [result (operation input)]
  (is (= expected-output result)))
```

### 2. Edge Case Tests

**Purpose**: Verify behavior at boundaries.

**Examples**:
- Empty database (0 datoms)
- Single datom database
- Database with only schema (no data)
- Huge values (multi-MB strings)

**Pattern**:
```clojure
(testing "edge case: empty database"
  (let [result (backup-empty-db)]
    (is (:success result))
    (is (zero? (:datom-count result)))))
```

### 3. Error Handling Tests

**Purpose**: Verify graceful failure and recovery.

**Examples**:
- S3 bucket doesn't exist → clear error
- Network timeout → retry with backoff
- Corrupt chunk → detect and report
- Disk full → fail with clear message

**Pattern**:
```clojure
(testing "handles missing bucket"
  (is (thrown-with-msg? Exception #"bucket does not exist"
        (backup-to-s3 conn {:bucket "nonexistent"}))))
```

### 4. Performance Tests

**Purpose**: Verify acceptable throughput and resource usage.

**Examples**:
- Backup throughput > 1 MB/s
- Memory usage < 512MB
- 1000 entities backup < 10 seconds

**Pattern**:
```clojure
(let [start (System/currentTimeMillis)
      result (backup-large-db)
      duration (- (System/currentTimeMillis) start)]
  (is (< duration 10000) "Should complete in < 10s"))
```

### 5. Idempotency Tests

**Purpose**: Verify operations can be safely retried.

**Examples**:
- Upload same chunk twice → second succeeds, no corruption
- Restore twice to same database → second fails cleanly
- Resume backup → skips completed chunks

**Pattern**:
```clojure
(let [result1 (operation)
      result2 (operation)]  ; Retry
  (is (= result1 result2)))  ; Idempotent
```

### 6. Concurrency Tests

**Purpose**: Verify thread-safety.

**Examples**:
- Multiple threads backing up different databases
- Backup one DB while restoring another
- Multiple migrations (should block)

**Pattern**:
```clojure
(let [futures (doall (repeatedly 4 #(future (operation))))]
  (doseq [f futures]
    (is (:success @f))))  ; All succeed
```

### 7. Property-Based Tests

**Purpose**: Verify properties hold for generated inputs.

**Examples**:
- For any database: backup then restore = identity
- For any chunk size: backup succeeds
- For any transaction sequence: capture then replay = original

**Pattern** (using test.check):
```clojure
(prop/for-all [db (gen/database)]
  (= db (restore (backup db))))
```

---

## Test Infrastructure

### Test Helpers (`test_helpers.clj`)

#### Database Helpers

**`create-test-db [config]`**
- Creates temporary Datahike database
- Returns connection
- Automatically cleaned up after test

**`populate-test-db [conn & opts]`**
- Populates database with generated data
- Options: `:user-count`, `:post-count`, `:comment-count`
- Uses realistic data (names, emails, content)

**`cleanup-test-db [conn]`**
- Closes connection
- Deletes database files
- Called automatically by `with-test-db` macro

#### Directory Helpers

**`temp-test-dir []`**
- Creates temporary directory for tests
- Returns path
- Automatically cleaned up after test

**`cleanup-test-dir [dir]`**
- Recursively deletes directory
- Safe (only deletes in `/tmp` or `/var/folders`)

**`verify-backup-structure [path backup-id]`**
- Checks manifest.edn exists
- Checks chunks/ directory exists
- Checks complete.marker exists
- Returns true/false

#### Assertion Helpers

**`assert-backup-successful [result]`**
- Asserts `:success` is true
- Asserts `:backup-id` exists
- Asserts `:datom-count` is positive
- Provides clear error message if fails

**`assert-dbs-equivalent [conn1 conn2]`**
- Compares datom counts
- Compares all datoms (ignoring transaction timestamps)
- Checks entity IDs are remapped correctly
- Detailed diff on failure

#### Test Macros

**`with-test-db`**
```clojure
(with-test-db config
  ;; Use conn here
  (backup conn))
;; conn closed, DB cleaned up automatically
```

**`with-test-dir`**
```clojure
(with-test-dir dir
  ;; Use dir here
  (backup-to-directory conn {:path dir}))
;; Directory deleted automatically
```

**`with-test-backup`**
```clojure
(with-test-backup [conn dir backup-id]
  ;; Backup already created
  (verify-backup backup-id))
;; Backup cleaned up automatically
```

### Docker Compose Configuration

**Services**:
- **PostgreSQL**: `datacamp-postgres` on port 5432
- **MySQL**: `datacamp-mysql` on port 3306
- **Redis**: `datacamp-redis` on port 6379
- **LocalStack**: `datacamp-localstack` on port 4566

**Health Checks**: All services have health checks to ensure readiness.

**Volumes**: Persistent volumes for data (cleaned by `bb docker:reset`).

**Networks**: Isolated network `datacamp-network`.

### Babashka Tasks for Test Execution

**Smart Service Management**:
```clojure
;; Tasks check if services are running
;; Start only what's needed
;; Stop only what was started
;; Example: test:postgres
(let [was-running (= "true\n" (shell "docker inspect -f '{{.State.Running}}' datacamp-postgres"))]
  (when (not was-running)
    (shell "docker-compose up -d postgres")
    (Thread/sleep 5000))
  (shell "lein test datacamp.postgres-test")
  (when (not was-running)
    (shell "docker-compose stop postgres")))
```

This ensures:
- Don't stop services user manually started
- Don't accumulate running services
- Fast execution (skip starting if already running)

---

## Running the Tests

### Quick Tests (No Dependencies)

```bash
bb test:quick
# Runs: directory-test, integration-test (memory backend)
# Duration: ~10 seconds
# Use: Rapid feedback during development
```

### Backend-Specific Tests

```bash
bb test:postgres   # PostgreSQL tests (~30s)
bb test:mysql      # MySQL tests (~30s)
bb test:redis      # Redis tests (~10s, currently skipped)
bb test:s3         # S3/LocalStack tests (~30s)
bb test:backend    # All backend tests (~2min)
```

### Migration Tests

```bash
bb test:migration                # All migration tests (~2min)
bb test:migration:complex        # Only complex 120k entity test (~1min)
bb test:migration:errors         # Only error handling tests (~30s)
```

### Full Test Suite

```bash
bb test:all        # All tests, smart service management (~5min)
bb test            # Start Docker, run all, stop Docker (~6min)
```

### Code Coverage

```bash
bb coverage
# Runs all tests with cloverage
# Generates target/coverage/index.html
# Duration: ~6 minutes
```

**Current Coverage** (as of latest run):
```
|------------------------+---------+---------|
|              Namespace | % Forms | % Lines |
|------------------------+---------+---------|
|   datacamp.compression |   70.73 |   75.00 |
|          datacamp.core |   61.52 |   70.48 |
|     datacamp.directory |   77.54 |   87.50 |
|      datacamp.metadata |   75.23 |   84.34 |
|     datacamp.migration |   45.54 |   65.86 |
|            datacamp.s3 |   38.78 |   50.77 |
| datacamp.serialization |   65.03 |   73.17 |
|         datacamp.utils |   26.05 |   45.12 |
|------------------------+---------+---------|
|              ALL FILES |   53.28 |   67.86 |
|------------------------+---------+---------|
```

**Coverage Goals**:
- Core backup/restore paths: > 80%
- Error handling paths: > 60%
- Utility functions: > 50%

### Manual Test Execution

```bash
# Specific test namespace
lein test datacamp.postgres-test

# Specific test function
lein test :only datacamp.postgres-test/test-postgres-basic-backup

# All tests
lein test
```

---

## Writing New Tests

### Test Template

```clojure
(ns datacamp.my-new-test
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.test-helpers :refer :all]))

(deftest test-my-feature
  (testing "My feature description"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 10)
              result (backup/backup-to-directory conn {:path test-dir}
                                                :database-id "test")]
          ;; Assertions
          (is (:success result) "Backup should succeed")
          (is (pos? (:datom-count result)) "Should have datoms")
          (assert-backup-valid test-dir (:backup-id result)))))))
```

### When to Add Tests

**New Feature**: Add tests for all public API functions.

**Bug Fix**: Add regression test that reproduces the bug, then fix it.

**Edge Case Discovered**: Add test for the edge case.

**Performance Requirement**: Add performance test with threshold.

### Test Naming Conventions

```clojure
;; Pattern: test-<component>-<operation>-<scenario>

;; Good
(deftest test-directory-backup-empty-database ...)
(deftest test-postgres-restore-large-dataset ...)
(deftest test-migration-error-during-backup ...)

;; Bad (not descriptive)
(deftest test-1 ...)
(deftest test-backup ...)
(deftest test-stuff ...)
```

### Assertion Best Practices

```clojure
;; ✅ GOOD: Clear message
(is (= expected actual) "Expected X, got Y")

;; ✅ GOOD: Test one thing
(is (:success result))
(is (= 100 (:datom-count result)))

;; ❌ BAD: No message
(is (= expected actual))

;; ❌ BAD: Testing too much at once
(is (and (:success result)
         (pos? (:datom-count result))
         (.exists manifest-file)))
```

### Test Data Generation

```clojure
;; Use test-helpers for realistic data
(populate-test-db conn
  :user-count 100
  :post-count 500
  :comment-count 1000)

;; Custom data for specific test
(d/transact conn [{:user/email "test@example.com"
                   :user/name "Test User"}])
```

---

## Test Patterns and Best Practices

### Pattern 1: Arrange-Act-Assert

```clojure
(deftest test-example
  ;; Arrange: Set up test data
  (let [conn (create-test-db config)
        _ (populate-test-db conn :user-count 10)]

    ;; Act: Perform operation
    (let [result (backup/backup-to-directory conn {:path dir})]

      ;; Assert: Verify result
      (is (:success result))
      (is (= 10 (count-users result))))))
```

### Pattern 2: Test Fixtures

```clojure
(use-fixtures :once
  (fn [f]
    (start-docker-services)
    (f)
    (stop-docker-services)))

(use-fixtures :each
  (fn [f]
    (clean-test-data)
    (f)))
```

### Pattern 3: Exception Testing

```clojure
;; Test that operation throws expected exception
(is (thrown-with-msg? ExceptionInfo #"bucket does not exist"
      (backup-to-s3 conn {:bucket "nonexistent"})))

;; Test exception data
(try
  (backup-to-s3 conn bad-config)
  (is false "Should have thrown exception")
  (catch ExceptionInfo e
    (is (= :bucket-not-found (:type (ex-data e))))))
```

### Pattern 4: Timeout Tests

```clojure
(deftest test-operation-completes-in-time
  (let [start (System/currentTimeMillis)
        result (operation)
        duration (- (System/currentTimeMillis) start)]
    (is (:success result))
    (is (< duration 5000) "Should complete in < 5s")))
```

### Pattern 5: Cleanup Pattern

```clojure
(deftest test-with-cleanup
  (let [resource (create-resource)]
    (try
      ;; Test logic
      (operation resource)
      (finally
        ;; Always cleanup
        (cleanup-resource resource)))))
```

### Pattern 6: Parameterized Tests

```clojure
(deftest test-multiple-chunk-sizes
  (doseq [chunk-size [32 64 128]] ; Test multiple values
    (testing (str "chunk size: " chunk-size "MB")
      (let [result (backup conn {:chunk-size (* chunk-size 1024 1024)})]
        (is (:success result))))))
```

---

## Debugging Test Failures

### Common Failure Scenarios

#### 1. Service Not Running

**Symptom**:
```
Connection refused: localhost:5432
```

**Diagnosis**:
```bash
bb docker:status  # Check if service is running
docker logs datacamp-postgres  # Check logs
```

**Fix**:
```bash
bb docker:restart
```

#### 2. Port Conflict

**Symptom**:
```
Port 5432 already in use
```

**Diagnosis**:
```bash
lsof -i :5432  # See what's using the port
```

**Fix**: Stop conflicting service or change port in `docker-compose.yml`.

#### 3. Data Pollution

**Symptom**: Test passes alone, fails in suite.

**Diagnosis**: Test leaves data that affects next test.

**Fix**: Use `with-test-db` and `with-test-dir` for automatic cleanup.

#### 4. Timing Issues

**Symptom**: Test sometimes passes, sometimes fails.

**Diagnosis**: Race condition, service not ready, etc.

**Fix**:
```clojure
;; Add wait for service to be ready
(Thread/sleep 2000)  ; Wait for service

;; Or better: poll until ready
(loop [attempts 10]
  (when (and (pos? attempts) (not (service-ready?)))
    (Thread/sleep 500)
    (recur (dec attempts))))
```

#### 5. Memory Issues

**Symptom**:
```
OutOfMemoryError: Java heap space
```

**Diagnosis**: Realizing full lazy sequence.

**Fix**: Check for `vec`, `doall`, `count` on datoms.

#### 6. Assertion Failures

**Symptom**:
```
Expected: 100
Actual: 98
```

**Diagnosis**: Add debug logging:
```clojure
(println "DEBUG: result =" result)
(is (= 100 result))
```

**Tools**:
- `clojure.pprint/pprint` - Pretty print data
- `clojure.stacktrace/print-stack-trace` - Full stack trace
- Timbre logging - Check logs in test output

### Test Debugging Checklist

1. Run test in isolation: `lein test :only namespace/test-name`
2. Check Docker services: `bb docker:status`
3. Check Docker logs: `bb docker:logs`
4. Add debug logging: `(println "DEBUG:" value)`
5. Check test helper cleanup: Are resources being cleaned?
6. Check environment variables: Are configs correct?
7. Run with verbose output: `lein test :verbose`

---

## Continuous Integration

Datacamp uses GitHub Actions for automated testing and continuous integration. Every push and pull request triggers a full test suite run with all backend services.

### Current CI Status

[![Tests](https://github.com/alekcz/datacamp/actions/workflows/test.yml/badge.svg)](https://github.com/alekcz/datacamp/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/alekcz/datacamp/branch/master/graph/badge.svg)](https://codecov.io/gh/alekcz/datacamp)

### CI Workflow

The automated CI pipeline ([`.github/workflows/test.yml`](../.github/workflows/test.yml)):

1. **Triggers**: Runs on every push to any branch and all pull requests
2. **Matrix Testing**: Tests against Java 11, 17, and 21 for broad compatibility
3. **Environment**: Ubuntu with Babashka, Leiningen, and Docker Compose
4. **Caching**: Maven dependencies cached for faster builds
5. **Testing**:
   - Java 11 & 21: `bb test:all` (runs tests with smart service management)
   - Java 17: `bb coverage --codecov` (runs tests with coverage reporting)
6. **Service Management**: Babashka tasks automatically start/stop required Docker services
7. **Reporting**: Uploads coverage to Codecov and artifacts
8. **Duration**: ~5-8 minutes for full test suite per Java version

### Key Features

- **Smart Service Management**: `bb` tasks handle Docker service lifecycle automatically
- **Matrix Testing**: Ensures compatibility with Java 11, 17, and 21
- **Zero Configuration**: No manual service setup - `bb` handles everything
- **Artifact Upload**: Coverage reports available for download
- **Coverage Integration**: Automatic Codecov updates with coverage trends
- **Fail Fast**: Matrix strategy allows parallel execution across Java versions

### Replicating CI Locally

To run tests exactly as CI does:

```bash
# Run tests with smart service management (just like CI)
bb test:all

# Or run coverage with smart service management (just like CI for Java 17)
bb coverage --codecov

# Both commands automatically:
# 1. Check which services are already running
# 2. Start only the services that aren't running
# 3. Run tests/coverage
# 4. Stop only the services they started
```

**Even simpler:**
```bash
# Complete workflow (start → test → stop)
bb test
```

### CI Best Practices

1. **Cache Dependencies**: CI caches `~/.m2/repository` for faster builds
2. **Service Readiness**: Always wait for health checks before testing
3. **Fail Fast**: Test failures immediately stop the workflow
4. **Artifacts**: Test results and coverage saved for debugging
5. **Branch Protection**: Require CI to pass before merging PRs
6. **Coverage Tracking**: Monitor coverage trends with Codecov

---

## Test Maintenance

### Regular Tasks

**Weekly**:
- Review test coverage report
- Check for flaky tests (sometimes pass/fail)
- Update test data generators if schema changes

**Monthly**:
- Review test execution time (optimize slow tests)
- Update Docker images to latest versions
- Clean up unused test helpers

**Per Release**:
- Run full test suite on all supported platforms
- Update test documentation if APIs changed
- Add regression tests for fixed bugs

### Adding Tests for Bug Fixes

1. **Reproduce Bug**: Write failing test that demonstrates bug
2. **Fix Bug**: Implement fix
3. **Verify Fix**: Test should now pass
4. **Document**: Add comment explaining what bug was prevented

Example:
```clojure
(deftest test-empty-chunk-handling
  ;; Regression test for issue #42
  ;; Empty chunks were causing NPE during restore
  (testing "handles empty chunks gracefully"
    (let [result (restore-with-empty-chunk)]
      (is (:success result)))))
```

---

## Conclusion

Datacamp's test strategy ensures reliability through:

1. **Comprehensive Coverage**: All public APIs tested
2. **Real Dependencies**: Test against actual databases and S3
3. **Multiple Test Types**: Unit, integration, end-to-end, performance
4. **Realistic Scenarios**: Production-like data volumes and error conditions
5. **Easy Execution**: Babashka tasks make testing frictionless
6. **Good Infrastructure**: Test helpers, Docker Compose, CI

The tests serve as both verification and documentation - they show how the library should be used and what behavior to expect.

When adding features:
1. Write tests first (TDD)
2. Ensure tests cover happy path and edge cases
3. Add error handling tests
4. Verify performance is acceptable
5. Update this document if testing strategy changes

When fixing bugs:
1. Add regression test that fails
2. Fix the bug
3. Verify test passes
4. Document the fix in test comments

**Remember**: Tests are as important as the code they test. Well-tested code is trustworthy code, and trust is essential for production database tooling.

---

**Document Version**: 1.0
**Last Updated**: 2025-01-19
**Maintainer**: Update this document when test strategy evolves
