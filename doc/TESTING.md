# Testing Guide for Datacamp

This document describes the test suite for the Datacamp library.

## Test Overview

The test suite covers:
- ✅ Directory-based backup/restore operations
- ✅ S3 backup/restore plumbing (unit-level; integration covered via examples)
- ✅ Live migration workflows (routing, cutover, recovery)
- ✅ PostgreSQL backend integration
- ✅ MySQL backend integration
- ⚠️ Redis backend integration (temporarily skipped due to an upstream konserve-redis issue; tests will detect and skip)
- ✅ End-to-end integration workflows

## Running Tests

### Quick Start with Docker Compose

The easiest way to run all tests is using Docker Compose with Babashka tasks:

```bash
# Start databases, run all tests, stop databases
bb test:with-docker
```

This single command handles the complete workflow.

### Individual Test Commands

```bash
# Quick tests (no external dependencies)
bb test:quick

# All tests
bb test:all

# Code coverage analysis
bb coverage          # Runs all tests with coverage report

# Specific test suites
bb test:directory    # Directory tests only
bb test:postgres     # PostgreSQL tests only
bb test:mysql        # MySQL tests only
bb test:redis        # Redis tests only
bb test:backend      # All backend tests (Postgres, MySQL, Redis)
bb test:s3           # S3 (MinIO) integration tests
bb test:integration  # Integration tests only
bb test:migration    # Live migration tests (basic + complex scenarios)
```

### Manual Test Execution

You can also run tests directly with Leiningen:

```bash
# Quick tests (no external dependencies)
lein test datacamp.directory-test datacamp.integration-test

# All tests
lein test

# Specific test namespace
lein test datacamp.postgres-test
```

## Test Suites

### 1. Directory Tests (`datacamp.directory-test`)

Tests for local directory backup operations.

**Coverage:**
- Basic backup to directory
- Backup with different Datahike backends (memory, file)
- List/verify/cleanup operations
- Custom chunk sizes
- Empty databases
- Large values
- Concurrent backups
- Performance benchmarks

**No external dependencies required.**

### 2. PostgreSQL Tests (`datacamp.postgres-test`)

Tests for PostgreSQL backend integration.

**Prerequisites:**
- Running PostgreSQL instance
- Database created for testing

**Docker Compose Setup (Recommended):**
```bash
# Start PostgreSQL (and other databases)
bb docker:start

# Check status
bb docker:status

# Run tests
bb test:postgres

# Stop when done
bb docker:stop
```

The Docker Compose configuration includes:
- PostgreSQL 15 Alpine on port 5432
- Default credentials: postgres/postgres
- Database: datahike_test
- Automatic initialization with required permissions

**Manual Environment Variables:**
```bash
export POSTGRES_HOST=localhost      # default
export POSTGRES_PORT=5432          # default
export POSTGRES_USER=postgres      # default
export POSTGRES_PASSWORD=postgres  # default
export POSTGRES_DB=datahike_test  # default
```

**Coverage:**
- Basic Postgres backup
- Large datasets
- Transaction consistency
- History preservation
- Connection pooling
- Backup/restore roundtrip

### 3. MySQL Tests (`datacamp.mysql-test`)

Tests for MySQL backend integration.

**Prerequisites:**
- Running MySQL instance
- Database created for testing

**Docker Compose Setup (Recommended):**
```bash
# Start MySQL (and other databases)
bb docker:start

# Check status
bb docker:status

# Run tests
bb test:mysql

# Stop when done
bb docker:stop
```

The Docker Compose configuration includes:
- MySQL 8.0 on port 3306
- Default credentials: root/rootpassword
- Database: datahike_test
- UTF-8 character set configuration
- Automatic initialization with required permissions

**Manual Environment Variables:**
```bash
export MYSQL_HOST=localhost     # default
export MYSQL_PORT=3306         # default
export MYSQL_USER=root         # default
export MYSQL_PASSWORD=rootpassword # default
export MYSQL_DB=datahike_test # default
```

**Coverage:**
- Basic MySQL backup
- Large datasets
- UTF-8 character support
- Transaction handling
- Multiple databases
- Performance benchmarks

### 4. Redis Tests (`datacamp.redis-test`)

Tests for Redis backend integration.

Note: datahike-redis currently depends on konserve-redis with a known issue. The Redis tests in this repo detect the condition and skip, printing an explanatory message. Once the upstream fix is available, remove the skip and run as below.

**Prerequisites (when enabled):**
- Running Redis instance

**Docker Compose Setup (Recommended):**
```bash
# Start Redis (and other databases)
bb docker:start

# Check status
bb docker:status

# Run tests
bb test:redis

# Stop when done
bb docker:stop
```

The Docker Compose configuration includes:
- Redis 7 Alpine on port 6379
- No password (local development)
- Persistent volume for data
- Health checks

**Manual Environment Variables:**
```bash
export REDIS_HOST=localhost # default
export REDIS_PORT=6379     # default
export REDIS_PASSWORD=     # optional
```

**Coverage (when enabled):**
- Basic Redis backup
- Memory-efficient streaming
- Fast operations
- Data persistence
- Concurrent access
- Key expiration handling
- Various data types

### 5. Live Migration Tests (`datacamp.migration-test`)

Exercises live migration core scenarios using memory/file backends (no external services required):

**Coverage:**
- Basic migration (mem → file) with transaction capture
- Continuous writes during migration and cutover
- Recovery from interruptions and state corruption
- Error injection and retry behavior
- Concurrent migration blocking

**Run:**
```bash
bb test:migration
# Or only the complex 120k-entity scenario
bb test:migration:complex
```

### 7. Integration Tests (`datacamp.integration-test`)

End-to-end workflow tests.

**Coverage:**
- Complete backup/verify workflows
- Multiple backends to same directory
- Backup rotation
- Comprehensive verification
- Error recovery
- Concurrent operations
- Large datasets

**No external dependencies required** (uses memory backend).

## Test Helpers

The test suite includes comprehensive helpers in `datacamp.test-helpers`:

### Database Helpers
- `create-test-db` - Create a test database
- `populate-test-db` - Add sample data
- `cleanup-test-db` - Clean up test databases
- `generate-test-users` - Generate user data
- `generate-test-posts` - Generate post data

### Directory Helpers
- `temp-test-dir` - Create temporary directory
- `cleanup-test-dir` - Clean up test directory
- `verify-backup-structure` - Check backup structure
- `count-backup-chunks` - Count chunks in backup
- `backup-size` - Get backup size

### Assertion Helpers
- `assert-backup-successful` - Verify backup succeeded
- `assert-backup-valid` - Verify backup structure
- `assert-dbs-equivalent` - Compare two databases

### Test Macros
- `with-test-db` - Run test with database cleanup
- `with-test-dir` - Run test with directory cleanup
- `with-test-backup` - Run test with backup cleanup

### Performance Helpers
- `measure-time` - Measure execution time
- `assert-performance` - Assert performance requirements

## Writing New Tests

### Basic Test Template

```clojure
(ns my-new-test
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datacamp.core :as backup]
            [datacamp.test-helpers :refer :all]))

(deftest my-test
  (testing "My test description"
    (with-test-dir test-dir
      (with-test-db config {:store {:backend :mem :id "test"}}
        (let [conn (d/connect config)
              _ (populate-test-db conn :user-count 10)
              result (backup/backup-to-directory conn {:path test-dir}
                                                :database-id "test")]
          (assert-backup-successful result)
          (assert-backup-valid (:path result)))))))
```

### Testing Best Practices

1. **Use test helpers** - Don't repeat setup/teardown code
2. **Clean up resources** - Use `with-*` macros for automatic cleanup
3. **Test isolation** - Each test should be independent
4. **Clear naming** - Test names should describe what they test
5. **Meaningful assertions** - Include descriptive error messages
6. **Performance tests** - Use `assert-performance` for timing checks

## Docker Management

### Managing Test Databases

```bash
# Start all test databases
bb docker:start

# Stop all test databases
bb docker:stop

# Restart databases
bb docker:restart

# View database logs
bb docker:logs

# Check database status and health
bb docker:status

# Reset databases (stop, remove volumes, start fresh)
bb docker:reset
```

### Optional Admin Tools

You can start optional admin interfaces for database management:

```bash
# Start with all admin tools
bb docker:tools

# Access tools at:
# - pgAdmin: http://localhost:5050 (admin@admin.com / admin)
# - phpMyAdmin: http://localhost:8080 (root / rootpassword)
# - Redis Commander: http://localhost:8081
```

## Continuous Integration

Datacamp uses GitHub Actions for continuous integration and testing. The CI pipeline automatically:

- Runs on every push to `main`, `master`, and `migration` branches
- Runs on all pull requests
- Tests against PostgreSQL, MySQL, Redis, and MinIO services
- Generates code coverage reports
- Uploads coverage to Codecov

### Build Status

[![Tests](https://github.com/alekcz/datacamp/actions/workflows/test.yml/badge.svg)](https://github.com/alekcz/datacamp/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/alekcz/datacamp/branch/main/graph/badge.svg)](https://codecov.io/gh/alekcz/datacamp)

### Workflow Configuration

The CI workflow is defined in [`.github/workflows/test.yml`](../.github/workflows/test.yml) and includes:

1. **Matrix Testing**: Tests run against Java 11, 17, and 21 for broad compatibility
2. **Service Setup**: PostgreSQL, MySQL, Redis, and MinIO containers
3. **Dependency Caching**: Maven repository caching for faster builds
4. **Test Execution**: Full test suite with all backends on each Java version
5. **Coverage Generation**: Cloverage report (Java 17 only) with Codecov integration
6. **Artifact Upload**: Test results for each Java version and coverage reports

**Java Compatibility**: The matrix strategy ensures Datacamp works on Java 11+ (LTS versions 11, 17, and 21).

### Running Locally Like CI

To replicate the CI environment locally:

```bash
# Start all services (matches CI setup)
bb docker:start

# Run tests
lein test

# Generate coverage
lein cloverage --codecov

# Stop services
bb docker:stop
```

## Test Coverage

Current test coverage:

- **Directory operations**: ~95%
- **Backup workflows**: ~90%
- **Backend integration**: ~85%
- **Error handling**: ~80%

## Troubleshooting

### Tests Failing to Connect to Services

First, check that services are running and healthy:

```bash
bb docker:status
```

If services aren't running, start them:

```bash
bb docker:start
```

View logs for specific issues:

```bash
bb docker:logs
```

### Manual Connection Testing

Test database connections directly:

```bash
# PostgreSQL
docker exec -it datacamp-postgres psql -U postgres -d datahike_test -c "SELECT 1;"

# MySQL
docker exec -it datacamp-mysql mysql -u root -prootpassword -D datahike_test -e "SELECT 1;"

# Redis
docker exec -it datacamp-redis redis-cli ping
```

### Port Conflicts

If you get "port already in use" errors:

```bash
# Check what's using the ports
lsof -i :5432  # PostgreSQL
lsof -i :3306  # MySQL
lsof -i :6379  # Redis

# Stop conflicting services or change ports in docker-compose.yml
```

### Database Reset

If databases are in a bad state:

```bash
# Complete reset (removes all data)
bb docker:reset
```

### Timeout Issues

Increase test timeouts in `project.clj`:

```clojure
:test-selectors {:default (complement :slow)
                 :slow :slow
                 :all (constantly true)}
```

### Docker Issues

```bash
# Check Docker is running
docker info

# Clean up Docker resources if needed
docker system prune

# Rebuild containers from scratch
bb docker:stop
docker-compose down -v
bb docker:start
```

## Performance Benchmarks

Expected performance on standard hardware:

| Operation | Dataset | Expected Time |
|-----------|---------|---------------|
| Directory backup | 100 users, 500 posts | < 10s |
| Postgres backup | 100 users, 400 posts | < 15s |
| MySQL backup | 100 users, 400 posts | < 15s |
| Redis backup | 50 users, 150 posts | < 5s |

## Contributing

When adding new features:

1. Write tests first (TDD)
2. Ensure tests pass locally
3. Run full test suite before PR
4. Update this document if adding new test categories

## Support

For test-related issues:
- Check test output for specific errors
- Verify external services are running
- Review environment variables
- Check [GitHub Issues](https://github.com/alekcz/datacamp/issues)
### 6. S3 (MinIO) Tests (`datacamp.s3-integration-test`)

Tests end-to-end backup/restore against a local MinIO instance.

**Prerequisites:**
- MinIO running (Docker Compose service `minio`)
- Credentials (defaults in tests): `minioadmin/minioadmin`

**Run:**
```bash
bb test:s3
```

The test will:
- Create a mem-backed Datahike database and populate it
- Back up to S3 using the MinIO endpoint `http://localhost:9000`
- Restore into a fresh database
- Verify the source and target databases have equivalent datoms
