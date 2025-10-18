# Datacamp Examples

This directory contains examples demonstrating how to use the Datacamp library for backing up Datahike databases.

## Quick Start: Simple Backup Example

The easiest way to get started is with the simple backup example that creates a small book library database and backs it up.

### Running the Simple Example

```bash
# From the project root, run in a REPL:
lein repl < examples/example_backup.clj
```

This example will:
1. Create an in-memory database with a book schema
2. Add 5 books to the database
3. Back up the database to `examples/backups/`
4. Verify the backup
5. Show you the backup details

**Expected Output:**
```
=== Datacamp Simple Backup Example ===

Step 1: Creating in-memory database...
Step 2: Defining schema...
Step 3: Adding books to the database...
Step 4: Querying the database...
  Found 5 books in the database:
    - "Pride and Prejudice" by Jane Austen (1813)
    - "The Great Gatsby" by F. Scott Fitzgerald (1925)
    - "The Hobbit" by J.R.R. Tolkien (1937)
    - "1984" by George Orwell (1949)
    - "To Kill a Mockingbird" by Harper Lee (1960)

Step 5: Creating backup in /tmp/datacamp-example...

✓ Backup completed successfully!
  Backup ID: 20251018-195132-a3f9c2  (UTC timestamp)
  Location: /tmp/datacamp-example/book-library/20251018-195132-a3f9c2
  Datoms backed up: 34
  Chunks created: 1
  Total size: 508 B
  Duration: 0.07 seconds
```

## Other Examples

### Basic Usage (`basic_usage.clj`)

Contains 10 practical examples covering:
- Simple S3 backup
- Backup with custom chunk sizes
- Listing and verifying backups
- Progress monitoring
- Error handling
- Scheduled backups
- Multi-database backups
- Incremental backups
- Backup with metadata

### Advanced Usage (`advanced_usage.clj`)

Contains 7 advanced patterns:
- Custom retry strategies
- Health monitoring
- Backup rotation policies
- Parallel backups
- Backup with hooks
- Custom compression
- Disaster recovery workflows

### Directory Usage (`directory_usage.clj`)

Contains 12 directory-specific examples:
- Local directory backups
- NAS/network drive backups
- Multiple backup locations
- Directory cleanup strategies

## Using Examples in Your REPL

You can also copy-paste specific examples into your REPL session:

```clojure
(require '[datahike.api :as d])
(require '[datacamp.core :as backup])

;; Create a database
(def cfg {:store {:backend :mem :id "my-db"}})
(d/create-database cfg)
(def conn (d/connect cfg))

;; Add some data
(d/transact conn [{:db/ident :person/name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}])
(d/transact conn [{:person/name "Alice"}
                  {:person/name "Bob"}])

;; Backup to local directory
(def result (backup/backup-to-directory
             conn
             {:path "/tmp/my-backup"}
             :database-id "my-db"))

;; Check the result
(println "Backed up" (:datom-count result) "datoms")
(println "Location:" (:path result))
```

## Example Files

| File | Description | Dependencies |
|------|-------------|--------------|
| `example_backup.clj` | Simple runnable example | None (memory backend) |
| `basic_usage.clj` | Basic backup patterns | AWS credentials for S3 |
| `advanced_usage.clj` | Advanced patterns | AWS credentials for S3 |
| `directory_usage.clj` | Local directory backups | None |

## Tips

1. **Start with `example_backup.clj`** - It's the easiest way to see Datacamp in action
2. **Check the backup location** - Look at the files created to understand the backup structure
3. **Modify the examples** - Change the data, schemas, and backup options to fit your needs
4. **Use directory backups for testing** - They don't require AWS credentials

## Need Help?

- Check the main [README.md](../README.md) for full API documentation
- See [QUICKSTART.md](../QUICKSTART.md) for a 5-minute guide
- Review [TESTING.md](../TESTING.md) for testing with different backends
