# Optimized Garbage Collection for Datahike

## Overview

The optimized GC implementation in datacamp provides two critical improvements over the standard Datahike GC:

1. **Resumable Marking**: The mark phase saves checkpoints periodically, allowing it to resume from where it left off if interrupted
2. **Batch Deletion**: The sweep phase deletes keys in batches rather than one-by-one, dramatically improving performance

All datacamp-specific data is stored under the `:datacamp` namespace key to prevent conflicts with Datahike or Konserve.

## Performance Improvements

### Before (Standard Datahike GC)
- **Mark phase**: 6 hours for 500GB database (non-resumable)
- **Sweep phase**: 2+ days (individual deletes)
- **Total time**: ~2.5 days
- **Risk**: If interrupted, must restart from beginning

### After (Optimized GC)
- **Mark phase**: 6 hours (resumable with checkpoints every 30 seconds)
- **Sweep phase**: 2-4 hours (batch deletes)
- **Total time**: 8-10 hours
- **Risk**: Can resume from last checkpoint if interrupted

## Usage

### Simplified API (Recommended)

The easiest way to use the optimized GC is through `datacamp.core`:

```clojure
(require '[datacamp.core :as datacamp]
         '[datahike.api :as d])

;; Connect to your database
(def conn (d/connect config))

;; Safe dry run (default) - see what would be deleted
(datacamp/gc! conn)

;; Actually run GC with 30 day retention
(datacamp/gc! conn :dry-run false :retention-days 30)

;; Check GC status
(datacamp/gc-status conn)
```

**Key Features:**
- **Safe by default**: Defaults to dry-run mode
- **Auto-resume**: Automatically resumes from checkpoint if interrupted
- **Auto-optimization**: Detects backend and applies optimal settings

### Advanced Usage

For more control, use the GC module directly:

```clojure
(require '[datacamp.gc :as gc]
         '[clojure.core.async :refer [<?!!]])

;; Run optimized GC with specific settings
(<?!! (gc/gc-storage-optimized! @conn
                                :batch-size 1000
                                :parallel-batches 2))
```

### Resumable GC

If your GC is interrupted (e.g., server restart, timeout), you can resume it:

```clojure
;; Start GC with a specific ID
(def gc-id "my-gc-2024-12-28")
(<?!! (gc/gc-storage-optimized! @conn
                                :resume-gc-id gc-id
                                :batch-size 1000))

;; If interrupted, resume with the same ID
(<?!! (gc/resume-gc! @conn gc-id))
```

### Monitoring GC Progress

```clojure
;; Check status of ongoing GC
(let [status (<?!! (gc/get-gc-status @conn))]
  (println "GC Status:")
  (println "  Status:" (:status status))
  (println "  Visited:" (:visited-count status))
  (println "  Reachable:" (:reachable-count status))
  (println "  Progress:" (:completed-branches status) "/"
           (+ (:completed-branches status) (:pending-branches status))))
```

### Time-based Retention

Remove commits older than a specific date:

```clojure
;; Keep only last 30 days of history
(let [retention-date (java.util.Date.
                      (- (System/currentTimeMillis)
                         (* 30 24 60 60 1000)))]
  (<?!! (gc/gc-storage-optimized! @conn
                                  :remove-before retention-date
                                  :batch-size 1000)))
```

### Dry Run Mode

Preview what would be deleted without actually deleting:

```clojure
(let [result (<?!! (gc/gc-storage-optimized! @conn :dry-run true))]
  (println "Would keep:" (:reachable-count result) "items")
  (println "Would delete:" (:would-delete-count result) "items"))
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `:remove-before` | `(Date. 0)` | Remove commits before this date |
| `:resume-gc-id` | `nil` | Resume GC with this ID |
| `:batch-size` | `1000` | Number of keys to delete per batch |
| `:parallel-batches` | `1` | Number of batches to delete in parallel |
| `:checkpoint-interval` | `100` | Commits between checkpoints |
| `:dry-run` | `false` | Preview what would be deleted |

## Backend-Specific Optimizations

The GC automatically optimizes settings based on your storage backend:

### S3
- Batch size: 1000 (S3 API limit)
- Parallel batches: 3
- Utilizes S3 batch delete API

### PostgreSQL/JDBC
- Batch size: 5000
- Parallel batches: 1
- Uses `DELETE WHERE key = ANY(array)`

### File System
- Batch size: 100
- Parallel batches: 10
- Parallel file deletion with thread pool

## Production Pattern

```clojure
(require '[datacamp.core :as datacamp])

(defn production-gc
  [conn]
  ;; Step 1: Always dry run first
  (let [dry-result (datacamp/gc! conn :retention-days 30)]
    (println "Would delete:" (:would-delete-count dry-result) "items")

    ;; Step 2: Run actual GC if confirmed
    (when (user-confirms?)
      ;; Auto-resumes if interrupted
      (datacamp/gc! conn :dry-run false :retention-days 30))))

;; The gc! function automatically:
;; - Checks for interrupted GC and resumes
;; - Detects backend and optimizes settings
;; - Saves checkpoints every 30 seconds
```

## How It Works

### Resumable Marking

1. **Checkpoint Creation**: Every 100 commits or 30 seconds, the GC saves its progress
2. **Checkpoint Storage**: Progress is stored in the Konserve store under `[:datacamp :gc-checkpoint]`
3. **Resume Logic**: If GC is interrupted, it loads the checkpoint and continues from where it left off
4. **Completion**: Checkpoint is deleted after successful GC
5. **Namespace Safety**: All datacamp data is stored under `:datacamp` key to prevent conflicts

### Batch Deletion

1. **Pre-filtering**: All keys to delete are identified upfront
2. **Batching**: Keys are grouped into batches (default 1000)
3. **Parallel Execution**: Multiple batches can be deleted in parallel
4. **Progress Reporting**: Regular updates on deletion progress

## Troubleshooting

### GC Takes Too Long

Increase batch size and parallelism:

```clojure
(gc/gc-storage-optimized! @conn
                         :batch-size 5000
                         :parallel-batches 4)
```

### Out of Memory During Mark Phase

Increase checkpoint frequency:

```clojure
(gc/gc-storage-optimized! @conn
                         :checkpoint-interval 50) ; Checkpoint every 50 commits
```

### GC Interrupted

Resume with the same GC ID:

```clojure
;; Check status first
(let [status (<?!! (gc/get-gc-status @conn))]
  (when (= (:status status) :in-progress)
    ;; Resume the interrupted GC
    (<?!! (gc/resume-gc! @conn (:gc-id status)))))
```

## Migration from Standard GC

1. **No Breaking Changes**: The optimized GC is a drop-in replacement
2. **First Run**: The first optimized GC on a large database may take longer as it builds initial checkpoints
3. **Checkpoint Cleanup**: Old checkpoints are automatically cleaned up after successful GC

## Future Improvements

Once battle-tested, these optimizations will be contributed back to:
- Datahike: Resumable marking functionality
- Konserve: Native batch operations protocol

## CLI Usage

```bash
# Run GC
clojure -M:run -m datacamp.gc-cli run /path/to/db

# Check status
clojure -M:run -m datacamp.gc-cli status /path/to/db

# Resume interrupted GC
clojure -M:run -m datacamp.gc-cli resume <gc-id> /path/to/db
```

## Monitoring and Metrics

The GC provides detailed metrics:

```clojure
{:reachable-count 150000    ; Number of reachable items
 :deleted-count 850000       ; Number of deleted items
 :duration-ms 28800000       ; Total duration in milliseconds
 :stats {:commits-processed 5000
         :checkpoint-saves 50}}
```

## Best Practices

1. **Schedule During Low Traffic**: Run GC during maintenance windows
2. **Monitor First Run**: The first GC on a large database needs careful monitoring
3. **Use Dry Run**: Always dry-run first on production databases
4. **Set Appropriate Retention**: Balance history needs with storage costs
5. **Enable Checkpointing**: Always use checkpointing for large databases

## Support

For issues or questions about the optimized GC:
1. Check the troubleshooting section above
2. Review the examples in `examples/optimized_gc.clj`
3. File an issue in the datacamp repository