# Datacamp Documentation

## Core Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Getting started with Datacamp
- **[GARBAGE-COLLECTION.md](GARBAGE-COLLECTION.md)** - Optimized garbage collection for Datahike
- **[automatic-gc.md](automatic-gc.md)** - Automatic GC for HTTP server deployments

## Feature Guides

### Backup and Restore
- Basic backup and restore operations
- Directory and S3 backend support
- Schema evolution handling

### Live Migration
- Zero-downtime database migration
- Continuous write support during migration
- Recovery and continuation mechanisms

### Garbage Collection
- **[GARBAGE-COLLECTION.md](GARBAGE-COLLECTION.md)** - Complete guide to optimized GC
  - Resumable marking phase
  - Batch deletion for 20-60x performance
  - Production deployment patterns
  - Monitoring and troubleshooting

### Parallel Processing
- Chunked datom processing
- Configurable parallelism levels
- Memory-efficient streaming

## API Reference

See the source code documentation:
- `src/datacamp/core.clj` - Public API
- `src/datacamp/gc.clj` - GC implementation
- `src/datacamp/migration.clj` - Live migration

## Examples

See the `examples/` directory for practical usage:
- `optimized_gc.clj` - Comprehensive GC examples
- `gc_simple.clj` - Simple GC usage patterns
- `gc_quickstart.clj` - Production quickstart guide
- `live_migration.clj` - Migration examples

## Testing

Run tests using Babashka:

```bash
# All tests
bb test:all

# Specific test suites
bb test:gc         # Garbage collection tests
bb test:migration  # Migration tests
bb test:backend    # Backend tests (PostgreSQL, MySQL, Redis, S3)

# Quick tests (no external dependencies)
bb test:quick
bb test:gc:quick
```

## Development

See `bb.edn` for all available development tasks:
- Docker environment management
- Test runners
- Code coverage
- REPL with test databases

Use `bb help` to see all available commands.