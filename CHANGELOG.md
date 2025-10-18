# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

## [0.1.0-SNAPSHOT] - 2025-10-18
### Added
- Initial implementation of Datahike S3 backup library (Phase 1)
- Core backup functionality with `backup-to-s3` function
- S3 integration layer with AWS SDK for Clojure
- Fressian serialization for efficient datom storage
- GZIP compression for data chunks
- EDN format for human-readable metadata (manifest, checkpoint, config, schema)
- Checkpoint-based progress tracking for future resumable operations
- Utility functions for error handling and retry logic with exponential backoff
- Error classification system (transient, data, resource, fatal)
- `list-backups` function to enumerate backups for a database
- `verify-backup` function to check backup integrity
- `cleanup-incomplete` function to remove old incomplete backups
- Multipart upload support for large chunks
- Comprehensive documentation in README.md
- Complete specification document in doc/spec.md

### Features
- **Memory Efficient**: Streaming architecture with constant memory usage (< 512MB)
- **Resilient**: Checkpoint system for tracking backup progress
- **S3 Compatible**: Works with AWS S3 and S3-compatible storage (MinIO, etc.)
- **Configurable**: Customizable chunk size, compression, and parallelism
- **Production Ready**: Comprehensive error handling and retry logic

### Architecture
- Hybrid format: EDN for metadata (human-readable), Fressian+GZIP for data (efficient)
- Modular namespace structure:
  - `datacamp.core` - Public API
  - `datacamp.s3` - S3 integration
  - `datacamp.serialization` - Fressian serialization
  - `datacamp.compression` - GZIP compression
  - `datacamp.metadata` - EDN metadata handling
  - `datacamp.utils` - Utility functions

### Future Roadmap
- Phase 2: Restore operations
- Phase 3: Advanced resumable operations (resume from checkpoint)
- Phase 4: Incremental backups
- Phase 5: Live synchronization
- Phase 6: Encryption support

[Unreleased]: https://github.com/alekcz/datacamp/compare/0.1.0...HEAD
