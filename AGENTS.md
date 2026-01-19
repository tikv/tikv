# AGENTS.md

This file provides guidance to AI agents when working with code in this repository.

## Developing Environment Tips

### Prerequisites

- `git` - Version control
- [`rustup`](https://rustup.rs/) - Rust installer and toolchain manager
- `make` - Build tool (run common workflows)
- `cmake` - Build tool (required for gRPC)
- `awk` - Pattern scanning/processing language
- [`protoc`](https://github.com/protocolbuffers/protobuf/releases) - Google protocol buffer compiler
- `C++ compiler` - gcc 5+ or clang (required for gRPC)

### Code Organization

**Source Structure:**

- `/src/` - Main TiKV server source code
  - `/src/config/` - Configuration definitions and parsing
  - `/src/coprocessor/` - TiDB push-down request handling (table scan, index scan, aggregation)
  - `/src/coprocessor_v2/` - Coprocessor V2 plugin system
  - `/src/import/` - SST file import functionality
  - `/src/server/` - gRPC server, connection handling, and service implementations
  - `/src/storage/` - Transaction and MVCC storage layer, including:
    - `/src/storage/mvcc/` - Multi-Version Concurrency Control implementation
    - `/src/storage/txn/` - Transaction processing logic

- `/components/` - Modular components and libraries
  - `/components/backup/` - Backup functionality
  - `/components/backup-stream/` - Log backup (PITR) streaming
  - `/components/batch-system/` - Batch processing system for Raft messages
  - `/components/cdc/` - Change Data Capture implementation
  - `/components/encryption/` - Data encryption at rest
  - `/components/engine_rocks/` - RocksDB engine implementation
  - `/components/engine_traits/` - Storage engine abstraction traits
  - `/components/error_code/` - Error code definitions
  - `/components/external_storage/` - External storage (S3, GCS, Azure) support
  - `/components/keys/` - Key encoding/decoding utilities
  - `/components/pd_client/` - Placement Driver (PD) client
  - `/components/raftstore/` - Raft consensus and region management
  - `/components/raftstore-v2/` - Raftstore V2 implementation
  - `/components/resolved_ts/` - Resolved timestamp tracking for CDC
  - `/components/resource_control/` - Resource control and quota management
  - `/components/security/` - TLS and security utilities
  - `/components/server/` - Server utilities and status server
  - `/components/sst_importer/` - SST file import handling
  - `/components/tikv_util/` - Common utilities used across TiKV
  - `/components/txn_types/` - Transaction type definitions

- `/cmd/` - Binary entry points
  - `/cmd/tikv-server/` - Main TiKV server binary
  - `/cmd/tikv-ctl/` - TiKV control utility

- `/tests/` - Integration tests
- `/fuzz/` - Fuzzing targets

## Building

```bash
# Build development version
make build

# Quick check without full compilation
cargo check --all

# Build release version
make release
```

## Testing

### Unit Tests

Standard Rust tests throughout the codebase.

#### How to run unit tests

```bash
# Run the full test suite
make test

# Run a specific test
./scripts/test $TESTNAME -- --nocapture

# Or using make with extra args
env EXTRA_CARGO_ARGS=$TESTNAME make test

# Using nextest (faster)
env EXTRA_CARGO_ARGS=$TESTNAME make test_with_nextest
```

### Code Quality

```bash
# Run formatter
make format

# Run clippy linter (use this instead of cargo clippy directly)
make clippy

# Run full development checks (format + clippy + tests)
make dev
```

The `make dev` command should pass before submitting a PR.

## Pull Request Instructions

### PR title

The PR title **must** follow one of these formats:

**Format 1 (Specific modules):** `module [, module2, module3]: what's changed`

**Format 2 (Repository-wide):** `*: what's changed`

Examples:

- `raftstore: fix snapshot generation race condition`
- `storage, txn: optimize commit path for single-key transactions`
- `*: upgrade rust toolchain to 1.75`

### PR description

The PR description **must** follow the template at `.github/pull_request_template.md`.

Key requirements:

1. **Issue linking**: There MUST be a line starting with `Issue Number:` linking relevant issues using `close #xxx` or `ref #xxx`
2. **Commit message**: Use the `commit-message` code block for detailed commit message body
3. **Check list**: Mark appropriate test types and side effects
4. **Release note**: Include release note in the `release-note` code block (or "None" if not applicable)

### Signing commits

All commits must be signed off for DCO (Developer Certificate of Origin):

```bash
git commit -s -m "your commit message"
```

The `-s` flag adds `Signed-off-by: Your Name <email>` to the commit.
