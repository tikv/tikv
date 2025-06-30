# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TiKV is a distributed, transactional key-value database written in Rust. It's part of the TiDB ecosystem and serves as a CNCF graduated project. The codebase is built on RocksDB for storage, Raft for consensus, and implements ACID transactions.

## Essential Commands

### Build Commands
```bash
make build          # Debug build
make release        # Release build with optimizations
make dev            # Development workflow (format + clippy + test)
```

### Testing
```bash
make test           # Run full test suite
make test_with_nextest  # Run tests with nextest (faster)
# Run specific test: cargo test <test_name>
```

### Code Quality
```bash
make format         # Format code with rustfmt
make clippy         # Run clippy linter
make audit          # Security audit of dependencies
```

### Development Workflow
```bash
make dev            # Recommended: runs format, clippy, and tests
```

## Architecture Overview

### Core Directory Structure

- **`src/`**: Main application code
  - `config/`: Configuration management
  - `server/`: Server implementation and gRPC services
  - `storage/`: Storage engine and transaction management
  - `coprocessor/`: Distributed computing framework
  - `import/`: Data import and ingestion

- **`components/`**: Modular component libraries (60+ crates)
  - `engine_rocks/`: RocksDB storage engine wrapper
  - `raftstore/`: Raft consensus implementation
  - `tikv_util/`: Common utilities and types
  - `pd_client/`: Placement Driver client
  - `batch-system/`: Batch processing system
  - `security/`: Security and encryption components

- **`tests/`**: Integration tests

### Key Binaries
- `tikv-server`: Main TiKV server
- `tikv-ctl`: Command-line administration tool

## Development Setup

### Rust Toolchain
The project uses Rust nightly (pinned to `nightly-2025-02-28`). The toolchain is configured in `rust-toolchain.toml`.

### Configuration
- Template configuration: `etc/config-template.toml`
- Feature flags available for memory allocators (jemalloc, tcmalloc, etc.)
- Build profiles: dev, release, dist_release

### Memory Allocators
Default is jemalloc. Can be configured with:
```bash
TIKV_ENABLE_FEATURES=mem-profiling make build
```

## Testing Strategy

### Test Types
- Unit tests within component crates
- Integration tests in `tests/` directory
- Failpoint testing for fault injection
- Performance benchmarks

### Running Specific Tests
```bash
# Run tests for a specific component
cargo test -p tikv_util

# Run integration tests
cargo test --test integration_<test_name>

# Run with specific features
cargo test --features "mem-profiling"
```

## Code Organization Patterns

### Modular Architecture
- Workspace-based Cargo project with 60+ crates
- Trait-based abstractions (especially for storage engines)
- Plugin architecture for coprocessors

### Performance Considerations
- Async/await with Tokio runtime
- Batch processing systems for efficiency
- Performance-critical paths marked with `#[PerformanceCriticalPath]`
- Configurable memory allocators

### Error Handling
- Comprehensive error types using `thiserror`
- Failpoint integration for testing error scenarios
- Structured logging with `slog`

## Key Configuration Files

- `Cargo.toml`: Workspace configuration and dependencies
- `Makefile`: Build automation and development commands
- `rust-toolchain.toml`: Rust toolchain specification
- `deny.toml`: Dependency security and licensing policy
- `.github/workflows/`: CI/CD configuration

## Security and Compliance

- FIPS 140-2 compliance support
- Regular security audits via `cargo audit`
- Memory safety through Rust's ownership system
- Encryption support for data at rest and in transit

## Common Development Patterns

When working with this codebase:

1. **Component Development**: Each major feature is typically its own crate in `components/`
2. **Testing**: Always include unit tests; integration tests for cross-component functionality
3. **Configuration**: Use the established configuration patterns from existing components
4. **Error Handling**: Follow the existing error type patterns using `thiserror`
5. **Async Code**: Use Tokio patterns consistent with the existing codebase
6. **Performance**: Consider batch operations and memory allocation patterns

## Build Troubleshooting

- Ensure you have the correct Rust toolchain (nightly-2025-02-28)
- For build issues, try `make clean` before rebuilding
- Check that all required system dependencies are installed (cmake, protoc, etc.)
- Use `make dev` for a comprehensive development check