# Fuzz Testing

This directory contains TiKV fuzz test cases as well as a CLI utility to run these fuzz tests using
a specific fuzzer.

Supported fuzzers:

- [libfuzzer](https://llvm.org/docs/LibFuzzer.html)
- [Honggfuzz](https://github.com/google/honggfuzz)

Planned to support:

- [AFL](http://lcamtuf.coredump.cx/afl/)

## Prerequisites

### Honggfuzz

See [honggfuzz-rs documentation](https://github.com/rust-fuzz/honggfuzz-rs).

## Run fuzz

You can run all test cases or some test cases by using the CLI utility in this directory:

```bash
# In TiKV directory
cargo run --package fuzz -- run [FUZZER] [FILTER]
```

**Example**: Run tests whose name contains "decimal" using Honggfuzz:

```bash
cargo run --package fuzz -- run honggfuzz decimal
```

**Example**: Run all tests using libfuzzer:

```bash
cargo run --package fuzz -- run libfuzzer
```
