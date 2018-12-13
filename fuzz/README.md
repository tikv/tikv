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

## Usage

### List Available Fuzz Targets

```bash
# In TiKV directory
cargo run --package fuzz -- list-targets
```

### Fuzz Specific Target with a Fuzzer

```bash
# In TiKV directory
cargo run --package fuzz -- run [FUZZER] [TARGET]
```
