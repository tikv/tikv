# Fuzz Testing

This directory contains TiKV fuzz test cases as well as a custom CLI utility, `fuzz`,
that builds and runs those tests using one of multiple fuzzers.

Supported fuzzers:

- [libfuzzer](https://llvm.org/docs/LibFuzzer.html)
- [Honggfuzz](https://github.com/google/honggfuzz)

Planned to support:

- [AFL](http://lcamtuf.coredump.cx/afl/)

## Prerequisites

### Honggfuzz

```sh
cargo install honggfuzz --version 0.5.34
```

Note that the version of the cargo plugin installed must be the same as the
library linked by the `fuzzer-honggfuzz` project template, here 0.5.34.

Building honggfuzz test cases with `cargo run -p fuzz -- run Honggfuzz <test>`
requires additional development libraries that will differ from system to
system. On a recent Ubuntu system those libraries could be installed with `sudo
apt install binutils-dev libunwind-dev`.

See [honggfuzz-rs documentation](https://github.com/rust-fuzz/honggfuzz-rs).

### AFL

```sh
cargo install afl
```

See [the fuzz.rs book](https://fuzz.rs/book/afl/setup.html).

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

Valid values for `[FUZZER]` are "Libfuzzer", "Honggfuzz", and "Afl".
