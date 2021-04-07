# Contributing to TiKV

Thanks for your interest in contributing to TiKV! This document outlines some of the conventions on building, running, and testing TiKV, the development workflow, commit message formatting, contact points and other resources.

TiKV has many dependent repositories. If you need any help or mentoring getting started, understanding the codebase, or making a PR (or anything else really), please ask on [Slack](https://tikv.org/chat), or [WeChat](./README.md#WeChat). If you don't know where to start, please click on the contributor icon below to get you on the right contributing path.

[<img src="images/contribution-map.png" alt="contribution-map" width="180">](https://github.com/pingcap/tidb-map/blob/master/maps/contribution-map.md#tikv-distributed-transactional-key-value-database)

## Building and setting up a development workspace

TiKV is mostly written in Rust, but has components written in C++ (RocksDB, gRPC). We are currently using the Rust nightly toolchain. To provide consistency, we use linters and automated formatting tools.

### Prerequisites

To build TiKV you'll need to at least have the following installed:

* `git` - Version control
* [`rustup`](https://rustup.rs/) - Rust installer and toolchain manager
* `make` - Build tool (run common workflows)
* `cmake` - Build tool (required for gRPC)
* `awk` - Pattern scanning/processing language
* C++ compiler - gcc 4.9+ (required for gRPC)

If you are targeting platforms other than x86_64 linux, you'll also need:

* [`llvm` and `clang`](http://releases.llvm.org/download.html) - Used to generate bindings for different platforms and build native libraries (required for grpcio, rocksdb).

### Getting the repository

```
git clone https://github.com/tikv/tikv.git
cd tikv
# Future instructions assume you are in this directory
```

### Configuring your Rust toolchain

`rustup` is the official toolchain manager for Rust, similar to `rvm` or `rbenv` from the Ruby world.

TiKV is pinned to a version of Rust using a `rust-toolchain` file. `rustup` and `cargo` will automatically use this file. We also use the `rustfmt` and `clippy` components, to install those:

```bash
rustup component add rustfmt
rustup component add clippy
```

### Building and testing

TiKV includes a `Makefile` that has common workflows and sets up a standard build environment. You can also use `cargo`, as you would in many other Rust projects. It can help to run a command in the same environment as the Makefile: this can avoid re-compilations due to environment changes. This is done by prefixing a command with `scripts/env`, for example: `./scripts/env cargo build`

You can build TiKV:

```bash
make build
```

During interactive development, you may prefer using `cargo check`, which will parse, borrow check, and lint your code, but not actually compile it:

```bash
cargo check --all
```

It is particularly handy alongside `cargo-watch`, which runs a command each time you change a file.

```bash
cargo install cargo-watch
cargo watch -s "cargo check --all"
```

When you're ready to test out your changes, use the `dev` task. It will format your codebase, build with `clippy` enabled, and run tests. This should run without failure before you create a PR. Unfortunately, some tests will fail intermittently and others don't pass on all platforms. If you're unsure, just ask!

```bash
make dev
```

You can run the test suite alone, or just run a specific test:

```bash
# Run the full suite
make test
# Run a specific test
./scripts/test $TESTNAME -- --nocapture
```

TiKV follows the Rust community coding style. We use Rustfmt and [Clippy](https://github.com/Manishearth/rust-clippy) to automatically format and lint our code. Using these tools is checked in our CI. These are as part of `make dev`, you can also run them alone:

```bash
# Run Rustfmt
cargo fmt
# Run Clippy (note that some lints are ignored, so `cargo clippy` will give many false positives)
make clippy
```

See the [style doc](https://github.com/rust-lang/rfcs/blob/master/style-guide/README.md) and the [API guidelines](https://rust-lang-nursery.github.io/api-guidelines/) for details on the conventions.

Please follow this style to make TiKV easy to review, maintain, and develop.

### Build issues

To reduce compilation time, TiKV builds do not include full debugging information by default &mdash; `release` and `bench` builds include no debuginfo; `dev` and `test` builds include line numbers only. The easiest way to enable debuginfo is to precede build commands with `RUSTFLAGS=-Cdebuginfo=1` (for line numbers), or `RUSTFLAGS=-Cdebuginfo=2` (for full debuginfo). For example,

```bash
RUSTFLAGS=-Cdebuginfo=2 make dev
RUSTFLAGS=-Cdebuginfo=2 cargo build
```

When building with make, cargo will automatically use [pipelined][p] compilation to increase the parallelism of the build. To turn on pipelining while using cargo directly, set `CARGO_BUILD_PIPELINING=true`.

[p]: https://internals.rust-lang.org/t/evaluating-pipelined-rustc-compilation/10199

## Running TiKV

To run TiKV as an actual key-value store, you will need to run it as a cluster (a cluster can have just one node, which is useful for testing). You can do this on a single machine or on multiple machines. You need to use [PD](https://github.com/tikv/pd) to manage the cluster (even if there is just one node on a single machine). Instructions are in our [docs](https://tikv.org/docs/dev/tasks/deploy/binary/) (if you build TiKV from source, then you don't need to download the binary).

### Configuration

Read our configuration guide to learn about various [configuration options](https://tikv.org/docs/dev/tasks/configure/introduction/). There is also a [configuration template](./etc/config-template.toml).

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

- Create a Git branch from where you want to base your work. This is usually master.
- Write code, add test cases, and commit your work (see below for message format).
- Run tests and make sure all tests pass.
- Push your changes to a branch in your fork of the repository and submit a pull request.
- Your PR will be reviewed by two maintainers, who may request some changes.
  * Once you've made changes, your PR must be re-reviewed and approved.
  * If the PR becomes out of date, you can use GitHub's 'update branch' button.
  * If there are conflicts, you can rebase (or merge) and resolve them locally. Then force push to your PR branch.
    You do not need to get re-review just for resolving conflicts, but you should request re-review if there are significant changes.
- Our CI system automatically tests all pull requests.
- Our bot will merge your PR. It can be summoned by commenting `/merge` or adding the `S: CanMerge` label (requires tests to pass and two approvals. You might have to ask your reviewer to do this).

See [Rustdoc of TiKV](https://tikv.github.io) for TiKV code documentation.

Thanks for your contributions!

### Finding something to work on

For beginners, we have prepared many suitable tasks for you. Checkout our [Help Wanted issues](https://github.com/tikv/tikv/issues?q=is%3Aopen+is%3Aissue+label%3Astatus%2Fhelp-wanted) for a list, in which we have also marked the difficulty level.

If you are planning something big, for example, relates to multiple components or changes current behaviors, make sure to open an issue to discuss with us before going on.

The TiKV team actively develops and maintains a bunch of dependencies used in TiKV, which you may be also interested in:

- [rust-prometheus](https://github.com/tikv/rust-prometheus): The Prometheus client for Rust, our metrics collecting and reporting library
- [rust-rocksdb](https://github.com/tikv/rust-rocksdb): Our RocksDB binding and wrapper for Rust
- [raft-rs](https://github.com/tikv/raft-rs): The Raft distributed consensus algorithm implemented in Rust
- [grpc-rs](https://github.com/tikv/grpc-rs): The gRPC library for Rust built on the gRPC C Core library and Rust Futures
- [fail-rs](https://github.com/tikv/fail-rs): Fail points for Rust

See more in [TiKV Community](https://github.com/tikv/community).

### Format of the commit message

We follow a rough convention for commit messages that is designed to answer two
questions: what changed and why. The subject line should feature the what and
the body of the commit should describe the why.

```
engine/raftkv: add comment for variable declaration.

Improve documentation.
```

The format can be described more formally as follows:

```
<subsystem>: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
Signed-off-by: <Name> <email address>
```

The first line is the subject and should be no longer than 50 characters, the other lines should be wrapped at 72 characters (see [this blog post](https://preslav.me/2015/02/21/what-s-with-the-50-72-rule/) for why).

If the change affects more than one subsystem, you can use comma to separate them like `util/codec,util/types:`.

If the change affects many subsystems, you can use ```*``` instead, like ```*:```.

The body of the commit message should describe why the change was made and at a high level, how the code works.

### Signing off the Commit

The project uses [DCO check](https://github.com/probot/dco#how-it-works) and the commit message must contain a `Signed-off-by` line for [Developer Certificate of Origin](https://developercertificate.org/).

Use option `git commit -s` to sign off your commits.


### Testing AWS

Testing AWS can be done without an AWS account by using [localstack](https://github.com/localstack/localstack).

```
git clone https://github.com/localstack/localstack.git
cd localstack
docker-compose up
```

For example, to test KMS, create a key:

```
pip install awscli-local
awslocal kms create-key`
```

Then add then use the returned ID in key-id:

```
[security.encryption.master-key]
type = "kms"
region = "us-west-2"
endpoint = "http://localhost:4566"
key-id = "KMS key id"
```

When you run TiKV, make sure to set the localstck credentials

```
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```
