# Contributing to TiKV

Thanks for your interest in contributing to TiKV! This document outlines some of the conventions on building, running, and testing TiKV, the development workflow, commit message formatting, contact points and other resources.

TiKV has many dependent repositories. If you need any help or mentoring getting started, understanding the codebase, or making a PR (or anything else really), please ask on [Slack](https://tikv.org/chat). If you don't know where to start, please click on the contributor icon below to get you on the right contributing path.

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
* C++ compiler - gcc 5+ (required for gRPC)

If you are targeting platforms other than x86_64/aarch64 Linux or macOS, you'll also need:

* [`llvm` and `clang`](http://releases.llvm.org/download.html) - Used to generate bindings for different platforms and build native libraries (required for grpcio, rocksdb).

### Getting the repository

```bash
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
# Or using make
env EXTRA_CARGO_ARGS=$TESTNAME make test
```

TiKV follows the Rust community coding style. We use Rustfmt and [Clippy](https://github.com/Manishearth/rust-clippy) to automatically format and lint our code. Using these tools is checked in our CI. These are as part of `make dev`, you can also run them alone:

```bash
# Run Rustfmt
make format
# Run Clippy (note that some lints are ignored, so `cargo clippy` will give many false positives)
make clippy
```

See the [style doc](https://github.com/rust-lang/rfcs/blob/master/style-guide/README.md) and the [API guidelines](https://rust-lang-nursery.github.io/api-guidelines/) for details on the conventions.

Please follow this style to make TiKV easy to review, maintain, and develop.

### Build issues

To reduce compilation time, TiKV builds do not include full debugging information by default &mdash; `release` and `bench` builds include no debuginfo; `dev` and `test` builds include full debug. To decrease compilation time with another ~5% (around 10 seconds for a 4 min build time), change the `debug = true` to `debug = 1` in the Cargo.toml file to only include line numbers for `dev` and `test`. Another way to change debuginfo is to precede build commands with `RUSTFLAGS=-Cdebuginfo=1` (for line numbers), or `RUSTFLAGS=-Cdebuginfo=2` (for full debuginfo). For example,

```bash
RUSTFLAGS=-Cdebuginfo=1 make dev
RUSTFLAGS=-Cdebuginfo=1 cargo build
```

When building with make, cargo will automatically use [pipelined][p] compilation to increase the parallelism of the build. To turn on pipelining while using cargo directly, set `CARGO_BUILD_PIPELINING=true`.

[p]: https://internals.rust-lang.org/t/evaluating-pipelined-rustc-compilation/10199

## Running TiKV

To run TiKV as an actual key-value store, you will need to run it as a cluster (a cluster can have just one node, which is useful for testing). You can do this on a single machine or on multiple machines. 

Use [PD](https://github.com/tikv/pd) to manage the cluster (even if just one node on a single machine). 

Instructions are in our [docs](https://tikv.org/docs/dev/tasks/deploy/binary/) (if you build TiKV from source, you could skip `1. Download package` and `tikv-server` is in directory `/target`).

Tips: It's recommended to increase the open file limit above 82920. WSL2 users may refer to [the comment](https://github.com/Microsoft/WSL/issues/1688#issuecomment-532767317) if having difficulty in changing the `ulimit`.

### Configuration

Read our configuration guide to learn about various [configuration options](https://tikv.org/docs/dev/tasks/configure/introduction/). There is also a [configuration template](./etc/config-template.toml).

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

- Make sure what you want to contribute is already traced as an issue (see below for linking issue).
  * We may discuss the problem and solution in the issue.
- Create a Git branch from where you want to base your work. This is usually master.
- Write code, add test cases, and commit your work (see below for message format).
- Run tests and make sure all tests pass.
- Push your changes to a branch in your fork of the repository and submit a pull request.
  * Make sure mention the issue, which is created at step 1, in the commit meesage.
- Your PR will be reviewed and may be requested some changes.
  * Once you've made changes, your PR must be re-reviewed and approved.
  * If the PR becomes out of date, you can use GitHub's 'update branch' button.
  * If there are conflicts, you can merge and resolve them locally. Then push to your PR branch.
    You do not need to get re-review just for resolving conflicts, but you should request re-review if there are significant changes.
- Our CI system automatically tests all pull requests.
- Our [bot](https://github.com/ti-chi-bot) will merge your PR. It can be summoned by commenting `/merge` (requires tests to pass and two approvals. You might have to ask your reviewer to do this).

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

### Linking issues

Code repositories in TiKV community require **ALL** the pull requests referring to its corresponding issues. In the pull request body, there **MUST** be one line starting with `Issue Number: ` and linking the relevant issues via the [keyword](https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword), for example:

If the pull request resolves the relevant issues, and you want GitHub to close these issues automatically after it merged into the default branch, you can use the syntax (`KEYWORD #ISSUE-NUMBER`) like this:

```
Issue Number: close #123
```

If the pull request links an issue but does not close it, you can use the keyword `ref` like this:

```
Issue Number: ref #456
```

Multiple issues should use full syntax for each issue and separate by a comma, like:

```
Issue Number: close #123, ref #456
```

For pull requests trying to close issues in a different repository, contributors need to first create an issue in the same repository and use this issue to track.

If the pull request body does not provide the required content, the bot will add the `do-not-merge/needs-linked-issue` label to the pull request to prevent it from being merged.

### Format of the commit message

The bot we use will extract the pull request title as the one-line subject and messages inside the `commit-message` code block as commit message body. For example, a pull request with title `pkg: what's changed in this one package` and body containing:

    ```commit-message
    any multiple line commit messages that go into
    the final commit message body

    * fix something 1
    * fix something 2
    ```

will get a final commit message:

```
pkg: what's changed in this one package (#12345)

any multiple line commit messages that go into
the final commit message body

* fix something 1
* fix something 2
```

The first line is the subject (the pull request title) and should be no longer than 50 characters, the other lines should be wrapped at 72 characters (see [this blog post](https://preslav.me/2015/02/21/what-s-with-the-50-72-rule/) for why).

If the change affects more than one subsystem, you can use comma to separate them like `util/codec,util/types:`.

If the change affects many subsystems, you can use ```*``` instead, like ```*:```.

The body of the commit message should describe why the change was made and at a high level, how the code works.

### Signing off the Commit

The project uses [DCO check](https://github.com/probot/dco#how-it-works) and the commit message must contain a `Signed-off-by` line for [Developer Certificate of Origin](https://developercertificate.org/).

Use option `git commit -s` to sign off your commits. The bot will group and distinguish the signatures from all your commits in the pull request and append them to the final commit message body. 


### Testing AWS

Testing AWS can be done without an AWS account by using [localstack](https://github.com/localstack/localstack).

```bash
git clone https://github.com/localstack/localstack.git
cd localstack
docker-compose up
```

For example, to test KMS, create a key:

```bash
pip install awscli-local
awslocal kms create-key`
```

Then add then use the returned ID in key-id:

```bash
[security.encryption.master-key]
type = "kms"
region = "us-west-2"
endpoint = "http://localhost:4566"
key-id = "KMS key id"
```

When you run TiKV, make sure to set the localstack credentials

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```
