## TiKV is a distributed Key-Value database powered by Rust and Raft

[![Build Status](https://circleci.com/gh/pingcap/tikv.svg?style=shield&circle-token=36bab0a8e43edb0941b31c38557d2d9d0d58f708)](https://circleci.com/gh/pingcap/tikv) [![Coverage Status](https://codecov.io/gh/pingcap/tikv/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/tikv) ![GitHub release](https://img.shields.io/github/release/pingcap/tikv.svg)

TiKV (The pronunciation is: /'taɪkeɪvi:/ tai-K-V, etymology: titanium) is a distributed Key-Value database which is based on the design of Google Spanner and HBase, but it is much simpler without dependency on any distributed file system. With the implementation of the Raft consensus algorithm in Rust and consensus state stored in RocksDB, it guarantees data consistency. Placement Driver which is introduced to implement sharding enables automatic data migration. The transaction model is similar to Google's Percolator with some performance improvements. TiKV also provides snapshot isolation (SI), snapshot isolation with lock (SQL: `SELECT ... FOR UPDATE`), and externally consistent reads and writes in distributed transactions.

TiKV has the following primary features:

- **Geo-Replication:** TiKV uses [Raft](http://raft.github.io/) and the [Placement Driver](https://github.com/pingcap/pd/) to support Geo-Replication.

- **Horizontal scalability:** With Placement Driver and carefully designed Raft groups, TiKV excels in horizontal scalability and can easily scale to 100+ TBs of data.

- **Consistent distributed transactions:** Similar to Google's Spanner, TiKV supports externally-consistent distributed transactions.

- **Coprocessor support:** Similar to Hbase, TiKV implements a coprocessor framework to support distributed computing.

- **Cooperates with [TiDB](https://github.com/pingcap/tidb):** Thanks to the internal optimization, TiKV and TiDB can work together to be a compelling database solution with high horizontal scalability, externally-consistent transations, and support for RDBMS and NoSQL design patterns.


## The TiKV Software Stack

![The TiKV software stack.](images/tikv_stack.png)

- **Placement Driver:** Placement Driver (PD) is the cluster manager of TiKV. PD periodically checks replication constraints to balance load and data automatically.
- **Store:** There is a RocksDB within each Store and it stores data into local disk.
- **Region:** Region is the basic unit of Key-Value data movement. Each Region is replicated to multiple Nodes. These multiple replicas form a Raft group.
- **Node:** A physical node in the cluster. Within each node, there are one or more Stores. Within each Store, there are many Regions.

When a node starts, the metadata of the Node, Store and Region are registered into PD. The status of each Region and Store is reported to PD regularly.


## Your First Test Drive

We have a [Docker Compose](https://github.com/pingcap/tidb-docker-compose/) you can use to test out [TiKV](https://github.com/pingcap/tikv) and [TiDB](https://github.com/pingcap/tidb).

```bash
git clone https://github.com/pingcap/tidb-docker-compose/
cd tidb-docker-compose
docker-compose up -d
```

Shortly after you will be able to connect with `mysql -h 127.0.0.1 -P 4000 -u root` and view the cluster metrics at [http://localhost:3000/](http://localhost:3000/).


## Setting Up a Development Workspace

The TiKV codebase is primarily written in Rust, but has components written in C++ (RocksDB) and Go (gRPC). In order to provide consistency and avoid opinion-based arguments, we make extensive use of linters and automated formatting tools. Additionally, due to Rust's youth we are currently utlizing nightly builds which provide access to many useful features.

### Checking Your Prerequisites

In order to build TiKV you will need (at least) the following packages available:

* `git` - Version control
* `rustup` - Rust toolchain manager
* `awk` - Pattern scanning/processing language
* `cmake` - Build tool (required for gRPC)
* `go` - Programming language (required for gRPC)
* `make` - Build tool (run common workflows)
* `clang` or `gcc` - C compiler toolchain

### Getting the Repository

```
git clone https://github.com/pingcap/tikv.git
cd tikv
# Future instructions assume you are in this repository
```

### Configuring Your Rust Toolchain

`rustup` is an official toolchain manager for Rust, akin to `rvm` or `rbenv` from the Ruby world.

TiKV uses the version of the Rust toolchain specified in `rust-toolchain`. `rustup` and `clippy` will automatically utilize this file. We also make use of the `rustfmt` component.

```bash
rustup component add rustfmt-preview
```

### Building & Testing

> While TiKV includes a `Makefile` with common workflows, you are also able to use `cargo` as you would a normal Rust project.

At this point you should be able to build TiKV:

```bash
make build
```

While interactively developing you may prefer using `cargo check`, which will do parse, borrow check, and lint run on your code, but not actually compile it. It is particularly handy alongside `cargo-watch` which will run a command each time you change a file.

```bash
cargo install cargo-watch
cargo watch -s "cargo check"
```

When you're ready to test out your changes you should use the `dev` task. It will format your codebase, build with `clippy` enabled, and run tests. This should run without fail before you make a PR.

```bash
make dev
```

You can run the full test suite locally, or just run a specific test:

```bash
# Run the full suite
make test
# Run a specific test
cargo test $TESTNAME
```

Any pull requests made will automatically be tested by our CI systems, so making sure the full suite passes before creating your PR is not strictly required. **All merged PRs must have passing CI tests.**

### Getting the Rest of the System Working

In order to get other components ([TiDB](https://github.com/pingcap/tidb) and [PD](https://github.com/pingcap/pd) working we suggest you follow the [development guide](https://github.com/pingcap/docs/blob/master/dev-guide/development.md) as you will need to (at least) have `pd-server` working alongside `tikv-server` in order to do integration level testing.

## Deploying To Production

**Use Ansible?** The official [Ansible deployment guide](https://github.com/pingcap/docs/blob/master/op-guide/ansible-deployment.md) outlines the recommended way of deploying a cluster of TiKV nodes alongside the other components of a cluster.

**Prefer Docker?** You can find a guide on deploying a cluster with [Docker](https://github.com/pingcap/docs/blob/master/op-guide/docker-deployment.md) to run the TiKV and the rest of the cluster.

**Want to roll your own?** That works too! We have written a [Binary deployment Guide](https://github.com/pingcap/docs/blob/master/op-guide/binary-deployment.md) which might help you as you piece together your own way of doing things.


## Using TiKV

TiKV is a component in the TiDB project. To run TiKV you must build and run it with PD, which is used to manage the cluster.

Currently the only interface to TiKV is the [TiDB Go client](https://github.com/pingcap/tidb/tree/master/store/tikv) and the [TiSpark Java client](https://github.com/pingcap/tispark/tree/master/tikv-client/src/main/java/com/pingcap/tikv).

**We would love it if you developed drivers in other languages.**

Also please refer to our [wiki page](https://github.com/pingcap/tikv/wiki/TiKV-Documentation) for the documentation. The documentation for TiKV is very simple for now, but we are continuously improving it! We also appreciate your contributions on it.


### Configuration

Read our configuration guide to learn about the various [configuration options](https://github.com/pingcap/docs/blob/master/op-guide/configuration.md).


## Contributing

Contributions are welcome! For beginners, we have prepared many suitable tasks for you. Please checkout our [Help Wanted issues](https://github.com/pingcap/tikv/issues?q=is%3Aissue+is%3Aopen+label%3A%22S%3A+HelpWanted%22) for a list, in which we have also marked the difficulty level we thought. If you are planning something big, for example, relates to multiple components or changes current behaviors, make sure to open an issue to discuss with us before going on :)

See [CONTRIBUTING](./CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

In addition, TiKV team actively develops and maintains a bunch of dependencies used in TiKV, which you may be also interested in:

- [rust-prometheus](https://github.com/pingcap/rust-prometheus): The Prometheus client for Rust, our metric collecting and reporting library

- [rust-rocksdb](https://github.com/pingcap/rust-rocksdb): Our RocksDB binding and wrapper for Rust

- [raft-rs](https://github.com/pingcap/raft-rs): The Raft distributed consensus algorithm implemented in Rust

- [grpc-rs](https://github.com/pingcap/grpc-rs): The gRPC library for Rust built on gRPC C Core library and Rust Futures

- [fail-rs](https://github.com/pingcap/fail-rs): Fail points for Rust


## License

TiKV is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.


## Acknowledgments

- Thanks [etcd](https://github.com/coreos/etcd) for providing some great open source tools.
- Thanks [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
- Thanks [mio](https://github.com/carllerche/mio) for providing metal IO library for Rust.
- Thanks [rust-clippy](https://github.com/Manishearth/rust-clippy). We do love the great project.
