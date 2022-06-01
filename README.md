<img src="images/tikv-logo.png" alt="tikv_logo" width="300"/>

## [Website](https://tikv.org) | [Documentation](https://tikv.org/docs/latest/concepts/overview/) | [Community Chat](https://tikv.org/chat)

[![Build Status](https://ci.pingcap.net/buildStatus/icon?job=tikv_ghpr_build_master)](https://ci.pingcap.net/blue/organizations/jenkins/tikv_ghpr_build_master/activity)
[![Coverage Status](https://codecov.io/gh/tikv/tikv/branch/master/graph/badge.svg)](https://codecov.io/gh/tikv/tikv)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/2574/badge)](https://bestpractices.coreinfrastructure.org/projects/2574)

TiKV is an open-source, distributed, and transactional key-value database. Unlike other traditional NoSQL systems, TiKV not only provides classical key-value APIs, but also transactional APIs with ACID compliance. Built in Rust and powered by Raft, TiKV was originally created by [PingCAP](https://en.pingcap.com) to complement [TiDB](https://github.com/pingcap/tidb), a distributed HTAP database compatible with the MySQL protocol.

The design of TiKV ('Ti' stands for titanium) is inspired by some great distributed systems from Google, such as BigTable, Spanner, and Percolator, and some of the latest achievements in academia in recent years, such as the Raft consensus algorithm.

If you're interested in contributing to TiKV, or want to build it from source, see [CONTRIBUTING.md](./CONTRIBUTING.md).

![cncf_logo](images/cncf.png#gh-light-mode-only)
![cncf_logo](images/cncf-white.png#gh-dark-mode-only)

TiKV is a graduated project of the [Cloud Native Computing Foundation](https://cncf.io/) (CNCF). If you are an organization that wants to help shape the evolution of technologies that are container-packaged, dynamically-scheduled and microservices-oriented, consider joining the CNCF. For details about who's involved and how TiKV plays a role, read the CNCF [announcement](https://www.cncf.io/announcements/2020/09/02/cloud-native-computing-foundation-announces-tikv-graduation/).

---

With the implementation of the Raft consensus algorithm in Rust and consensus state stored in RocksDB, TiKV guarantees data consistency. [Placement Driver (PD)](https://github.com/pingcap/pd/), which is introduced to implement auto-sharding, enables automatic data migration. The transaction model is similar to Google's Percolator with some performance improvements. TiKV also provides snapshot isolation (SI), snapshot isolation with lock (SQL: `SELECT ... FOR UPDATE`), and externally consistent reads and writes in distributed transactions.

TiKV has the following key features:

- **Geo-Replication**

    TiKV uses [Raft](http://raft.github.io/) and the Placement Driver to support Geo-Replication.

- **Horizontal scalability**

    With PD and carefully designed Raft groups, TiKV excels in horizontal scalability and can easily scale to 100+ TBs of data.

- **Consistent distributed transactions**

    Similar to Google's Spanner, TiKV supports externally-consistent distributed transactions.

- **Coprocessor support**

    Similar to HBase, TiKV implements a coprocessor framework to support distributed computing.

- **Cooperates with [TiDB](https://github.com/pingcap/tidb)**

    Thanks to the internal optimization, TiKV and TiDB can work together to be a compelling database solution with high horizontal scalability, externally-consistent transactions, support for RDBMS, and NoSQL design patterns.

## Governance

See [Governance](https://github.com/tikv/community/blob/master/GOVERNANCE.md).

## Documentation

For instructions on deployment, configuration, and maintenance of TiKV,see TiKV documentation on our [website](https://tikv.org/docs/4.0/tasks/introduction/). For more details on concepts and designs behind TiKV, see [Deep Dive TiKV](https://tikv.org/deep-dive/introduction/).

> **Note:**
>
> We have migrated our documentation from the [TiKV's wiki page](https://github.com/tikv/tikv/wiki/) to the [official website](https://tikv.org/docs). The original Wiki page is discontinued. If you have any suggestions or issues regarding documentation, offer your feedback [here](https://github.com/tikv/website).

## TiKV adopters

You can view the list of [TiKV Adopters](https://tikv.org/adopters/).

## TiKV software stack

![The TiKV software stack](images/tikv_stack.png)

- **Placement Driver:** PD is the cluster manager of TiKV, which periodically checks replication constraints to balance load and data automatically.
- **Store:** There is a RocksDB within each Store and it stores data into the local disk.
- **Region:** Region is the basic unit of Key-Value data movement. Each Region is replicated to multiple Nodes. These multiple replicas form a Raft group.
- **Node:** A physical node in the cluster. Within each node, there are one or more Stores. Within each Store, there are many Regions.

When a node starts, the metadata of the Node, Store and Region are recorded into PD. The status of each Region and Store is reported to PD regularly.

## Quick start

### Deploy a playground with TiUP

The most quickest to try out TiKV with TiDB is using TiUP, a component manager for TiDB.

You can see [this page](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb#deploy-a-local-test-environment-using-tiup-playground) for a step by step tutorial.

### Deploy a playground with binary

TiKV is able to run separately with PD, which is the minimal deployment required.

1. Download and extract binaries.

```bash
$ export TIKV_VERSION=v4.0.12
$ export GOOS=darwin  # only {darwin, linux} are supported
$ export GOARCH=amd64 # only {amd64, arm64} are supported
$ curl -O  https://tiup-mirrors.pingcap.com/tikv-$TIKV_VERSION-$GOOS-$GOARCH.tar.gz
$ curl -O  https://tiup-mirrors.pingcap.com/pd-$TIKV_VERSION-$GOOS-$GOARCH.tar.gz
$ tar -xzf tikv-$TIKV_VERSION-$GOOS-$GOARCH.tar.gz
$ tar -xzf pd-$TIKV_VERSION-$GOOS-$GOARCH.tar.gz
```

2. Start PD instance.

```bash
$ ./pd-server --name=pd --data-dir=/tmp/pd/data --client-urls="http://127.0.0.1:2379" --peer-urls="http://127.0.0.1:2380" --initial-cluster="pd=http://127.0.0.1:2380" --log-file=/tmp/pd/log/pd.log
```

3. Start TiKV instance.

```bash
$ ./tikv-server --pd-endpoints="127.0.0.1:2379" --addr="127.0.0.1:20160" --data-dir=/tmp/tikv/data --log-file=/tmp/tikv/log/tikv.log
```

4. Install TiKV Client(Python) and verify the deployment, required Python 3.5+.

```bash
$ pip3 install -i https://test.pypi.org/simple/ tikv-client
```

```python
from tikv_client import RawClient

client = RawClient.connect("127.0.0.1:2379")

client.put(b'foo', b'bar')
print(client.get(b'foo')) # b'bar'

client.put(b'foo', b'baz')
print(client.get(b'foo')) # b'baz'
```

### Deploy a cluster with TiUP

You can see [this manual](./doc/deploy.md) of production-like cluster deployment presented by @c4pt0r.

### Build from source

See [CONTRIBUTING.md](./CONTRIBUTING.md).

## Client drivers

- [Go](https://github.com/tikv/client-go) (The most stable and widely used)
- [Java](https://github.com/tikv/client-java)
- [Rust](https://github.com/tikv/client-rust)
- [C](https://github.com/tikv/client-c)

If you want to try the Go client, see [Go Client](https://tikv.org/docs/4.0/reference/clients/go/).

## Security

### Security audit

A third-party security auditing was performed by Cure53. See the full report [here](./security/Security-Audit.pdf).

### Reporting Security Vulnerabilities

To report a security vulnerability, please send an email to [TiKV-security](mailto:tikv-security@lists.cncf.io) group.

See [Security](./security/SECURITY.md) for the process and policy followed by the TiKV project.

## Communication

Communication within the TiKV community abides by [TiKV Code of Conduct](./CODE_OF_CONDUCT.md). Here is an excerpt:

> In the interest of fostering an open and welcoming environment, we as
contributors and maintainers pledge to making participation in our project and
our community a harassment-free experience for everyone, regardless of age, body
size, disability, ethnicity, sex characteristics, gender identity and expression,
level of experience, education, socio-economic status, nationality, personal
appearance, race, religion, or sexual identity and orientation.

### Social Media

- [Twitter](https://twitter.com/tikvproject)
- [Blog](https://tikv.org/blog/)
- [Reddit](https://www.reddit.com/r/TiKV)
- Post questions or help answer them on [Stack Overflow](https://stackoverflow.com/questions/tagged/tikv)

### Slack

Join the TiKV community on [Slack](https://slack.tidb.io/invite?team=tikv-wg&channel=general) - Sign up and join channels on TiKV topics that interest you.

## License

TiKV is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgments

- Thanks [etcd](https://github.com/coreos/etcd) for providing some great open source tools.
- Thanks [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
- Thanks [rust-clippy](https://github.com/rust-lang/rust-clippy). We do love the great project.
