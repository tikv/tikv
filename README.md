<img src="images/tikv-logo.png" alt="tikv_logo" width="300"/>

## [Website](https://tikv.org) | [Documentation](https://tikv.org/docs/latest/concepts/overview/) | [Community Chat](https://tikv.org/chat)

[![Build Status](https://internal.pingcap.net/idc-jenkins/buildStatus/icon?job=build_tikv_multi_branch%2Fmaster)](https://internal.pingcap.net/idc-jenkins/job/build_tikv_multi_branch/)
[![Coverage Status](https://codecov.io/gh/tikv/tikv/branch/master/graph/badge.svg)](https://codecov.io/gh/tikv/tikv)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/2574/badge)](https://bestpractices.coreinfrastructure.org/projects/2574)

TiKV is an open-source, distributed, and transactional key-value database. Unlike other traditional NoSQL systems, TiKV not only provides classical key-value APIs, but also transactional APIs with ACID compliance. Built in Rust and powered by Raft, TiKV was originally created to complement [TiDB](https://github.com/pingcap/tidb), a distributed HTAP database compatible with the MySQL protocol.

The design of TiKV ('Ti' stands for titanium) is inspired by some great distributed systems from Google, such as BigTable, Spanner, and Percolator, and some of the latest achievements in academia in recent years, such as the Raft consensus algorithm.

If you're interested in contributing to TiKV, or want to build it from source, see [CONTRIBUTING.md](./CONTRIBUTING.md).

![cncf_logo](images/cncf.png)

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

## TiKV roadmap

You can see the [TiKV Roadmap](docs/ROADMAP.md).

## TiKV software stack

![The TiKV software stack](images/tikv_stack.png)

- **Placement Driver:** PD is the cluster manager of TiKV, which periodically checks replication constraints to balance load and data automatically.
- **Store:** There is a RocksDB within each Store and it stores data into the local disk.
- **Region:** Region is the basic unit of Key-Value data movement. Each Region is replicated to multiple Nodes. These multiple replicas form a Raft group.
- **Node:** A physical node in the cluster. Within each node, there are one or more Stores. Within each Store, there are many Regions.

When a node starts, the metadata of the Node, Store and Region are recorded into PD. The status of each Region and Store is reported to PD regularly.

## Try TiKV

TiKV was originally a component of [TiDB](https://github.com/pingcap/tidb). To run TiKV you must build and run it with PD, which is used to manage a TiKV cluster. You can use TiKV together with TiDB or separately on its own.

We provide multiple deployment methods, but it is recommended to use our Ansible deployment for production environment. The TiKV documentation is available on [TiKV's website](https://tikv.org/docs/4.0/concepts/overview/).

### Testing deployment

- [Try TiKV and TiDB](https://tikv.org/docs/4.0/tasks/try/introduction/)

    You can use [`tidb-docker-compose`](https://github.com/pingcap/tidb-docker-compose/) to quickly test TiKV and TiDB on a single machine. This is the easiest way. For other ways, see [TiDB documentation](https://docs.pingcap.com/).

- Try TiKV separately

    - [Deploy TiKV Using Docker Stack](https://tikv.org/docs/4.0/tasks/try/docker-stack/): To quickly test TiKV separately without TiDB on a single machine
    - [Deploy TiKV Using Docker](https://tikv.org/docs/4.0/tasks/deploy/docker/): To deploy a multi-node TiKV testing cluster using Docker
    - [Deploy TiKV Using Binary Files](https://tikv.org/docs/4.0/tasks/deploy/binary/): To deploy a TiKV cluster using binary files on a single node or on multiple nodes

### Production deployment

For the production environment, use [TiDB Ansible](https://github.com/pingcap/tidb-ansible) to deploy the cluster.

- [Deploy TiDB Using Ansible](https://docs.pingcap.com/tidb/stable/online-deployment-using-ansible)
- [Deploy TiKV separately Using Ansible](https://tikv.org/docs/4.0/tasks/deploy/ansible/)

## Client drivers

Currently, the interfaces to TiKV are the [TiDB Go client](https://github.com/pingcap/tidb/tree/master/store/tikv) and the [TiSpark Java client](https://github.com/pingcap/tispark/tree/master/tikv-client/src/main/java/com/pingcap/tikv).

These are the clients for TiKV:

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

### WeChat

The TiKV community is also available on WeChat. If you want to join our WeChat group, send a request mail to [zhangyanqing@pingcap.com](mailto:zhangyanqing@pingcap.com), with your personal information that includes the following:

- WeChat ID (**Required**)
- A contribution you've made to TiKV, such as a PR (**Required**)
- Other basic information

We will invite you in right away.

## License

TiKV is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgments

- Thanks [etcd](https://github.com/coreos/etcd) for providing some great open source tools.
- Thanks [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
- Thanks [rust-clippy](https://github.com/Manishearth/rust-clippy). We do love the great project.
