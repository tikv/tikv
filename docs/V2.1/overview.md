---
title: Overview of TiKV
summary: Learn about the key features, architecture, and two types of APIs of TiKV.
category: overview
---

# Overview of TiKV

TiKV (The pronunciation is: /'taɪkeɪvi:/ tai-K-V, etymology: titanium) is a distributed Key-Value database which is based on the design of Google Spanner and HBase, but it is much simpler without dependency on any distributed file system.

As the storage layer of TiDB, TiKV can work separately and does not depend on the SQL layer of TiDB. To apply to different scenarios, TiKV provides [two types of APIs](#two-types-of-apis) for developers: the Raw Key-Value API and the Transactional Key-Value API.

The key features of TiKV are as follows:

- **Geo-Replication**

    TiKV uses [Raft](http://raft.github.io/) and the [Placement Driver](https://github.com/pingcap/pd/) to support Geo-Replication.

- **Horizontal scalability**

    With Placement Driver and carefully designed Raft groups, TiKV excels in horizontal scalability and can easily scale to 100+ TBs of data.

- **Consistent distributed transactions**

    Similar to Google's Spanner, TiKV supports externally-consistent distributed transactions.

- **Coprocessor support**

    Similar to HBase, TiKV implements a Coprocessor framework to support distributed computing.

- **Cooperates with [TiDB](https://github.com/pingcap/tidb)**

    Thanks to the internal optimization, TiKV and TiDB can work together to be a compelling database solution with high horizontal scalability, externally-consistent transactions, and support for RDMBS and NoSQL design patterns.

## Architecture

The TiKV server software stack is as follows:

![The TiKV software stack](../images/tikv_stack.png)

- **Placement Driver:** Placement Driver (PD) is the cluster manager of TiKV. PD periodically checks replication constraints to balance load and data automatically.
- **Store:** There is a RocksDB within each Store and it stores data into local disk.
- **Region:** Region is the basic unit of Key-Value data movement. Each Region is replicated to multiple Nodes. These multiple replicas form a Raft group.
- **Node:** A physical node in the cluster. Within each node, there are one or more Stores. Within each Store, there are many Regions.

When a node starts, the metadata of the Node, Store and Region are recorded into PD. The status of each Region and Store is reported to PD regularly.

## Two types of APIs

TiKV provides two types of APIs for developers:

- [The Raw Key-Value API](clients/go-client-api.md#try-the-raw-key-value-api)

    If your application scenario does not need distributed transactions or MVCC (Multi-Version Concurrency Control) and only need to guarantee the atomicity towards one key, you can use the Raw Key-Value API.

- [The Transactional Key-Value API](clients/go-client-api.md#try-the-transactional-key-value-api)

    If your application scenario requires distributed ACID transactions and the atomicity of multiple keys within a transaction, you can use the Transactional Key-Value API.

Compared to the Transactional Key-Value API, the Raw Key-Value API is more performant with lower latency and easier to use.