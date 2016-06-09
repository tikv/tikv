## TiKV is a distributed Key-Value database powered by Rust and Raft


[![Build Status](https://travis-ci.org/pingcap/tikv.svg?branch=master)](https://travis-ci.org/pingcap/tikv) [![Coverage Status](https://coveralls.io/repos/github/pingcap/tikv/badge.svg?branch=master)](https://coveralls.io/github/pingcap/tikv)


TiKV is a distributed Key-Value database which is based on the design of Google Spanner and HBase, but is much simpler without dependency on any distributed file system. With the implementation of the Raft consensus algorithm in Rust and consensus state storing in RocksDB, it guarantees data consistency. Placement driver which is introduced to implement sharding enables automatic data migration. The transaction model is similar to Google's Percolator with some performance improvements. TiKV also provides snapshot isolation (SI), serializable snapshot isolation (SSI), and externally consistent reads and writes in distributed transactions. See [Tikv-server software stack](#tikv-server-software-stack) for more information. TiKV has the following primary features:

- __Geo-Replication__  
TiKV uses Raft and placement driver to support Geo-Replication. 

- __Horizontal scalability__  
With carefully designed Raft groups and placement driver, TiKV excels in horizontal scalability and can easily scale up to hold 100s of TBs of data.

- __Consistent distributed transactions__  
Similar to Google's Spanner, TiKV supports externally-consistent distributed transactions. 

- __Coprocessor support__  
Similar to Hbase, TiKV implements the coprocessor framework to support distributed computing.

- __Working with [TiDB](https://github.com/pingcap/tidb)__  
Thanks to the internal optimization, TiKV and TiDB can work together to be the best database system that specializes in horizontal scalability, support for externally-consistent transactions, as well as a focus on supporting both traditional RDBMS and NoSQL.

### Required Rust version

Rust Nightly is required.

### Tikv-server software stack
This figure represents tikv-server software stack. 

![image](images/tikv_stack.png)

- Placement driver: With tikv-server, Placement driver is the most important part which merges placement driver and zonemaster in google's Spanner. pd maintains metas of all regions via etcd. A Timestamp system plugs in pd, which provide time oracle in global.
- Node：A physical node in cluster. Node id must be unique in global.
- Store：A node has one or some stores. Generally a store involves one disk. Each store maps to different paths, and store id must be unique in global either. Multiple stores primarily support a plurality of disks in one node.
- Region：Region is a logical concept. Key-Value datas are grouped by region. Region is the smallest unit of data movement, that's geographic-replication unit. Every region is supported by a raft group and region id must be unique in global. 
- Peer: Peer is a logical concept. It stands for a raft-worked participant in a raft group. A peer in raft-worked group maybe turn up three roles, which are candidate, leader and follower.

When node starts, the ids of node, store and region must be registered into pd as well as their metas. Leaders of raft groups regularly report region state to pd. Pd control split/merge between regions.

A store starts up a rocksdb. Store also record its meta and raft group information in local as well as pd has managed them all via etcd. 

### Contributing

See [CONTRIBUTING](./CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

### License

TiKV is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.


### Acknowledgments
- Thanks [etcd](https://github.com/coreos/etcd) for providing some great open source tools.
- Thanks [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
- Thanks [mio](https://github.com/carllerche/mio) for providing metal IO library for Rust.
- Thanks [rust-clippy](https://github.com/Manishearth/rust-clippy). We do love the great project. 
