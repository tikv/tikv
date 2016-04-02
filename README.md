## TiKV is a distributed KV database powered by Rust.


[![Build Status](https://travis-ci.org/pingcap/tikv.svg?branch=master)](https://travis-ci.org/pingcap/tikv)


TiKV is inspired by the design of Google Spanner and HBase, but much simpler (Doesn't depend on any distributed file system).

- __Geo-Replication__  
TiKV uses Raft to support Geo-Replication. We have ported etcd's raft implementation to Rust.


- __Horizontal scalability__  


- __Consistent distributed transactions__  


- __Coprocessor support__  


- __Working with [TiDB](https://github.com/pingcap/tidb)__  




