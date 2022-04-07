# TiDB Engine Extensions Library

## Abstract

This repository is to introduce a [TiKV](https://github.com/tikv/tikv) based `c dynamic library` for extending storage system in `TiDB` cluster.
It aims to export current multi-raft framework to other engines and make them be able to provide services(read/write) as `raftstore` directly.

## Background

Initially, such framework was designed for `Realtime HTAP` scenarios.
There is already a distributed OLTP storage product `TiKV`, and we could extend other kind of realtime analytics system based on current multi-raft mechanism to handle more complicated scenarios.
For example, assume a strong schema-aware storage node could be accessed as a raftstore with special identification labels.
Third-party components can use [Placement Rules](https://docs.pingcap.com/tidb/stable/configure-placement-rules), provided by `PD`, to schedule learner/voter replicas into it.
If such storage system has supported `Multi-raft RSM`, `Percolator Transaction Model` and `Transaction Read Protocol`, just like `TiFlash`(a distributed column-based storage) does, it will be appropriate for `HTAP` cases.

If transaction is not required, like most `OLAP` cases which only guarantee `Eventual Consistency`, and what matters more is throughput rather than latency.
Then, data(formed by table schema or other pattern) could be R/W from this kind of raftstore directly.

## Design

### Overview

Generally speaking, there are two storage components in TiKV for maintaining multi-raft RSM: `RaftEngine` and `KvEngine`.
KvEngine is mainly used for applying raft command and providing key-value services.
RaftEngine will parse its own committed raft log into corresponding normal/admin raft commands, which will be handled by the apply process.
Multiple modifications about region data/meta/apply-state will be encapsulated into one `Write Batch` and written into KvEngine atomically.
It is an option to replace KvEngine with `Engine Traits`.
But it's not easy to guarantee atomicity while writing/reading dynamic key-value pair(such as meta/apply-state) and patterned data(strong schema) together for other storage systems.
Besides, a few modules and components(like importer or lighting) reply on the SST format of KvEngine in TiKV.
It may cost a lot to achieve such a replacement.

It's suggested to let the apply process work as usual but only persist meta and state information to bring a few intrusive modifications against the original logic of TiKV.
i.e., we must replace everywhere that may write normal region data with related interfaces.
Unlike KvEngine, the storage system(called `engine-store`) under such a framework should be aware of the transition about multi-raft RSM from these interfaces.
The `engine-store` must have the ability to deal with raft commands to handle queries with region epoch.

The `region snapshot` presents the complete region information(data/meta/apply-state) at a specific apply-state.

Anyway, because there are at least two asynchronous runtimes in one program, the best practice of such raft store is to guarantee `External Consistency` by `region snapshot`.
The raft logs persisted in RaftEngine are the `WAL(Write-ahead Log)` of the apply process.
Index of raft entry within the same region peer is monotonic increasing.
If the process is interrupted at the middle step, it should replay from the last persisted apply-state after the restart.
Until a safe point is reached, related modifications are not visible to others.

`Idempotency` is an essential property for `External Consistency`, which means such a system could handle outdated raft commands. A practical way is like:

- Fsync snapshot in `engine-store` atomically
- Fsync region snapshot in `raftstore-proxy` atomically
- Make RaftEngine only GC raft log whose index is smaller than persisted apply-state
- `engine-store` should screen out raft commands with outdated apply-state during apply process
- `engine-store` should recover from the middle step by overwriting and must NOT provide services until caught up with the latest state

Such architecture inherited several important features from TiKV, such as distributed fault tolerance/recovery, automatic re-balancing, etc.
It's also convenient for PD to maintain this kind of storage system by the existing way as long as it works as `raft store`.

#### Interfaces

Since the program language `Rust`, which TiKV uses, has zero-cost abstractions, it's straightforward to let different threads interact with each other by `FFI`(Foreign Function Interface).
Such mode brings almost no overhead.
However, any caller must be pretty clear about the exact safe/unsafe operations boundary.
The structure used by different runtimes through interfaces must have the same memory layout.

It's feasible to refactor TiKV source code and extract parts of the necessary process into interfaces. The main categories are like:

- applying normal-write raft command
- applying admin raft command
- peer detection: destroy peer
- region snapshot: pre-handle/apply region snapshot
- SST file reader
- applying `IngestSst` command
- replica read: batch read-index
- encryption: get file; new file; delete file; link file; rename file;
- status services: metrics; CPU profile; config; thread stats; self-defined API;
- store stats: key/bytes R/W stats; disk stats; `engine-store` stats;
- tools/utils

TiKV can split or merge regions to make the partitions more flexible.
When the size of a region exceeds the limit, it will split into two or more regions, and its range would change from `[a, c)` to `[a, b)` and `[b, c)`.
When the sizes of two consecutive regions are small enough, TiKV will merge them into one, and their range would change from `[a, b)` and `[b, c)` to `[a, c)`.

We must persist the region snapshot when executing admin raft commands about `split`, `merge` or `change peer` because such commands will change the core properties(`version`, `conf version`, `start/end key`) of multi-raft RSM.
Ignorable admin command `CompactLog` may trigger raft log GC in `RaftEngine`.
Thus, to execute such commands, it's required to persist region snapshot.
But while executing normal-write command, which won't change region meta, the decision of persisting can be pushed down to `engine-store`.

When the region in the current store is illegal or pending removal, it will execute a `destroy-peer` task to clean useless data.

According to the basic transaction log replication, a leader peer must commit or apply each writing action before returning success ACK to the client.
When any peer tries to respond to queries, it should get the latest committed index from the leader and wait until the apply-state caught up to ensure it has enough context.
For learners/followers or even leaders, the `Read Index` is a practical choice to check the latest `Lease` because it's easy to make any peer of region group provide read service under the same logic as the overhead of read-index itself is insignificant.

When the leader peer has reclaimed related raft log or other peers can not proceed with RSM in the current context, other peers can request a region snapshot from the leader.
However, the region snapshot data, whose format is TiKV's `SST` file, is not usually used by other storage systems directly.
The standard process has been divided into several parts to accelerate the speed of applying region snapshot data:

- `SST File Reader` to read key-value one by one from SST files
- Multi-thread pool to pre-handle SST files into the self-defined structure of `engine-store`
- Delete old data within [start-key, end-key) of the new region strictly.
- Apply self-defined structure by original sequence

Interfaces about `IngestSst` are the core to be compatible with `TiDB Lighting` and `BR` for the `HTAP` scenario.
It can substantially speed up data loading/restoring.
`SST File Reader` is also useful when applying the `IngestSst` raft command.

Encryption is essential for `DBaaS`(database as a service).
To be compatible with TiKV, a data key manager with the same logic is indispensable, especially for rotating data encryption keys or using the KMS service.

Status services like metrics, CPU/Memory profile(flame graph), or other self-defined stats can effectively support the diagnosis.
It's suggested to encapsulate those into one status server and let other external components visit through the status address.
We could also reuse most of the original metrics of TiKV, and an optional way is to add a specific prefix for each name.

When maintaining DWAL, it's practical to batch raft msg before fsync as long as latency is tolerable to reduce IOPS(mainly in RaftEngine) and make it system-friendly with poor performance.

## Usage

There are two exposed extern "C" functions in [raftstore-proxy](raftstore-proxy/src/lib.rs):

- `print_raftstore_proxy_version`: print necessary version information(just like TiKV does) into standard output.
- `run_raftstore_proxy_ffi`:
    - the main entry accepts established function pointer interfaces and command arguments.
    - it's suggested to run main entry function in another independent thread because it will block current context.

To use this library, please follow the steps below: 
- Install `grpc`, `protobuf`, `c++`, `rust`.
- Include this project as submodule.
- Modify [FFI Source Code](raftstore-proxy/ffi/src/RaftStoreProxyFFI) under namspace `DB` if necessary and run `make gen_proxy_ffi`.
- Run `ENGINE_LABEL_VALUE=xxx make release`
  - label `engine:${ENGINE_LABEL_VALUE}` will be added to store info automatically
  - prefix `${ENGINE_LABEL_VALUE}_proxy_` will be added to each metrics name;
- Include FFI header files and implement related interfaces (mainly `struct EngineStoreServerHelper` and `struct RaftStoreProxyFFIHelper`) by `c++`.
- Compile and link target library `target/release/lib${ENGINE_LABEL_VALUE}_proxy.dylib|so`.

## Interfaces Description

TBD.

## TODO

- support R/W as `Leader`
- resources control
- async future framework
- direct writing

## Contact

[Zhigao Tong](http://github.com/solotzg) ([tongzhigao@pingcap.com](mailto:tongzhigao@pingcap.com))

## License

Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgments

- Thanks [tikv](https://github.com/tikv/tikv) for providing source code.
- Thanks [pd](https://github.com/tikv/pd) for providing `placement rules`.
