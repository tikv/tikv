# TiDB Engine Extensions Library

## Abstract

This proposal is to introduce a TiKV based `c dynamic library` for extending storage system in `TiDB` cluster. This
library aims to export current multi-raft framework to other engines and make them be able to provide services(
read/write) as raftstore directly.

## Background

Initially, such framework was designed for `Realtime HTAP` scenes.

There is already a distributed OLTP storage product `TiKV`, and we could extend other kind of realtime analytics system
based on current multi-raft mechanism to handle more complicated scenes. For example, assume strong schema-aware storage
nodes could be accessed as raftstore with special identification labels. Third-party components can
use [Placement Rules](https://docs.pingcap.com/tidb/stable/configure-placement-rules), provided by `PD`, to schedule
learner/voter replica on them. If such storage system has supported `Multi-raft FSM`, `Percolator Transaction Model`
and `Coprocessor Protocol`, just like `TiFlash`(a distributed column-based storage) does, it will be appropriate
for `HTAP` cases. If transaction is not required, like most OLAP cases which only guarantee `Eventual Consistency`, and
what matters more is throughput rather than latency. Then, data(formed by table schema or other pattern) could be R/W
from this kind of raftstore directly.

For this purpose, it's necessary to implement a multi-raft library to help integrate other system into raftstore.

## Proposal

### Overview

In order to be compatible with process of TiKV and reduce risks about corner cases, such as dealing with snapshot and
region merge, there is no need to reimplement from scratch. It's feasible to refactor TiKV source code and extract parts
of necessary process into interfaces since this framework is not designed for OLTP. The main categories are like:

- apply normal write raft command
- apply admin raft command
- peer detect: create/destroy peer
- ingest sst
- store stats: key/bytes R/W stats; disk stats; storage engine stats;
- snapshot: apply TiKV/self-defined snapshot; generate/serialize self-defined snapshot;
- region stats: approximate size/keys
- region split: scan split keys
- replica read: region read index
- encryption: get file; new file; delete file; link file; rename file;
- status services: metrics; cpu profile; self-defined stats;

Generally speaking, there are two storage components, `RaftEngine` and `KvEngine`, in TiKV for maintaining multi-raft
RSM(Replicated State Machine). KvEngine is mainly used for applying raft command and providing key-value services. Each
time raft log has been committed in RaftEngine, it will be parsed into normal/admin raft command and be handled by apply
process. Multiple modifications about region data/meta/apply-state will be encapsulated into one `Write Batch` and
written into KvEngine atomically. Maybe replacing KvEngine an option, but for other storage system, it's not easy to
guarantee atomicity while writing or reading dynamic key-value pair(such as meta/apply-state) and patterned data(strong
schema) together. Besides, there are a few modules and components(like importer or lighting) reply on the sst format of
KvEngine. It may take a lot of cost to achieve such replacing.

In order to bring few intrusive modifications against original logic of TiKV, it's suggested to let apply process work
as usual but only persist meta and state information. It means each place, where may write normal region data, must be
replaced with related interfaces. Not like KvEngine, storage system under such framework, should be aware of transition
about multi-raft state machine from these interfaces and must have ability about dealing with raft commands, so as to
handle queries with region epoch.

Anyway, there are at least two asynchronous runtimes in one program, therefore, the best practice of such raftstore is
to guarantee `External Consistency`. Actually, the raft log persisted in RaftEngine is the `WAL(Write Ahead Log)` of
apply processes. If process is interrupted at middle step, it should replay from last persisted apply-index after
restarted, and related modifications cannot be witnessed from outside until meets check-point. When any peer tries to
respond queries, it should get latest committed-index from leader and wait until apply-index caught up, in order to make
sure it has had enough context. No matter for learner/follower or even leader, `Read Index` is a good choice to check
latest `Lease`, because it's easy to make any peer of region group provide read service under same logic as long as
overhead of read-index(quite light rpc call) itself is insignificant.

`Idempotency` is also an important property, which means such system could handle outdated raft commands. An optional
way is like:

- fsync important meta/data when necessary
- tell and ignore commands with outdated apply-index
- recover from any middle step by overwriting

Since the program language `Rust`, which TiKV is based on, has zero-cost abstractions. It's very easy to let different
processes interact with almost little cost by `FFI`(Foreign Function Interface). But any caller must be quite clear
about the boundary of safe/unsafe operations exactly and make sure the interfaces, used by different runtimes, must have
same memory layout.

### Direct Writing

To support direct writing without transaction, storage system must implement related interfaces about behavior of
leader:

- scan range of a region to generate keys for split if reach threshold
- get approximate size/keys information of a region
- report R/W stats
- snapshot
    - generate self-defined snapshot and serialize into file
    - apply snapshot from TiKV or self-defined storage

Then, `Placement Rules` could be used to let PD transfer region groups under specific ranges from TiKV to raftstores
with special labels(such as "engine":"xxxx").

### Other Features

Function about `IngestSST` is the core to be compatible with `TiDB Lighting` and `BR` for `HTAP` scenes. It can
substantially speed up data loading/restoring, but is not suitable for direct writing. Besides, the data format of this
kind of SST is defined by TiKV, which might be strongly depended by other components, and it's not easy to support
self-defined storage. At the same time, modules about `CDC` and `BR` are no longer needed and should be removed to avoid
affecting related components in cluster.

Encryption is also an important feature for `DBaaS`(database as a service). To be compatible with TiKV, a data key
manager with same logic is indispensable, especially for rotating data encryption key or using KMS service.

Status services like metrics, cpu/mem profile(flame graph) or other self-defined stats can provide effective support for
locating performance bottlenecks. It's suggested to encapsulate those into one status server and let other external
components visit through status address. Most of original metrics of TiKV could be reused and an optional way is to add
specific prefix for each name.

## Rationale

There haven't been any attempt to abstract relevant layers about multi-raft state machine of TiKV and make a library to
help extend self-defined raftstore. Obviously, such library is not designed to provide comprehensive supports for OLTP
scenes equivalent to TiKV. The main battlefields of it are about `HTAP` or OLAP. Other storage systems do not need to
consider about distributed fault tolerance/recovery or automatic re-balancing, because the library itself has inherited
these important features from TiKV.

## Implementation

There are total two exposed functions:

- print-version: print necessary version information(just like TiKV does) into standard output.
- main-entry:
    - refactor the main entry of TiKV into a `extern "C"` function, and let caller input established function pointers
      which will be invoked dynamically.
    - it's suggested to run main entry function in another independent thread because it will block current context.

Each corresponding module listed above should be refactored to interact with external storage system. Label with key "
engine" is forbidden in dynamic config and should be specified when compiling.

## Usage

- install `grpc`, `protobuf`, `c++`
- include this project as submodule
- modify [FFI Source Code](raftstore-proxy/ffi/src/RaftStoreProxyFFI) under namspace `DB` and run [make gen_proxy_ffi](gen-proxy-ffi/bin.rs)
- implement related interfaces(
  mainly [struct EngineStoreServerHelper](raftstore-proxy/ffi/src/RaftStoreProxyFFI/ProxyFFI.h)) by `c++`
- run `export ENGINE_LABEL_VALUE=xxx` # labels "engine":"xxx" will be added to store info automatically
- run `make release`
- compile and link target library `target/release/lib${ENGINE_LABEL_VALUE}_proxy.dylib|so`

## TODO

- support R/W as `Leader`
- resources control
- async future framework

## Contact

[tongzhigao@pingcap.com](mailto:tongzhigao@pingcap.com)

## License

Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgments

- Thanks [tikv](https://github.com/tikv/tikv) for providing source code.
- Thanks [pd](https://github.com/tikv/pd) for providing `placement rules`.
