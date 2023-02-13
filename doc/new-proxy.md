# New TiFlash Proxy

Author(s): [CalvinNeo](github.com/CalvinNeo)

## Motivation

TiFlash Proxy is used to be a fork of TiKV which can replicate data from TiKV to TiFlash. However, since TiKV is upgrading rapidly, it brings lots of troubles for the old version:
1. Proxy can't cherry-pick TiKV 's bugfix in time.
2. It is hard to take proxy into account when developing TiKV.

## Design

Generally speaking, there are two storage components in TiKV for maintaining multi-raft RSM: `RaftEngine` and `KvEngine`：
1. KvEngine is mainly used for applying raft command and providing key-value services.
   Multiple modifications about region data/meta/apply-state will be encapsulated into one `Write Batch` and written into KvEngine atomically.
2. RaftEngine will parse its own committed raft log into corresponding normal/admin raft commands, which will be handled by the apply process.

It is an option to wrap a self-defined KvEngine by TiKV's `Engine Traits`. This new KvEngine holds a original TiKV's `RocksEngine` and will do the following filtering:
1. For metadata like `RaftApplyState`, we store them in `RocksEngine`.
2. For KV data in `write`/`lock`/`default` cf, we forward them to TiFlash and will no longer write to `RocksEngine`.

However, it may cost a lot to achieve such a replacement:
1. It's not easy to guarantee atomicity while writing/reading dynamic key-value pair(such as meta/apply-state) and patterned data(strong schema) together for other storage systems.
2. A few modules and components(like importer or lighting) reply on the SST format of KvEngine in TiKV. For example, thoses SST files shall be transformed to adapt a column storage engine.
3. A flush to storage layer may be more expensive in other storage engine than TiKV.

Apart from `Engine Traits`, we also need `coprocessor`s to observe and control TiKV's applying procedures:
1. An observer before execution of raft commands, which can:
   1. Filter incompatible commands for TiFlash.
   1. Nofify TiFlash do some preliminary work.
2. An observer after execution of raft commands, which can suggest a persistence.
3. An observer when receiving tasks for applying snapshots, which allows TiFlash pre-handling multiple snapsnots in parallel.
4. An observer after snapshots are applied, which informs TiFlash to do a immediate persistence.
5. An observer when a peer is destroyed.
6. An observer for empty raft entry.
   1. We can't ignore empty raft entry, otherwise can cause wait index timeout.
7. An observer to fetch used/total size of storage.
   1. TiFlash supports multi-disks, so we need to report correct storage information.
8. An observer controls whether to persist before calling `finish_for` and `commit`.

The whole work can be divided into two parts:
1. TiKV side
   TiKV provides new engine traits interfaces and observers.
2. TiFlash(Proxy) side
   By implementing these new interfaces and observers, TiFlash can receive data from TiKV,

### TiKV side
As described in [tikv#12849](https://github.com/tikv/tikv/issues/12849), we provide a mechanism that enables external modules including but not limited to proxy to obtain data through raft apply state machine.

Different from the way of capturing kv change log which is used by TiCDC, Proxy uses regions instead of tables as granularity which is smaller. Proxy also retains the raft state machine, which allows us to manipulate the apply process more finely.

### TiFlash(Proxy) side
As described in [tiflash#5170](https://github.com/pingcap/tiflash/issues/5170).
After refactoring, the Proxy can be divided into several crate/modules:
1. `proxy_server`
   This is a replacement of `components/server`.
2. `mock-engine-store`
   This is a replacement of `components/test_raftstore` and the old `mock-engine-store`.
3. `engine_store_ffi`
   This is decoupled from `components/raftstore`. The observers are also implemented in this crate.
4. `engine_tiflash`
   This is the self-defined `KvEngine`.
5. `raftstore-proxy`
   As before, this crate serves as the entry point for the proxy. The [definition of ffi interfaces](raftstore-proxy/ffi/src/RaftStoreProxyFFI) is also located here.
6. `gen-proxy-ffi`
   As before, this crate generates interface code into `engine_store_ffi`.