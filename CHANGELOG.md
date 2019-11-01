# TiKV Change Log
All notable changes to this project are documented in this file.
See also [TiDB Changelog](https://github.com/pingcap/tidb/blob/master/CHANGELOG.md) and [PD Changelog](https://github.com/pingcap/pd/blob/master/CHANGELOG.md).

## [Unreleased]
- Make several `INFO` logs `DEBUG` and refined some log messages. (https://github.com/tikv/tikv/pull/4768)
- `make docker` now builds the `pingcap/tikv` image, instead of the previous Dockerfile. (https://github.com/tikv/tikv/pull/5776)
- `make dist_artifacts` now creates compressed archives of TiKV binaries and Docker images. (https://github.com/tikv/tikv/pull/5777)

## [3.0.3] - 2019-08-29
+ Fix the issue that ReadIndex might fail to respond to requests because of duplicate context [#5256](https://github.com/tikv/tikv/pull/5256)
+ Fix potential scheduling jitters caused by premature `PutStore` [#5277](https://github.com/tikv/tikv/pull/5277)
+ Fix incorrect timestamps reported from Region heartbeats [#5296](https://github.com/tikv/tikv/pull/5296)
+ Fix potential TiKV panics during region merge [#5291](https://github.com/tikv/tikv/pull/5291)
+ Speed up leader change check for the dead lock detector [#5317](https://github.com/tikv/tikv/pull/5317)
+ Support using `grpc env` to create deadlock clients [#5346](https://github.com/tikv/tikv/pull/5346)
+ Add `config-check` to check whether the configuration is correct [#5349](https://github.com/tikv/tikv/pull/5349)
+ Fix the issue that ReadIndex does not return anything when there is no leader [#5351](https://github.com/tikv/tikv/pull/5351)
+ Exclude shared block cache from core dump [#5322](https://github.com/tikv/tikv/pull/5322)

## [3.0.2] - 2019-08-06
* Fix the bug that TiKV panics if the Raft Log is not written in time [#5160](https://github.com/tikv/tikv/pull/5160)
* Fix the bug that the panic information is not written into the log file after TiKV panics [#5198](https://github.com/tikv/tikv/pull/5198)
* Fix the bug that the insert operation might be incorrectly performed in the pessimistic transaction [#5203](https://github.com/tikv/tikv/pull/5203)
* Lower the output level of some logs that require no manual intervention to INFO [#5193](https://github.com/tikv/tikv/pull/5193)
* Improve the accuracy of monitoring the storage engine size [#5200](https://github.com/tikv/tikv/pull/5200)
* Improve the accuracy of the Region size in TiKV Control [#5195](https://github.com/tikv/tikv/pull/5195)
* Improve the performance of the deadlock detector for pessimistic locks [#5192](https://github.com/tikv/tikv/pull/5192)
* Improve the performance of GC in the Titan storage engine [#5197](https://github.com/tikv/tikv/pull/5197)

## [3.0.1] - 2019-06-16
- Add the statistics of the size of blob files in statistics information [#5060](https://github.com/tikv/tikv/pull/5060)
- Fix the core dump issue caused by the incorrectly cleaned memory resources when the process exits [#5053](https://github.com/tikv/tikv/pull/5053)
- Add all monitoring metrics related to the Titan engine [#4772](https://github.com/tikv/tikv/pull/4772), [#4836](https://github.com/tikv/tikv/pull/4836)
- Add the number of open file handles for Titan when counting the number of open file handles to avoid the issue that no file handle is available because of inaccurate statistics of file handles [#5026](https://github.com/tikv/tikv/pull/5026)
- Set `blob_run_mode` to decide whether to enable the Titan engine on a specific CF [#4991](https://github.com/tikv/tikv/pull/4991)
- Fix the issue that the read operations cannot get the commit information of pessimistic transactions [#5067](https://github.com/tikv/tikv/pull/5067)
- Add the `blob-run-mode` configuration parameter to control the running mode of the Titan engine, and its value can be `normal`, `read-only` or `fallback` [#4865](https://github.com/tikv/tikv/pull/4865)
Improve the performance of detecting deadlocks [#5089](https://github.com/tikv/tikv/pull/5089)

## [3.0.0] - 2019-06-28
+ Engine
  - Introduce Titan, a key-value plugin that improves write performance for
  scenarios with value sizes greater than 1KiB, and relieves write
  amplification in certain degrees
  - Optimize memory management to reduce memory allocation and copying for `Iterator Key Bound Option` 
  - Support `block cache` sharing among different column families
+ Server
  - Support reversed `raw_scan` and `raw_batch_scan`
  - Support batch receiving and sending Raft messages, improving TPS by 7% for write intensive scenarios
  - Support getting monitoring information via HTTP
  - Support Local Reader in RawKV to improve performance  
  - Reduce context switch overhead from `batch commands`
+ Raftstore
  - Support Multi-thread Raftstore and Multi-thread Apply to improve scalabilities,
    concurrency capacity, and resource usage within a single node.
    Performance improves by 70% under the same level of pressure
  - Support checking RocksDB Level 0 files before applying snapshots to avoid write stall
  - Support Hibernate Regions to optimize CPU consumption from RaftStore (Experimental)
  - Remove the local reader thread
+ Transaction
  - Support distributed GC and concurrent lock resolving for improved GC performance
  - Support the pessimistic transaction model (Experimental)
  - Modify the semantics of `Insert` to allow Prewrite to succeed only when there is no Key
  - Remove `txn scheduler `
  - Add monitoring items related to `read index` and `GC worker`
+ Coprocessor
  - Refactor the computation framework to implement vector operators, computation
  using vector expressions, and vector aggregations to improve performance  
  - Support providing operator execution status for the `EXPLAIN ANALYZE` statement
  in TiDB  
  - Switch to the `work-stealing` thread pool model to reduce context switch cost
+ Misc
  - Develop a unified log format specification with restructured log system to
  facilitate collection and analysis by tools
  - Add performance metrics related to configuration information and key bound crossing. 

## [3.0.0-rc.3] - 2019-06-21
+ Engine
    - Check iterator status when scanning. [4936](https://github.com/tikv/tikv/pull/4936)
    - Fix the issue that ingested files and directory are not synchronized. [4937](https://github.com/tikv/tikv/pull/4937)
+ Server
    - Sanitize block size configuration. [4928](https://github.com/tikv/tikv/pull/4928)
    - Support replicating the `delete_range` request without deleting the data when applying. [4490](https://github.com/tikv/tikv/pull/4490)
    - Add read index related metrics. [4830](https://github.com/tikv/tikv/pull/4830)
    - Add GC worker related metrics. [4922](https://github.com/tikv/tikv/pull/4922)
+ Raftstore
    - Fix the issue that local reader cache is not cleared correctly. [4778](https://github.com/tikv/tikv/pull/4778)
    - Fix request latency jetter when transferring leader and conf changes. [4734](https://github.com/tikv/tikv/pull/4734)
    - Remove invalid empty callbacks. [4682](https://github.com/tikv/tikv/pull/4682)
    - Clear stale reads after role change. [4810](https://github.com/tikv/tikv/pull/4810)
    - Synchronize all CF files for the received snapshots. [4807](https://github.com/tikv/tikv/pull/4807)
    - Fix missing fsync calls for snapshots. [4850](https://github.com/tikv/tikv/pull/4850)
+ Coprocessor
    - Improve coprocessor batch executor. [4877](https://github.com/tikv/tikv/pull/4877)
+ Transaction 
    - Support `ResolveLockLite` to allow only resolving specified lock keys. [4882](https://github.com/tikv/tikv/pull/4882)
    - Improve pessimistic lock transaction. [4889](https://github.com/tikv/tikv/pull/4889)
+ Tikv-ctl 
    - Improve `bad-regions` and `tombstone` subcommands. [4862](https://github.com/tikv/tikv/pull/4862)
+ Misc
    - Add dist_release. [4841](https://github.com/tikv/tikv/pull/4841)

## [3.0.0-rc.2] - 2019-05-28
+ Engine
    - Support multiple column families sharing a block cache [#4563](https://github.com/tikv/tikv/pull/4563)
+ Server
    - Remove `TxnScheduler` [#4098](https://github.com/tikv/tikv/pull/4098)
    - Support pessimistic lock transactions [#4698](https://github.com/tikv/tikv/pull/4698)
+ Raftstore
    - Support hibernate Regions to reduce the consumption of the raftstore CPU [#4591](https://github.com/tikv/tikv/pull/4591)
    - Fix the issue that the leader does not reply to the `ReadIndex` requests for the learner [#4653](https://github.com/tikv/tikv/pull/4653)
    - Fix the issue of transferring leader failure in some cases [#4684](https://github.com/tikv/tikv/pull/4684)
    - Fix the possible dirty read issue in some cases [#4688](https://github.com/tikv/tikv/pull/4688)
    - Fix the issue that a snapshot lacks data in some cases [#4716](https://github.com/tikv/tikv/pull/4716)
+ Coprocessor
    - Add more RPN functions
        - `LogicalOr` [#4691](https://github.com/tikv/tikv/pull/4601)
        - `LTReal` [#4602](https://github.com/tikv/tikv/pull/4602)
        - `LEReal` [#4602](https://github.com/tikv/tikv/pull/4602)
        - `GTReal` [#4602](https://github.com/tikv/tikv/pull/4602)
        - `GEReal` [#4602](https://github.com/tikv/tikv/pull/4602)
        - `NEReal` [#4602](https://github.com/tikv/tikv/pull/4602)
        - `EQReal` [#4602](https://github.com/tikv/tikv/pull/4602)
        - `IsNull` [#4720](https://github.com/tikv/tikv/pull/4720)
        - `IsTrue` [#4720](https://github.com/tikv/tikv/pull/4720)
        - `IsFalse` [#4720](https://github.com/tikv/tikv/pull/4720)
        - Support comparison arithmetic for `Int` [#4625](https://github.com/tikv/tikv/pull/4625)
        - Support comparison arithmetic for `Decimal` [#4625](https://github.com/tikv/tikv/pull/4625)
        - Support comparison arithmetic for  `String` [#4625](https://github.com/tikv/tikv/pull/4625)
        - Support comparison arithmetic for  `Time` [#4625](https://github.com/tikv/tikv/pull/4625)
        - Support comparison arithmetic for  `Duration` [#4625](https://github.com/tikv/tikv/pull/4625)
        - Support comparison arithmetic for  `Json` [#4625](https://github.com/tikv/tikv/pull/4625)
        - Support plus arithmetic for `Int` [#4733](https://github.com/tikv/tikv/pull/4733)
        - Support plus arithmetic for `Real` [#4733](https://github.com/tikv/tikv/pull/4733)
        - Support plus arithmetic for `Decimal` [#4733](https://github.com/tikv/tikv/pull/4733)
        - Support MOD functions for `Int` [#4727](https://github.com/tikv/tikv/pull/4727)
        - Support MOD functions for `Real` [#4727](https://github.com/tikv/tikv/pull/4727)
        - Support MOD functions for `Decimal` [#4727](https://github.com/tikv/tikv/pull/4727)
        - Support minus arithmetic for `Int` [#4746](https://github.com/tikv/tikv/pull/4746)
        - Support minus arithmetic for `Real` [#4746](https://github.com/tikv/tikv/pull/4746)
        - Support minus arithmetic for `Decimal` [#4746](https://github.com/tikv/tikv/pull/4746)

## [3.0.0-rc.1] - 2019-05-10
+ Engine
    - Fix the issue that may cause incorrect statistics on read traffic [#4436](https://github.com/tikv/tikv/pull/4436)
    - Fix the issue that may cause prefix extractor panic when deleting a range [#4503](https://github.com/tikv/tikv/pull/4503)
    - Optimize memory management to reduce memory allocation and copying for `Iterator Key Bound Option` [#4537](https://github.com/tikv/tikv/pull/4537)
    - Fix the issue that failing to consider learner log gap may in some cases cause panic [#4559](https://github.com/tikv/tikv/pull/4559)
    - Support `block cache` sharing  among different `column families`[#4612](https://github.com/tikv/tikv/pull/4612)
+ Server
    - Reduce context switch overhead of  `batch commands` [#4473](https://github.com/tikv/tikv/pull/4473)
    - Check the validity of seek iterator status [#4470](https://github.com/tikv/tikv/pull/4470)
+ RaftStore
    - Support configurable `properties index distance` [#4517](https://github.com/tikv/tikv/pull/4517)
+ Coprocessor
    - Add batch index scan executor [#4419](https://github.com/tikv/tikv/pull/4419)
    - Add vectorized evaluation framework [#4322](https://github.com/tikv/tikv/pull/4322)
    - Add execution summary framework for batch executors [#4433](https://github.com/tikv/tikv/pull/4433)
    - Check the maximum column when constructing the RPN expression to avoid invalid column offset that may cause evaluation panic [#4481](https://github.com/tikv/tikv/pull/4481)
    - Add `BatchLimitExecutor` [#4469](https://github.com/tikv/tikv/pull/4469)
    - Replace the original `futures-cpupool` with `tokio-threadpool` in ReadPool to reduce context switch [#4486](https://github.com/tikv/tikv/pull/4486)
    - Add batch aggregation framework [#4533](https://github.com/tikv/tikv/pull/4533)
    - Add `BatchSelectionExecutor` [#4562](https://github.com/tikv/tikv/pull/4562)
    - Add batch aggression function `AVG` [#4570](https://github.com/tikv/tikv/pull/4570)
    - Add RPN function `LogicalAnd`[#4575](https://github.com/tikv/tikv/pull/4575)
+ Misc
    - Support `tcmalloc` as a memory allocator [#4370](https://github.com/tikv/tikv/pull/4370)

## [3.0.0-beta.1] - 2019-03-26
- Optimize the Coprocessor calculation execution framework and implement the TableScan section, with the Single TableScan performance improved by 5% ~ 30%
    - Implement the definition of the `BatchRows` row and the `BatchColumn` column [#3660](https://github.com/tikv/tikv/pull/3660)
    - Implement `VectorLike` to support accessing encoded and decoded data in the same way [#4242](https://github.com/tikv/tikv/pull/4242)
    - Define the `BatchExecutor` to interface and implement the way of converting requests to `BatchExecutor` [#4243](https://github.com/tikv/tikv/pull/4243)
    - Implement transforming the expression tree into the RPN format [#4329](https://github.com/tikv/tikv/pull/4329)
    - Implement the `BatchTableScanExecutor` vectorization calculation operator [#4351](https://github.com/tikv/tikv/pull/4351)
- Unify the log format for easy collection and analysis by tools
- Support using the Local Reader to read in the Raw Read interface [#4222](https://github.com/tikv/tikv/pull/4222)
- Add metrics about configuration information [#4206](https://github.com/tikv/tikv/pull/4206)
- Add metrics about key exceeding bound [#4255](https://github.com/tikv/tikv/pull/4255)
- Add an option to control panic or return an error when encountering the key exceeding bound error [#4254](https://github.com/tikv/tikv/pull/4254)
- Add support for the `INSERT` operation, make prewrite succeed only when keys do not exist, and eliminate `Batch Get` [#4085](https://github.com/tikv/tikv/pull/4085)
- Use more fair batch strategy in the Batch System [#4200](https://github.com/tikv/tikv/pull/4200)
- Support Raw scan in tikv-ctl [#3825](https://github.com/tikv/tikv/pull/3825)
- Support hibernating regions [#4591](https://github.com/tikv/tikv/pull/4591)

## [3.0.0-beta] - 2019-01-18
- Support distributed GC [#3179](https://github.com/tikv/tikv/pull/3179)
- Check RocksDB Level 0 files before applying snapshots to avoid Write Stall [#3606](https://github.com/tikv/tikv/pull/3606)
- Support reverse `raw_scan` and `raw_batch_scan` [#3724](https://github.com/tikv/tikv/pull/3724)
- Support using HTTP to obtain monitoring information [#3855](https://github.com/tikv/tikv/pull/3855)
- Support DST better [#3786](https://github.com/tikv/tikv/pull/3786)
- Support receiving and sending Raft messages in batch [#3913](https://github.com/tikv/tikv/pull/3913)
- Introduce a new storage engine Titan [#3985](https://github.com/tikv/tikv/pull/3985)
- Upgrade gRPC to v1.17.2 [#4023](https://github.com/tikv/tikv/pull/4023)
- Support receiving the client requests and sending replies in batch [#4043](https://github.com/tikv/tikv/pull/4043)
- Support multi-thread Apply [#4044](https://github.com/tikv/tikv/pull/4044)
- Support multi-thread Raftstore [#4066](https://github.com/tikv/tikv/pull/4066)

## [2.1.17] - 2019-09-11
- Fix the incorrect result of counting keys in a Region in some cases [#5415](https://github.com/tikv/tikv/pull/5415)
- Add the `config-check` option in TiKV to check whether the TiKV configuration item is valid [#5391](https://github.com/tikv/tikv/pull/5391)
- Optimize the starting process to reduce jitters caused by restarting nodes [#5277](https://github.com/tikv/tikv/pull/5277)
- Optimize the resolving locking process in some cases to speed up resolving locking for transactions [#5339](https://github.com/tikv/tikv/pull/5339)
- Optimize the `get_txn_commit_info` process to speed up committing transactions [#5062](https://github.com/tikv/tikv/pull/5062)
- Simplify Raft-related logs [#5425](https://github.com/tikv/tikv/pull/5425)
- Resolve the issue that TiKV exits abnormally in some cases [#5441](https://github.com/tikv/tikv/pull/5441)

## [2.1.16] - 2019-08-15
* Return region error when TiKV is closing [#4820](https://github.com/tikv/tikv/pull/4820)
* Support reverse `raw_scan` and `raw_batch_scan` [#5148](https://github.com/tikv/tikv/pull/5148)

## [2.1.15] - 2019-07-18
* Unify the log format [#5083](https://github.com/tikv/tikv/pull/5083)
* Improve the accuracy of Region's approximate size or keys in extreme cases to improve the accuracy of scheduling [#5085](https://github.com/tikv/tikv/pull/5085)

## [2.1.14] - 2019-07-04
- Optimize processing the empty callback when processing the Raftstore message to avoid sending unnecessary message [#4682](https://github.com/tikv/tikv/pull/4682)

## [2.1.13] - 2019-06-21
- Fix the issue that incomplete snapshots are generated in the system caused by the iterator not checking the status [#4940](https://github.com/tikv/tikv/pull/4940)
- Add a feature to check the validity for the `block-size` configuration [#4930](https://github.com/tikv/tikv/pull/4930)

## [2.1.12] - 2019-06-13
- Fix the issue that Regions are not available during the leader transfer process in extreme conditions [#4799](https://github.com/tikv/tikv/pull/4734)
- Fix the issue that TiKV loses data when the power of the machine fails abnormally, caused by delayed data flush to the disk when receiving snapshots [#4850](https://github.com/tikv/tikv/pull/4850)

## [2.1.11] - 2019-06-03
- Fix the issue that the learner reads an empty index when there is only one leader and one learner [#4751](https://github.com/tikv/tikv/pull/4751)
- Process `ScanLock` and `ResolveLock` in the thread pool with a high priority to reduce their impacts on commands with a normal priority [#4791](https://github.com/tikv/tikv/pull/4791)
- Synchronize all CF files for received snapshots [#4811](https://github.com/tikv/tikv/pull/4811)

## [2.1.10] - 2019-05-21
* Reject transfer leader when the region recently changed config [#4684](https://github.com/tikv/tikv/pull/4684)
* Add priority label to coprocessor metrics [#4643](https://github.com/tikv/tikv/pull/4643)
* Fix the issue that read index may read stale data during transfering leader [#4724](https://github.com/tikv/tikv/pull/4724)
* Fix the issue that `CommitMerge` may cause TiKV unable to restart [#4615](https://github.com/tikv/tikv/pull/4615)
* Fix unknown logs [#4730](https://github.com/tikv/tikv/pull/4730)

## [2.1.9] - 2019-05-06
* Fix potential quorum changes when transferring leader (https://github.com/pingcap/raft-rs/issues/221)
* Fix the Importer bug that some SST files fail to be imported but it still returns successful import result [#4566](https://github.com/tikv/tikv/pull/4566)
* Support setting a speed limit in Importer when uploading SST files to TiKV [#4607](https://github.com/tikv/tikv/pull/4607)
* Change Importer RocksDB SST default compression method to `lz4` to reduce CPU consumption [#4624](https://github.com/tikv/tikv/pull/4624)

## [2.1.8] - 2019-04-11
* Fix the issue of wrong statistics of the read traffic [#4441](https://github.com/tikv/tikv/pull/4441)
* Fix the raftstore performance issue when checking to decide whether to process pending snapshots when many Regions exist [#4484](https://github.com/tikv/tikv/pull/4484)
* Do not ingest files when the number of level 0 SST files exceeds `level_zero_slowdown_writes_trigger/2` [#4464](https://github.com/tikv/tikv/pull/4464)

# [2.1.6] - 2019-03-15
* Fix the `StoreNotMatch` issue caused by decoding protobuf error in some cases [#4303](https://github.com/tikv/tikv/pull/4303)
* Improve import speed by increasing default region-split-size to 512 MiB [#4347](https://github.com/tikv/tikv/pull/4347)
* Fix OOM issue by storing the intermediate SST files on disk instead of memory [#4348](https://github.com/tikv/tikv/pull/4348)
* Restrict memory usage by RocksDB [#4350](https://github.com/tikv/tikv/pull/4350)
* Fix the issue that scattering Region doesn't take effect [#4352](https://github.com/tikv/tikv/pull/4352)

## [2.1.5] - 2019-02-28
* Fix the panic issue caused by Region merge in some cases [#4235](https://github.com/tikv/tikv/pull/4235)
* Fix the issue that Importer fails to import data in some cases [#4223](https://github.com/tikv/tikv/pull/4223)
* Fix the `KeyNotInRegion` error in some cases [#4125](https://github.com/tikv/tikv/pull/4125)
* Add the detailed `StoreNotMatch` error message [#3885](https://github.com/tikv/tikv/pull/3885)

## [2.1.4] - 2019-02-14
* Fix the abnormal result issue of the event listener in some cases #4126
* Fix the duplicate write issue when closing TiKV #4146

## [2.1.3] - 2019-01-27
* Support obtaining the monitoring information using the HTTP method #3855
* Fix the NULL issue of data_format #4075
* Add verifying the range for scan requests #4124

## [2.1.2] - 2018-12-21
- Support the configuration format in the unit of `DAY` (`d`) and fix the configuration compatibility issue [#3931](https://github.com/tikv/tikv/pull/3931)
- Fix the possible panic issue caused by `Approximate Size Split` [#3942](https://github.com/tikv/tikv/pull/3942)
- Fix two issues about Region merge [#3822](https://github.com/tikv/tikv/pull/3822), [#3873](https://github.com/tikv/tikv/pull/3873)

## [2.1.1] - 2018-12-12
- Avoid transferring the leader to a newly created peer, to optimize the possible delay [#3878](https://github.com/tikv/tikv/pull/3878)

## [2.1.0] - 2018-11-30
* Coprocessor
    - Add more built-in functions
    - [Add Coprocessor `ReadPool` to improve the concurrency in processing the requests](https://github.com/tikv/rfcs/blob/master/text/2017-12-22-read-pool.md)
    - Fix the time function parsing issue and the time zone related issues
    - Optimize the memory usage for pushdown aggregation computing
* Transaction
    - Optimize the read logic and memory usage of MVCC to improve the performance of the scan operation and the performance of full table scan is 1 time better than that in TiDB 2.0
    - Fold the continuous Rollback records to ensure the read performance
    - [Add the `UnsafeDestroyRange` API to support to collecting space for the dropping table/index](https://github.com/tikv/rfcs/blob/master/text/2018-08-29-unsafe-destroy-range.md)
    - Separate the GC module to reduce the impact on write
    - Add the`upper bound` support in the `kv_scan` command
* Raftstore
    - Improve the snapshot writing process to avoid RocksDB stall
    - [Add the `LocalReader` thread to process read requests and reduce the delay for read requests](https://github.com/tikv/rfcs/pull/17)
    - [Support `BatchSplit` to avoid large Region brought by large amounts of write](https://github.com/tikv/rfcs/pull/6)
    - Support `Region Split` according to statistics to reduce the I/O overhead
    - Support `Region Split` according to the number of keys to improve the concurrency of index scan
    - Improve the Raft message process to avoid unnecessary delay brought by `Region Split`
    - Enable the `PreVote` feature by default to reduce the impact of network isolation on services
* Storage Engine
    - Fix the `CompactFiles` bug in RocksDB and reduce the impact on importing data using Lightning
    - Upgrade RocksDB to v5.15 to fix the possible issue of snapshot file corruption
    - Improve `IngestExternalFile` to avoid the issue that flush could block write
* tikv-ctl
    - [Add the `ldb` command to diagnose RocksDB related issues](https://github.com/tikv/tikv/blob/master/docs/tools/tikv-control.md#ldb-command)
    - The `compact` command supports specifying whether to compact data in the bottommost level
* Tools
    - Fast full import of large amounts of data: [TiDB-Lightning](https://pingcap.com/docs/tools/lightning/overview-architecture/)
    - Support new [TiDB-Binlog](https://pingcap.com/docs/tools/tidb-binlog-cluster/)

## [2.1.0-rc.5] - 2018-11-12
- Improve the error message of `WriteConflict` [#3750](https://github.com/tikv/tikv/pull/3750)
- Add the panic mark file [#3746](https://github.com/tikv/tikv/pull/3746)
- Downgrade grpcio to avoid the segment fault issue caused by the new version of gRPC [#3650](https://github.com/tikv/tikv/pull/3650)
- Add the upper limit to the `kv_scan` interface [#3749](https://github.com/tikv/tikv/pull/3749)

## [2.1.0-rc.4] - 2018-10-23
- Optimize the RocksDB Write stall issue caused by applying snapshots [#3606](https://github.com/tikv/tikv/pull/3606)
- Add raftstore `tick` metrics [#3657](https://github.com/tikv/tikv/pull/3657)
- Upgrade RocksDB and fix the Write block issue and that the source file might be damaged by the Write operation when performing `IngestExternalFile` [#3661](https://github.com/tikv/tikv/pull/3661)
- Upgrade grpcio and fix the issue that “too many pings” is wrongly reported [#3650](https://github.com/tikv/tikv/pull/3650)

## [2.1.0-rc.3] - 2018-09-29
### Performance
- Optimize the concurrency for coprocessor requests [#3515](https://github.com/tikv/tikv/pull/3515)
### New features
- Add the support for Log functions [#3603](https://github.com/tikv/tikv/pull/3603)
- Add the support for the `sha1` function [#3612](https://github.com/tikv/tikv/pull/3612)
- Add the support for the `truncate_int` function [#3532](https://github.com/tikv/tikv/pull/3532)
- Add the support for the `year` function [#3622](https://github.com/tikv/tikv/pull/3622)
- Add the support for the `truncate_real` function [#3633](https://github.com/tikv/tikv/pull/3633)
### Bug fixes
- Fix the reporting error behavior related to time functions [#3487](https://github.com/tikv/tikv/pull/3487), [#3615](https://github.com/tikv/tikv/pull/3615)
- Fix the issue that the time parsed from string is inconsistent with that in TiDB [#3589](https://github.com/tikv/tikv/pull/3589)

## [2.1.0-rc.2] - 2018-09-17
### Performance
* Support splitting Regions based on statistics estimation to reduce the I/O cost [#3511](https://github.com/tikv/tikv/pull/3511)
* Reduce clone in the transaction scheduler [#3530](https://github.com/tikv/tikv/pull/3530)
### Improvements
* Add the pushdown support for a large number of built-in functions
* Add the `leader-transfer-max-log-lag` configuration to fix the failure issue of leader scheduling in specific scenarios [#3507](https://github.com/tikv/tikv/pull/3507)
* Add the `max-open-engines` configuration to limit the number of engines opened by `tikv-importer` simultaneously [#3496](https://github.com/tikv/tikv/pull/3496)
* Limit the cleanup speed of garbage data to reduce the impact on `snapshot apply` [#3547](https://github.com/tikv/tikv/pull/3547)
* Broadcast the commit message for crucial Raft messages to avoid unnecessary delay [#3592](https://github.com/tikv/tikv/pull/3592)
### Bug fixes
* Fix the leader election issue caused by discarding the `PreVote` message of the newly split Region [#3557](https://github.com/tikv/tikv/pull/3557)
* Fix follower related statistics after merging Regions [#3573](https://github.com/tikv/tikv/pull/3573)
* Fix the issue that the local reader uses obsolete Region information [#3565](https://github.com/tikv/tikv/pull/3565)
* Support UnsafeDestroyRange API to speedup garbage data cleaning after table/index has been truncated/dropped [#3560](https://github.com/tikv/tikv/pull/3560)

## [2.1.0-rc.1] - 2018-08-24
### Features
* Support `batch split` to avoid too large Regions caused by the Write operation on hot Regions
* Support splitting Regions based on the number of rows to improve the index scan efficiency
### Performance
* Use `LocalReader` to separate the Read operation from the raftstore thread to lower the Read latency
* Refactor the MVCC framework, optimize the memory usage and improve the scan Read performance
* Support splitting Regions based on statistics estimation to reduce the I/O usage
* Optimize the issue that the Read performance is affected by continuous Write operations on the rollback record
* Reduce the memory usage of pushdown aggregation computing
### Improvements
* Add the pushdown support for a large number of built-in functions and better charset support
* Optimize the GC workflow, improve the GC speed and decrease the impact of GC on the system
* Enable `prevote` to speed up service recovery when the network is abnormal
* Add the related configuration items of RocksDB log files
* Adjust the default configuration of `scheduler_latch`
* Support setting whether to compact the data in the bottom layer of RocksDB when using tikv-ctl to compact data manually
* Add the check for environment variables when starting TiKV
* Support dynamically configuring the `dynamic_level_bytes` parameter based on the existing data
* Support customizing the log format
* Integrate tikv-fail in tikv-ctl
* Add I/O metrics of threads
### Bug fixes
* Fix decimal related issues
* Fix the issue that `gRPC max_send_message_len` is set mistakenly
* Fix the issue caused by misconfiguration of `region_size`

## [2.1.0-beta] - 2018-06-30
### Features
* Upgrade Rust to the `nightly-2018-06-14` version
* Provide a `Raft PreVote` configuration to avoid leader reelection generated when network recovers after network isolation
* Add a metric to display the number of files and `ingest` related information in each layer of RocksDB
* Print `key` with too many versions when GC works
### Performance
* Use `static metric` to optimize multi-label metric performance (YCSB `raw get` is improved by 3%)
* Remove `box` in multiple modules and use patterns to improve the operating performance (YCSB `raw get` is improved by 3%)
* Use `asynchronous log` to improve the performance of writing logs
* Add a metric to collect the thread status
* Decease memory copy times by decreasing `box` used in the application to improve the performance

## [2.0.11] - 2019-01-12
* Fix two issues about Region merge #4003 and #4004

## [2.0.10] - 2018-12-16
* Avoid transferring the leader to a newly created peer, to optimize the possible delay #3929
* Fix redundant Region heartbeats #3930

## [2.0.9] - 2018-11-19
* Add the end-key limit to the kv_scan interface #3749
* Abandon the max-tasks-xxx configuration and add max-tasks-per-worker-xxx #3093
* Fix the CompactFiles issue in RocksDB #3789

## [2.0.8] - 2018-10-15
### Bug fix
* Fix the issue that the memory consumed by Raftstore EntryCache keeps increasing when a node goes down 3529

## [2.0.7] - 2018-09-17
### Improvements
* Enable dynamic-level-bytes by default to reduce space amplification
### Bug Fixes
* Update Region's approximate size and approximate keys count after Region merge

## [2.0.6] 0 - 2018-08-03
### Improvements
* Enlarge scheduler’s default slots to reduce false conflicts
* Reduce continuous records of rollback transactions, to improve the Read
* performance when conflicts are extremely severe
* Limit the size and number of RocksDB log files, to reduce unnecessary
* disk usage in long-running condition
### Bug Fixes
* Fix the crash issue when converting the data type from string to decimal

## [2.0.5] - 2018-07-06
* Fix the potential overflow issue in decimal operations
* Fix the dirty read issue that might occur in the process of merge

## [2.0.4] - 2018-06-16
### Features
* Add the RocksDB `PerfContext` interface for debugging
* Add the `region-properties` command for `tikv-ctl`
### Improvements
* Make GC record the log when GC encounters many versions of data
* Remove the `import-mode` parameter
### Bug Fixes
* Fix the issue that `reverse-seek` is slow when many RocksDB tombstones exist
* Fix the crash issue caused by `do_sub`

## [2.0.3] - 2018-06-01
### Bug Fixes
* Correct wrong peer meta for learners
* Report an error instead of getting a result if divisor/dividend is 0 in do_div_mod

## [2.0.2] - 2018-05-21
### Improvements
* Support configuring more gRPC related parameters
* Support configuring the timeout range of leader election
### Bug Fixes
* Fix the issue that the Raft log is not printed
* Fix the issue that obsolete learner is not deleted
* Fix the issue that the snapshot intermediate file is mistakenly deleted

## [2.0.1] - 2018-05-16
### Performance
* Reduced number of `thread_yield` calls
* Fix the issue that `SELECT FOR UPDATE` prevents others from reading
### Improvements
* More verbose logs for slow query
* Speed up delete range
### Bug Fixes
* Fix the bug that raftstore is accidentally blocked when generating the snapshot
* Fix the issue that Learner cannot be successfully elected in special conditions
* Fix the issue that split might cause dirty read in extreme conditions
* Correct the default value of the read thread pool configuration

## [2.0.0] - 2018-04-27
### Features
* Protect critical configuration from incorrect modification
* Support `Region Merge` [experimental]
* Add the `Raw DeleteRange` API
* Add the `GetMetric` API
* Add `Raw Batch Put`, `Raw Batch Get`, `Raw Batch Delete` and `Raw Batch Scan`
* Add Column Family options for the RawKV API and support executing operation on a specific Column Family
* Support Streaming and Streaming Aggregation in Coprocessor
* Support configuring the request timeout of Coprocessor
* Carry timestamps with Region heartbeats
* Support modifying some RocksDB parameters online, such as `block-cache-size`
* Support configuring the behavior of Coprocessor when it encounters some warnings or errors
* Support starting in the importing data mode to reduce write amplification during the data importing process
* Support manually splitting Region in halves
* Improve the data recovery tool `tikv-ctl`
* Return more statistics in Coprocessor to guide the behavior of TiDB
* Support the `ImportSST` API to import SST files [experimental]
* Add the TiKV Importer binary to integrate with TiDB Lightning to import data quickly [experimental]
### Performance
* Optimize read performance using `ReadPool` and increase the `raw_get/get/batch_get` by 30%
* Improve metrics performance
* Inform PD immediately once the Raft snapshot process is completed to speed up balancing
* Solve performance jitter caused by RocksDB flushing
* Optimize the space reclaiming mechanism after deleting data
* Speed up garbage cleaning while starting the server
* Reduce the I/O overhead during replica migration using `DeleteFilesInRanges`
### Stability
* Fix the issue that gRPC call does not returned when the PD leader switches
* Fix the issue that it is slow to offline nodes caused by snapshots
* Limit the temporary space usage consumed by migrating replicas
* Report the Regions that cannot elect a leader for a long time
* Update the Region size information in time according to compaction events
* Limit the size of scan lock to avoid request timeout
* Limit the memory usage when receiving snapshots to avoid OOM
* Increase the speed of CI test
* Fix the OOM issue caused by too many snapshots
* Configure `keepalive` of gRPC
* Fix the OOM issue caused by an increase of the Region number

## [2.0.0-rc6] - 2018-04-19
### Improvements
* Reduce lock contention in Worker
* Add metrics to the FuturePool
### Bug Fixes
* Fix misused metrics in Coprocessor

## [2.0.0-rc.5] - 2018-04-17
### New Features
* Support compacting Regions in `tikv-ctl`
* Add raw batch put/get/delete/scan API for TiKV service
* Add ImportKV service
* Support eval error in Coprocessor
* Support dynamic adjustment of RocksDB cache size by `tikv-ctl`
* Collect number of rows scanned for each range in Coprocessor
* Support treating overflow as warning in Coprocessor
* Support learner in raftstore
### Improvements
* Increase snap GC timeout

## [2.0.0-rc.4] - 2018-04-01
### New Features
* Limit the memory usage during receiving snapshots, to avoid OOM in extreme conditions
* Support configuring the behavior of Coprocessor when it encounters warnings
* Support importing the data pattern in TiKV
* Support splitting Region in the middle
### Improvements
* Fix the issue that too many logs are output caused by leader missing when TiKV is isolated
* Use crossbeam channel in worker

## [2.0.0-rc.3] - 2018-03-23
### New Features
* Support Region Merge
* Add the Raw DeleteRange API
* Add the GetMetric API
* Support streaming in Coprocessor
* Support modifying RocksDB parameters online
### Improvements
* Inform PD immediately once the Raft snapshot process is completed, to speed up balancing
* Reduce the I/O fluctuation caused by RocksDB sync files
* Optimize the space reclaiming mechanism after deleting data
* Improve the data recovery tool `tikv-ctl`
* Fix the issue that it is slow to make nodes down caused by snapshot
* Increase the raw_get/get/batch_get by 30% with ReadPool
* Support configuring the request timeout of Coprocessor
* Carry time information in Region heartbeats
* Limit the space usage of snapshot files to avoid consuming too much disk space
* Record and report the Regions that cannot elect a leader for a long time
* Speed up garbage cleaning when starting the server
* Update the size information about the corresponding Region according to compaction events
* Limit the size of scan lock to avoid request timeout
* Use DeleteRange to speed up Region deletion

## [2.0.0-rc.2] - 2018-03-15
### New Features
* Implement IngestSST API
* `tikv-ctl` now can send consistency-check requests to TiKV
* Support dumping stats of RocksDB and malloc in `tikv-ctl`
### Improvements
* Reclaim disk space after data have been deleted

## [2.0.0-rc.1] - 2018-03-09
### New Features
* Protect important configuration which cannot be changed after initial configuration
* Check whether SSD is used when you start the cluster
### Improvements
* Fix the issue that gRPC call is not cancelled when PD leaders switch
* Optimize the read performance using ReadPool, and improve the performance by 30% for raw get
* Improve metrics and optimize the usage of metrics

## [1.1.0-beta] - 2018-02-24
### Improvements
* Traverse locks using offset + limit to avoid potential GC problems
* Support resolving locks in batches to improve GC speed
* Support GC concurrency to improve GC speed
* Update the Region size using the RocksDB compaction listener for more accurate PD scheduling
* Delete the outdated data in batches using DeleteFilesInRanges, to make TiKV start faster
* Configure the Raft snapshot max size to avoid the retained files taking up too much space
* Support more recovery operations in tikv-ctl
* Optimize the ordered flow aggregation operation

## [1.1.0-alpha] - 2018-01-19
### New Features
* Support Raft learner
* Support TLS
### Improvements
* Optimize Raft Snapshot and reduce the I/O overhead
* Optimize the RocksDB configuration to improve performance
* Optimize count (*) and query performance of unique index in Coprocessor
* Solve the reconnection issue between PD and TiKV
* Enhance the features of the data recovery tool `tikv-ctl`
* Support the Delete Range feature
* Support splitting according to table in Regions
* Support setting the I/O limit caused by snapshot
* Improve the flow control mechanism

## [1.0.8] - 2018-02-11
### Improvements
* Use DeleteFilesInRanges to clear stale data and improve the TiKV starting speed
* Sync the metadata of the received Snapshot compulsorily to ensure its safety
### Bug Fixes
* Use Decimal in Coprocessor sum

## [1.0.7] - 2018-01-22
### Improvements
* Support key-only option in Table Scan executor
* Support the remote mode in tikv-ctl
* Fix the loss of scheduling command from PD
### Bug Fixes
* Fix the format compatibility issue of tikv-ctl proto
* Add timeout in Push metric

## [1.0.5] - 2017-12-26
* Fix the issue that it is slow to get the CPU ID using the get_cpuid function.
* Support the dynamic-level-bytes parameter to improve the space collection situation.

## [1.0.4] - 2017-12-11
* Fix a possible performance issue when a snapshot is applied
* Fix the performance issue for reverse scan after removing a lot of data
* Fix the wrong encoded result for the Decimal type under special circumstances

## [1.0.2] - 2017-11-13
* Support splitting table to ensure one region does not contain data from multiple tables.
* Limit the length of a key to be no more than 4 KB.
* More accurate read traffic statistics.
* Implement deep protection on the coprocessor stack.
* Fix the LIKE behavior and the do_div_mod bug.

## [1.0.1] - 2017-10-30
* Support flow control with write bytes.
* Reduce Raft allocation.
* Increase coprocessor stack size to 10MB.
* Remove the useless log from coprocessor.

## [1.0.0] - 2017-10-15
* Coprocessor now supports more pushdown functions
* Support pushing down the sampling operation
* Support manually triggering data compact to collect space quickly
* Improve the performance and stability
* Add a Debug API for debugging
