# TiKV Change Log
All notable changes to this project are documented in this file.
See also [TiDB change log][tidb_change_log] and [PD change log][pd_change_log].

[tidb_change_log]: https://github.com/pingcap/tidb/blob/master/CHANGELOG.md
[pd_change_log]: https://github.com/pingcap/pd/blob/master/CHANGELOG.md

## [2.0.0] - 2018-04-27
### New Features
* `tikv-ctl` now can recover MVCC data corruption.
* Support column families for raw key/value API.
### Improvements
* Gc Raft logs cache eagerly to reduce memory usage.
### Bug Fixes
* Fix a `tikv-ctl` bug might cause TiKV panic.

## [2.0.0-rc6] - 2018-04-19
### New Features
### Improvements
* Reduce lock contention in Worker.
* Add metrics to the FuturePool.
### Bug Fixes
* Fix misused metrics in coprocessor.

## [2.0.0-rc.5] - 2018-04-17
### New Features
* Support compacting Regions in `tikv-ctl`.
* Add raw batch put/get/delete/scan API for TiKV service.
* Add ImportKV service.
* Support eval error in Coprocessor.
* Support dynamic adjustment of RocksDB cache size by `tikv-ctl`.
* Collect number of rows scanned for each range in Coprocessor.
* Support treating overflow as warning in Coprocessor.
* Support learner in raftstore.
### Improvements
* Increase snap GC timeout.

## [2.0.0-rc.4] - 2018-04-01
### New Features
* Limit the memory usage during receiving snapshots, to avoid OOM in extreme conditions.
* Support configuring the behavior of Coprocessor when it encounters warnings.
* Support importing the data pattern in TiKV.
* Support splitting Region in the middle.
### Improvements
* Fix the issue that too many logs are output caused by leader missing when TiKV is isolated
* Use crossbeam channel in worker.

## [2.0.0-rc.3] - 2018-03-23
### New Features
* Support Region Merge.
* Add the Raw DeleteRange API.
* Add the GetMetric API.
* Support streaming in Coprocessor.
* Support modifying RocksDB parameters online.
### Improvements
* Inform PD immediately once the Raft snapshot process is completed, to speed up balancing.
* Reduce the I/O fluctuation caused by RocksDB sync files.
* Optimize the space reclaiming mechanism after deleting data.
* Improve the data recovery tool `tikv-ctl`.
* Fix the issue that it is slow to make nodes down caused by snapshot
* Increase the raw_get/get/batch_get by 30% with ReadPool.
* Support configuring the request timeout of Coprocessor.
* Carry time information in Region heartbeats.
* Limit the space usage of snapshot files to avoid consuming too much disk space.
* Record and report the Regions that cannot elect a leader for a long time.
* Speed up garbage cleaning when starting the server.
* Update the size information about the corresponding Region according to compaction events.
* Limit the size of scan lock to avoid request timeout.
* Use DeleteRange to speed up Region deletion.

## [2.0.0-rc.2] - 2018-03-15
### New Features
* Implement IngestSST API.
* `tikv-ctl` now can send consistency-check requests to TiKV.
* Support dumping stats of RocksDB and malloc in `tikv-ctl`.
### Improvements
* Reclaim disk space after data have been deleted.

## [2.0.0-rc.1] - 2018-03-09
### New Features
* Protect important configuration which cannot be changed after initial configuration.
* Check whether SSD is used when you start the cluster.
### Improvements
* Fix the issue that gRPC call is not cancelled when PD leaders switch.
* Optimize the read performance using ReadPool, and improve the performance by 30% for raw get.
* Improve metrics and optimize the usage of metrics.

## [1.1.0-beta] - 2018-02-24
### Improvements
* Traverse locks using offset + limit to avoid potential GC problems.
* Support resolving locks in batches to improve GC speed.
* Support GC concurrency to improve GC speed.
* Update the Region size using the RocksDB compaction listener for more accurate PD scheduling.
* Delete the outdated data in batches using DeleteFilesInRanges, to make TiKV start faster.
* Configure the Raft snapshot max size to avoid the retained files taking up too much space.
* Support more recovery operations in tikv-ctl.
* Optimize the ordered flow aggregation operation.

## [1.0.8] - 2018-02-11
### Improvements
* Use DeleteFilesInRanges to clear stale data and improve the TiKV starting speed.
* Sync the metadata of the received Snapshot compulsorily to ensure its safety.
### Bug Fixes
* Using Decimal in Coprocessor sum.

## [1.0.7] - 2018-01-22
### Improvements
* Support key-only option in Table Scan executor.
* Support the remote mode in tikv-ctl.
* Fix the loss of scheduling command from PD.
### Bug Fixes
* Fix the format compatibility issue of tikv-ctl proto.
* Add timeout in Push metric.


## [1.1.0-alpha] - 2018-01-19
### New Features
* Support Raft learner.
* Support TLS.
### Improvements
* Optimize Raft Snapshot and reduce the I/O overhead.
* Optimize the RocksDB configuration to improve performance.
* Optimize count (*) and query performance of unique index in Coprocessor.
* Solve the reconnection issue between PD and TiKV
* Enhance the features of the data recovery tool `tikv-ctl`.
* Support the Delete Range feature.
* Support splitting according to table in Regions.
* Support setting the I/O limit caused by snapshot.
* Improve the flow control mechanism.

