# TiKV Change Log

## 2.0.0-rc.5
### New Features
* Support compacting regions in tikv-ctl.
* Add raw batch put/get/delete/scan API for TiKV service.
* Add ImportKV service.
* Support eval error in Coprocessor.
* Support dynamic adjustment of rocksdb cache size by tikv-ctl.
* Collect number of rows scanned for each range in Coprocessor.
* Support treating overflow as warning in Coprocessor.
* Support learner in raftstore.
### Improves
* Increase snap gc timeout.
### Bug Fixes

## 2.0.0-rc.4
### New Features
* Limit the memory usage during receiving snapshots, to avoid OOM in extreme conditions.
* Support configuring the behavior of Coprocessor when it encounters warnings.
* Support importing the data pattern in TiKV.
* Support splitting Region in the middle.
### Improves
* Fix the issue that too many logs are output caused by leader missing when TiKV is isolated
* Use crossbeam channel in worker.
### Bug Fixes

## 2.0.0-rc.3
### New Features
* Support Region Merge.
* Add the Raw DeleteRange API.
* Add the GetMetric API.
* Support streaming in Coprocessor.
* Support modifying RocksDB parameters online.
### Improves
* Inform PD immediately once the Raft snapshot process is completed, to speed up balancing.
* Reduce the I/O fluctuation caused by RocksDB sync files.
* Optimize the space reclaiming mechanism after deleting data.
* Improve the data recovery tool tikv-ctl.
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
