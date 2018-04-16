# TiKV Change Log

## Unreleased

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
* Support configuring the behavior of Coprocessor when it encounters warnings
* Support importing the data pattern in TiKV
* Support splitting Region in the middle
### Improves
* Fix the issue that too many logs are output caused by leader missing when TiKV is isolated
* Use crossbeam channel in worker.
### Bug Fixes
