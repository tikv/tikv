// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use engine_rocks::raw::*;
use fail::fail_point;
use raftstore::store::fsm::StoreMeta;
use tikv_util::{self, set_panic_mark, warn, worker::*};

use crate::metric::*;

pub const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(10);
const MAX_DAMAGED_FILES_NUM: usize = 2;

pub struct RecoveryRunner {
    db: Arc<DB>,
    store_meta: Arc<Mutex<StoreMeta>>,
    // Considering that files will not be too much, it is enough to use `Vec`.
    damaged_files: Vec<FileInfo>,
    max_hang_duration: Duration,
    check_duration: Duration,
}

#[derive(Clone)]
struct FileInfo {
    // Corrupted file name. example: /000033.sst
    name: String,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
    // Time to generate recovery task, be used to record whether the timeout
    start_time: Instant,
}

impl Debug for FileInfo {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(
            fmt,
            "name:{:?}, smallest_key:{:?}, largest_key:{:?}, elapsed_secs:{}",
            self.name,
            self.smallest_key,
            self.largest_key,
            self.start_time.elapsed().as_secs_f64(),
        )
    }
}

impl Runnable for RecoveryRunner {
    type Task = String;

    fn run(&mut self, sst: String) {
        self.generate_scheduling_tasks(&sst);
    }
}

impl RunnableWithTimer for RecoveryRunner {
    fn get_interval(&self) -> Duration {
        self.check_duration
    }

    fn on_timeout(&mut self) {
        self.check_damaged_files();
    }
}

impl RecoveryRunner {
    pub fn new(
        db: Arc<DB>,
        store_meta: Arc<Mutex<StoreMeta>>,
        max_hang_duration: Duration,
        check_duration: Duration,
    ) -> Self {
        RecoveryRunner {
            db,
            store_meta,
            damaged_files: vec![],
            max_hang_duration,
            check_duration,
        }
    }

    fn generate_scheduling_tasks(&mut self, path: &str) {
        if self.exist_scheduling_regions(path) {
            return;
        }

        let live_files = self.db.get_live_files();
        for i in 0..live_files.get_files_count() {
            if path == live_files.get_name(i as i32) {
                let f = FileInfo {
                    name: live_files.get_name(i as i32),
                    smallest_key: live_files.get_smallestkey(i as i32).to_owned(),
                    largest_key: live_files.get_largestkey(i as i32).to_owned(),
                    start_time: Instant::now(),
                };

                // only support recovering data key for now.
                if !keys::validate_data_key(&f.smallest_key)
                    || (!keys::validate_data_key(&f.largest_key)
                        && f.largest_key.as_slice() != keys::DATA_MAX_KEY)
                {
                    self.set_panic_mark_and_panic(
                        path,
                        &format!(
                            "key range mismatch, smallest key:{:?}, largest key:{:?}",
                            &f.smallest_key, &f.largest_key
                        ),
                    );
                }

                // defensive behavior
                if self.damaged_files.len() >= MAX_DAMAGED_FILES_NUM {
                    self.set_panic_mark_and_panic(path, "too many damaged files detected");
                }

                // lock the store_meta and check if the range exist.
                if self.check_overlap_damaged_regions(&f) {
                    self.damaged_files.push(f);
                    TIKV_ROCKSDB_DAMAGED_FILES.inc();
                }

                return;
            }
        }
    }

    // `sst_path` has been processed and is still in a damaged state.
    fn exist_scheduling_regions(&self, sst_path: &str) -> bool {
        self.damaged_files.iter().any(|f| f.name == sst_path)
    }

    // Cleans up obsolete damaged files and panics if some files are not handled in time.
    fn check_damaged_files(&mut self) {
        if self.damaged_files.is_empty() {
            return;
        }

        let mut new_damaged_files = self.damaged_files.clone();
        new_damaged_files.retain(|f| {
            if f.start_time.elapsed() > self.max_hang_duration {
                self.set_panic_mark_and_panic(
                    &f.name,
                    &format!("recovery job exceeded {:?}", self.max_hang_duration),
                );
            }
            self.check_overlap_damaged_regions(f)
        });
        TIKV_ROCKSDB_DAMAGED_FILES.set(new_damaged_files.len() as i64);
        self.damaged_files = new_damaged_files;
    }

    // Check whether the StoreMeta contains the region range, if it contains,
    // recorded fault region ids to report to PD and add file info into `damaged_files`.
    //
    // Acquire meta lock.
    fn check_overlap_damaged_regions(&self, file: &FileInfo) -> bool {
        let mut meta = self.store_meta.lock().unwrap();

        let overlap =
            meta.update_overlap_damaged_ranges(&file.name, &file.smallest_key, &file.largest_key);
        if !overlap {
            fail_point!("sst_recovery_before_delete_files");
            // The sst file can be deleted safely and set `include_end` to `true` otherwise the
            // file with the same largest key will be skipped.
            // Here store meta lock should be held to prevent peers from being added back.
            self.db
                .delete_files_in_range(&file.smallest_key, &file.largest_key, true)
                .unwrap();
            self.must_file_not_exist(&file.name);

            TIKV_ROCKSDB_DAMAGED_FILES_DELETED.inc();
            warn!(
                "damaged file has been deleted";
                "file" => &file.name,
                "smallest_key" => ?&file.smallest_key,
                "largest_key" => ?&file.largest_key,
            );
        }

        overlap
    }

    fn set_panic_mark_and_panic(&self, sst: &str, err: &str) {
        set_panic_mark();
        panic!(
            "Failed to recover sst file: {}, error: {}, damaged_files:{:?}",
            sst, err, self.damaged_files
        );
    }

    fn must_file_not_exist(&self, fname: &str) {
        let live_files = self.db.get_live_files();
        for i in 0..live_files.get_files_count() {
            if live_files.get_name(i as i32) == fname {
                // `delete_files_in_range` can't delete L0 files.
                self.set_panic_mark_and_panic(fname, "file still exists, it may belong L0");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use engine_rocks::raw_util;
    use kvproto::metapb::{Peer, Region};
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_sst_recovery_runner_check_overlap() {
        let path = Builder::new()
            .prefix("test_sst_recovery_runner")
            .tempdir()
            .unwrap();
        let db = Arc::new(
            raw_util::new_engine(path.path().to_str().unwrap(), None, &["cf"], None).unwrap(),
        );

        db.put(b"z2", b"val").unwrap();
        db.put(b"z7", b"val").unwrap();
        // generate SST file.
        db.compact_range(None, None);

        let files = db.get_live_files();
        assert_eq!(files.get_smallestkey(0), b"z2");
        assert_eq!(files.get_largestkey(0), b"z7");

        // create r1 [z0, z2] r2 [z1, z3] r3 [z2, z4] r4 [z7, z8] r5 [z8 z9] to test.
        let mut region_ranges = BTreeMap::<Vec<u8>, u64>::new();
        region_ranges.insert(b"z2".to_vec(), 1);
        region_ranges.insert(b"z3".to_vec(), 2);
        region_ranges.insert(b"z4".to_vec(), 3);
        region_ranges.insert(b"z8".to_vec(), 4);
        region_ranges.insert(b"z9".to_vec(), 5);
        let mut store_meta = StoreMeta::new(10);
        store_meta.region_ranges = region_ranges;

        add_region_to_store_meta(&mut store_meta, 1, b"0".to_vec());
        add_region_to_store_meta(&mut store_meta, 2, b"1".to_vec());
        add_region_to_store_meta(&mut store_meta, 3, b"2".to_vec());
        add_region_to_store_meta(&mut store_meta, 4, b"7".to_vec());
        add_region_to_store_meta(&mut store_meta, 5, b"8".to_vec());

        let meta = Arc::new(Mutex::new(store_meta));
        let runner = RecoveryRunner::new(
            db,
            meta.clone(),
            Duration::from_millis(200),
            Duration::from_millis(100),
        );
        let mut worker = LazyWorker::new("abc");
        worker.start_with_timer(runner);
        let tx = worker.scheduler();
        tx.schedule(files.get_name(0)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));

        let damaged_ids = meta.lock().unwrap().get_all_damaged_region_ids();
        assert_eq!(damaged_ids.len(), 3);
        assert!(damaged_ids.contains(&2));
        assert!(damaged_ids.contains(&3));
        assert!(damaged_ids.contains(&4));
    }

    fn add_region_to_store_meta(meta: &mut StoreMeta, id: u64, start_key: Vec<u8>) {
        let mut r = Region::new();
        r.set_id(id);
        r.set_start_key(start_key);
        let peers = vec![Peer::default()];
        r.set_peers(peers.into());
        meta.regions.insert(id, r);
    }
}
