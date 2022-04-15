// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::Bound::{Excluded, Unbounded};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use engine_rocks::raw::*;
use raftstore::store::fsm::StoreMeta;
use tikv_util::{self, set_panic_mark, worker::*};

const CHECK_INTERVAL: Duration = Duration::from_secs(10);

pub struct RecoveryRunner {
    db: Arc<DB>,
    store_meta: Arc<Mutex<StoreMeta>>,
    // Considering that files will not be too much, it is enough to use `Vec`.
    damaged_files: Vec<FileInfo>,
    max_damage_duration: Duration,
}

#[derive(Debug, Clone)]
struct FileInfo {
    // Corrupted file name. example: /000033.sst
    name: String,
    smallestkey: Vec<u8>,
    largestkey: Vec<u8>,
    overlap_region_ids: Vec<u64>,
    // Time to generate recovery task, be used to record whether the timeout
    start_time: Instant,
}

impl Runnable for RecoveryRunner {
    type Task = String;

    fn run(&mut self, sst: String) {
        self.generate_scheduling_tasks(&sst);
    }
}

impl RunnableWithTimer for RecoveryRunner {
    fn get_interval(&self) -> Duration {
        CHECK_INTERVAL
    }

    fn on_timeout(&mut self) {
        self.check_damaged_files();
    }
}

impl RecoveryRunner {
    pub fn new(db: Arc<DB>, store_meta: Arc<Mutex<StoreMeta>>, check_duration: Duration) -> Self {
        RecoveryRunner {
            db,
            store_meta,
            damaged_files: vec![],
            max_damage_duration: check_duration,
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
                    smallestkey: live_files.get_smallestkey(i as i32).to_owned(),
                    largestkey: live_files.get_largestkey(i as i32).to_owned(),
                    overlap_region_ids: Vec::new(),
                    start_time: Instant::now(),
                };

                // only support recovering data key for now.
                if !keys::validate_data_key(&f.smallestkey)
                    || (!keys::validate_data_key(&f.largestkey)
                        && f.largestkey.as_slice() != keys::DATA_MAX_KEY)
                {
                    set_panic_mark_and_panic(
                        path,
                        &format!(
                            "key range mismatch, small key:{:?}, large key:{:?}",
                            &f.smallestkey, &f.largestkey
                        ),
                    );
                }

                // lock the store_meta and check if the range exist.
                if !self.check_overlap_damaged_regions(f) {
                    set_panic_mark_and_panic(path, "Can't find overlap regions");
                }

                return;
            }
        }
    }

    // `sst_path` has been processed and is still in a damaged state.
    fn exist_scheduling_regions(&self, sst_path: &str) -> bool {
        self.damaged_files.iter().any(|f| f.name == sst_path)
    }

    // Periodically check for recorded damaged files。
    //
    // Acquire meta lock.
    fn check_damaged_files(&mut self) {
        for index in 0..self.damaged_files.len() {
            if self.damaged_files[index].start_time.elapsed() > self.max_damage_duration {
                let f = self.damaged_files.remove(index);
                let fname = f.name.clone();
                // file may be put into `damaged_files` again here, but when this happens,
                // panic will be triggered, so it doesn’t matter.
                if self.check_overlap_damaged_regions(f) {
                    set_panic_mark_and_panic(
                        &fname,
                        &format!("recovery job exceeded {:?}", self.max_damage_duration),
                    );
                }
            }
        }
    }

    // Check whether the StoreMeta contains the region range, if it contains,
    // recorded fault region ids to report to PD and add file info into `damaged_files`.
    //
    // Acquire meta lock.
    fn check_overlap_damaged_regions(&mut self, mut file: FileInfo) -> bool {
        let mut meta = match self.store_meta.lock() {
            Ok(meta) => meta,
            Err(e) => {
                set_panic_mark_and_panic(&file.name, &format!("{}", e));
                return false;
            }
        };

        let mut overlap = false;

        // Find overlapping region ids.
        // Condition:
        // end_key > file.smallestkey
        // start_key <= file.largestkey
        let mut ids = vec![];
        for (_, id) in meta
            .region_ranges
            .range((Excluded(file.smallestkey.clone()), Unbounded::<Vec<u8>>))
        {
            let region = &meta.regions[id];
            if keys::enc_start_key(region) <= file.largestkey {
                ids.push(*id);
                file.overlap_region_ids.push(*id);
                overlap = true;
            }
        }

        if !overlap {
            // TODO: User trigger to delete sst file here. `delete_file` only allow
            // delete files in the last level, consider `delete_files_in_range`.
            for id in ids {
                meta.damaged_regions_id.remove(&id);
            }
        } else if !self.exist_scheduling_regions(&file.name) {
            // This sst file was detected for the first time.
            for id in ids {
                meta.damaged_regions_id.insert(id);
            }
            self.damaged_files.push(file);
        }
        overlap
    }
}

fn set_panic_mark_and_panic(sst: &str, err: &str) {
    set_panic_mark();
    panic!("Failed to recover sst file: {}, error: {}", sst, err);
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_rocks::raw_util;
    use kvproto::metapb::{Peer, Region};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tempfile::Builder;

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
        // generate SST file range from z3 to z8
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
        let runner = RecoveryRunner::new(db, meta.clone(), Duration::from_millis(100));
        let mut worker = LazyWorker::new("abc");
        worker.start_with_timer(runner);
        let tx = worker.scheduler();
        tx.schedule(files.get_name(0)).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));

        assert_eq!(meta.lock().unwrap().damaged_regions_id.len(), 3);
        assert!(meta.lock().unwrap().damaged_regions_id.get(&2).is_some());
        assert!(meta.lock().unwrap().damaged_regions_id.get(&3).is_some());
        assert!(meta.lock().unwrap().damaged_regions_id.get(&4).is_some());
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
