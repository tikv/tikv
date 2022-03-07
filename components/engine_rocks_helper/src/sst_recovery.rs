// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::Bound::{Excluded, Unbounded};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use engine_rocks::raw::*;
use raftstore::store::fsm::StoreMeta;
use tikv_util::{self, set_panic_mark, worker::*};

pub const CHECK_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
struct FileInfo {
    // Corrupted file name. example: /000033.sst
    name: String,
    smallestkey: Vec<u8>,
    largestkey: Vec<u8>,
    overlap_region_ids: Vec<u64>,
    // Time to generate recovery task.
    instant: Instant,
}

pub struct RecoveryRunner {
    db: Arc<DB>,
    meta: Arc<Mutex<StoreMeta>>,
    // Considering that files will not be too much, it is enough to use `Vec`.
    scheduling_files: Vec<FileInfo>,
    check_duration: Duration,
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
        self.check_scheduling_files();
    }
}

impl RecoveryRunner {
    pub fn new(db: Arc<DB>, meta: Arc<Mutex<StoreMeta>>, check_duration: Duration) -> Self {
        RecoveryRunner {
            db,
            meta,
            scheduling_files: vec![],
            check_duration,
        }
    }

    fn generate_scheduling_tasks(&mut self, path: &str) {
        // resume rocksdb directly to avoid other write blocking.
        if !self.scheduling_files.is_empty() {
            println!("exist files:{:?}", self.scheduling_files);
            // The second resume will panic when corrupted files have not been removed.
            return;
        }
        self.db.resume().unwrap();

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
                    instant: Instant::now(),
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

    fn exist_scheduling_regions(&self, sst_path: &str) -> bool {
        for region in &self.scheduling_files {
            if region.name == sst_path {
                return true;
            }
        }
        false
    }

    // Acquire meta lock.
    fn check_scheduling_files(&mut self) {
        for index in 0..self.scheduling_files.len() {
            if self.scheduling_files[index].instant.elapsed() > 3 * self.check_duration {
                let f = self.scheduling_files.remove(index);
                let fname = f.name.clone();
                // file may be put into `scheduling_files` again here, but when this happens,
                // panic will be triggered, so it doesn’t matter.
                if self.check_overlap_damaged_regions(f) {
                    set_panic_mark_and_panic(
                        &fname,
                        &format!("recovery job exceeded {:?}", 3 * self.check_duration),
                    );
                }
            }
        }
    }
    // Check whether the StoreMeta contains the region range, if it contains,
    // recorded fault region ids to report to PD, if not, delete the file directly.
    //
    // Acquire meta lock.
    fn check_overlap_damaged_regions(&mut self, mut file: FileInfo) -> bool {
        let mut meta = self.meta.lock().unwrap();
        let mut overlap = false;

        // Find overlapping region ids.
        // Condition:
        // end_key > file.smallestkey
        // start_key < file.largestkey
        let mut ids = vec![];
        for (_, id) in meta
            .region_ranges
            .range((Excluded(file.smallestkey.clone()), Unbounded::<Vec<u8>>))
        {
            let region = &meta.regions[id];
            if keys::enc_start_key(region) < file.largestkey {
                ids.push(*id);
                file.overlap_region_ids.push(*id);
                overlap = true;
            }
        }

        if !overlap {
            // File must be in last level.
            // todo: consider delete_files_in_range
            self.db.delete_file(&file.name).unwrap();
            // No need to clean up `scheduling_files` again.
            for id in ids {
                meta.damaged_regions_id.remove(&id);
            }
        } else if !self.exist_scheduling_regions(&file.name) {
            // This file was detected for the first time.
            for id in ids {
                meta.damaged_regions_id.insert(id);
            }
            self.scheduling_files.push(file);
        }
        overlap
    }
}

fn set_panic_mark_and_panic(sst: &str, err: &str) {
    set_panic_mark();
    panic!("Failed to recover sst file: {}, error: {}", sst, err);
}
