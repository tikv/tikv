use crate::row::collector::GLOBAL_COLLOCTERS;

use collections::HashMap;
use std::cell::RefCell;
use std::ops::{Deref, Sub};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tikv_util::time::Instant;

pub fn add_thread_scan_row(count: u64) {
    CURRENT_REQ_ROW.with(|r| {
        r.row_stats.borrow_mut().read_row_count += count;
    })
}

pub fn add_thread_scan_index(count: u64) {
    CURRENT_REQ_ROW.with(|r| {
        r.row_stats.borrow_mut().read_index_count += count;
    })
}

pub fn on_poll_begin() {
    CURRENT_REQ_ROW.with(|r| {
        r.row_stats.borrow_mut().reset();
    })
}

pub fn on_poll_finish(tag: Vec<u8>) {
    CURRENT_REQ_ROW.with(|r| {
        let row_stats = r.row_stats.borrow();
        if row_stats.read_row_count > 0 || row_stats.read_index_count > 0 {
            r.row_records
                .borrow_mut()
                .records
                .entry(tag)
                .or_insert(RowStats::default())
                .merge(row_stats.deref());
            println!("add row stats， {:?}", row_stats.deref());
        }

        let duration = r.last_collect_instant.borrow().saturating_elapsed();
        if duration.as_millis() > 1000 {
            println!("send row stats start");
            let row_records = r.row_records.borrow_mut().take_and_reset();
            let row_records = Arc::new(row_records);
            {
                let guard = GLOBAL_COLLOCTERS.lock().unwrap();
                for (_id, collector) in guard.iter() {
                    collector.collect_row(row_records.clone());
                }
            }

            *r.last_collect_instant.borrow_mut() = Instant::now();
            println!("send row stats");
        }
    })
}

pub struct LocalReqRowTag {
    last_collect_instant: RefCell<Instant>,
    row_stats: RefCell<RowStats>,
    row_records: RefCell<RowRecords>,
}

thread_local! {
    pub static CURRENT_REQ_ROW: LocalReqRowTag = LocalReqRowTag{
        last_collect_instant: RefCell::new(Instant::now()),
        row_stats: RefCell::new(RowStats::default()),
        row_records: RefCell::new(RowRecords::default()),
    }
}

#[derive(Debug, Default)]
pub struct RowStats {
    pub read_index_count: u64,
    pub read_row_count: u64,
}

impl RowStats {
    pub fn reset(&mut self) {
        self.read_row_count = 0;
        self.read_index_count = 0;
    }

    pub fn merge(&mut self, other: &Self) {
        self.read_row_count += other.read_row_count;
        self.read_index_count += other.read_index_count;
    }
}

#[derive(Debug)]
pub struct RowRecords {
    pub begin_unix_time_secs: u64,

    // tag -> RowStats
    pub records: HashMap<Vec<u8>, RowStats>,
}

impl Default for RowRecords {
    fn default() -> Self {
        let now_unix_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Clock may have gone backwards");
        Self {
            begin_unix_time_secs: now_unix_time.as_secs(),
            records: HashMap::default(),
        }
    }
}

impl RowRecords {
    fn take_and_reset(&mut self) -> Self {
        let mut records = HashMap::default();
        for (k, v) in self.records.drain() {
            records.insert(k, v);
        }
        let old_begin_unix_time_secs = self.begin_unix_time_secs;
        let now_unix_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Clock may have gone backwards");
        self.begin_unix_time_secs = now_unix_time.as_secs();
        Self {
            begin_unix_time_secs: old_begin_unix_time_secs,
            records,
        }
    }
}
