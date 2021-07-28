use crate::row::collector::GLOBAL_COLLOCTERS;

use collections::HashMap;
use std::cell::RefCell;
use std::ops::Deref;
use std::sync::Arc;
use tikv_util::time::Instant;

pub fn inc_thread_scan_row() {
    CURRENT_REQ_ROW.with(|r| {
        r.row_stats.borrow_mut().read_row_count += 1;
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
            r.row_stats_map
                .borrow_mut()
                .entry(tag)
                .or_insert(RowStats::default())
                .merge(row_stats.deref());
            println!("add row stats， {:?}", row_stats.deref());
        }

        let duration = r.last_collect_instant.borrow().saturating_elapsed();
        if duration.as_millis() > 1000 {
            println!("send row stats start");
            let mut row_stats_map = HashMap::default();
            for (k, v) in r.row_stats_map.borrow_mut().drain() {
                row_stats_map.insert(k, v);
            }
            let row_stats_map = Arc::new(row_stats_map);
            {
                let guard = GLOBAL_COLLOCTERS.lock().unwrap();
                for (_id, collector) in guard.iter() {
                    collector.collect_row(row_stats_map.clone());
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
    row_stats_map: RefCell<HashMap<Vec<u8>, RowStats>>,
}

thread_local! {
    pub static CURRENT_REQ_ROW: LocalReqRowTag = LocalReqRowTag{
        last_collect_instant: RefCell::new(Instant::now()),
        row_stats: RefCell::new(RowStats::default()),
        row_stats_map: RefCell::new(HashMap::default()),
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
