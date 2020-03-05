// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::GcWorkerConfigManager;
use engine::rocks::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterFactory,
    DBCompactionFilter, WriteOptions, DB,
};
use engine_rocks::RocksWriteBatch;
use engine_traits::Mutable;
use engine_traits::WriteBatch;
use tikv_util::time::Instant;
use txn_types::{Key, WriteRef, WriteType};

const DEFAULT_DELETE_BATCH_SIZE: usize = 256 * 1024;

struct GcContext {
    db: Arc<DB>,
    safe_point: Arc<AtomicU64>,
    cfg_tracker: GcWorkerConfigManager,
}

lazy_static! {
    static ref GC_CONTEXT: Mutex<Option<GcContext>> = Mutex::new(None);
}

pub fn init_compaction_filter(
    db: Arc<DB>,
    safe_point: Arc<AtomicU64>,
    cfg_tracker: GcWorkerConfigManager,
) {
    info!("initialize GC context for compaction filter");
    let mut gc_context = GC_CONTEXT.lock().unwrap();
    *gc_context = Some(GcContext {
        db,
        safe_point,
        cfg_tracker,
    });
}

pub struct WriteCompactionFilterFactory;

impl CompactionFilterFactory for WriteCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        _context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let gc_context_option = GC_CONTEXT.lock().unwrap();
        let gc_context = match *gc_context_option {
            Some(ref ctx) => ctx,
            None => return std::ptr::null_mut(),
        };
        if !gc_context.cfg_tracker.value().enable_compaction_filter {
            return std::ptr::null_mut();
        }
        if gc_context.safe_point.load(Ordering::Relaxed) == 0 {
            // Safe point has not been initialized yet.
            return std::ptr::null_mut();
        }

        let name = CString::new("write_compaction_filter").unwrap();
        let db = Arc::clone(&gc_context.db);
        let safe_point = Arc::clone(&gc_context.safe_point);

        let filter = Box::new(WriteCompactionFilter::new(db, safe_point));
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct WriteCompactionFilter {
    safe_point: Arc<AtomicU64>,
    db: Arc<DB>,

    write_batch: RocksWriteBatch,
    key_prefix: Vec<u8>,
    remove_older: bool,

    versions: usize,
    rows: usize,
    stale_versions: usize,
    deleted: usize,
    default_deleted: usize,
    skipped: usize, // DELETE mark should be kept in compaction filter.
    start: Instant,
}

impl WriteCompactionFilter {
    fn new(db: Arc<DB>, safe_point: Arc<AtomicU64>) -> Self {
        let wb = RocksWriteBatch::with_capacity(Arc::clone(&db), DEFAULT_DELETE_BATCH_SIZE);
        WriteCompactionFilter {
            safe_point,
            db,
            write_batch: wb,
            key_prefix: vec![],
            remove_older: false,

            versions: 0,
            rows: 0,
            stale_versions: 0,
            deleted: 0,
            default_deleted: 0,
            skipped: 0,
            start: Instant::now_coarse(),
        }
    }

    fn delete_default_key(&self, key: &[u8]) {
        self.write_batch.delete(key).unwrap();
        if self.write_batch.data_size() > DEFAULT_DELETE_BATCH_SIZE {
            let mut opts = WriteOptions::new();
            opts.set_sync(false);
            self.db
                .write_opt(self.write_batch.as_inner(), &opts)
                .unwrap();
            self.write_batch.clear();
        }
    }
}

impl Drop for WriteCompactionFilter {
    fn drop(&mut self) {
        if !self.write_batch.is_empty() {
            let mut opts = WriteOptions::new();
            opts.set_sync(true);
            self.db
                .write_opt(self.write_batch.as_inner(), &opts)
                .unwrap();
        }
        info!(
            "WriteCompactionFilter uses {}s", self.start.elapsed_secs();
            "versions" => self.versions,
            "stale_versions" => self.stale_versions,
            "rows" => self.rows,
            "deleted" => self.deleted,
            "default_deleted" => self.default_deleted,
            "skipped" => self.skipped,
        );
        // GC_DELETED_VERSIONS.inc_by(self.deleted as i64);
    }
}

impl CompactionFilter for WriteCompactionFilter {
    fn filter(
        &mut self,
        _level: usize,
        key: &[u8],
        value: &[u8],
        _: &mut Vec<u8>,
        _: &mut bool,
    ) -> bool {
        let safe_point = self.safe_point.load(Ordering::Acquire);
        if safe_point == 0 {
            return false;
        }

        let (key_prefix, commit_ts) = match Key::split_on_ts_for(key) {
            Ok((key, ts)) => (key, ts),
            // Invalid MVCC keys, don't touch them.
            Err(_) => return false,
        };

        self.versions += 1;
        if self.key_prefix != key_prefix {
            self.key_prefix.clear();
            self.key_prefix.extend_from_slice(key_prefix);
            self.remove_older = false;
        }

        if commit_ts.into_inner() > safe_point {
            return false;
        }

        self.stale_versions += 1;
        let mut filtered = self.remove_older;
        let WriteRef {
            write_type,
            start_ts,
            short_value,
        } = WriteRef::parse(value).unwrap();
        if !self.remove_older {
            // here `filtered` must be false.
            match write_type {
                WriteType::Rollback | WriteType::Lock => filtered = true,
                WriteType::Delete => {
                    self.remove_older = true;
                    self.skipped += 1; // Currently `WriteType::Delete` will always be kept.
                }
                WriteType::Put => {
                    self.remove_older = true;
                    self.rows += 1;
                }
            }
        }

        if filtered {
            if short_value.is_none() {
                let key = Key::from_encoded_slice(key_prefix).append_ts(start_ts);
                self.delete_default_key(key.as_encoded());
                self.default_deleted += 1;
            }
            self.deleted += 1;
        }

        filtered
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::kv::RocksEngine;
    use engine::rocks::util::{compact_range, get_cf_handle};

    pub fn gc_by_compact(engine: &RocksEngine, _: &[u8], safe_point: u64) {
        let kv = engine.get_rocksdb();
        let safe_point = Arc::new(AtomicU64::new(safe_point));
        let cfg = GcWorkerConfigManager(Arc::new(Default::default()));
        cfg.0.update(|v| v.enable_compaction_filter = true);
        init_compaction_filter(Arc::clone(&kv), safe_point, cfg);
        let handle = get_cf_handle(&kv, "write").unwrap();
        compact_range(&kv, handle, None, None, false, 1);
    }
}
