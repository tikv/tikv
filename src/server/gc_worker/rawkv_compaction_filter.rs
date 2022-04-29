// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::server::gc_worker::compaction_filter::{
    CompactionFilterStats, DEFAULT_DELETE_BATCH_COUNT, GC_COMPACTION_FAILURE,
    GC_COMPACTION_FILTERED, GC_COMPACTION_FILTER_ORPHAN_VERSIONS, GC_COMPACTION_MVCC_ROLLBACK,
    GC_CONTEXT,
};
use crate::server::gc_worker::GcTask;
use crate::storage::mvcc::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM};
use api_version::api_v2::RAW_KEY_PREFIX;
use api_version::{ApiV2, KvFormat};
use engine_rocks::raw::{
    new_compaction_filter_raw, CompactionFilter, CompactionFilterContext, CompactionFilterDecision,
    CompactionFilterFactory, CompactionFilterValueType, DBCompactionFilter,
};
use engine_rocks::RocksEngine;
use engine_traits::raw_ttl::ttl_current_ts;
use engine_traits::MiscExt;
use prometheus::local::LocalHistogram;
use raftstore::coprocessor::RegionInfoProvider;
use std::ffi::CString;
use std::mem;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tikv_util::worker::{ScheduleError, Scheduler};
use txn_types::Key;

pub struct RawCompactionFilterFactory;

impl CompactionFilterFactory for RawCompactionFilterFactory {
    fn create_compaction_filter(
        &self,
        context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        //---------------- GC context --------------
        let gc_context_option = GC_CONTEXT.lock().unwrap();
        let gc_context = match *gc_context_option {
            Some(ref ctx) => ctx,
            None => return std::ptr::null_mut(),
        };
        //---------------- GC context END --------------

        let db = gc_context.db.clone();
        let gc_scheduler = gc_context.gc_scheduler.clone();
        let store_id = gc_context.store_id;
        let region_info_provider = gc_context.region_info_provider.clone();

        let current = ttl_current_ts();
        let safe_point = gc_context.safe_point.load(Ordering::Relaxed);
        if safe_point == 0 {
            // Safe point has not been initialized yet.
            debug!("skip gc in compaction filter because of no safe point");
            return std::ptr::null_mut();
        }

        let filter = RawCompactionFilter::new(
            db,
            safe_point,
            gc_scheduler,
            current,
            context,
            (store_id, region_info_provider),
        );
        let name = CString::new("raw_compaction_filter").unwrap();
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

struct RawCompactionFilter {
    safe_point: u64,
    engine: RocksEngine,
    gc_scheduler: Scheduler<GcTask<RocksEngine>>,
    current_ts: u64,
    mvcc_key_prefix: Vec<u8>,
    mvcc_deletions: Vec<Key>,
    regions_provider: (u64, Arc<dyn RegionInfoProvider>),

    // Some metrics about implementation detail.
    versions: usize,
    filtered: usize,
    total_versions: usize,
    total_filtered: usize,
    orphan_versions: usize,
    versions_hist: LocalHistogram,
    filtered_hist: LocalHistogram,

    encountered_errors: bool,
}

thread_local! {
    static STATS: CompactionFilterStats = CompactionFilterStats::default();
}

impl Drop for RawCompactionFilter {
    // NOTE: it's required that `CompactionFilter` is dropped before the compaction result
    // becomes installed into the DB instance.
    fn drop(&mut self) {
        self.raw_gc_mvcc_deletions();

        self.engine.sync_wal().unwrap();

        self.switch_key_metrics();
        self.flush_metrics();
    }
}

impl CompactionFilter for RawCompactionFilter {
    fn featured_filter(
        &mut self,
        level: usize,
        key: &[u8],
        sequence: u64,
        value: &[u8],
        value_type: CompactionFilterValueType,
    ) -> CompactionFilterDecision {
        if self.encountered_errors {
            // If there are already some errors, do nothing.
            return CompactionFilterDecision::Keep;
        }

        match self.do_filter(level, key, sequence, value, value_type) {
            Ok(decision) => decision,
            Err(e) => {
                warn!("compaction filter meet error: {}", e);
                GC_COMPACTION_FAILURE.with_label_values(&["filter"]).inc();
                self.encountered_errors = true;
                CompactionFilterDecision::Keep
            }
        }
    }
}

impl RawCompactionFilter {
    fn new(
        engine: RocksEngine,
        safe_point: u64,
        gc_scheduler: Scheduler<GcTask<RocksEngine>>,
        ts: u64,
        _context: &CompactionFilterContext,
        regions_provider: (u64, Arc<dyn RegionInfoProvider>),
    ) -> Self {
        // Safe point must have been initialized.
        assert!(safe_point > 0);
        debug!("gc in compaction filter"; "safe_point" => safe_point);
        RawCompactionFilter {
            safe_point,
            engine,
            gc_scheduler,
            current_ts: ts,
            mvcc_key_prefix: vec![],
            mvcc_deletions: Vec::with_capacity(DEFAULT_DELETE_BATCH_COUNT),
            regions_provider,

            versions: 0,
            filtered: 0,
            total_versions: 0,
            total_filtered: 0,
            orphan_versions: 0,
            versions_hist: MVCC_VERSIONS_HISTOGRAM.local(),
            filtered_hist: GC_DELETE_VERSIONS_HISTOGRAM.local(),

            encountered_errors: false,
        }
    }

    fn do_filter(
        &mut self,
        _start_level: usize,
        key: &[u8],
        _sequence: u64,
        value: &[u8],
        value_type: CompactionFilterValueType,
    ) -> Result<CompactionFilterDecision, String> {
        if !key.starts_with(keys::DATA_PREFIX_KEY) {
            return Ok(CompactionFilterDecision::Keep);
        }

        let (mvcc_key_prefix, commit_ts) = split_ts(key)?;

        // not RawKV or targetValue, skip
        let key_mode = key[1];
        if key_mode != RAW_KEY_PREFIX || value_type != CompactionFilterValueType::Value {
            return Ok(CompactionFilterDecision::Keep);
        }

        if self.mvcc_key_prefix != mvcc_key_prefix {
            self.switch_key_metrics();
            self.mvcc_key_prefix.clear();
            self.mvcc_key_prefix.extend_from_slice(mvcc_key_prefix);
            if commit_ts >= self.safe_point {
                return Ok(CompactionFilterDecision::Keep);
            }
            let raw_value = ApiV2::decode_raw_value(value)?;
            // the lastest version ,and it's deleted or expaired ttl , need to be send to async gc task
            if !raw_value.is_valid(self.current_ts) {
                self.raw_handle_bottommost_delete();
                if self.mvcc_deletions.len() >= DEFAULT_DELETE_BATCH_COUNT {
                    self.raw_gc_mvcc_deletions();
                }
                // the lastest version ,and it's deleted or expaired ttl , need to be send to async gc task
            }
            // the lastest version ,and not deleted or expaired ttl , need to be retained
            Ok(CompactionFilterDecision::Keep)
        } else {
            if commit_ts >= self.safe_point {
                return Ok(CompactionFilterDecision::Keep);
            }

            self.filtered += 1;
            // ts < safepoint ,and it's not the lastest version, it's need to be removed.
            Ok(CompactionFilterDecision::Remove)
        }
    }

    fn raw_gc_mvcc_deletions(&mut self) {
        if !self.mvcc_deletions.is_empty() {
            let empty = Vec::with_capacity(DEFAULT_DELETE_BATCH_COUNT);
            let task = GcTask::RawGcKeys {
                keys: mem::replace(&mut self.mvcc_deletions, empty), //gc_keys->gc_key->gc->gc.run
                safe_point: self.safe_point.into(),
                store_id: self.regions_provider.0,
                region_info_provider: self.regions_provider.1.clone(),
            };
            self.schedule_gc_task(task, false);
        }
    }

    // `log_on_error` indicates whether to print an error log on scheduling failures.
    // It's only enabled for `GcTask::OrphanVersions`.
    fn schedule_gc_task(&self, task: GcTask<RocksEngine>, log_on_error: bool) {
        match self.gc_scheduler.schedule(task) {
            Ok(_) => {}
            Err(e) => {
                if log_on_error {
                    error!("compaction filter schedule {} fail", e);
                }
                match e {
                    ScheduleError::Full(_) => {
                        GC_COMPACTION_FAILURE.with_label_values(&["full"]).inc();
                    }
                    ScheduleError::Stopped(_) => {
                        GC_COMPACTION_FAILURE.with_label_values(&["stopped"]).inc();
                    }
                }
            }
        }
    }

    fn raw_handle_bottommost_delete(&mut self) {
        // Valid MVCC records should begin with `DATA_PREFIX`.
        debug!("raw_handle_bottommost_delete:");
        debug_assert_eq!(self.mvcc_key_prefix[0], keys::DATA_PREFIX);
        let key = Key::from_encoded_slice(&self.mvcc_key_prefix[1..]);
        self.mvcc_deletions.push(key);
    }

    fn switch_key_metrics(&mut self) {
        if self.versions != 0 {
            self.versions_hist.observe(self.versions as f64);
            self.total_versions += self.versions;
            self.versions = 0;
        }
        if self.filtered != 0 {
            self.filtered_hist.observe(self.filtered as f64);
            self.total_filtered += self.filtered;
            self.filtered = 0;
        }
    }

    fn flush_metrics(&self) {
        GC_COMPACTION_FILTERED.inc_by(self.total_filtered as u64);
        GC_COMPACTION_FILTER_ORPHAN_VERSIONS
            .with_label_values(&["generated"])
            .inc_by(self.orphan_versions as u64);
        if let Some((versions, filtered)) = STATS.with(|stats| {
            stats.versions.update(|x| x + self.total_versions);
            stats.filtered.update(|x| x + self.total_filtered);
            if stats.need_report() {
                return Some(stats.prepare_report());
            }
            None
        }) {
            if filtered > 0 {
                info!("RawKV Compaction filter reports"; "total" => versions, "filtered" => filtered);
            }
        }
    }
}

pub fn split_ts(key: &[u8]) -> Result<(&[u8], u64), String> {
    match Key::split_on_ts_for(key) {
        Ok((key, ts)) => Ok((key, ts.into_inner())),
        Err(_) => Err(format!(
            "invalid write cf key: {}",
            log_wrappers::Value(key)
        )),
    }
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use api_version::RawValue;
    use kvproto::kvrpcpb::ApiVersion;
    use std::thread;

    use crate::config::DbConfig;
    use crate::server::gc_worker::TestGCRunner;
    use crate::storage::kv::TestEngineBuilder;
    use engine_traits::{Peekable, SyncMutable, CF_DEFAULT};
    use std::time::Duration;
    use txn_types::TimeStamp;

    pub fn make_key(key: &[u8], ts: u64) -> Vec<u8> {
        let key1 = Key::from_raw(key)
            .append_ts(TimeStamp::new(ts))
            .as_encoded()
            .to_vec();
        let res = keys::data_key(key1.as_slice());
        res
    }

    #[test]
    fn test_raw_compaction_filter() {
        let mut cfg = DbConfig::default();
        cfg.writecf.disable_auto_compactions = true;
        cfg.writecf.dynamic_level_bytes = false;

        let engine = TestEngineBuilder::new()
            .api_version(ApiVersion::V2)
            .build_with_cfg(&cfg)
            .unwrap();
        let raw_engine = engine.get_rocksdb();
        let mut gc_runner = TestGCRunner::new(0);

        let value1 = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(TimeStamp::max().physical()),
            is_delete: false,
        };

        let user_key = b"r\0aaaaaaaaaaa";

        raw_engine
            .put_cf(
                CF_DEFAULT,
                make_key(user_key, 100).as_slice(),
                &ApiV2::encode_raw_value_owned(value1.clone()),
            )
            .unwrap();
        raw_engine
            .put_cf(
                CF_DEFAULT,
                make_key(user_key, 90).as_slice(),
                &ApiV2::encode_raw_value_owned(value1.clone()),
            )
            .unwrap();
        raw_engine
            .put_cf(
                CF_DEFAULT,
                make_key(user_key, 70).as_slice(),
                &ApiV2::encode_raw_value_owned(value1),
            )
            .unwrap();

        gc_runner.safe_point(80).gc_raw(&raw_engine);
        //Wait gc  end
        thread::sleep(Duration::from_millis(1000));

        // 70 < safepoint(80), this version was removed
        let isexit70 = raw_engine
            .get_value_cf(CF_DEFAULT, make_key(b"r\0a", 70).as_slice())
            .unwrap()
            .is_none();
        assert_eq!(isexit70, true);

        gc_runner.safe_point(90).gc_raw(&raw_engine);
        // Wait gc end
        thread::sleep(Duration::from_millis(1000));

        let isexit100 = raw_engine
            .get_value_cf(CF_DEFAULT, make_key(user_key, 100).as_slice())
            .unwrap()
            .is_none();
        let isexit90 = raw_engine
            .get_value_cf(CF_DEFAULT, make_key(user_key, 90).as_slice())
            .unwrap()
            .is_none();

        // ts(100) > safepoint(80), need to be retained.
        assert_eq!(isexit100, false);

        // ts(90) == safepoint(90), need to be retained.
        assert_eq!(isexit90, false);
    }

    fn split_ts(key: &[u8]) -> Result<(&[u8], u64), String> {
        match Key::split_on_ts_for(key) {
            Ok((key, ts)) => Ok((key, ts.into_inner())),
            Err(_) => Err(format!(
                "invalid write cf key: {}",
                log_wrappers::Value(key)
            )),
        }
    }

    #[test]
    fn test_raw_call_gctask() {
        let engine = TestEngineBuilder::new()
            .api_version(ApiVersion::V2)
            .build()
            .unwrap();
        let raw_engine = engine.get_rocksdb();
        let mut gc_runner = TestGCRunner::new(0);

        let mut gc_and_check = |expect_tasks: bool, prefix: &[u8]| {
            gc_runner.safe_point(500).gc_raw(&raw_engine);

            // Wait up to 1 second, and treat as no task if timeout.
            if let Ok(Some(task)) = gc_runner.gc_receiver.recv_timeout(Duration::new(1, 0)) {
                assert!(expect_tasks, "a GC task is expected");
                match task {
                    GcTask::RawGcKeys { keys, .. } => {
                        assert_eq!(keys.len(), 1);
                        let got = keys[0].as_encoded();
                        let expect = Key::from_encoded_slice(prefix);
                        assert_eq!(got, &expect.as_encoded()[1..]);
                    }
                    _ => unreachable!(),
                }
                return;
            }
            assert!(!expect_tasks, "no GC task is expected");
        };

        let value1 = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(TimeStamp::max().into_inner()),
            is_delete: false,
        };

        let value_is_delete = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(10),
            is_delete: true,
        };

        let user_key_del = b"r\0aaaaaaaaaaa";

        // is_delete = true , it will call scheduler gctask
        raw_engine
            .put_cf(
                CF_DEFAULT,
                make_key(user_key_del, 9).as_slice(),
                &ApiV2::encode_raw_value_owned(value_is_delete),
            )
            .unwrap();
        raw_engine
            .put_cf(
                CF_DEFAULT,
                make_key(user_key_del, 5).as_slice(),
                &ApiV2::encode_raw_value_owned(value1.clone()),
            )
            .unwrap();
        raw_engine
            .put_cf(
                CF_DEFAULT,
                make_key(user_key_del, 1).as_slice(),
                &ApiV2::encode_raw_value_owned(value1),
            )
            .unwrap();

        let check_key_del = make_key(user_key_del, 1);
        let (prefix_del, _commit_ts) = split_ts(check_key_del.as_slice()).unwrap();
        gc_and_check(true, prefix_del);

        // ttl expire
        let value_ttl_expired = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(10),
            is_delete: false,
        };

        let user_key_expire = b"r\0bbbbbbbbbbb";

        // expire ttl , it will call scheduler gctask
        raw_engine
            .put_cf(
                CF_DEFAULT,
                make_key(user_key_expire, 9).as_slice(),
                &ApiV2::encode_raw_value_owned(value_ttl_expired.clone()),
            )
            .unwrap();
        raw_engine
            .put_cf(
                CF_DEFAULT,
                make_key(user_key_expire, 5).as_slice(),
                &ApiV2::encode_raw_value_owned(value_ttl_expired.clone()),
            )
            .unwrap();
        raw_engine
            .put_cf(
                CF_DEFAULT,
                make_key(user_key_expire, 1).as_slice(),
                &ApiV2::encode_raw_value_owned(value_ttl_expired),
            )
            .unwrap();

        let check_key_expire = make_key(user_key_expire, 1);
        let (prefix_expire, _commit_ts) = split_ts(check_key_expire.as_slice()).unwrap();
        gc_and_check(true, prefix_expire);
    }
}
