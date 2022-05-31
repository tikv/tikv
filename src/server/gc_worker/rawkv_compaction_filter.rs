// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ffi::CString,
    mem,
    sync::{atomic::Ordering, Arc},
};

use api_version::{ApiV2, KeyMode, KvFormat};
use engine_rocks::{
    raw::{
        new_compaction_filter_raw, CompactionFilter, CompactionFilterContext,
        CompactionFilterDecision, CompactionFilterFactory, CompactionFilterValueType,
        DBCompactionFilter,
    },
    RocksEngine,
};
use engine_traits::{raw_ttl::ttl_current_ts, MiscExt};
use prometheus::local::LocalHistogram;
use raftstore::coprocessor::RegionInfoProvider;
use tikv_util::worker::{ScheduleError, Scheduler};
use txn_types::Key;

use crate::{
    server::gc_worker::{
        compaction_filter::{
            CompactionFilterStats, DEFAULT_DELETE_BATCH_COUNT, GC_COMPACTION_FAILURE,
            GC_COMPACTION_FILTERED, GC_COMPACTION_FILTER_ORPHAN_VERSIONS, GC_CONTEXT,
        },
        GcTask,
    },
    storage::mvcc::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM},
};

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

        // If the key mode is not KeyMode::Raw or value_type is not CompactionFilterValueType::Value, it's needed to be retained.
        let key_mode = ApiV2::parse_key_mode(keys::origin_key(key));
        if key_mode != KeyMode::Raw || value_type != CompactionFilterValueType::Value {
            return Ok(CompactionFilterDecision::Keep);
        }

        let (mvcc_key_prefix, commit_ts) = ApiV2::split_ts(key)?;

        if self.mvcc_key_prefix != mvcc_key_prefix {
            self.switch_key_metrics();
            self.mvcc_key_prefix.clear();
            self.mvcc_key_prefix.extend_from_slice(mvcc_key_prefix);
            if commit_ts.into_inner() >= self.safe_point {
                return Ok(CompactionFilterDecision::Keep);
            }

            self.versions += 1;
            let raw_value = ApiV2::decode_raw_value(value)?;
            // If it's the latest version, and it's deleted or expired, it needs to be sent to GCWorker to be processed asynchronously.
            if !raw_value.is_valid(self.current_ts) {
                self.raw_handle_delete();
                if self.mvcc_deletions.len() >= DEFAULT_DELETE_BATCH_COUNT {
                    self.raw_gc_mvcc_deletions();
                }
            }
            // 1. If it's the latest version, and it's neither deleted nor expired, it's needed to be retained.
            // 2. If it's the latest version, and it's deleted or expired, while we do async gctask to deleted or expired records, both put records and deleted/expired records are actually kept within the compaction filter.
            Ok(CompactionFilterDecision::Keep)
        } else {
            if commit_ts.into_inner() >= self.safe_point {
                return Ok(CompactionFilterDecision::Keep);
            }

            self.versions += 1;
            self.filtered += 1;
            // If it's ts < safepoint, and it's not the latest version, it's need to be removed.
            Ok(CompactionFilterDecision::Remove)
        }
    }

    fn raw_gc_mvcc_deletions(&mut self) {
        if !self.mvcc_deletions.is_empty() {
            let empty = Vec::with_capacity(DEFAULT_DELETE_BATCH_COUNT);
            let task = GcTask::RawGcKeys {
                keys: mem::replace(&mut self.mvcc_deletions, empty),
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

    fn raw_handle_delete(&mut self) {
        debug_assert_eq!(self.mvcc_key_prefix[0], keys::DATA_PREFIX);
        let key = Key::from_encoded_slice(&self.mvcc_key_prefix[1..]);
        self.mvcc_deletions.push(key);
    }

    // TODO some refactor to avoid duplicated codes.
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

#[cfg(test)]
pub mod tests {

    use std::time::Duration;

    use api_version::RawValue;
    use engine_traits::{DeleteStrategy, Peekable, Range, CF_DEFAULT};
    use kvproto::kvrpcpb::{ApiVersion, Context};
    use tikv_kv::{Engine, Modify, WriteData};
    use txn_types::TimeStamp;

    use super::*;
    use crate::{
        config::DbConfig, server::gc_worker::TestGCRunner, storage::kv::TestEngineBuilder,
    };

    pub fn make_key(key: &[u8], ts: u64) -> Vec<u8> {
        let encode_key = ApiV2::encode_raw_key(key, Some(ts.into()));
        let res = keys::data_key(encode_key.as_encoded());
        res
    }

    #[test]
    fn test_raw_compaction_filter() {
        let mut cfg = DbConfig::default();
        cfg.defaultcf.disable_auto_compactions = true;
        cfg.defaultcf.dynamic_level_bytes = false;

        let engine = TestEngineBuilder::new()
            .api_version(ApiVersion::V2)
            .build_with_cfg(&cfg)
            .unwrap();
        let raw_engine = engine.get_rocksdb();
        let mut gc_runner = TestGCRunner::new(0);

        let user_key = b"r\0aaaaaaaaaaa";

        let test_raws = vec![
            (user_key, 100, false),
            (user_key, 90, false),
            (user_key, 70, false),
        ];

        let modifies = test_raws
            .into_iter()
            .map(|(key, ts, is_delete)| {
                (
                    make_key(key, ts),
                    ApiV2::encode_raw_value(RawValue {
                        user_value: &[0; 10][..],
                        expire_ts: Some(TimeStamp::max().into_inner()),
                        is_delete,
                    }),
                )
            })
            .map(|(k, v)| Modify::Put(CF_DEFAULT, Key::from_encoded_slice(k.as_slice()), v))
            .collect();

        let ctx = Context {
            api_version: ApiVersion::V2,
            ..Default::default()
        };
        let batch = WriteData::from_modifies(modifies);

        engine.write(&ctx, batch).unwrap();

        gc_runner.safe_point(80).gc_raw(&raw_engine);

        // If ts(70) < safepoint(80), and this userkey's latest verion is not deleted or expired, this version will be removed in do_filter.
        let entry70 = raw_engine
            .get_value_cf(CF_DEFAULT, make_key(b"r\0a", 70).as_slice())
            .unwrap();
        assert!(entry70.is_none());

        gc_runner.safe_point(90).gc_raw(&raw_engine);

        let entry100 = raw_engine
            .get_value_cf(CF_DEFAULT, make_key(user_key, 100).as_slice())
            .unwrap();
        let entry90 = raw_engine
            .get_value_cf(CF_DEFAULT, make_key(user_key, 90).as_slice())
            .unwrap();

        // If ts(100) > safepoint(80), it's need to be retained.
        assert!(entry100.is_some());

        // If ts(90) == safepoint(90), it's need to be retained.
        assert!(entry90.is_some());
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
        let user_key_del = b"r\0aaaaaaaaaaa";
        let user_key_not_del = b"r\0zzzzzzzzzzz";

        // If it's deleted, it will call async scheduler GcTask.
        let test_raws = vec![
            (user_key_not_del, 630, false),
            (user_key_not_del, 620, false),
            (user_key_not_del, 610, false),
            (user_key_not_del, 430, false),
            (user_key_not_del, 420, false),
            (user_key_not_del, 410, false),
            (user_key_del, 9, true),
            (user_key_del, 5, false),
            (user_key_del, 1, false),
        ];

        let modifies = test_raws
            .into_iter()
            .map(|(key, ts, is_delete)| {
                (
                    make_key(key, ts),
                    ApiV2::encode_raw_value(RawValue {
                        user_value: &[0; 10][..],
                        expire_ts: Some(TimeStamp::max().into_inner()),
                        is_delete,
                    }),
                )
            })
            .map(|(k, v)| Modify::Put(CF_DEFAULT, Key::from_encoded_slice(k.as_slice()), v))
            .collect();

        let ctx = Context {
            api_version: ApiVersion::V2,
            ..Default::default()
        };

        let batch = WriteData::from_modifies(modifies);

        engine.write(&ctx, batch).unwrap();

        let check_key_del = make_key(user_key_del, 1);
        let (prefix_del, _commit_ts) = ApiV2::split_ts(check_key_del.as_slice()).unwrap();
        gc_and_check(true, prefix_del);

        // Clean the engine, prepare for later tests.
        let range_start_key =
            keys::data_key(ApiV2::encode_raw_key(user_key_del, None).as_encoded());
        let range_end_key = keys::data_key(
            ApiV2::encode_raw_key(user_key_not_del, Some(TimeStamp::new(1))).as_encoded(),
        );
        raw_engine
            .delete_ranges_cf(
                CF_DEFAULT,
                DeleteStrategy::DeleteByKey,
                &[Range::new(
                    range_start_key.as_slice(),
                    range_end_key.as_slice(),
                )],
            )
            .unwrap();

        let user_key_expire = b"r\0bbbbbbbbbbb";

        // If it's expired, it will call async scheduler GcTask.
        let test_expired_raws = vec![
            (user_key_expire, 9, false),
            (user_key_expire, 5, false),
            (user_key_expire, 1, false),
        ];

        let modifies: Vec<Modify> = test_expired_raws
            .into_iter()
            .map(|(key, ts, is_delete)| {
                (
                    make_key(key, ts),
                    ApiV2::encode_raw_value(RawValue {
                        user_value: &[0; 10][..],
                        expire_ts: Some(10),
                        is_delete,
                    }),
                )
            })
            .map(|(k, v)| Modify::Put(CF_DEFAULT, Key::from_encoded_slice(k.as_slice()), v))
            .collect();

        let batch = WriteData::from_modifies(modifies);

        engine.write(&ctx, batch).unwrap();

        let check_key_expire = make_key(user_key_expire, 1);
        let (prefix_expired, _commit_ts) = ApiV2::split_ts(check_key_expire.as_slice()).unwrap();
        gc_and_check(true, prefix_expired);
    }
}
