use std::ffi::CString;
use std::mem;
use std::sync::atomic::Ordering;
use prometheus::local::LocalHistogram;
use api_version::{APIV2, APIVersion, KeyMode};
use api_version::api_v2::RAW_KEY_PREFIX;
use engine_rocks::raw::{CompactionFilter, CompactionFilterContext, CompactionFilterDecision, CompactionFilterFactory, CompactionFilterValueType, DBCompactionFilter, new_compaction_filter_raw};
use engine_rocks::RocksEngine;
use engine_traits::MiscExt;
use engine_traits::raw_ttl::ttl_current_ts;
use tikv_util::worker::{ScheduleError, Scheduler};
use txn_types::Key;
use crate::server::gc_worker::compaction_filter::{DEFAULT_DELETE_BATCH_COUNT, GC_COMPACTION_FAILURE, GC_COMPACTION_FILTER_ORPHAN_VERSIONS, GC_COMPACTION_FILTERED, GC_COMPACTION_MVCC_ROLLBACK, GC_CONTEXT,CompactionFilterStats};
use crate::server::gc_worker::GcTask;
use crate::storage::mvcc::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM};

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
        let current = ttl_current_ts();
        let safe_point = gc_context.safe_point.load(Ordering::Relaxed);
        let filter = RawCompactionFilter::new(db, safe_point, gc_scheduler, current, context);
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
    is_bottommost_level: bool,

    // Some metrics about implementation detail.
    versions: usize,
    filtered: usize,
    total_versions: usize,
    total_filtered: usize,
    mvcc_rollback_and_locks: usize,
    orphan_versions: usize,
    versions_hist: LocalHistogram,
    filtered_hist: LocalHistogram,
    //
    // #[cfg(any(test, feature = "failpoints"))]
    // callbacks_on_drop: Vec<Arc<dyn Fn(&RawCompactionFilter) + Send + Sync>>,
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

        // #[cfg(any(test, feature = "failpoints"))]
        // for callback in &self.callbacks_on_drop {
        //     callback(self);
        // }
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
        match self.do_filter(level, key, sequence, value, value_type) {
            Ok(decision) => decision,
            Err(e) => {
                warn!("compaction filter meet error: {}", e);
                GC_COMPACTION_FAILURE.with_label_values(&["filter"]).inc();
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
        context: &CompactionFilterContext,
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
            is_bottommost_level: context.is_bottommost_level(),

            versions: 0,
            filtered: 0,
            total_versions: 0,
            total_filtered: 0,
            mvcc_rollback_and_locks: 0,
            orphan_versions: 0,
            versions_hist: MVCC_VERSIONS_HISTOGRAM.local(),
            filtered_hist: GC_DELETE_VERSIONS_HISTOGRAM.local(),
            // #[cfg(any(test, feature = "failpoints"))]
            // callbacks_on_drop: {
            //     let ctx = GC_CONTEXT.lock().unwrap();
            //     ctx.as_ref().unwrap().callbacks_on_drop.clone()
            // },
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
        self.print_array("key.clone:", key.clone().to_vec());
        // remove prefix 'z'
        let current_key = keys::origin_key(key);
        let key_mode = APIV2::parse_key_mode(current_key);

        // not RawKV or targetValue
        if key_mode != KeyMode::Raw || value_type != CompactionFilterValueType::Value {
            return Ok(CompactionFilterDecision::Keep);
        }

        let (mvcc_key_prefix_vec, commit_ts_opt) =
            APIV2::decode_raw_key(&Key::from_encoded_slice(current_key), true).unwrap();
        let mvcc_key_prefix = mvcc_key_prefix_vec.as_slice();
        let commit_ts = commit_ts_opt.unwrap().into_inner();

        // mvcc_key_prefix_vec = 'r' , skip this key
        if mvcc_key_prefix_vec.clone().len() == 1 {
            return Ok(CompactionFilterDecision::Keep);
        }

        if self.mvcc_key_prefix != mvcc_key_prefix {
            self.switch_key_metrics();
            self.mvcc_key_prefix.clear();
            self.mvcc_key_prefix.extend_from_slice(mvcc_key_prefix);
            if commit_ts >= self.safe_point {
                return Ok(CompactionFilterDecision::Keep);
            }
            let raw_value = APIV2::decode_raw_value(&value)?;
            // the lastest version ,and it's deleted or expaired ttl , need to be send to async gc task
            if raw_value.is_delete || raw_value.expire_ts.unwrap() < self.current_ts {
                self.raw_handle_bottommost_delete();
                if self.mvcc_deletions.len() >= DEFAULT_DELETE_BATCH_COUNT {
                    self.raw_gc_mvcc_deletions();
                }
            } else {
                // the lastest version ,and not deleted or expaired ttl , need to be retained
                return Ok(CompactionFilterDecision::Keep);
            }
        } else {
            if commit_ts >= self.safe_point {
                return Ok(CompactionFilterDecision::Keep);
            }

            self.filtered += 1;
            // ts < safepoint ,and it's not the lastest version, it's need to be removed.
            return Ok(CompactionFilterDecision::Remove);
        }

        // the lastest version ,and it's deleted or expaired ttl , need to be send to async gc task
        return Ok(CompactionFilterDecision::Keep);
    }

    fn raw_gc_mvcc_deletions(&mut self) {
        if !self.mvcc_deletions.is_empty() {
            let empty = Vec::with_capacity(DEFAULT_DELETE_BATCH_COUNT);
            let task = GcTask::RawGcKeys {
                keys: mem::replace(&mut self.mvcc_deletions, empty), //gc_keys->gc_key->gc->gc.run
                safe_point: self.safe_point.into(),
            };
            self.schedule_gc_task(task, false);
        }
        self.mvcc_deletions.clear();
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
    fn print_array(&mut self, name: &str, arr: Vec<u8>) {
        for i in 0..arr.len() {
            info!("arr {}:{}:{}", name, i, arr[i]);
            println!("arr {}:{}:{}", name, i, arr[i]);
        }
        // println!("arr {}:{}", name, resstr);
    }
    fn raw_handle_bottommost_delete(&mut self) {
        // Valid MVCC records should begin with `RAW_KEY_PREFIX`.
        debug!("raw_handle_bottommost_delete:");
        debug_assert_eq!(self.mvcc_key_prefix[0], RAW_KEY_PREFIX);
        self.print_array("mvcc_key_prefix.push", self.mvcc_key_prefix.to_vec());
        let key = Key::from_encoded_slice(&self.mvcc_key_prefix);
        self.mvcc_deletions.push(key); // key= user key
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
        GC_COMPACTION_MVCC_ROLLBACK.inc_by(self.mvcc_rollback_and_locks as u64);
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
                info!("Compaction filter reports"; "total" => versions, "filtered" => filtered);
            }
        }
    }
}


#[cfg(test)]
pub mod tests {

    use super::*;
    use api_version::RawValue;
    use kvproto::kvrpcpb::ApiVersion;
    use std::thread;

    use crate::config::DbConfig;
    use crate::storage::kv::TestEngineBuilder;
    use engine_traits::{
        DeleteStrategy, MiscExt, Peekable, Range, SyncMutable, CF_DEFAULT ,
    };
    use std::time::Duration;
    use txn_types::TimeStamp;
    use crate::server::gc_worker::TestGCRunner;

    pub fn makeKey(key: &[u8], ts: i32) -> Vec<u8> {
        let key1 = Key::from_raw(key)
            .append_ts(TimeStamp::new(ts as u64))
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
        let value = vec![b'v'; 512];
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
                makeKey(user_key, 100).as_slice(),
                &APIV2::encode_raw_value_owned(value1.clone()),
            )
            .unwrap();
        raw_engine
            .put_cf(
                CF_DEFAULT,
                makeKey(user_key, 90).as_slice(),
                &APIV2::encode_raw_value_owned(value1.clone()),
            )
            .unwrap();
        raw_engine
            .put_cf(
                CF_DEFAULT,
                makeKey(user_key, 70).as_slice(),
                &APIV2::encode_raw_value_owned(value1.clone()),
            )
            .unwrap();

        gc_runner.safe_point(80).gcRaw(&raw_engine);
        //Wait gc  end
        thread::sleep(Duration::from_millis(1000));

        // 70 < safepoint(80), this version was removed
        let isexit70 = raw_engine
            .get_value_cf(CF_DEFAULT, makeKey(b"r\0a", 70).as_slice())
            .unwrap()
            .is_none();
        assert_eq!(isexit70, true);

        gc_runner.safe_point(90).gcRaw(&raw_engine);
        // Wait gc end
        thread::sleep(Duration::from_millis(1000));

        let isexit100 = raw_engine
            .get_value_cf(CF_DEFAULT, makeKey(user_key, 100).as_slice())
            .unwrap()
            .is_none();
        let isexit90 = raw_engine
            .get_value_cf(CF_DEFAULT, makeKey(user_key, 90).as_slice())
            .unwrap()
            .is_none();

        // ts(100) > safepoint(80), need to be retained.
        assert_eq!(isexit100, false);

        // ts(90) == safepoint(90), need to be retained.
        assert_eq!(isexit90, false);
    }

    #[test]
    fn test_raw_call_gctask() {
        let engine = TestEngineBuilder::new()
            .api_version(ApiVersion::V2)
            .build()
            .unwrap();
        let raw_engine = engine.get_rocksdb();
        let value = vec![b'v'; 512];
        let mut gc_runner = TestGCRunner::new(0);

        let mut gc_and_check = |expect_tasks: bool, prefix: &[u8]| {
            gc_runner.safe_point(500).gcRaw(&raw_engine);

            // Wait up to 1 second, and treat as no task if timeout.
            if let Ok(Some(task)) = gc_runner.gc_receiver.recv_timeout(Duration::new(1, 0)) {
                assert!(expect_tasks, "a GC task is expected");
                match task {
                    GcTask::RawGcKeys { keys, .. } => {
                        assert_eq!(keys.len(), 1);
                        let got = keys[0].as_encoded();
                        let expect = Key::from_raw(prefix).append_ts(9.into());
                        let (usekey, userts) = APIV2::decode_raw_key(&expect, true).unwrap();
                        assert_eq!(got, &usekey);
                    }
                    _ => unreachable!(),
                }
                return;
            }
            assert!(!expect_tasks, "no GC task is expected");
        };

        let value1 = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(10),
            is_delete: false,
        };

        let value_is_delete = RawValue {
            user_value: vec![0; 10],
            expire_ts: Some(10),
            is_delete: true,
        };

        let user_key = b"r\0aaaaaaaaaaa";

        raw_engine
            .put_cf(
                CF_DEFAULT,
                makeKey(user_key, 9).as_slice(),
                &APIV2::encode_raw_value_owned(value_is_delete.clone()),
            )
            .unwrap();
        raw_engine
            .put_cf(
                CF_DEFAULT,
                makeKey(user_key, 5).as_slice(),
                &APIV2::encode_raw_value_owned(value1.clone()),
            )
            .unwrap();
        raw_engine
            .put_cf(
                CF_DEFAULT,
                makeKey(user_key, 1).as_slice(),
                &APIV2::encode_raw_value_owned(value1.clone()),
            )
            .unwrap();

        gc_and_check(true, user_key);
    }
}