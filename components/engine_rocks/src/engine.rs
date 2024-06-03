// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{any::Any, sync::Arc};

use engine_traits::{
    IterOptions, Iterable, KvEngine, Peekable, ReadOptions, Result, SnapshotContext, SyncMutable,
};
use rocksdb::{DBIterator, Writable, DB};
use tikv_util::info;

use crate::{
    db_vector::RocksDbVector, options::RocksReadOptions, r2e, util::get_cf_handle,
    RocksEngineIterator, RocksSnapshot,
};

#[cfg(feature = "trace-lifetime")]
mod trace {
    //! Trace tools for tablets.
    //!
    //! It's hard to know who is holding the rocksdb reference when trying to
    //! debug why the tablet is not deleted. The module will record the
    //! backtrace and thread name when the tablet is created or clone. So
    //! after print all the backtrace, we can easily figure out who is
    //! leaking the tablet.
    //!
    //! To use the feature, you need to compile tikv-server with
    //! trace-tabelt-lifetime feature. For example, `env
    //! ENABLE_FEATURES=trace-tablet-lifetime make release`. And then query the trace information by `curl http://ip:status_port/region/id?trace-tablet=1`.

    use std::{
        backtrace::Backtrace,
        collections::BTreeMap,
        ops::Bound::Included,
        sync::{
            atomic::{AtomicU64, Ordering},
            Mutex,
        },
    };

    use rocksdb::DB;

    static CNT: AtomicU64 = AtomicU64::new(0);

    fn inc_id() -> u64 {
        CNT.fetch_add(1, Ordering::Relaxed)
    }

    struct BacktraceInfo {
        bt: Backtrace,
        name: String,
    }

    impl BacktraceInfo {
        fn default() -> Self {
            BacktraceInfo {
                bt: Backtrace::force_capture(),
                name: std::thread::current().name().unwrap_or("").to_string(),
            }
        }
    }

    #[derive(PartialEq, PartialOrd, Eq, Ord, Clone, Copy, Default, Debug)]
    struct TabletTraceKey {
        region_id: u64,
        suffix: u64,
        addr: u64,
        alloc_id: u64,
    }

    lazy_static::lazy_static! {
        static ref TABLET_TRACE: Mutex<BTreeMap<TabletTraceKey, BacktraceInfo>> = Mutex::new(BTreeMap::default());
    }

    pub fn list(id: u64) -> Vec<String> {
        let min = TabletTraceKey {
            region_id: id,
            suffix: 0,
            addr: 0,
            alloc_id: 0,
        };
        let max = TabletTraceKey {
            region_id: id,
            suffix: u64::MAX,
            addr: u64::MAX,
            alloc_id: u64::MAX,
        };
        let traces = TABLET_TRACE.lock().unwrap();
        traces
            .range((Included(min), Included(max)))
            .map(|(k, v)| {
                format!(
                    "{}_{} {} {} {}",
                    k.region_id, k.suffix, k.addr, v.name, v.bt
                )
            })
            .collect()
    }

    #[derive(Debug)]
    pub struct TabletTraceId(TabletTraceKey);

    impl TabletTraceId {
        pub fn new(path: &str, db: &DB) -> Self {
            let mut name = path.split('/');
            let name = name.next_back().unwrap();
            let parts: Vec<_> = name.split('_').collect();
            if parts.len() == 2 {
                let id: u64 = parts[0].parse().unwrap();
                let suffix: u64 = parts[1].parse().unwrap();
                let bt = BacktraceInfo::default();
                let key = TabletTraceKey {
                    region_id: id,
                    suffix,
                    addr: db as *const _ as u64,
                    alloc_id: inc_id(),
                };
                TABLET_TRACE.lock().unwrap().insert(key, bt);
                Self(key)
            } else {
                Self(Default::default())
            }
        }
    }

    impl Clone for TabletTraceId {
        fn clone(&self) -> Self {
            if self.0.region_id != 0 {
                let bt = BacktraceInfo::default();
                let mut key = self.0;
                key.alloc_id = inc_id();
                TABLET_TRACE.lock().unwrap().insert(key, bt);
                Self(key)
            } else {
                Self(self.0)
            }
        }
    }

    impl Drop for TabletTraceId {
        fn drop(&mut self) {
            if self.0.region_id != 0 {
                TABLET_TRACE.lock().unwrap().remove(&self.0);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct RocksEngine {
    db: Arc<DB>,
    support_multi_batch_write: bool,
    #[cfg(feature = "trace-lifetime")]
    _id: trace::TabletTraceId,
}

impl RocksEngine {
    pub fn new(db: DB) -> RocksEngine {
        let db = Arc::new(db);
        RocksEngine {
            support_multi_batch_write: db.get_db_options().is_enable_multi_batch_write(),
            #[cfg(feature = "trace-lifetime")]
            _id: trace::TabletTraceId::new(db.path(), &db),
            db,
        }
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        &self.db
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.db.clone()
    }

    pub fn support_multi_batch_write(&self) -> bool {
        self.support_multi_batch_write
    }

    #[cfg(feature = "trace-lifetime")]
    pub fn trace(region_id: u64) -> Vec<String> {
        trace::list(region_id)
    }
}

impl KvEngine for RocksEngine {
    type Snapshot = RocksSnapshot;

    fn snapshot(&self, _: Option<SnapshotContext>) -> RocksSnapshot {
        RocksSnapshot::new(self.db.clone())
    }

    fn sync(&self) -> Result<()> {
        self.db.sync_wal().map_err(r2e)
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        let e: &dyn Any = &self.db;
        e.downcast_ref().expect("bad engine downcast")
    }

    #[cfg(feature = "testexport")]
    fn inner_refcount(&self) -> usize {
        Arc::strong_count(&self.db)
    }
}

impl Iterable for RocksEngine {
    type Iterator = RocksEngineIterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let handle = get_cf_handle(&self.db, cf)?;
        let opt: RocksReadOptions = opts.into();
        Ok(RocksEngineIterator::from_raw(DBIterator::new_cf(
            self.db.clone(),
            handle,
            opt.into_raw(),
        )))
    }
}

impl Peekable for RocksEngine {
    type DbVector = RocksDbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<RocksDbVector>> {
        let opt: RocksReadOptions = opts.into();
        let v = self.db.get_opt(key, &opt.into_raw()).map_err(r2e)?;
        Ok(v.map(RocksDbVector::from_raw))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<RocksDbVector>> {
        let opt: RocksReadOptions = opts.into();
        let handle = get_cf_handle(&self.db, cf)?;
        let v = self
            .db
            .get_cf_opt(handle, key, &opt.into_raw())
            .map_err(r2e)?;
        Ok(v.map(RocksDbVector::from_raw))
    }
}

impl SyncMutable for RocksEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value).map_err(r2e)
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.db, cf)?;
        self.db.put_cf(handle, key, value).map_err(r2e)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key).map_err(r2e)
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        if cf != "lock" {
            info!(
                "delete_cf";
                "cf" => ?cf,
                "key" => log_wrappers::Value(key),
            );
        }
        let handle = get_cf_handle(&self.db, cf)?;
        self.db.delete_cf(handle, key).map_err(r2e)
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.db.delete_range(begin_key, end_key).map_err(r2e)
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.db, cf)?;
        self.db
            .delete_range_cf(handle, begin_key, end_key)
            .map_err(r2e)
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{Iterable, KvEngine, Peekable, SyncMutable, CF_DEFAULT};
    use kvproto::metapb::Region;
    use proptest::prelude::*;
    use rocksdb::{DBOptions, FlushOptions, SeekKey, TitanDBOptions, Writable, DB};
    use tempfile::Builder;
    use tikv_util::config::ReadableSize;

    use crate::{util, RocksSnapshot};

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = util::new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT, cf]).unwrap();

        let mut r = Region::default();
        r.set_id(10);

        let key = b"key";
        engine.put_msg(key, &r).unwrap();
        engine.put_msg_cf(cf, key, &r).unwrap();

        let snap = engine.snapshot(None);

        let mut r1: Region = engine.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_cf: Region = engine.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r1_cf);

        let mut r2: Region = snap.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r2);
        let r2_cf: Region = snap.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r2_cf);

        r.set_id(11);
        engine.put_msg(key, &r).unwrap();
        r1 = engine.get_msg(key).unwrap().unwrap();
        r2 = snap.get_msg(key).unwrap().unwrap();
        assert_ne!(r1, r2);

        let b: Option<Region> = engine.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = util::new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT, cf]).unwrap();

        engine.put(b"k1", b"v1").unwrap();
        engine.put_cf(cf, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get_value(b"k1").unwrap().unwrap(), b"v1");
        engine.get_value_cf("foo", b"k1").unwrap_err();
        assert_eq!(&*engine.get_value_cf(cf, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = util::new_engine(path.path().to_str().unwrap(), &[CF_DEFAULT, cf]).unwrap();

        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();
        engine.put_cf(cf, b"a1", b"v1").unwrap();
        engine.put_cf(cf, b"a2", b"v22").unwrap();

        let mut data = vec![];
        engine
            .scan(CF_DEFAULT, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v2".to_vec()),
            ]
        );
        data.clear();

        engine
            .scan(cf, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v22".to_vec()),
            ]
        );
        data.clear();

        let pair = engine.seek(CF_DEFAULT, b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek(CF_DEFAULT, b"a3").unwrap().is_none());
        let pair_cf = engine.seek(cf, b"a1").unwrap().unwrap();
        assert_eq!(pair_cf, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek(cf, b"a3").unwrap().is_none());

        let mut index = 0;
        engine
            .scan(CF_DEFAULT, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                index += 1;
                Ok(index != 1)
            })
            .unwrap();

        assert_eq!(data.len(), 1);

        let snap = RocksSnapshot::new(engine.get_sync_db());

        engine.put(b"a3", b"v3").unwrap();
        assert!(engine.seek(CF_DEFAULT, b"a3").unwrap().is_some());

        let pair = snap.seek(CF_DEFAULT, b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(snap.seek(CF_DEFAULT, b"a3").unwrap().is_none());

        data.clear();

        snap.scan(CF_DEFAULT, b"", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 2);
    }

    #[derive(Clone, Debug)]
    enum Operation {
        Put(Vec<u8>, Vec<u8>),
        Get(Vec<u8>),
        Delete(Vec<u8>),
        Scan(Vec<u8>, usize),
        DeleteRange(Vec<u8>, Vec<u8>),
        Sync(),
    }

    fn gen_operations(value_size: usize) -> impl Strategy<Value = Vec<Operation>> {
        let key_size: usize = 16;
        prop::collection::vec(
            prop_oneof![
                (
                    prop::collection::vec(prop::num::u8::ANY, 0..key_size),
                    prop::collection::vec(prop::num::u8::ANY, 0..value_size)
                )
                    .prop_map(|(k, v)| Operation::Put(k, v)),
                prop::collection::vec(prop::num::u8::ANY, 0..key_size)
                    .prop_map(|k| Operation::Get(k)),
                prop::collection::vec(prop::num::u8::ANY, 0..key_size)
                    .prop_map(|k| Operation::Delete(k)),
                (
                    prop::collection::vec(prop::num::u8::ANY, 0..key_size),
                    0..10usize
                )
                    .prop_map(|(k, v)| Operation::Scan(k, v)),
                (
                    prop::collection::vec(prop::num::u8::ANY, 0..key_size),
                    prop::collection::vec(prop::num::u8::ANY, 0..key_size)
                )
                    .prop_map(|(k1, k2)| Operation::DeleteRange(k1, k2)),
                Just(Operation::Sync()),
            ],
            0..100,
        )
    }

    fn scan_kvs(db: &DB, start: &[u8], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut iter = db.iter();
        iter.seek(SeekKey::Key(start)).unwrap();
        let mut num = 0;
        let mut res = vec![];
        while num < limit {
            if iter.valid().unwrap() {
                let k = iter.key().to_vec();
                let v = iter.value().to_vec();

                res.push((k, v));
                num += 1;
            } else {
                break;
            }
            iter.next().unwrap();
        }
        res
    }

    fn test_rocks_titan_basic_operations(
        operations: Vec<Operation>,
        min_blob_size: u64,
        enable_dict_compress: bool,
    ) {
        let path_rocks = Builder::new()
            .prefix("test_rocks_titan_basic_operations_rocks")
            .tempdir()
            .unwrap();
        let path_titan = Builder::new()
            .prefix("test_rocks_titan_basic_operations_titan")
            .tempdir()
            .unwrap();
        let mut tdb_opts = TitanDBOptions::new();
        tdb_opts.set_min_blob_size(min_blob_size);
        if enable_dict_compress {
            tdb_opts.set_compression_options(
                -14,                                 // window_bits
                32767,                               // level
                0,                                   // strategy
                ReadableSize::kb(16).0 as i32,       // zstd dict size
                ReadableSize::kb(16).0 as i32 * 100, // zstd sample size
            );
        }
        let mut opts = DBOptions::new();
        opts.set_titandb_options(&tdb_opts);
        opts.create_if_missing(true);

        let db_titan = DB::open(opts, path_titan.path().to_str().unwrap()).unwrap();

        opts = DBOptions::new();
        opts.create_if_missing(true);

        let db_rocks = DB::open(opts, path_rocks.path().to_str().unwrap()).unwrap();
        let mut flush = false;
        for op in operations {
            match op {
                Operation::Put(k, v) => {
                    db_rocks.put(&k, &v).unwrap();
                    db_titan.put(&k, &v).unwrap();
                }
                Operation::Get(k) => {
                    let res_rocks = db_rocks.get(&k).unwrap();
                    let res_titan = db_titan.get(&k).unwrap();
                    assert_eq!(res_rocks.as_deref(), res_titan.as_deref());
                }
                Operation::Delete(k) => {
                    db_rocks.delete(&k).unwrap();
                    db_titan.delete(&k).unwrap();
                }
                Operation::Scan(k, limit) => {
                    let res_rocks = scan_kvs(&db_rocks, &k, limit);
                    let res_titan = scan_kvs(&db_titan, &k, limit);
                    assert_eq!(res_rocks, res_titan);
                }
                Operation::DeleteRange(k1, k2) => {
                    if k1 <= k2 {
                        db_rocks.delete_range(&k1, &k2).unwrap();
                        db_titan.delete_range(&k1, &k2).unwrap();
                    } else {
                        db_rocks.delete_range(&k2, &k1).unwrap();
                        db_titan.delete_range(&k2, &k1).unwrap();
                    }
                }
                Operation::Sync() => {
                    if !flush {
                        let mut opts = FlushOptions::default();
                        opts.set_wait(false);
                        let _ = db_rocks.flush(&opts);
                        let _ = db_titan.flush(&opts);
                        flush = true;
                    }
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]
        #[test]
        fn test_rocks_titan_basic_ops(operations in gen_operations(1000)) {
            test_rocks_titan_basic_operations(operations.clone(), 8, true);
        }

        #[test]
        fn test_rocks_titan_basic_ops_large_min_blob_size(operations in gen_operations(1000)) {
            // titan actually is not enabled
            test_rocks_titan_basic_operations(operations, 1024, false);
        }
    }
}
