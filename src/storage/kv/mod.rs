// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::UnsafeCell;
use std::fmt;
use std::time::Duration;
use std::{error, ptr, result};

use engine::rocks::TablePropertiesCollection;
use engine::IterOption;
use engine::{CfName, CF_DEFAULT};
use kvproto::errorpb::Error as ErrorHeader;
use kvproto::kvrpcpb::Context;

use crate::into_other::IntoOther;
use crate::raftstore::coprocessor::SeekRegionCallback;
use crate::storage::{Key, Value};

mod btree_engine;
mod compact_listener;
mod cursor;
mod perf_context;
mod rocksdb_engine;
mod stats;

pub use self::btree_engine::{BTreeEngine, BTreeEngineIterator, BTreeEngineSnapshot};
pub use self::compact_listener::{CompactedEvent, CompactionListener};
pub use self::cursor::{Cursor, CursorBuilder};
pub use self::perf_context::{PerfStatisticsDelta, PerfStatisticsInstant};
pub use self::rocksdb_engine::{RocksEngine, RocksSnapshot, TestEngineBuilder};
pub use self::stats::{
    CFStatistics, FlowStatistics, FlowStatsReporter, Statistics, StatisticsSummary,
};

pub const SEEK_BOUND: u64 = 8;
const DEFAULT_TIMEOUT_SECS: u64 = 5;

pub type Callback<T> = Box<dyn FnOnce((CbContext, Result<T>)) + Send>;
pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub struct CbContext {
    pub term: Option<u64>,
}

impl CbContext {
    pub fn new() -> CbContext {
        CbContext { term: None }
    }
}

#[derive(Debug)]
pub enum Modify {
    Delete(CfName, Key),
    Put(CfName, Key, Value),
    // cf_name, start_key, end_key, notify_only
    DeleteRange(CfName, Key, Key, bool),
}

pub trait Engine: Send + Clone + 'static {
    type Snap: Snapshot;

    fn async_write(&self, ctx: &Context, batch: Vec<Modify>, callback: Callback<()>) -> Result<()>;
    fn async_snapshot(&self, ctx: &Context, callback: Callback<Self::Snap>) -> Result<()>;

    fn write(&self, ctx: &Context, batch: Vec<Modify>) -> Result<()> {
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
        match wait_op!(|cb| self.async_write(ctx, batch, cb), timeout) {
            Some((_, res)) => res,
            None => Err(Error::from(ErrorInner::Timeout(timeout))),
        }
    }

    fn snapshot(&self, ctx: &Context) -> Result<Self::Snap> {
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
        match wait_op!(|cb| self.async_snapshot(ctx, cb), timeout) {
            Some((_, res)) => res,
            None => Err(Error::from(ErrorInner::Timeout(timeout))),
        }
    }

    fn put(&self, ctx: &Context, key: Key, value: Value) -> Result<()> {
        self.put_cf(ctx, CF_DEFAULT, key, value)
    }

    fn put_cf(&self, ctx: &Context, cf: CfName, key: Key, value: Value) -> Result<()> {
        self.write(ctx, vec![Modify::Put(cf, key, value)])
    }

    fn delete(&self, ctx: &Context, key: Key) -> Result<()> {
        self.delete_cf(ctx, CF_DEFAULT, key)
    }

    fn delete_cf(&self, ctx: &Context, cf: CfName, key: Key) -> Result<()> {
        self.write(ctx, vec![Modify::Delete(cf, key)])
    }
}

pub trait Snapshot: Send + Clone {
    type Iter: Iterator;

    fn get(&self, key: &Key) -> Result<Option<Value>>;
    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>>;
    fn iter(&self, iter_opt: IterOption, mode: ScanMode) -> Result<Cursor<Self::Iter>>;
    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: IterOption,
        mode: ScanMode,
    ) -> Result<Cursor<Self::Iter>>;
    fn get_properties(&self) -> Result<TablePropertiesCollection> {
        self.get_properties_cf(CF_DEFAULT)
    }
    fn get_properties_cf(&self, _: CfName) -> Result<TablePropertiesCollection> {
        Err(box_err!("no user properties"))
    }
    // The minimum key this snapshot can retrieve.
    #[inline]
    fn lower_bound(&self) -> Option<&[u8]> {
        None
    }
    // The maximum key can be fetched from the snapshot should less than the upper bound.
    #[inline]
    fn upper_bound(&self) -> Option<&[u8]> {
        None
    }

    /// Retrieves a version that represents the modification status of the underlying data.
    /// Version should be changed when underlying data is changed.
    ///
    /// If the engine does not support data version, then `None` is returned.
    #[inline]
    fn get_data_version(&self) -> Option<u64> {
        None
    }
}

pub trait Iterator: Send {
    fn next(&mut self) -> bool;
    fn prev(&mut self) -> bool;
    fn seek(&mut self, key: &Key) -> Result<bool>;
    fn seek_for_prev(&mut self, key: &Key) -> Result<bool>;
    fn seek_to_first(&mut self) -> bool;
    fn seek_to_last(&mut self) -> bool;
    fn valid(&self) -> bool;
    fn status(&self) -> Result<()>;

    fn validate_key(&self, _: &Key) -> Result<()> {
        Ok(())
    }

    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
}

pub trait RegionInfoProvider: Send + Clone + 'static {
    /// Find the first region `r` whose range contains or greater than `from_key` and the peer on
    /// this TiKV satisfies `filter(peer)` returns true.
    fn seek_region(&self, from: &[u8], filter: SeekRegionCallback) -> Result<()>;
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ScanMode {
    Forward,
    Backward,
    Mixed,
}

quick_error! {
    #[derive(Debug)]
    pub enum ErrorInner {
        Request(err: ErrorHeader) {
            from()
            description("request to underhook engine failed")
            display("{:?}", err)
        }
        Timeout(d: Duration) {
            description("request timeout")
            display("timeout after {:?}", d)
        }
        EmptyRequest {
            description("an empty request")
            display("an empty request")
        }
        Other(err: Box<dyn error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

impl From<engine::Error> for ErrorInner {
    fn from(err: engine::Error) -> ErrorInner {
        ErrorInner::Request(err.into())
    }
}

impl From<engine_traits::Error> for ErrorInner {
    fn from(err: engine_traits::Error) -> ErrorInner {
        ErrorInner::Request(err.into_other())
    }
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match *self {
            ErrorInner::Request(ref e) => Some(ErrorInner::Request(e.clone())),
            ErrorInner::Timeout(d) => Some(ErrorInner::Timeout(d)),
            ErrorInner::EmptyRequest => Some(ErrorInner::EmptyRequest),
            ErrorInner::Other(_) => None,
        }
    }
}

pub struct Error(pub Box<ErrorInner>);

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        self.0.maybe_clone().map(Error::from)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        std::error::Error::description(&self.0)
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.0)
    }
}

impl From<ErrorInner> for Error {
    #[inline]
    fn from(e: ErrorInner) -> Self {
        Error(Box::new(e))
    }
}

impl<T: Into<ErrorInner>> From<T> for Error {
    #[inline]
    default fn from(err: T) -> Self {
        let err = err.into();
        err.into()
    }
}

thread_local! {
    // A pointer to thread local engine. Use raw pointer and `UnsafeCell` to reduce runtime check.
    static TLS_ENGINE_ANY: UnsafeCell<*mut ()> = UnsafeCell::new(ptr::null_mut());
}

/// Execute the closure on the thread local engine.
///
/// Safety: precondition: `TLS_ENGINE_ANY` is non-null.
pub unsafe fn with_tls_engine<E: Engine, F, R>(f: F) -> R
where
    F: FnOnce(&E) -> R,
{
    TLS_ENGINE_ANY.with(|e| {
        let engine = &*(*e.get() as *const E);
        f(engine)
    })
}

/// Set the thread local engine.
///
/// Postcondition: `TLS_ENGINE_ANY` is non-null.
pub fn set_tls_engine<E: Engine>(engine: E) {
    // Safety: we check that `TLS_ENGINE_ANY` is null to ensure we don't leak an existing
    // engine; we ensure there are no other references to `engine`.
    TLS_ENGINE_ANY.with(move |e| unsafe {
        if (*e.get()).is_null() {
            let engine = Box::into_raw(Box::new(engine)) as *mut ();
            *e.get() = engine;
        }
    });
}

/// Destroy the thread local engine.
///
/// Safety: the current tls engine must have the same type as `E` (or at least
/// there destructors must be compatible).
/// Postcondition: `TLS_ENGINE_ANY` is null.
pub unsafe fn destroy_tls_engine<E: Engine>() {
    // Safety: we check that `TLS_ENGINE_ANY` is non-null, we must ensure that references
    // to `TLS_ENGINE_ANY` can never be stored outside of `TLS_ENGINE_ANY`.
    TLS_ENGINE_ANY.with(|e| {
        let ptr = *e.get();
        if !ptr.is_null() {
            drop(Box::from_raw(ptr as *mut E));
            *e.get() = ptr::null_mut();
        }
    });
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::{CfName, Key};
    use engine::IterOption;
    use engine::CF_DEFAULT;
    use kvproto::kvrpcpb::Context;
    use tikv_util::codec::bytes;

    pub const TEST_ENGINE_CFS: &[CfName] = &["cf"];

    pub fn must_put<E: Engine>(engine: &E, key: &[u8], value: &[u8]) {
        engine
            .put(&Context::default(), Key::from_raw(key), value.to_vec())
            .unwrap();
    }

    pub fn must_put_cf<E: Engine>(engine: &E, cf: CfName, key: &[u8], value: &[u8]) {
        engine
            .put_cf(&Context::default(), cf, Key::from_raw(key), value.to_vec())
            .unwrap();
    }

    pub fn must_delete<E: Engine>(engine: &E, key: &[u8]) {
        engine
            .delete(&Context::default(), Key::from_raw(key))
            .unwrap();
    }

    pub fn must_delete_cf<E: Engine>(engine: &E, cf: CfName, key: &[u8]) {
        engine
            .delete_cf(&Context::default(), cf, Key::from_raw(key))
            .unwrap();
    }

    pub fn assert_has<E: Engine>(engine: &E, key: &[u8], value: &[u8]) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap().unwrap(), value);
    }

    pub fn assert_has_cf<E: Engine>(engine: &E, cf: CfName, key: &[u8], value: &[u8]) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        assert_eq!(
            snapshot.get_cf(cf, &Key::from_raw(key)).unwrap().unwrap(),
            value
        );
    }

    pub fn assert_none<E: Engine>(engine: &E, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap(), None);
    }

    pub fn assert_none_cf<E: Engine>(engine: &E, cf: CfName, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        assert_eq!(snapshot.get_cf(cf, &Key::from_raw(key)).unwrap(), None);
    }

    fn assert_seek<E: Engine>(engine: &E, key: &[u8], pair: (&[u8], &[u8])) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = snapshot
            .iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CFStatistics::default();
        cursor.seek(&Key::from_raw(key), &mut statistics).unwrap();
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    fn assert_reverse_seek<E: Engine>(engine: &E, key: &[u8], pair: (&[u8], &[u8])) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = snapshot
            .iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CFStatistics::default();
        cursor
            .reverse_seek(&Key::from_raw(key), &mut statistics)
            .unwrap();
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    fn assert_near_seek<I: Iterator>(cursor: &mut Cursor<I>, key: &[u8], pair: (&[u8], &[u8])) {
        let mut statistics = CFStatistics::default();
        assert!(
            cursor
                .near_seek(&Key::from_raw(key), &mut statistics)
                .unwrap(),
            hex::encode_upper(key)
        );
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    fn assert_near_reverse_seek<I: Iterator>(
        cursor: &mut Cursor<I>,
        key: &[u8],
        pair: (&[u8], &[u8]),
    ) {
        let mut statistics = CFStatistics::default();
        assert!(
            cursor
                .near_reverse_seek(&Key::from_raw(key), &mut statistics)
                .unwrap(),
            hex::encode_upper(key)
        );
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    pub fn test_base_curd_options<E: Engine>(engine: &E) {
        test_get_put(engine);
        test_batch(engine);
        test_empty_seek(engine);
        test_seek(engine);
        test_near_seek(engine);
        test_cf(engine);
        test_empty_write(engine);
    }

    fn test_get_put<E: Engine>(engine: &E) {
        assert_none(engine, b"x");
        must_put(engine, b"x", b"1");
        assert_has(engine, b"x", b"1");
        must_put(engine, b"x", b"2");
        assert_has(engine, b"x", b"2");
    }

    fn test_batch<E: Engine>(engine: &E) {
        engine
            .write(
                &Context::default(),
                vec![
                    Modify::Put(CF_DEFAULT, Key::from_raw(b"x"), b"1".to_vec()),
                    Modify::Put(CF_DEFAULT, Key::from_raw(b"y"), b"2".to_vec()),
                ],
            )
            .unwrap();
        assert_has(engine, b"x", b"1");
        assert_has(engine, b"y", b"2");

        engine
            .write(
                &Context::default(),
                vec![
                    Modify::Delete(CF_DEFAULT, Key::from_raw(b"x")),
                    Modify::Delete(CF_DEFAULT, Key::from_raw(b"y")),
                ],
            )
            .unwrap();
        assert_none(engine, b"y");
        assert_none(engine, b"y");
    }

    fn test_seek<E: Engine>(engine: &E) {
        must_put(engine, b"x", b"1");
        assert_seek(engine, b"x", (b"x", b"1"));
        assert_seek(engine, b"a", (b"x", b"1"));
        assert_reverse_seek(engine, b"x1", (b"x", b"1"));
        must_put(engine, b"z", b"2");
        assert_seek(engine, b"y", (b"z", b"2"));
        assert_seek(engine, b"x\x00", (b"z", b"2"));
        assert_reverse_seek(engine, b"y", (b"x", b"1"));
        assert_reverse_seek(engine, b"z", (b"x", b"1"));
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut iter = snapshot
            .iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CFStatistics::default();
        assert!(!iter
            .seek(&Key::from_raw(b"z\x00"), &mut statistics)
            .unwrap());
        assert!(!iter
            .reverse_seek(&Key::from_raw(b"x"), &mut statistics)
            .unwrap());
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }

    fn test_near_seek<E: Engine>(engine: &E) {
        must_put(engine, b"x", b"1");
        must_put(engine, b"z", b"2");
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = snapshot
            .iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"a", (b"x", b"1"));
        assert_near_reverse_seek(&mut cursor, b"z1", (b"z", b"2"));
        assert_near_reverse_seek(&mut cursor, b"x1", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"y", (b"z", b"2"));
        assert_near_seek(&mut cursor, b"x\x00", (b"z", b"2"));
        let mut statistics = CFStatistics::default();
        assert!(!cursor
            .near_seek(&Key::from_raw(b"z\x00"), &mut statistics)
            .unwrap());
        // Insert many key-values between 'x' and 'z' then near_seek will fallback to seek.
        for i in 0..super::SEEK_BOUND {
            let key = format!("y{}", i);
            must_put(engine, key.as_bytes(), b"3");
        }
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = snapshot
            .iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"z", (b"z", b"2"));

        must_delete(engine, b"x");
        must_delete(engine, b"z");
        for i in 0..super::SEEK_BOUND {
            let key = format!("y{}", i);
            must_delete(engine, key.as_bytes());
        }
    }

    fn test_empty_seek<E: Engine>(engine: &E) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut cursor = snapshot
            .iter(IterOption::default(), ScanMode::Mixed)
            .unwrap();
        let mut statistics = CFStatistics::default();
        assert!(!cursor
            .near_reverse_seek(&Key::from_raw(b"x"), &mut statistics)
            .unwrap());
        assert!(!cursor
            .near_reverse_seek(&Key::from_raw(b"z"), &mut statistics)
            .unwrap());
        assert!(!cursor
            .near_reverse_seek(&Key::from_raw(b"w"), &mut statistics)
            .unwrap());
        assert!(!cursor
            .near_seek(&Key::from_raw(b"x"), &mut statistics)
            .unwrap());
        assert!(!cursor
            .near_seek(&Key::from_raw(b"z"), &mut statistics)
            .unwrap());
        assert!(!cursor
            .near_seek(&Key::from_raw(b"w"), &mut statistics)
            .unwrap());
    }

    macro_rules! assert_seek {
        ($cursor:ident, $func:ident, $k:expr, $res:ident) => {{
            let mut statistics = CFStatistics::default();
            assert_eq!(
                $cursor.$func(&$k, &mut statistics).unwrap(),
                $res.is_some(),
                "assert_seek {} failed exp {:?}",
                $k,
                $res
            );
            if let Some((ref k, ref v)) = $res {
                assert_eq!(
                    $cursor.key(&mut statistics),
                    bytes::encode_bytes(k.as_bytes()).as_slice()
                );
                assert_eq!($cursor.value(&mut statistics), v.as_bytes());
            }
        }};
    }

    #[derive(PartialEq, Eq, Clone, Copy)]
    enum SeekMode {
        Normal,
        Reverse,
        ForPrev,
    }

    // use step to control the distance between target key and current key in cursor.
    fn test_linear_seek<S: Snapshot>(
        snapshot: &S,
        mode: ScanMode,
        seek_mode: SeekMode,
        start_idx: usize,
        step: usize,
    ) {
        let mut cursor = snapshot.iter(IterOption::default(), mode).unwrap();
        let mut near_cursor = snapshot.iter(IterOption::default(), mode).unwrap();
        let limit = (SEEK_BOUND as usize * 10 + 50 - 1) * 2;

        for (_, mut i) in (start_idx..(SEEK_BOUND as usize * 30))
            .enumerate()
            .filter(|&(i, _)| i % step == 0)
        {
            if seek_mode != SeekMode::Normal {
                i = SEEK_BOUND as usize * 30 - 1 - i;
            }
            let key = format!("key_{:03}", i);
            let seek_key = Key::from_raw(key.as_bytes());
            let exp_kv = if i <= 100 {
                match seek_mode {
                    SeekMode::Reverse => None,
                    SeekMode::ForPrev if i < 100 => None,
                    SeekMode::Normal | SeekMode::ForPrev => {
                        Some(("key_100".to_owned(), "value_50".to_owned()))
                    }
                }
            } else if i <= limit {
                if seek_mode == SeekMode::Reverse {
                    Some((
                        format!("key_{}", (i - 1) / 2 * 2),
                        format!("value_{}", (i - 1) / 2),
                    ))
                } else if seek_mode == SeekMode::ForPrev {
                    Some((format!("key_{}", i / 2 * 2), format!("value_{}", i / 2)))
                } else {
                    Some((
                        format!("key_{}", (i + 1) / 2 * 2),
                        format!("value_{}", (i + 1) / 2),
                    ))
                }
            } else if seek_mode != SeekMode::Normal {
                Some((
                    format!("key_{:03}", limit),
                    format!("value_{:03}", limit / 2),
                ))
            } else {
                None
            };

            match seek_mode {
                SeekMode::Reverse => {
                    assert_seek!(cursor, reverse_seek, seek_key, exp_kv);
                    assert_seek!(near_cursor, near_reverse_seek, seek_key, exp_kv);
                }
                SeekMode::Normal => {
                    assert_seek!(cursor, seek, seek_key, exp_kv);
                    assert_seek!(near_cursor, near_seek, seek_key, exp_kv);
                }
                SeekMode::ForPrev => {
                    assert_seek!(cursor, seek_for_prev, seek_key, exp_kv);
                    assert_seek!(near_cursor, near_seek_for_prev, seek_key, exp_kv);
                }
            }
        }
    }

    pub fn test_linear<E: Engine>(engine: &E) {
        for i in 50..50 + SEEK_BOUND * 10 {
            let key = format!("key_{}", i * 2);
            let value = format!("value_{}", i);
            must_put(engine, key.as_bytes(), value.as_bytes());
        }
        let snapshot = engine.snapshot(&Context::default()).unwrap();

        for step in 1..SEEK_BOUND as usize * 3 {
            for start in 0..10 {
                test_linear_seek(
                    &snapshot,
                    ScanMode::Forward,
                    SeekMode::Normal,
                    start * SEEK_BOUND as usize,
                    step,
                );
                test_linear_seek(
                    &snapshot,
                    ScanMode::Backward,
                    SeekMode::Reverse,
                    start * SEEK_BOUND as usize,
                    step,
                );
                test_linear_seek(
                    &snapshot,
                    ScanMode::Backward,
                    SeekMode::ForPrev,
                    start * SEEK_BOUND as usize,
                    step,
                );
            }
        }
        for &seek_mode in &[SeekMode::Reverse, SeekMode::Normal, SeekMode::ForPrev] {
            for step in 1..SEEK_BOUND as usize * 3 {
                for start in 0..10 {
                    test_linear_seek(
                        &snapshot,
                        ScanMode::Mixed,
                        seek_mode,
                        start * SEEK_BOUND as usize,
                        step,
                    );
                }
            }
        }
    }

    fn test_cf<E: Engine>(engine: &E) {
        assert_none_cf(engine, "cf", b"key");
        must_put_cf(engine, "cf", b"key", b"value");
        assert_has_cf(engine, "cf", b"key", b"value");
        must_delete_cf(engine, "cf", b"key");
        assert_none_cf(engine, "cf", b"key");
    }

    fn test_empty_write<E: Engine>(engine: &E) {
        engine.write(&Context::default(), vec![]).unwrap_err();
    }

    pub fn test_cfs_statistics<E: Engine>(engine: &E) {
        must_put(engine, b"foo", b"bar1");
        must_put(engine, b"foo2", b"bar2");
        must_put(engine, b"foo3", b"bar3"); // deleted
        must_put(engine, b"foo4", b"bar4");
        must_put(engine, b"foo42", b"bar42"); // deleted
        must_put(engine, b"foo5", b"bar5"); // deleted
        must_put(engine, b"foo6", b"bar6");
        must_delete(engine, b"foo3");
        must_delete(engine, b"foo42");
        must_delete(engine, b"foo5");

        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut iter = snapshot
            .iter(IterOption::default(), ScanMode::Forward)
            .unwrap();

        let mut statistics = CFStatistics::default();
        iter.seek(&Key::from_raw(b"foo30"), &mut statistics)
            .unwrap();

        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo4"));
        assert_eq!(iter.value(&mut statistics), b"bar4");
        assert_eq!(statistics.seek, 1);

        let mut statistics = CFStatistics::default();
        iter.near_seek(&Key::from_raw(b"foo55"), &mut statistics)
            .unwrap();

        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo6"));
        assert_eq!(iter.value(&mut statistics), b"bar6");
        assert_eq!(statistics.seek, 0);
        assert_eq!(statistics.next, 1);

        let mut statistics = CFStatistics::default();
        iter.prev(&mut statistics);

        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo4"));
        assert_eq!(iter.value(&mut statistics), b"bar4");
        assert_eq!(statistics.prev, 1);

        iter.prev(&mut statistics);
        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo2"));
        assert_eq!(iter.value(&mut statistics), b"bar2");
        assert_eq!(statistics.prev, 2);

        iter.prev(&mut statistics);
        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo"));
        assert_eq!(iter.value(&mut statistics), b"bar1");
        assert_eq!(statistics.prev, 3);
    }
}
