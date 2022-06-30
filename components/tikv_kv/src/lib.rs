// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! There are multiple [`Engine`](kv::Engine) implementations, [`RaftKv`](crate::server::raftkv::RaftKv)
//! is used by the [`Server`](crate::server::Server). The [`BTreeEngine`](kv::BTreeEngine) and
//! [`RocksEngine`](RocksEngine) are used for testing only.

#![feature(min_specialization)]
#![feature(generic_associated_types)]

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate tikv_util;

mod btree_engine;
mod cursor;
pub mod metrics;
mod mock_engine;
mod raftstore_impls;
mod rocksdb_engine;
mod stats;

use std::{
    cell::UnsafeCell,
    error,
    num::NonZeroU64,
    ptr, result,
    sync::Arc,
    time::{Duration, Instant},
};

use engine_traits::{
    CfName, IterOptions, KvEngine as LocalEngine, Mutable, MvccProperties, ReadOptions, WriteBatch,
    CF_DEFAULT, CF_LOCK,
};
use error_code::{self, ErrorCode, ErrorCodeExt};
use futures::prelude::*;
use into_other::IntoOther;
use kvproto::{
    errorpb::Error as ErrorHeader,
    kvrpcpb::{Context, DiskFullOpt, ExtraOp as TxnExtraOp, KeyRange},
    raft_cmdpb,
};
use pd_client::BucketMeta;
use raftstore::store::{PessimisticLockPair, TxnExt};
use thiserror::Error;
use tikv_util::{deadline::Deadline, escape, time::ThreadReadId};
use tracker::with_tls_tracker;
use txn_types::{Key, PessimisticLock, TimeStamp, TxnExtra, Value};

pub use self::{
    btree_engine::{BTreeEngine, BTreeEngineIterator, BTreeEngineSnapshot},
    cursor::{Cursor, CursorBuilder},
    mock_engine::{ExpectedWrite, MockEngineBuilder},
    rocksdb_engine::{RocksEngine, RocksSnapshot},
    stats::{
        CfStatistics, FlowStatistics, FlowStatsReporter, StageLatencyStats, Statistics,
        StatisticsSummary, RAW_VALUE_TOMBSTONE,
    },
};

pub const SEEK_BOUND: u64 = 8;
const DEFAULT_TIMEOUT_SECS: u64 = 5;

pub type Callback<T> = Box<dyn FnOnce(Result<T>) + Send>;
pub type ExtCallback = Box<dyn FnOnce() + Send>;
pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Modify {
    Delete(CfName, Key),
    Put(CfName, Key, Value),
    PessimisticLock(Key, PessimisticLock),
    // cf_name, start_key, end_key, notify_only
    DeleteRange(CfName, Key, Key, bool),
}

impl Modify {
    pub fn size(&self) -> usize {
        let cf = match self {
            Modify::Delete(cf, _) => cf,
            Modify::Put(cf, ..) => cf,
            Modify::PessimisticLock(..) => &CF_LOCK,
            Modify::DeleteRange(..) => unreachable!(),
        };
        let cf_size = if cf == &CF_DEFAULT { 0 } else { cf.len() };

        match self {
            Modify::Delete(_, k) => cf_size + k.as_encoded().len(),
            Modify::Put(_, k, v) => cf_size + k.as_encoded().len() + v.len(),
            Modify::PessimisticLock(k, _) => cf_size + k.as_encoded().len(), // FIXME: inaccurate
            Modify::DeleteRange(..) => unreachable!(),
        }
    }

    pub fn key(&self) -> &Key {
        match self {
            Modify::Delete(_, ref k) => k,
            Modify::Put(_, ref k, _) => k,
            Modify::PessimisticLock(ref k, _) => k,
            Modify::DeleteRange(..) => unreachable!(),
        }
    }
}

impl From<Modify> for raft_cmdpb::Request {
    fn from(m: Modify) -> raft_cmdpb::Request {
        let mut req = raft_cmdpb::Request::default();
        match m {
            Modify::Delete(cf, k) => {
                let mut delete = raft_cmdpb::DeleteRequest::default();
                delete.set_key(k.into_encoded());
                if cf != CF_DEFAULT {
                    delete.set_cf(cf.to_string());
                }
                req.set_cmd_type(raft_cmdpb::CmdType::Delete);
                req.set_delete(delete);
            }
            Modify::Put(cf, k, v) => {
                let mut put = raft_cmdpb::PutRequest::default();
                put.set_key(k.into_encoded());
                put.set_value(v);
                if cf != CF_DEFAULT {
                    put.set_cf(cf.to_string());
                }
                req.set_cmd_type(raft_cmdpb::CmdType::Put);
                req.set_put(put);
            }
            Modify::PessimisticLock(k, lock) => {
                let v = lock.into_lock().to_bytes();
                let mut put = raft_cmdpb::PutRequest::default();
                put.set_key(k.into_encoded());
                put.set_value(v);
                put.set_cf(CF_LOCK.to_string());
                req.set_cmd_type(raft_cmdpb::CmdType::Put);
                req.set_put(put);
            }
            Modify::DeleteRange(cf, start_key, end_key, notify_only) => {
                let mut delete_range = raft_cmdpb::DeleteRangeRequest::default();
                delete_range.set_cf(cf.to_string());
                delete_range.set_start_key(start_key.into_encoded());
                delete_range.set_end_key(end_key.into_encoded());
                delete_range.set_notify_only(notify_only);
                req.set_cmd_type(raft_cmdpb::CmdType::DeleteRange);
                req.set_delete_range(delete_range);
            }
        };
        req
    }
}

// For test purpose only.
// It's used to simulate observer actions in `rocksdb_engine`. See `RocksEngine::async_write_ext()`.
impl From<raft_cmdpb::Request> for Modify {
    fn from(mut req: raft_cmdpb::Request) -> Modify {
        let name_to_cf = |name: &str| -> Option<CfName> {
            engine_traits::name_to_cf(name).or_else(|| {
                for c in TEST_ENGINE_CFS {
                    if name == *c {
                        return Some(c);
                    }
                }
                None
            })
        };

        match req.get_cmd_type() {
            raft_cmdpb::CmdType::Delete => {
                let delete = req.mut_delete();
                Modify::Delete(
                    name_to_cf(delete.get_cf()).unwrap(),
                    Key::from_encoded(delete.take_key()),
                )
            }
            raft_cmdpb::CmdType::Put => {
                let put = req.mut_put();
                Modify::Put(
                    name_to_cf(put.get_cf()).unwrap(),
                    Key::from_encoded(put.take_key()),
                    put.take_value(),
                )
            }
            raft_cmdpb::CmdType::DeleteRange => {
                let delete_range = req.mut_delete_range();
                Modify::DeleteRange(
                    name_to_cf(delete_range.get_cf()).unwrap(),
                    Key::from_encoded(delete_range.take_start_key()),
                    Key::from_encoded(delete_range.take_end_key()),
                    delete_range.get_notify_only(),
                )
            }
            _ => {
                unimplemented!()
            }
        }
    }
}

impl PessimisticLockPair for Modify {
    fn as_pair(&self) -> (&Key, &PessimisticLock) {
        match self {
            Modify::PessimisticLock(k, lock) => (k, lock),
            _ => panic!("not a pessimistic lock"),
        }
    }

    fn into_pair(self) -> (Key, PessimisticLock) {
        match self {
            Modify::PessimisticLock(k, lock) => (k, lock),
            _ => panic!("not a pessimistic lock"),
        }
    }
}

#[derive(Default)]
pub struct WriteData {
    pub modifies: Vec<Modify>,
    pub extra: TxnExtra,
    pub deadline: Option<Deadline>,
    pub disk_full_opt: DiskFullOpt,
}

impl WriteData {
    pub fn new(modifies: Vec<Modify>, extra: TxnExtra) -> Self {
        Self {
            modifies,
            extra,
            deadline: None,
            disk_full_opt: DiskFullOpt::NotAllowedOnFull,
        }
    }

    pub fn from_modifies(modifies: Vec<Modify>) -> Self {
        Self::new(modifies, TxnExtra::default())
    }

    pub fn size(&self) -> usize {
        let mut total = 0;
        for m in &self.modifies {
            total += m.size();
        }
        total
    }

    pub fn set_allowed_on_disk_almost_full(&mut self) {
        self.disk_full_opt = DiskFullOpt::AllowedOnAlmostFull
    }

    pub fn set_disk_full_opt(&mut self, level: DiskFullOpt) {
        self.disk_full_opt = level
    }
}

#[derive(Debug, Clone, Default)]
pub struct SnapContext<'a> {
    pub pb_ctx: &'a Context,
    pub read_id: Option<ThreadReadId>,
    pub start_ts: TimeStamp,
    // `key_ranges` is used in replica read. It will send to
    // the leader via raft "read index" to check memory locks.
    pub key_ranges: Vec<KeyRange>,
}

/// Engine defines the common behaviour for a storage engine type.
pub trait Engine: Send + Clone + 'static {
    type Snap: Snapshot;
    type Local: LocalEngine;

    /// Local storage engine.
    fn kv_engine(&self) -> Self::Local;

    fn snapshot_on_kv_engine(&self, start_key: &[u8], end_key: &[u8]) -> Result<Self::Snap>;

    /// Write modifications into internal local engine directly.
    fn modify_on_kv_engine(&self, modifies: Vec<Modify>) -> Result<()>;

    fn async_snapshot(&self, ctx: SnapContext<'_>, cb: Callback<Self::Snap>) -> Result<()>;

    fn async_write(&self, ctx: &Context, batch: WriteData, write_cb: Callback<()>) -> Result<()>;

    /// Writes data to the engine asynchronously with some extensions.
    ///
    /// When the write request is proposed successfully, the `proposed_cb` is invoked.
    /// When the write request is finished, the `write_cb` is invoked.
    fn async_write_ext(
        &self,
        ctx: &Context,
        batch: WriteData,
        write_cb: Callback<()>,
        _proposed_cb: Option<ExtCallback>,
        _committed_cb: Option<ExtCallback>,
    ) -> Result<()> {
        self.async_write(ctx, batch, write_cb)
    }

    fn write(&self, ctx: &Context, batch: WriteData) -> Result<()> {
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
        wait_op!(|cb| self.async_write(ctx, batch, cb), timeout)
            .unwrap_or_else(|| Err(Error::from(ErrorInner::Timeout(timeout))))
    }

    fn release_snapshot(&self) {}

    fn snapshot(&self, ctx: SnapContext<'_>) -> Result<Self::Snap> {
        let timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
        wait_op!(|cb| self.async_snapshot(ctx, cb), timeout)
            .unwrap_or_else(|| Err(Error::from(ErrorInner::Timeout(timeout))))
    }

    fn put(&self, ctx: &Context, key: Key, value: Value) -> Result<()> {
        self.put_cf(ctx, CF_DEFAULT, key, value)
    }

    fn put_cf(&self, ctx: &Context, cf: CfName, key: Key, value: Value) -> Result<()> {
        self.write(
            ctx,
            WriteData::from_modifies(vec![Modify::Put(cf, key, value)]),
        )
    }

    fn delete(&self, ctx: &Context, key: Key) -> Result<()> {
        self.delete_cf(ctx, CF_DEFAULT, key)
    }

    fn delete_cf(&self, ctx: &Context, cf: CfName, key: Key) -> Result<()> {
        self.write(ctx, WriteData::from_modifies(vec![Modify::Delete(cf, key)]))
    }

    fn get_mvcc_properties_cf(
        &self,
        _: CfName,
        _safe_point: TimeStamp,
        _start: &[u8],
        _end: &[u8],
    ) -> Option<MvccProperties> {
        None
    }

    // Some engines have a `TxnExtraScheduler`. This method is to send the extra
    // to the scheduler.
    fn schedule_txn_extra(&self, _txn_extra: TxnExtra) {}
}

/// A Snapshot is a consistent view of the underlying engine at a given point in time.
///
/// Note that this is not an MVCC snapshot, that is a higher level abstraction of a view of TiKV
/// at a specific timestamp. This snapshot is lower-level, a view of the underlying storage.
pub trait Snapshot: Sync + Send + Clone {
    type Iter: Iterator;
    type Ext<'a>: SnapshotExt
    where
        Self: 'a;

    /// Get the value associated with `key` in default column family
    fn get(&self, key: &Key) -> Result<Option<Value>>;

    /// Get the value associated with `key` in `cf` column family
    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>>;

    /// Get the value associated with `key` in `cf` column family, with Options in `opts`
    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> Result<Option<Value>>;
    fn iter(&self, iter_opt: IterOptions) -> Result<Self::Iter>;
    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> Result<Self::Iter>;
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

    fn ext(&self) -> Self::Ext<'_>;
}

pub trait SnapshotExt {
    /// Retrieves a version that represents the modification status of the underlying data.
    /// Version should be changed when underlying data is changed.
    ///
    /// If the engine does not support data version, then `None` is returned.
    fn get_data_version(&self) -> Option<u64> {
        None
    }

    fn is_max_ts_synced(&self) -> bool {
        true
    }

    fn get_term(&self) -> Option<NonZeroU64> {
        None
    }

    fn get_txn_extra_op(&self) -> TxnExtraOp {
        TxnExtraOp::Noop
    }

    fn get_txn_ext(&self) -> Option<&Arc<TxnExt>> {
        None
    }

    fn get_buckets(&self) -> Option<Arc<BucketMeta>> {
        None
    }
}

pub struct DummySnapshotExt;

impl SnapshotExt for DummySnapshotExt {}

pub trait Iterator: Send {
    fn next(&mut self) -> Result<bool>;
    fn prev(&mut self) -> Result<bool>;
    fn seek(&mut self, key: &Key) -> Result<bool>;
    fn seek_for_prev(&mut self, key: &Key) -> Result<bool>;
    fn seek_to_first(&mut self) -> Result<bool>;
    fn seek_to_last(&mut self) -> Result<bool>;
    fn valid(&self) -> Result<bool>;

    fn validate_key(&self, _: &Key) -> Result<()> {
        Ok(())
    }

    /// Only be called when `self.valid() == Ok(true)`.
    fn key(&self) -> &[u8];
    /// Only be called when `self.valid() == Ok(true)`.
    fn value(&self) -> &[u8];
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ScanMode {
    Forward,
    Backward,
    Mixed,
}

#[derive(Debug, Error)]
pub enum ErrorInner {
    #[error("{0:?}")]
    Request(ErrorHeader),
    #[error("timeout after {0:?}")]
    Timeout(Duration),
    #[error("an empty requets")]
    EmptyRequest,
    #[error("key is locked (backoff or cleanup) {0:?}")]
    KeyIsLocked(kvproto::kvrpcpb::LockInfo),
    #[error("unknown error {0:?}")]
    Other(#[from] Box<dyn error::Error + Send + Sync>),
}

impl From<ErrorHeader> for ErrorInner {
    fn from(err: ErrorHeader) -> Self {
        Self::Request(err)
    }
}

impl From<engine_traits::Error> for ErrorInner {
    fn from(err: engine_traits::Error) -> Self {
        Self::Request(err.into_other())
    }
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match *self {
            ErrorInner::Request(ref e) => Some(ErrorInner::Request(e.clone())),
            ErrorInner::Timeout(d) => Some(ErrorInner::Timeout(d)),
            ErrorInner::EmptyRequest => Some(ErrorInner::EmptyRequest),
            ErrorInner::KeyIsLocked(ref info) => Some(ErrorInner::KeyIsLocked(info.clone())),
            ErrorInner::Other(_) => None,
        }
    }
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error(#[from] pub Box<ErrorInner>);

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        self.0.maybe_clone().map(Error::from)
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

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Request(e) => e.error_code(),
            ErrorInner::KeyIsLocked(_) => error_code::storage::KEY_IS_LOCKED,
            ErrorInner::Timeout(_) => error_code::storage::TIMEOUT,
            ErrorInner::EmptyRequest => error_code::storage::EMPTY_REQUEST,
            ErrorInner::Other(_) => error_code::storage::UNKNOWN,
        }
    }
}

thread_local! {
    // A pointer to thread local engine. Use raw pointer and `UnsafeCell` to reduce runtime check.
    static TLS_ENGINE_ANY: UnsafeCell<*mut ()> = UnsafeCell::new(ptr::null_mut());
}

/// Execute the closure on the thread local engine.
///
/// # Safety
///
/// Precondition: `TLS_ENGINE_ANY` is non-null.
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
/// Postcondition: `TLS_ENGINE_ANY` is null.
///
/// # Safety
///
/// The current tls engine must have the same type as `E` (or at least
/// there destructors must be compatible).
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

/// Get a snapshot of `engine`.
pub fn snapshot<E: Engine>(
    engine: &E,
    ctx: SnapContext<'_>,
) -> impl std::future::Future<Output = Result<E::Snap>> {
    let begin = Instant::now();
    let (callback, future) =
        tikv_util::future::paired_must_called_future_callback(drop_snapshot_callback::<E>);
    let val = engine.async_snapshot(ctx, callback);
    // make engine not cross yield point
    async move {
        val?; // propagate error
        let result = future
            .map_err(|cancel| Error::from(ErrorInner::Other(box_err!(cancel))))
            .await?;
        with_tls_tracker(|tracker| {
            tracker.metrics.get_snapshot_nanos += begin.elapsed().as_nanos() as u64;
        });
        fail_point!("after-snapshot");
        result
    }
}

pub fn drop_snapshot_callback<E: Engine>() -> Result<E::Snap> {
    let bt = backtrace::Backtrace::new();
    warn!("async snapshot callback is dropped"; "backtrace" => ?bt);
    let mut err = ErrorHeader::default();
    err.set_message("async snapshot callback is dropped".to_string());
    Err(Error::from(ErrorInner::Request(err)))
}

/// Write modifications into a `BaseRocksEngine` instance.
pub fn write_modifies(kv_engine: &impl LocalEngine, modifies: Vec<Modify>) -> Result<()> {
    fail_point!("rockskv_write_modifies", |_| Err(box_err!("write failed")));

    let mut wb = kv_engine.write_batch();
    for rev in modifies {
        let res = match rev {
            Modify::Delete(cf, k) => {
                if cf == CF_DEFAULT {
                    trace!("RocksEngine: delete"; "key" => %k);
                    wb.delete(k.as_encoded())
                } else {
                    trace!("RocksEngine: delete_cf"; "cf" => cf, "key" => %k);
                    wb.delete_cf(cf, k.as_encoded())
                }
            }
            Modify::Put(cf, k, v) => {
                if cf == CF_DEFAULT {
                    trace!("RocksEngine: put"; "key" => %k, "value" => escape(&v));
                    wb.put(k.as_encoded(), &v)
                } else {
                    trace!("RocksEngine: put_cf"; "cf" => cf, "key" => %k, "value" => escape(&v));
                    wb.put_cf(cf, k.as_encoded(), &v)
                }
            }
            Modify::PessimisticLock(k, lock) => {
                let v = lock.into_lock().to_bytes();
                trace!("RocksEngine: put lock"; "key" => %k, "values" => escape(&v));
                wb.put_cf(CF_LOCK, k.as_encoded(), &v)
            }
            Modify::DeleteRange(cf, start_key, end_key, notify_only) => {
                trace!(
                    "RocksEngine: delete_range_cf";
                    "cf" => cf,
                    "start_key" => %start_key,
                    "end_key" => %end_key,
                    "notify_only" => notify_only,
                );
                if !notify_only {
                    wb.delete_range_cf(cf, start_key.as_encoded(), end_key.as_encoded())
                } else {
                    Ok(())
                }
            }
        };
        // TODO: turn the error into an engine error.
        if let Err(msg) = res {
            return Err(box_err!("{}", msg));
        }
    }
    wb.write()?;
    Ok(())
}

pub const TEST_ENGINE_CFS: &[CfName] = &["cf"];

pub mod tests {
    use tikv_util::codec::bytes;

    use super::*;

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
        let snapshot = engine.snapshot(Default::default()).unwrap();
        assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap().unwrap(), value);
    }

    pub fn assert_has_cf<E: Engine>(engine: &E, cf: CfName, key: &[u8], value: &[u8]) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        assert_eq!(
            snapshot.get_cf(cf, &Key::from_raw(key)).unwrap().unwrap(),
            value
        );
    }

    pub fn assert_none<E: Engine>(engine: &E, key: &[u8]) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap(), None);
    }

    pub fn assert_none_cf<E: Engine>(engine: &E, cf: CfName, key: &[u8]) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        assert_eq!(snapshot.get_cf(cf, &Key::from_raw(key)).unwrap(), None);
    }

    fn assert_seek<E: Engine>(engine: &E, key: &[u8], pair: (&[u8], &[u8])) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut cursor = Cursor::new(
            snapshot.iter(IterOptions::default()).unwrap(),
            ScanMode::Mixed,
            false,
        );
        let mut statistics = CfStatistics::default();
        cursor.seek(&Key::from_raw(key), &mut statistics).unwrap();
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    fn assert_reverse_seek<E: Engine>(engine: &E, key: &[u8], pair: (&[u8], &[u8])) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut cursor = Cursor::new(
            snapshot.iter(IterOptions::default()).unwrap(),
            ScanMode::Mixed,
            false,
        );
        let mut statistics = CfStatistics::default();
        cursor
            .reverse_seek(&Key::from_raw(key), &mut statistics)
            .unwrap();
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    fn assert_near_seek<I: Iterator>(cursor: &mut Cursor<I>, key: &[u8], pair: (&[u8], &[u8])) {
        let mut statistics = CfStatistics::default();
        assert!(
            cursor
                .near_seek(&Key::from_raw(key), &mut statistics)
                .unwrap(),
            "{}",
            log_wrappers::hex_encode_upper(key)
        );
        assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
        assert_eq!(cursor.value(&mut statistics), pair.1);
    }

    fn assert_near_reverse_seek<I: Iterator>(
        cursor: &mut Cursor<I>,
        key: &[u8],
        pair: (&[u8], &[u8]),
    ) {
        let mut statistics = CfStatistics::default();
        assert!(
            cursor
                .near_reverse_seek(&Key::from_raw(key), &mut statistics)
                .unwrap(),
            "{}",
            log_wrappers::hex_encode_upper(key)
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
                WriteData::from_modifies(vec![
                    Modify::Put(CF_DEFAULT, Key::from_raw(b"x"), b"1".to_vec()),
                    Modify::Put(CF_DEFAULT, Key::from_raw(b"y"), b"2".to_vec()),
                ]),
            )
            .unwrap();
        assert_has(engine, b"x", b"1");
        assert_has(engine, b"y", b"2");

        engine
            .write(
                &Context::default(),
                WriteData::from_modifies(vec![
                    Modify::Delete(CF_DEFAULT, Key::from_raw(b"x")),
                    Modify::Delete(CF_DEFAULT, Key::from_raw(b"y")),
                ]),
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
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut iter = Cursor::new(
            snapshot.iter(IterOptions::default()).unwrap(),
            ScanMode::Mixed,
            false,
        );
        let mut statistics = CfStatistics::default();
        assert!(
            !iter
                .seek(&Key::from_raw(b"z\x00"), &mut statistics)
                .unwrap()
        );
        assert!(
            !iter
                .reverse_seek(&Key::from_raw(b"x"), &mut statistics)
                .unwrap()
        );
        must_delete(engine, b"x");
        must_delete(engine, b"z");
    }

    fn test_near_seek<E: Engine>(engine: &E) {
        must_put(engine, b"x", b"1");
        must_put(engine, b"z", b"2");
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut cursor = Cursor::new(
            snapshot.iter(IterOptions::default()).unwrap(),
            ScanMode::Mixed,
            false,
        );
        assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"a", (b"x", b"1"));
        assert_near_reverse_seek(&mut cursor, b"z1", (b"z", b"2"));
        assert_near_reverse_seek(&mut cursor, b"x1", (b"x", b"1"));
        assert_near_seek(&mut cursor, b"y", (b"z", b"2"));
        assert_near_seek(&mut cursor, b"x\x00", (b"z", b"2"));
        let mut statistics = CfStatistics::default();
        assert!(
            !cursor
                .near_seek(&Key::from_raw(b"z\x00"), &mut statistics)
                .unwrap()
        );
        // Insert many key-values between 'x' and 'z' then near_seek will fallback to seek.
        for i in 0..super::SEEK_BOUND {
            let key = format!("y{}", i);
            must_put(engine, key.as_bytes(), b"3");
        }
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut cursor = Cursor::new(
            snapshot.iter(IterOptions::default()).unwrap(),
            ScanMode::Mixed,
            false,
        );
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
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut cursor = Cursor::new(
            snapshot.iter(IterOptions::default()).unwrap(),
            ScanMode::Mixed,
            false,
        );
        let mut statistics = CfStatistics::default();
        assert!(
            !cursor
                .near_reverse_seek(&Key::from_raw(b"x"), &mut statistics)
                .unwrap()
        );
        assert!(
            !cursor
                .near_reverse_seek(&Key::from_raw(b"z"), &mut statistics)
                .unwrap()
        );
        assert!(
            !cursor
                .near_reverse_seek(&Key::from_raw(b"w"), &mut statistics)
                .unwrap()
        );
        assert!(
            !cursor
                .near_seek(&Key::from_raw(b"x"), &mut statistics)
                .unwrap()
        );
        assert!(
            !cursor
                .near_seek(&Key::from_raw(b"z"), &mut statistics)
                .unwrap()
        );
        assert!(
            !cursor
                .near_seek(&Key::from_raw(b"w"), &mut statistics)
                .unwrap()
        );
    }

    macro_rules! assert_seek {
        ($cursor:ident, $func:ident, $k:expr, $res:ident) => {{
            let mut statistics = CfStatistics::default();
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
        let mut cursor = Cursor::new(snapshot.iter(IterOptions::default()).unwrap(), mode, false);
        let mut near_cursor =
            Cursor::new(snapshot.iter(IterOptions::default()).unwrap(), mode, false);
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
        let snapshot = engine.snapshot(Default::default()).unwrap();

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
        engine
            .write(&Context::default(), WriteData::default())
            .unwrap_err();
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

        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut iter = Cursor::new(
            snapshot.iter(IterOptions::default()).unwrap(),
            ScanMode::Forward,
            false,
        );

        let mut statistics = CfStatistics::default();
        iter.seek(&Key::from_raw(b"foo30"), &mut statistics)
            .unwrap();

        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo4"));
        assert_eq!(iter.value(&mut statistics), b"bar4");
        assert_eq!(statistics.seek, 1);

        let mut statistics = CfStatistics::default();
        iter.near_seek(&Key::from_raw(b"foo55"), &mut statistics)
            .unwrap();

        assert_eq!(iter.key(&mut statistics), &*bytes::encode_bytes(b"foo6"));
        assert_eq!(iter.value(&mut statistics), b"bar6");
        assert_eq!(statistics.seek, 0);
        assert_eq!(statistics.next, 1);

        let mut statistics = CfStatistics::default();
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

#[cfg(test)]
mod unit_tests {
    use engine_traits::CF_WRITE;

    use super::*;
    use crate::raft_cmdpb;

    #[test]
    fn test_modifies_to_requests() {
        let modifies = vec![
            Modify::Delete(CF_DEFAULT, Key::from_encoded_slice(b"k-del")),
            Modify::Put(
                CF_WRITE,
                Key::from_encoded_slice(b"k-put"),
                b"v-put".to_vec(),
            ),
            Modify::PessimisticLock(
                Key::from_encoded_slice(b"k-lock"),
                PessimisticLock {
                    primary: b"primary".to_vec().into_boxed_slice(),
                    start_ts: 100.into(),
                    ttl: 200,
                    for_update_ts: 101.into(),
                    min_commit_ts: 102.into(),
                },
            ),
            Modify::DeleteRange(
                CF_DEFAULT,
                Key::from_encoded_slice(b"kd-start"),
                Key::from_encoded_slice(b"kd-end"),
                false,
            ),
        ];

        let requests = vec![
            {
                let mut delete = raft_cmdpb::DeleteRequest::default();
                delete.set_key(b"k-del".to_vec());

                let mut req = raft_cmdpb::Request::default();
                req.set_cmd_type(raft_cmdpb::CmdType::Delete);
                req.set_delete(delete);
                req
            },
            {
                let mut put = raft_cmdpb::PutRequest::default();
                put.set_cf("write".to_string());
                put.set_key(b"k-put".to_vec());
                put.set_value(b"v-put".to_vec());

                let mut req = raft_cmdpb::Request::default();
                req.set_cmd_type(raft_cmdpb::CmdType::Put);
                req.set_put(put);
                req
            },
            {
                let mut put = raft_cmdpb::PutRequest::default();
                put.set_cf("lock".to_string());
                put.set_key(b"k-lock".to_vec());
                put.set_value(
                    PessimisticLock {
                        primary: b"primary".to_vec().into_boxed_slice(),
                        start_ts: 100.into(),
                        ttl: 200,
                        for_update_ts: 101.into(),
                        min_commit_ts: 102.into(),
                    }
                    .into_lock()
                    .to_bytes(),
                );

                let mut req = raft_cmdpb::Request::default();
                req.set_cmd_type(raft_cmdpb::CmdType::Put);
                req.set_put(put);
                req
            },
            {
                let mut delete_range = raft_cmdpb::DeleteRangeRequest::default();
                delete_range.set_cf("default".to_string());
                delete_range.set_start_key(b"kd-start".to_vec());
                delete_range.set_end_key(b"kd-end".to_vec());
                delete_range.set_notify_only(false);

                let mut req = raft_cmdpb::Request::default();
                req.set_cmd_type(raft_cmdpb::CmdType::DeleteRange);
                req.set_delete_range(delete_range);
                req
            },
        ];

        assert_eq!(
            modifies
                .clone()
                .into_iter()
                .map(Into::into)
                .collect::<Vec<raft_cmdpb::Request>>(),
            requests
        );

        let expect_requests: Vec<_> = modifies
            .into_iter()
            .map(|m| match m {
                Modify::PessimisticLock(k, lock) => {
                    Modify::Put(CF_LOCK, k, lock.into_lock().to_bytes())
                }
                _ => m,
            })
            .collect();
        assert_eq!(
            requests
                .into_iter()
                .map(Into::into)
                .collect::<Vec<Modify>>(),
            expect_requests
        )
    }
}
