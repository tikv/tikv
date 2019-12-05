// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Multi-version concurrency control functionality.

mod lock;
mod metrics;
mod reader;
mod txn;
mod write;

pub use self::lock::{Lock, LockType};
pub use self::reader::*;
pub use self::txn::{MvccTxn, MAX_TXN_WRITE_SIZE};
pub use self::write::{Write, WriteRef, WriteType};
pub use crate::new_txn;
pub use keys::TimeStamp;

use std::error;
use std::fmt;
use std::io;
use std::sync::Arc;
use tikv_util::collections::HashSet;
use tikv_util::metrics::CRITICAL_ERROR;
use tikv_util::{panic_when_unexpected_key_or_data, set_panic_mark};

const TS_SET_USE_VEC_LIMIT: usize = 8;

/// A hybrid immutable set for timestamps.
#[derive(Debug, Clone, PartialEq)]
pub enum TsSet {
    /// When the set is empty, avoid the useless cloning of Arc.
    Empty,
    /// `Vec` is suitable when the set is small or the set is barely used, and it doesn't worth
    /// converting a `Vec` into a `HashSet`.
    Vec(Arc<Vec<TimeStamp>>),
    /// `Set` is suitable when there are many timestamps **and** it will be queried multiple times.
    Set(Arc<HashSet<TimeStamp>>),
}

impl Default for TsSet {
    #[inline]
    fn default() -> TsSet {
        TsSet::Empty
    }
}

impl TsSet {
    /// Create a `TsSet` from the given vec of timestamps. It will select the proper internal
    /// collection type according to the size.
    #[inline]
    pub fn new(ts: Vec<TimeStamp>) -> Self {
        if ts.is_empty() {
            TsSet::Empty
        } else if ts.len() <= TS_SET_USE_VEC_LIMIT {
            // If there are too few elements in `ts`, use Vec directly instead of making a Set.
            TsSet::Vec(Arc::new(ts))
        } else {
            TsSet::Set(Arc::new(ts.into_iter().collect()))
        }
    }

    pub fn from_u64s(ts: Vec<u64>) -> Self {
        // This conversion is safe because TimeStamp is a transparent wrapper over u64.
        let ts = unsafe { ::std::mem::transmute::<Vec<u64>, Vec<TimeStamp>>(ts) };
        Self::new(ts)
    }

    pub fn vec_from_u64s(ts: Vec<u64>) -> Self {
        // This conversion is safe because TimeStamp is a transparent wrapper over u64.
        let ts = unsafe { ::std::mem::transmute::<Vec<u64>, Vec<TimeStamp>>(ts) };
        Self::vec(ts)
    }

    /// Create a `TsSet` from the given vec of timestamps, but it will be forced to use `Vec` as the
    /// internal collection type. When it's sure that the set will be queried at most once, use this
    /// is better than `TsSet::new`, since both the querying on `Vec` and the conversion from `Vec`
    /// to `HashSet` is O(N).
    #[inline]
    pub fn vec(ts: Vec<TimeStamp>) -> Self {
        if ts.is_empty() {
            TsSet::Empty
        } else {
            TsSet::Vec(Arc::new(ts))
        }
    }

    /// Query whether the given timestamp is contained in the set.
    #[inline]
    pub fn contains(&self, ts: TimeStamp) -> bool {
        match self {
            TsSet::Empty => false,
            TsSet::Vec(vec) => vec.contains(&ts),
            TsSet::Set(set) => set.contains(&ts),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum ErrorInner {
        Engine(err: crate::storage::kv::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Io(err: io::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: tikv_util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        KeyIsLocked(info: kvproto::kvrpcpb::LockInfo) {
            description("key is locked (backoff or cleanup)")
            display("key is locked (backoff or cleanup) {:?}", info)
        }
        BadFormatLock { description("bad format lock data") }
        BadFormatWrite { description("bad format write data") }
        Committed { commit_ts: TimeStamp } {
            description("txn already committed")
            display("txn already committed @{}", commit_ts)
        }
        PessimisticLockRolledBack { start_ts: TimeStamp, key: Vec<u8> } {
            description("pessimistic lock already rollbacked")
            display("pessimistic lock already rollbacked, start_ts:{}, key:{}", start_ts, hex::encode_upper(key))
        }
        TxnLockNotFound { start_ts: TimeStamp, commit_ts: TimeStamp, key: Vec<u8> } {
            description("txn lock not found")
            display("txn lock not found {}-{} key:{}", start_ts, commit_ts, hex::encode_upper(key))
        }
        TxnNotFound { start_ts:  TimeStamp, key: Vec<u8> } {
            description("txn not found")
            display("txn not found {} key: {}", start_ts, hex::encode_upper(key))
        }
        LockTypeNotMatch { start_ts: TimeStamp, key: Vec<u8>, pessimistic: bool } {
            description("lock type not match")
            display("lock type not match, start_ts:{}, key:{}, pessimistic:{}", start_ts, hex::encode_upper(key), pessimistic)
        }
        WriteConflict { start_ts: TimeStamp, conflict_start_ts: TimeStamp, conflict_commit_ts: TimeStamp, key: Vec<u8>, primary: Vec<u8> } {
            description("write conflict")
            display("write conflict, start_ts:{}, conflict_start_ts:{}, conflict_commit_ts:{}, key:{}, primary:{}",
                    start_ts, conflict_start_ts, conflict_commit_ts, hex::encode_upper(key), hex::encode_upper(primary))
        }
        Deadlock { start_ts: TimeStamp, lock_ts: TimeStamp, lock_key: Vec<u8>, deadlock_key_hash: u64 } {
            description("deadlock")
            display("deadlock occurs between txn:{} and txn:{}, lock_key:{}, deadlock_key_hash:{}",
                    start_ts, lock_ts, hex::encode_upper(lock_key), deadlock_key_hash)
        }
        AlreadyExist { key: Vec<u8> } {
            description("already exists")
            display("key {} already exists", hex::encode_upper(key))
        }
        DefaultNotFound { key: Vec<u8> } {
            description("write cf corresponding value not found in default cf")
            display("default not found: key:{}, maybe read truncated/dropped table data?", hex::encode_upper(key))
        }
        CommitTsExpired { start_ts: TimeStamp, commit_ts: TimeStamp, key: Vec<u8>, min_commit_ts: TimeStamp } {
            description("commit_ts less than lock's min_commit_ts")
            display("try to commit key {} with commit_ts {} but min_commit_ts is {}", hex::encode_upper(key), commit_ts, min_commit_ts)
        }
        KeyVersion { description("bad format key(version)") }
        PessimisticLockNotFound { start_ts: TimeStamp, key: Vec<u8> } {
            description("pessimistic lock not found when prewrite")
            display("pessimistic lock not found, start_ts:{}, key:{}", start_ts, hex::encode_upper(key))
        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
    }
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match self {
            ErrorInner::Engine(e) => e.maybe_clone().map(ErrorInner::Engine),
            ErrorInner::Codec(e) => e.maybe_clone().map(ErrorInner::Codec),
            ErrorInner::KeyIsLocked(info) => Some(ErrorInner::KeyIsLocked(info.clone())),
            ErrorInner::BadFormatLock => Some(ErrorInner::BadFormatLock),
            ErrorInner::BadFormatWrite => Some(ErrorInner::BadFormatWrite),
            ErrorInner::TxnLockNotFound {
                start_ts,
                commit_ts,
                key,
            } => Some(ErrorInner::TxnLockNotFound {
                start_ts: *start_ts,
                commit_ts: *commit_ts,
                key: key.to_owned(),
            }),
            ErrorInner::TxnNotFound { start_ts, key } => Some(ErrorInner::TxnNotFound {
                start_ts: *start_ts,
                key: key.to_owned(),
            }),
            ErrorInner::LockTypeNotMatch {
                start_ts,
                key,
                pessimistic,
            } => Some(ErrorInner::LockTypeNotMatch {
                start_ts: *start_ts,
                key: key.to_owned(),
                pessimistic: *pessimistic,
            }),
            ErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key,
                primary,
            } => Some(ErrorInner::WriteConflict {
                start_ts: *start_ts,
                conflict_start_ts: *conflict_start_ts,
                conflict_commit_ts: *conflict_commit_ts,
                key: key.to_owned(),
                primary: primary.to_owned(),
            }),
            ErrorInner::Deadlock {
                start_ts,
                lock_ts,
                lock_key,
                deadlock_key_hash,
            } => Some(ErrorInner::Deadlock {
                start_ts: *start_ts,
                lock_ts: *lock_ts,
                lock_key: lock_key.to_owned(),
                deadlock_key_hash: *deadlock_key_hash,
            }),
            ErrorInner::AlreadyExist { key } => Some(ErrorInner::AlreadyExist { key: key.clone() }),
            ErrorInner::DefaultNotFound { key } => Some(ErrorInner::DefaultNotFound {
                key: key.to_owned(),
            }),
            ErrorInner::CommitTsExpired {
                start_ts,
                commit_ts,
                key,
                min_commit_ts,
            } => Some(ErrorInner::CommitTsExpired {
                start_ts: *start_ts,
                commit_ts: *commit_ts,
                key: key.clone(),
                min_commit_ts: *min_commit_ts,
            }),
            ErrorInner::KeyVersion => Some(ErrorInner::KeyVersion),
            ErrorInner::Committed { commit_ts } => Some(ErrorInner::Committed {
                commit_ts: *commit_ts,
            }),
            ErrorInner::PessimisticLockRolledBack { start_ts, key } => {
                Some(ErrorInner::PessimisticLockRolledBack {
                    start_ts: *start_ts,
                    key: key.to_owned(),
                })
            }
            ErrorInner::PessimisticLockNotFound { start_ts, key } => {
                Some(ErrorInner::PessimisticLockNotFound {
                    start_ts: *start_ts,
                    key: key.to_owned(),
                })
            }
            ErrorInner::Io(_) | ErrorInner::Other(_) => None,
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

impl From<codec::Error> for ErrorInner {
    fn from(err: codec::Error) -> Self {
        box_err!("{}", err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Generates `DefaultNotFound` error or panic directly based on config.
#[inline(never)]
pub fn default_not_found_error(key: Vec<u8>, hint: &str) -> Error {
    CRITICAL_ERROR
        .with_label_values(&["default value not found"])
        .inc();
    if panic_when_unexpected_key_or_data() {
        set_panic_mark();
        panic!(
            "default value not found for key {:?} when {}",
            hex::encode_upper(&key),
            hint,
        );
    } else {
        error!(
            "default value not found";
            "key" => log_wrappers::Key(&key),
            "hint" => hint,
        );
        Error::from(ErrorInner::DefaultNotFound { key })
    }
}

pub mod tests {
    use super::*;
    use crate::storage::{Engine, Modify, Mutation, Options, ScanMode, Snapshot, TxnStatus};
    use engine::CF_WRITE;
    use keys::Key;
    use kvproto::kvrpcpb::{Context, IsolationLevel};

    fn write<E: Engine>(engine: &E, ctx: &Context, modifies: Vec<Modify>) {
        if !modifies.is_empty() {
            engine.write(ctx, modifies).unwrap();
        }
    }

    pub fn must_get<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>, expect: &[u8]) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        assert_eq!(
            reader.get(&Key::from_raw(key), ts.into()).unwrap().unwrap(),
            expect
        );
    }

    pub fn must_get_rc<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>, expect: &[u8]) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Rc);
        assert_eq!(
            reader.get(&Key::from_raw(key), ts.into()).unwrap().unwrap(),
            expect
        );
    }

    pub fn must_get_none<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        assert!(reader
            .get(&Key::from_raw(key), ts.into())
            .unwrap()
            .is_none());
    }

    pub fn must_get_err<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        assert!(reader.get(&Key::from_raw(key), ts.into()).is_err());
    }

    // Insert has a constraint that key should not exist
    pub fn try_prewrite_insert<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Result<()> {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts.into(), true).unwrap();
        txn.prewrite(
            Mutation::Insert((Key::from_raw(key), value.to_vec())),
            pk,
            &Options::default(),
        )?;
        write(engine, &ctx, txn.into_modifies());
        Ok(())
    }

    pub fn must_prewrite_put_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
        options: Options,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts.into(), true).unwrap();
        let mutation = Mutation::Put((Key::from_raw(key), value.to_vec()));
        if options.for_update_ts.is_zero() {
            txn.prewrite(mutation, pk, &options).unwrap();
        } else {
            txn.pessimistic_prewrite(mutation, pk, is_pessimistic_lock, &options)
                .unwrap();
        }
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_prewrite_put<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        let options = Options::default();
        must_prewrite_put_impl(engine, key, value, pk, ts, false, options);
    }

    pub fn must_pessimistic_prewrite_put<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        let mut options = Options::default();
        options.for_update_ts = for_update_ts.into();
        must_prewrite_put_impl(engine, key, value, pk, ts, is_pessimistic_lock, options);
    }

    pub fn must_pessimistic_prewrite_put_with_ttl<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
        lock_ttl: u64,
    ) {
        let mut options = Options::default();
        options.for_update_ts = for_update_ts.into();
        options.lock_ttl = lock_ttl;
        must_prewrite_put_impl(engine, key, value, pk, ts, is_pessimistic_lock, options);
    }

    pub fn must_prewrite_put_for_large_txn<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        ttl: u64,
        for_update_ts: impl Into<TimeStamp>,
    ) {
        let mut options = Options::default();
        options.lock_ttl = ttl;
        let ts = ts.into();
        options.min_commit_ts = (ts.into_inner() + 1).into();
        let for_update_ts = for_update_ts.into();
        options.for_update_ts = for_update_ts;
        must_prewrite_put_impl(
            engine,
            key,
            value,
            pk,
            ts,
            !for_update_ts.is_zero(),
            options,
        );
    }

    fn must_prewrite_put_err_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) -> Error {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts.into(), true).unwrap();
        let mut options = Options::default();
        let for_update_ts = for_update_ts.into();
        options.for_update_ts = for_update_ts;
        let mutation = Mutation::Put((Key::from_raw(key), value.to_vec()));
        if for_update_ts.is_zero() {
            txn.prewrite(mutation, pk, &options).unwrap_err()
        } else {
            txn.pessimistic_prewrite(mutation, pk, is_pessimistic_lock, &options)
                .unwrap_err()
        }
    }

    pub fn must_prewrite_put_err<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) -> Error {
        must_prewrite_put_err_impl(engine, key, value, pk, ts, TimeStamp::zero(), false)
    }

    pub fn must_pessimistic_prewrite_put_err<E: Engine>(
        engine: &E,
        key: &[u8],
        value: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) -> Error {
        must_prewrite_put_err_impl(
            engine,
            key,
            value,
            pk,
            ts,
            for_update_ts,
            is_pessimistic_lock,
        )
    }

    fn must_prewrite_delete_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts.into(), true).unwrap();
        let mut options = Options::default();
        let for_update_ts = for_update_ts.into();
        options.for_update_ts = for_update_ts;
        let mutation = Mutation::Delete(Key::from_raw(key));
        if for_update_ts.is_zero() {
            txn.prewrite(mutation, pk, &options).unwrap();
        } else {
            txn.pessimistic_prewrite(mutation, pk, is_pessimistic_lock, &options)
                .unwrap();
        }
        engine.write(&ctx, txn.into_modifies()).unwrap();
    }

    pub fn must_prewrite_delete<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        must_prewrite_delete_impl(engine, key, pk, ts, TimeStamp::zero(), false);
    }

    pub fn must_pessimistic_prewrite_delete<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        must_prewrite_delete_impl(engine, key, pk, ts, for_update_ts, is_pessimistic_lock);
    }

    fn must_prewrite_lock_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts.into(), true).unwrap();
        let mut options = Options::default();
        let for_update_ts = for_update_ts.into();
        options.for_update_ts = for_update_ts;
        let mutation = Mutation::Lock(Key::from_raw(key));
        if for_update_ts.is_zero() {
            txn.prewrite(mutation, pk, &options).unwrap();
        } else {
            txn.pessimistic_prewrite(mutation, pk, is_pessimistic_lock, &options)
                .unwrap();
        }
        engine.write(&ctx, txn.into_modifies()).unwrap();
    }

    pub fn must_prewrite_lock<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        must_prewrite_lock_impl(engine, key, pk, ts, TimeStamp::zero(), false);
    }

    pub fn must_prewrite_lock_err<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, ts.into(), true).unwrap();
        assert!(txn
            .prewrite(Mutation::Lock(Key::from_raw(key)), pk, &Options::default())
            .is_err());
    }

    pub fn must_pessimistic_prewrite_lock<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        is_pessimistic_lock: bool,
    ) {
        must_prewrite_lock_impl(engine, key, pk, ts, for_update_ts, is_pessimistic_lock);
    }

    pub fn must_acquire_pessimistic_lock_impl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        options: Options,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        txn.acquire_pessimistic_lock(Key::from_raw(key), pk, false, &options)
            .unwrap();
        let modifies = txn.into_modifies();
        if !modifies.is_empty() {
            engine.write(&ctx, modifies).unwrap();
        }
    }

    pub fn must_acquire_pessimistic_lock<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) {
        must_acquire_pessimistic_lock_with_ttl(engine, key, pk, start_ts, for_update_ts, 0);
    }

    pub fn must_acquire_pessimistic_lock_with_ttl<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        ttl: u64,
    ) {
        let mut options = Options::default();
        options.for_update_ts = for_update_ts.into();
        options.lock_ttl = ttl;
        must_acquire_pessimistic_lock_impl(engine, key, pk, start_ts, options);
    }

    pub fn must_acquire_pessimistic_lock_for_large_txn<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
        lock_ttl: u64,
    ) {
        must_acquire_pessimistic_lock_with_ttl(engine, key, pk, start_ts, for_update_ts, lock_ttl);
    }

    pub fn must_acquire_pessimistic_lock_err<E: Engine>(
        engine: &E,
        key: &[u8],
        pk: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) -> Error {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        let mut options = Options::default();
        options.for_update_ts = for_update_ts.into();
        txn.acquire_pessimistic_lock(Key::from_raw(key), pk, false, &options)
            .unwrap_err()
    }

    pub fn must_pessimistic_rollback<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        txn.pessimistic_rollback(Key::from_raw(key), for_update_ts.into())
            .unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_commit<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        txn.commit(Key::from_raw(key), commit_ts.into()).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_commit_err<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        assert!(txn.commit(Key::from_raw(key), commit_ts.into()).is_err());
    }

    pub fn must_rollback<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        txn.collapse_rollback(false);
        txn.rollback(Key::from_raw(key)).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_rollback_collapsed<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        txn.rollback(Key::from_raw(key)).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_rollback_err<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        assert!(txn.rollback(Key::from_raw(key)).is_err());
    }

    pub fn must_cleanup<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        txn.cleanup(Key::from_raw(key), current_ts.into()).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_cleanup_err<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
    ) -> Error {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        txn.cleanup(Key::from_raw(key), current_ts.into())
            .unwrap_err()
    }

    pub fn must_txn_heart_beat<E: Engine>(
        engine: &E,
        primary_key: &[u8],
        start_ts: impl Into<TimeStamp>,
        advise_ttl: u64,
        expect_ttl: u64,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        let ttl = txn
            .txn_heart_beat(Key::from_raw(primary_key), advise_ttl)
            .unwrap();
        write(engine, &ctx, txn.into_modifies());
        assert_eq!(ttl, expect_ttl);
    }

    pub fn must_txn_heart_beat_err<E: Engine>(
        engine: &E,
        primary_key: &[u8],
        start_ts: impl Into<TimeStamp>,
        advise_ttl: u64,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, start_ts.into(), true).unwrap();
        txn.txn_heart_beat(Key::from_raw(primary_key), advise_ttl)
            .unwrap_err();
    }

    pub fn must_check_txn_status<E: Engine>(
        engine: &E,
        primary_key: &[u8],
        lock_ts: impl Into<TimeStamp>,
        caller_start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
        rollback_if_not_exist: bool,
        expect_status: TxnStatus,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, lock_ts.into(), true).unwrap();
        let (txn_status, _) = txn
            .check_txn_status(
                Key::from_raw(primary_key),
                caller_start_ts.into(),
                current_ts.into(),
                rollback_if_not_exist,
            )
            .unwrap();
        assert_eq!(txn_status, expect_status);
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_check_txn_status_err<E: Engine>(
        engine: &E,
        primary_key: &[u8],
        lock_ts: impl Into<TimeStamp>,
        caller_start_ts: impl Into<TimeStamp>,
        current_ts: impl Into<TimeStamp>,
        rollback_if_not_exist: bool,
    ) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn = MvccTxn::new(snapshot, lock_ts.into(), true).unwrap();
        txn.check_txn_status(
            Key::from_raw(primary_key),
            caller_start_ts.into(),
            current_ts.into(),
            rollback_if_not_exist,
        )
        .unwrap_err();
    }

    pub fn must_gc<E: Engine>(engine: &E, key: &[u8], safe_point: impl Into<TimeStamp>) {
        let ctx = Context::default();
        let snapshot = engine.snapshot(&ctx).unwrap();
        let mut txn =
            MvccTxn::for_scan(snapshot, Some(ScanMode::Forward), TimeStamp::zero(), true).unwrap();
        txn.gc(Key::from_raw(key), safe_point.into()).unwrap();
        write(engine, &ctx, txn.into_modifies());
    }

    pub fn must_locked<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(lock.ts, start_ts.into());
        assert_ne!(lock.lock_type, LockType::Pessimistic);
    }

    pub fn must_locked_with_ttl<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        ttl: u64,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(lock.ts, start_ts.into());
        assert_ne!(lock.lock_type, LockType::Pessimistic);
        assert_eq!(lock.ttl, ttl);
    }

    pub fn must_large_txn_locked<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        ttl: u64,
        min_commit_ts: impl Into<TimeStamp>,
        is_pessimistic: bool,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(lock.ts, start_ts.into());
        assert_eq!(lock.ttl, ttl);
        assert_eq!(lock.min_commit_ts, min_commit_ts.into());
        if is_pessimistic {
            assert_eq!(lock.lock_type, LockType::Pessimistic);
        } else {
            assert_ne!(lock.lock_type, LockType::Pessimistic);
        }
    }

    pub fn must_pessimistic_locked<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        for_update_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let lock = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(lock.ts, start_ts.into());
        assert_eq!(lock.for_update_ts, for_update_ts.into());
        assert_eq!(lock.lock_type, LockType::Pessimistic);
    }

    pub fn must_unlocked<E: Engine>(engine: &E, key: &[u8]) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        assert!(reader.load_lock(&Key::from_raw(key)).unwrap().is_none());
    }

    pub fn must_written<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        tp: WriteType,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let k = Key::from_raw(key).append_ts(commit_ts.into());
        let v = snapshot.get_cf(CF_WRITE, &k).unwrap().unwrap();
        let write = WriteRef::parse(&v).unwrap();
        assert_eq!(write.start_ts, start_ts.into());
        assert_eq!(write.write_type, tp);
    }

    pub fn must_seek_write_none<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        assert!(reader
            .seek_write(&Key::from_raw(key), ts.into())
            .unwrap()
            .is_none());
    }

    pub fn must_seek_write<E: Engine>(
        engine: &E,
        key: &[u8],
        ts: impl Into<TimeStamp>,
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        write_type: WriteType,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let (t, write) = reader
            .seek_write(&Key::from_raw(key), ts.into())
            .unwrap()
            .unwrap();
        assert_eq!(t, commit_ts.into());
        assert_eq!(write.start_ts, start_ts.into());
        assert_eq!(write.write_type, write_type);
    }

    pub fn must_get_commit_ts<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);
        let (ts, write_type) = reader
            .get_txn_commit_info(&Key::from_raw(key), start_ts.into())
            .unwrap()
            .unwrap();
        assert_ne!(write_type, WriteType::Rollback);
        assert_eq!(ts, commit_ts.into());
    }

    pub fn must_get_commit_ts_none<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);

        let ret = reader.get_txn_commit_info(&Key::from_raw(key), start_ts.into());
        assert!(ret.is_ok());
        match ret.unwrap() {
            None => {}
            Some((_, write_type)) => {
                assert_eq!(write_type, WriteType::Rollback);
            }
        }
    }

    pub fn must_get_rollback_ts<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);

        let start_ts = start_ts.into();
        let (ts, write_type) = reader
            .get_txn_commit_info(&Key::from_raw(key), start_ts)
            .unwrap()
            .unwrap();
        assert_eq!(ts, start_ts);
        assert_eq!(write_type, WriteType::Rollback);
    }

    pub fn must_get_rollback_ts_none<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);

        let ret = reader
            .get_txn_commit_info(&Key::from_raw(key), start_ts.into())
            .unwrap();
        assert_eq!(ret, None);
    }

    pub fn must_get_rollback_protected<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        protected: bool,
    ) {
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true, IsolationLevel::Si);

        let start_ts = start_ts.into();
        let (ts, write) = reader
            .seek_write(&Key::from_raw(key), start_ts)
            .unwrap()
            .unwrap();
        assert_eq!(ts, start_ts);
        assert_eq!(write.write_type, WriteType::Rollback);
        assert_eq!(write.as_ref().is_protected(), protected);
    }

    pub fn must_scan_keys<E: Engine>(
        engine: &E,
        start: Option<&[u8]>,
        limit: usize,
        keys: Vec<&[u8]>,
        next_start: Option<&[u8]>,
    ) {
        let expect = (
            keys.into_iter().map(Key::from_raw).collect(),
            next_start.map(|x| Key::from_raw(x).append_ts(TimeStamp::zero())),
        );
        let snapshot = engine.snapshot(&Context::default()).unwrap();
        let mut reader =
            MvccReader::new(snapshot, Some(ScanMode::Mixed), false, IsolationLevel::Si);
        assert_eq!(
            reader.scan_keys(start.map(Key::from_raw), limit).unwrap(),
            expect
        );
    }

    #[test]
    fn test_ts_set() {
        let s = TsSet::new(vec![]);
        assert_eq!(s, TsSet::Empty);
        assert!(!s.contains(1.into()));

        let s = TsSet::vec(vec![]);
        assert_eq!(s, TsSet::Empty);

        let s = TsSet::from_u64s(vec![1, 2]);
        assert_eq!(s, TsSet::Vec(Arc::new(vec![1.into(), 2.into()])));
        assert!(s.contains(1.into()));
        assert!(s.contains(2.into()));
        assert!(!s.contains(3.into()));

        let s2 = TsSet::vec(vec![1.into(), 2.into()]);
        assert_eq!(s2, s);

        let big_ts_list: Vec<TimeStamp> =
            (0..=TS_SET_USE_VEC_LIMIT as u64).map(Into::into).collect();
        let s = TsSet::new(big_ts_list.clone());
        assert_eq!(
            s,
            TsSet::Set(Arc::new(big_ts_list.clone().into_iter().collect()))
        );
        assert!(s.contains(1.into()));
        assert!(s.contains((TS_SET_USE_VEC_LIMIT as u64).into()));
        assert!(!s.contains((TS_SET_USE_VEC_LIMIT as u64 + 1).into()));

        let s = TsSet::vec(big_ts_list.clone());
        assert_eq!(s, TsSet::Vec(Arc::new(big_ts_list)));
    }
}
