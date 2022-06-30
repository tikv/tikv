// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
//! Multi-version concurrency control functionality.

mod consistency_check;
pub(super) mod metrics;
pub(crate) mod reader;
pub(super) mod txn;

use std::{error, io};

use error_code::{self, ErrorCode, ErrorCodeExt};
use kvproto::kvrpcpb::{Assertion, IsolationLevel};
use thiserror::Error;
use tikv_util::{metrics::CRITICAL_ERROR, panic_when_unexpected_key_or_data, set_panic_mark};
pub use txn_types::{
    Key, Lock, LockType, Mutation, TimeStamp, Value, Write, WriteRef, WriteType,
    SHORT_VALUE_MAX_LEN,
};

pub use self::{
    consistency_check::{Mvcc as MvccConsistencyCheckObserver, MvccInfoIterator},
    metrics::{GC_DELETE_VERSIONS_HISTOGRAM, MVCC_VERSIONS_HISTOGRAM},
    reader::*,
    txn::{GcInfo, MvccTxn, ReleasedLock, MAX_TXN_WRITE_SIZE},
};

#[derive(Debug, Error)]
pub enum ErrorInner {
    #[error("{0}")]
    Kv(#[from] crate::storage::kv::Error),

    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    Codec(#[from] tikv_util::codec::Error),

    #[error("key is locked (backoff or cleanup) {0:?}")]
    KeyIsLocked(kvproto::kvrpcpb::LockInfo),

    #[error("{0}")]
    BadFormat(#[source] txn_types::Error),

    #[error(
        "txn already committed, start_ts: {}, commit_ts: {}, key: {}",
        .start_ts, .commit_ts, log_wrappers::Value::key(.key)
    )]
    Committed {
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        key: Vec<u8>,
    },

    #[error(
        "pessimistic lock already rollbacked, start_ts:{}, key:{}",
        .start_ts, log_wrappers::Value::key(.key)
    )]
    PessimisticLockRolledBack { start_ts: TimeStamp, key: Vec<u8> },

    #[error(
        "txn lock not found {}-{} key:{}",
        .start_ts, .commit_ts, log_wrappers::Value::key(.key)
    )]
    TxnLockNotFound {
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        key: Vec<u8>,
    },

    #[error("txn not found {} key: {}", .start_ts, log_wrappers::Value::key(.key))]
    TxnNotFound { start_ts: TimeStamp, key: Vec<u8> },

    #[error(
        "lock type not match, start_ts:{}, key:{}, pessimistic:{}",
        .start_ts, log_wrappers::Value::key(.key), .pessimistic
    )]
    LockTypeNotMatch {
        start_ts: TimeStamp,
        key: Vec<u8>,
        pessimistic: bool,
    },

    #[error(
        "write conflict, start_ts:{}, conflict_start_ts:{}, conflict_commit_ts:{}, key:{}, primary:{}",
        .start_ts, .conflict_start_ts, .conflict_commit_ts,
        log_wrappers::Value::key(.key), log_wrappers::Value::key(.primary)
    )]
    WriteConflict {
        start_ts: TimeStamp,
        conflict_start_ts: TimeStamp,
        conflict_commit_ts: TimeStamp,
        key: Vec<u8>,
        primary: Vec<u8>,
    },

    #[error(
        "deadlock occurs between txn:{} and txn:{}, lock_key:{}, deadlock_key_hash:{}",
        .start_ts, .lock_ts, log_wrappers::Value::key(.lock_key), .deadlock_key_hash
    )]
    Deadlock {
        start_ts: TimeStamp,
        lock_ts: TimeStamp,
        lock_key: Vec<u8>,
        deadlock_key_hash: u64,
        wait_chain: Vec<kvproto::deadlock::WaitForEntry>,
    },

    #[error("key {} already exists", log_wrappers::Value::key(.key))]
    AlreadyExist { key: Vec<u8> },

    #[error(
        "default not found: key:{}, maybe read truncated/dropped table data?",
        log_wrappers::Value::key(.key)
    )]
    DefaultNotFound { key: Vec<u8> },

    #[error(
        "try to commit key {} with commit_ts {} but min_commit_ts is {}",
        log_wrappers::Value::key(.key), .commit_ts, .min_commit_ts
    )]
    CommitTsExpired {
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        key: Vec<u8>,
        min_commit_ts: TimeStamp,
    },

    #[error("bad format key(version)")]
    KeyVersion,

    #[error(
        "pessimistic lock not found, start_ts:{}, key:{}",
        .start_ts, log_wrappers::Value::key(.key)
    )]
    PessimisticLockNotFound { start_ts: TimeStamp, key: Vec<u8> },

    #[error(
        "min_commit_ts {} is larger than max_commit_ts {}, start_ts: {}",
        .min_commit_ts, .max_commit_ts, .start_ts
    )]
    CommitTsTooLarge {
        start_ts: TimeStamp,
        min_commit_ts: TimeStamp,
        max_commit_ts: TimeStamp,
    },

    #[error(
        "assertion on data failed, start_ts:{}, key:{}, assertion:{:?}, existing_start_ts:{}, existing_commit_ts:{}",
        .start_ts, log_wrappers::Value::key(.key), .assertion, .existing_start_ts, .existing_commit_ts
    )]
    AssertionFailed {
        start_ts: TimeStamp,
        key: Vec<u8>,
        assertion: Assertion,
        existing_start_ts: TimeStamp,
        existing_commit_ts: TimeStamp,
    },

    #[error("{0:?}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
}

impl ErrorInner {
    pub fn maybe_clone(&self) -> Option<ErrorInner> {
        match self {
            ErrorInner::Kv(e) => e.maybe_clone().map(ErrorInner::Kv),
            ErrorInner::Codec(e) => e.maybe_clone().map(ErrorInner::Codec),
            ErrorInner::KeyIsLocked(info) => Some(ErrorInner::KeyIsLocked(info.clone())),
            ErrorInner::BadFormat(e) => e.maybe_clone().map(ErrorInner::BadFormat),
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
                wait_chain,
            } => Some(ErrorInner::Deadlock {
                start_ts: *start_ts,
                lock_ts: *lock_ts,
                lock_key: lock_key.to_owned(),
                deadlock_key_hash: *deadlock_key_hash,
                wait_chain: wait_chain.clone(),
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
            ErrorInner::Committed {
                start_ts,
                commit_ts,
                key,
            } => Some(ErrorInner::Committed {
                start_ts: *start_ts,
                commit_ts: *commit_ts,
                key: key.clone(),
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
            ErrorInner::CommitTsTooLarge {
                start_ts,
                min_commit_ts,
                max_commit_ts,
            } => Some(ErrorInner::CommitTsTooLarge {
                start_ts: *start_ts,
                min_commit_ts: *min_commit_ts,
                max_commit_ts: *max_commit_ts,
            }),
            ErrorInner::AssertionFailed {
                start_ts,
                key,
                assertion,
                existing_start_ts,
                existing_commit_ts,
            } => Some(ErrorInner::AssertionFailed {
                start_ts: *start_ts,
                key: key.clone(),
                assertion: *assertion,
                existing_start_ts: *existing_start_ts,
                existing_commit_ts: *existing_commit_ts,
            }),
            ErrorInner::Io(_) | ErrorInner::Other(_) => None,
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

impl From<codec::Error> for ErrorInner {
    fn from(err: codec::Error) -> Self {
        box_err!("{}", err)
    }
}

impl From<::pd_client::Error> for ErrorInner {
    fn from(err: ::pd_client::Error) -> Self {
        box_err!("{}", err)
    }
}

impl From<txn_types::Error> for ErrorInner {
    fn from(err: txn_types::Error) -> Self {
        match err {
            txn_types::Error(box txn_types::ErrorInner::Io(e)) => ErrorInner::Io(e),
            txn_types::Error(box txn_types::ErrorInner::Codec(e)) => ErrorInner::Codec(e),
            txn_types::Error(box txn_types::ErrorInner::BadFormatLock)
            | txn_types::Error(box txn_types::ErrorInner::BadFormatWrite) => {
                ErrorInner::BadFormat(err)
            }
            txn_types::Error(box txn_types::ErrorInner::KeyIsLocked(lock_info)) => {
                ErrorInner::KeyIsLocked(lock_info)
            }
            txn_types::Error(box txn_types::ErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key,
                primary,
            }) => ErrorInner::WriteConflict {
                start_ts,
                conflict_start_ts,
                conflict_commit_ts,
                key,
                primary,
            },
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self.0.as_ref() {
            ErrorInner::Kv(e) => e.error_code(),
            ErrorInner::Io(_) => error_code::storage::IO,
            ErrorInner::Codec(e) => e.error_code(),
            ErrorInner::KeyIsLocked(_) => error_code::storage::KEY_IS_LOCKED,
            ErrorInner::BadFormat(e) => e.error_code(),
            ErrorInner::Committed { .. } => error_code::storage::COMMITTED,
            ErrorInner::PessimisticLockRolledBack { .. } => {
                error_code::storage::PESSIMISTIC_LOCK_ROLLED_BACK
            }
            ErrorInner::TxnLockNotFound { .. } => error_code::storage::TXN_LOCK_NOT_FOUND,
            ErrorInner::TxnNotFound { .. } => error_code::storage::TXN_NOT_FOUND,
            ErrorInner::LockTypeNotMatch { .. } => error_code::storage::LOCK_TYPE_NOT_MATCH,
            ErrorInner::WriteConflict { .. } => error_code::storage::WRITE_CONFLICT,
            ErrorInner::Deadlock { .. } => error_code::storage::DEADLOCK,
            ErrorInner::AlreadyExist { .. } => error_code::storage::ALREADY_EXIST,
            ErrorInner::DefaultNotFound { .. } => error_code::storage::DEFAULT_NOT_FOUND,
            ErrorInner::CommitTsExpired { .. } => error_code::storage::COMMIT_TS_EXPIRED,
            ErrorInner::KeyVersion => error_code::storage::KEY_VERSION,
            ErrorInner::PessimisticLockNotFound { .. } => {
                error_code::storage::PESSIMISTIC_LOCK_NOT_FOUND
            }
            ErrorInner::CommitTsTooLarge { .. } => error_code::storage::COMMIT_TS_TOO_LARGE,
            ErrorInner::AssertionFailed { .. } => error_code::storage::ASSERTION_FAILED,
            ErrorInner::Other(_) => error_code::storage::UNKNOWN,
        }
    }
}

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
            &log_wrappers::Value::key(&key),
            hint,
        );
    } else {
        error!(
            "default value not found";
            "key" => &log_wrappers::Value::key(&key),
            "hint" => hint,
        );
        Error::from(ErrorInner::DefaultNotFound { key })
    }
}

pub mod tests {
    use std::borrow::Cow;

    use engine_traits::CF_WRITE;
    use kvproto::kvrpcpb::Context;
    use txn_types::Key;

    use super::*;
    use crate::storage::kv::{Engine, Modify, ScanMode, SnapContext, Snapshot, WriteData};

    pub fn write<E: Engine>(engine: &E, ctx: &Context, modifies: Vec<Modify>) {
        if !modifies.is_empty() {
            engine
                .write(ctx, WriteData::from_modifies(modifies))
                .unwrap();
        }
    }

    pub fn must_get<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>, expect: &[u8]) {
        let ts = ts.into();
        let ctx = SnapContext::default();
        let snapshot = engine.snapshot(ctx).unwrap();
        let mut reader = SnapshotReader::new(ts, snapshot, true);
        let key = &Key::from_raw(key);

        check_lock(&mut reader, key, ts).unwrap();
        assert_eq!(reader.get(key, ts).unwrap().unwrap(), expect);
    }

    pub fn must_get_no_lock_check<E: Engine>(
        engine: &E,
        key: &[u8],
        ts: impl Into<TimeStamp>,
        expect: &[u8],
    ) {
        let ts = ts.into();
        let ctx = SnapContext::default();
        let snapshot = engine.snapshot(ctx).unwrap();
        let mut reader = SnapshotReader::new(ts, snapshot, true);
        assert_eq!(
            reader.get(&Key::from_raw(key), ts).unwrap().unwrap(),
            expect
        );
    }

    /// Checks if there is a lock which blocks reading the key at the given ts.
    /// Returns the blocking lock as the `Err` variant.
    fn check_lock(
        reader: &mut SnapshotReader<impl Snapshot>,
        key: &Key,
        ts: TimeStamp,
    ) -> Result<()> {
        if let Some(lock) = reader.load_lock(key)? {
            if let Err(e) = Lock::check_ts_conflict(
                Cow::Owned(lock),
                key,
                ts,
                &Default::default(),
                IsolationLevel::Si,
            ) {
                return Err(e.into());
            }
        }
        Ok(())
    }

    pub fn must_get_none<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let ts = ts.into();
        let ctx = SnapContext::default();
        let snapshot = engine.snapshot(ctx).unwrap();
        let mut reader = SnapshotReader::new(ts, snapshot, true);
        let key = &Key::from_raw(key);
        check_lock(&mut reader, key, ts).unwrap();
        assert!(reader.get(key, ts).unwrap().is_none());
    }

    pub fn must_get_err<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let ts = ts.into();
        let ctx = SnapContext::default();
        let snapshot = engine.snapshot(ctx).unwrap();
        let mut reader = SnapshotReader::new(ts, snapshot, true);
        let key = &Key::from_raw(key);
        if check_lock(&mut reader, key, ts).is_err() {
            return;
        }
        assert!(reader.get(key, ts).is_err());
    }

    pub fn must_locked<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) -> Lock {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
        let lock = reader.load_lock(&Key::from_raw(key)).unwrap().unwrap();
        assert_eq!(lock.ts, start_ts.into());
        assert_ne!(lock.lock_type, LockType::Pessimistic);
        lock
    }

    pub fn must_locked_with_ttl<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        ttl: u64,
    ) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
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
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
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

    pub fn must_unlocked<E: Engine>(engine: &E, key: &[u8]) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
        assert!(reader.load_lock(&Key::from_raw(key)).unwrap().is_none());
    }

    pub fn must_written<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        tp: WriteType,
    ) -> Write {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let k = Key::from_raw(key).append_ts(commit_ts.into());
        let v = snapshot.get_cf(CF_WRITE, &k).unwrap().unwrap();
        let write = WriteRef::parse(&v).unwrap();
        assert_eq!(write.start_ts, start_ts.into());
        assert_eq!(write.write_type, tp);
        write.to_owned()
    }

    pub fn must_have_write<E: Engine>(
        engine: &E,
        key: &[u8],
        commit_ts: impl Into<TimeStamp>,
    ) -> Write {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let k = Key::from_raw(key).append_ts(commit_ts.into());
        let v = snapshot.get_cf(CF_WRITE, &k).unwrap().unwrap();
        let write = WriteRef::parse(&v).unwrap();
        write.to_owned()
    }

    pub fn must_not_have_write<E: Engine>(engine: &E, key: &[u8], commit_ts: impl Into<TimeStamp>) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let k = Key::from_raw(key).append_ts(commit_ts.into());
        let v = snapshot.get_cf(CF_WRITE, &k).unwrap();
        assert!(v.is_none());
    }

    pub fn must_seek_write_none<E: Engine>(engine: &E, key: &[u8], ts: impl Into<TimeStamp>) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
        assert!(
            reader
                .seek_write(&Key::from_raw(key), ts.into())
                .unwrap()
                .is_none()
        );
    }

    pub fn must_seek_write<E: Engine>(
        engine: &E,
        key: &[u8],
        ts: impl Into<TimeStamp>,
        start_ts: impl Into<TimeStamp>,
        commit_ts: impl Into<TimeStamp>,
        write_type: WriteType,
    ) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);
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
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = SnapshotReader::new(start_ts.into(), snapshot, true);
        let (ts, write_type) = reader
            .get_txn_commit_record(&Key::from_raw(key))
            .unwrap()
            .info()
            .unwrap();
        assert_ne!(write_type, WriteType::Rollback);
        assert_eq!(ts, commit_ts.into());
    }

    pub fn must_get_commit_ts_none<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = SnapshotReader::new(start_ts.into(), snapshot, true);

        let ret = reader.get_txn_commit_record(&Key::from_raw(key));
        assert!(ret.is_ok());
        match ret.unwrap().info() {
            None => {}
            Some((_, write_type)) => {
                assert_eq!(write_type, WriteType::Rollback);
            }
        }
    }

    pub fn must_get_rollback_ts<E: Engine>(engine: &E, key: &[u8], start_ts: impl Into<TimeStamp>) {
        let start_ts = start_ts.into();
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = SnapshotReader::new(start_ts, snapshot, true);

        let (ts, write_type) = reader
            .get_txn_commit_record(&Key::from_raw(key))
            .unwrap()
            .info()
            .unwrap();
        assert_eq!(ts, start_ts);
        assert_eq!(write_type, WriteType::Rollback);
    }

    pub fn must_get_rollback_ts_none<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
    ) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = SnapshotReader::new(start_ts.into(), snapshot, true);

        let ret = reader
            .get_txn_commit_record(&Key::from_raw(key))
            .unwrap()
            .info();
        assert_eq!(ret, None);
    }

    pub fn must_get_rollback_protected<E: Engine>(
        engine: &E,
        key: &[u8],
        start_ts: impl Into<TimeStamp>,
        protected: bool,
    ) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);

        let start_ts = start_ts.into();
        let (ts, write) = reader
            .seek_write(&Key::from_raw(key), start_ts)
            .unwrap()
            .unwrap();
        assert_eq!(ts, start_ts);
        assert_eq!(write.write_type, WriteType::Rollback);
        assert_eq!(write.as_ref().is_protected(), protected);
    }

    pub fn must_get_overlapped_rollback<E: Engine, T: Into<TimeStamp>>(
        engine: &E,
        key: &[u8],
        start_ts: T,
        overlapped_start_ts: T,
        overlapped_write_type: WriteType,
        gc_fence: Option<T>,
    ) {
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, None, true);

        let start_ts = start_ts.into();
        let overlapped_start_ts = overlapped_start_ts.into();
        let (ts, write) = reader
            .seek_write(&Key::from_raw(key), start_ts)
            .unwrap()
            .unwrap();
        assert_eq!(ts, start_ts);
        assert!(write.has_overlapped_rollback);
        assert_eq!(write.start_ts, overlapped_start_ts);
        assert_eq!(write.write_type, overlapped_write_type);
        assert_eq!(write.gc_fence, gc_fence.map(|x| x.into()));
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
        let snapshot = engine.snapshot(Default::default()).unwrap();
        let mut reader = MvccReader::new(snapshot, Some(ScanMode::Mixed), false);
        assert_eq!(
            reader.scan_keys(start.map(Key::from_raw), limit).unwrap(),
            expect
        );
    }
}
