// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Core data types.

use std::fmt::Debug;

use kvproto::kvrpcpb;
use txn_types::{Key, LastChange, Value};

use crate::storage::{
    Callback, Result,
    errors::SharedError,
    lock_manager::WaitTimeout,
    mvcc::{Lock, LockType, TimeStamp, Write, WriteType},
    txn::ProcessResult,
};

/// `MvccInfo` stores all mvcc information of given key.
/// Used by `MvccGetByKey` and `MvccGetByStartTs`.
#[derive(Debug, Default, Clone)]
pub struct MvccInfo {
    pub lock: Option<Lock>,
    /// commit_ts and write
    pub writes: Vec<(TimeStamp, Write)>,
    /// start_ts and value
    pub values: Vec<(TimeStamp, Value)>,
}

impl MvccInfo {
    pub fn into_proto(self) -> kvrpcpb::MvccInfo {
        fn extract_2pc_values(res: Vec<(TimeStamp, Value)>) -> Vec<kvrpcpb::MvccValue> {
            res.into_iter()
                .map(|(start_ts, value)| {
                    let mut value_info = kvrpcpb::MvccValue::default();
                    value_info.set_start_ts(start_ts.into_inner());
                    value_info.set_value(value);
                    value_info
                })
                .collect()
        }

        fn extract_2pc_writes(res: Vec<(TimeStamp, Write)>) -> Vec<kvrpcpb::MvccWrite> {
            res.into_iter()
                .map(|(commit_ts, write)| {
                    let mut write_info = kvrpcpb::MvccWrite::default();
                    let op = match write.write_type {
                        WriteType::Put => kvrpcpb::Op::Put,
                        WriteType::Delete => kvrpcpb::Op::Del,
                        WriteType::Lock => kvrpcpb::Op::Lock,
                        WriteType::Rollback => kvrpcpb::Op::Rollback,
                    };
                    write_info.set_type(op);
                    write_info.set_start_ts(write.start_ts.into_inner());
                    write_info.set_commit_ts(commit_ts.into_inner());
                    write_info.set_short_value(write.short_value.unwrap_or_default());
                    write_info.set_has_overlapped_rollback(write.has_overlapped_rollback);
                    if let Some(gc_fence) = write.gc_fence {
                        write_info.set_has_gc_fence(true);
                        write_info.set_gc_fence(gc_fence.into_inner());
                    }
                    if !matches!(
                        write.last_change,
                        LastChange::NotExist | LastChange::Exist { .. }
                    ) {
                        let (last_change_ts, versions) = write.last_change.to_parts();
                        write_info.set_last_change_ts(last_change_ts.into_inner());
                        write_info.set_versions_to_last_change(versions);
                    }
                    write_info
                })
                .collect()
        }

        let mut mvcc_info = kvrpcpb::MvccInfo::default();
        if let Some(lock) = self.lock {
            let mut lock_info = kvrpcpb::MvccLock::default();
            let op = match lock.lock_type {
                LockType::Put => kvrpcpb::Op::Put,
                LockType::Delete => kvrpcpb::Op::Del,
                LockType::Lock => kvrpcpb::Op::Lock,
                LockType::Pessimistic => kvrpcpb::Op::PessimisticLock,
            };
            lock_info.set_type(op);
            lock_info.set_start_ts(lock.ts.into_inner());
            lock_info.set_primary(lock.primary);
            lock_info.set_short_value(lock.short_value.unwrap_or_default());
            if matches!(
                lock.last_change,
                LastChange::NotExist | LastChange::Exist { .. }
            ) {
                let (last_change_ts, versions) = lock.last_change.to_parts();
                lock_info.set_last_change_ts(last_change_ts.into_inner());
                lock_info.set_versions_to_last_change(versions);
            }
            mvcc_info.set_lock(lock_info);
        }
        let vv = extract_2pc_values(self.values);
        let vw = extract_2pc_writes(self.writes);
        mvcc_info.set_writes(vw.into());
        mvcc_info.set_values(vv.into());
        mvcc_info
    }
}

/// Represents the status of a transaction.
#[derive(PartialEq, Debug)]
pub enum TxnStatus {
    /// The txn was already rolled back before.
    RolledBack,
    /// The txn is just rolled back due to expiration.
    TtlExpire,
    /// The txn is just rolled back due to lock not exist.
    LockNotExist,
    /// The txn haven't yet been committed.
    Uncommitted {
        lock: Lock,
        min_commit_ts_pushed: bool,
    },
    /// The txn was committed.
    Committed { commit_ts: TimeStamp },
    /// The primary key is pessimistically rolled back.
    PessimisticRollBack,
    /// The txn primary key is not found and nothing is done.
    LockNotExistDoNothing,
}

impl TxnStatus {
    pub fn uncommitted(lock: Lock, min_commit_ts_pushed: bool) -> Self {
        Self::Uncommitted {
            lock,
            min_commit_ts_pushed,
        }
    }

    pub fn committed(commit_ts: TimeStamp) -> Self {
        Self::Committed { commit_ts }
    }

    // Returns if the transaction is already committed or rolled back.
    pub fn is_decided(&self) -> bool {
        matches!(
            self,
            TxnStatus::RolledBack | TxnStatus::TtlExpire | TxnStatus::Committed { .. }
        )
    }
}

#[derive(Debug)]
pub struct PrewriteResult {
    pub locks: Vec<Result<()>>,
    pub min_commit_ts: TimeStamp,
    pub one_pc_commit_ts: TimeStamp,
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(test, derive(Default))]
pub struct PessimisticLockParameters {
    pub pb_ctx: kvrpcpb::Context,
    pub primary: Vec<u8>,
    pub start_ts: TimeStamp,
    pub lock_ttl: u64,
    pub for_update_ts: TimeStamp,
    pub wait_timeout: Option<WaitTimeout>,
    pub return_values: bool,
    pub min_commit_ts: TimeStamp,
    pub check_existence: bool,
    pub is_first_lock: bool,
    pub lock_only_if_exists: bool,

    /// Whether it's allowed for an pessimistic lock request to acquire the lock
    /// even there is write conflict (i.e. the latest version's `commit_ts` is
    /// greater than the current request's `for_update_ts`.
    ///
    /// When this is true, it's also inferred that the request is resumable,
    /// which means, if such a request encounters a lock of another
    /// transaction and it waits for the lock, it can resume executing and
    /// continue trying to acquire the lock when it's woken up. Also see:
    /// [`super::lock_manager::lock_waiting_queue`]
    pub allow_lock_with_conflict: bool,
}

/// Represents the result of pessimistic lock on a single key.
#[derive(Debug, Clone)]
pub enum PessimisticLockKeyResult {
    /// The lock is acquired successfully, returning no additional information.
    Empty,
    /// The lock is acquired successfully, and the previous value is read and
    /// returned.
    Value(Option<Value>),
    /// The lock is acquired successfully, and also checked if the key exists
    /// previously.
    Existence(bool),
    /// There is a write conflict, but the lock is acquired ignoring the write
    /// conflict.
    LockedWithConflict {
        /// The previous value of the key.
        value: Option<Value>,
        /// The `commit_ts` of the latest Write record found on this key. This
        /// is also the actual `for_update_ts` written to the lock.
        conflict_ts: TimeStamp,
    },
    /// The key is already locked and lock-waiting is needed.
    Waiting,
    /// Failed to acquire the lock due to some error.
    Failed(SharedError),
}

impl PessimisticLockKeyResult {
    pub fn new_success(
        need_value: bool,
        need_check_existence: bool,
        locked_with_conflict_ts: Option<TimeStamp>,
        value: Option<Value>,
    ) -> Self {
        if let Some(conflict_ts) = locked_with_conflict_ts {
            Self::LockedWithConflict { value, conflict_ts }
        } else if need_value {
            Self::Value(value)
        } else if need_check_existence {
            Self::Existence(value.is_some())
        } else {
            Self::Empty
        }
    }

    pub fn unwrap_value(self) -> Option<Value> {
        match self {
            Self::Value(v) => v,
            x => panic!(
                "pessimistic lock key result expected to be a value, got {:?}",
                x
            ),
        }
    }

    pub fn unwrap_existence(self) -> bool {
        match self {
            Self::Existence(e) => e,
            x => panic!(
                "pessimistic lock key result expected to be existence, got {:?}",
                x
            ),
        }
    }

    pub fn assert_empty(&self) {
        match self {
            Self::Empty => (),
            x => panic!(
                "pessimistic lock key result not match, expected Empty, got {:?}",
                x
            ),
        }
    }

    #[cfg(test)]
    pub fn assert_value(&self, expected_value: Option<&[u8]>) {
        match self {
            Self::Value(v) if v.as_ref().map(|v| v.as_slice()) == expected_value => (),
            x => panic!(
                "pessimistic lock key result not match, expected Value({:?}), got {:?}",
                expected_value, x
            ),
        }
    }

    #[cfg(test)]
    pub fn assert_existence(&self, expected_existence: bool) {
        match self {
            Self::Existence(e) if *e == expected_existence => (),
            x => panic!(
                "pessimistic lock key result not match, expected Existence({:?}), got {:?}",
                expected_existence, x
            ),
        }
    }

    #[cfg(test)]
    pub fn assert_locked_with_conflict(
        &self,
        expected_value: Option<&[u8]>,
        expected_conflict_ts: impl Into<TimeStamp>,
    ) {
        let expected_conflict_ts = expected_conflict_ts.into();
        match self {
            Self::LockedWithConflict { value, conflict_ts }
                if value.as_ref().map(|v| v.as_slice()) == expected_value
                    && *conflict_ts == expected_conflict_ts => {}
            x => panic!(
                "pessimistic lock key result not match, expected LockedWithConflict{{ value: {:?}, conflict_ts: {} }}, got {:?}",
                expected_value, expected_conflict_ts, x
            ),
        }
    }

    #[cfg(test)]
    pub fn assert_waiting(&self) {
        assert!(matches!(self, Self::Waiting));
    }

    pub fn unwrap_err(&self) -> SharedError {
        match self {
            Self::Failed(e) => e.clone(),
            x => panic!(
                "pessimistic lock key result not match expected Failed, got {:?}",
                x,
            ),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PessimisticLockResults(pub Vec<PessimisticLockKeyResult>);

impl PessimisticLockResults {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn push(&mut self, key_res: PessimisticLockKeyResult) {
        self.0.push(key_res);
    }

    pub fn into_pb(self) -> (Vec<kvrpcpb::PessimisticLockKeyResult>, Option<SharedError>) {
        let mut error = None;
        let res = self
            .0
            .into_iter()
            .map(|res| {
                let mut res_pb = kvrpcpb::PessimisticLockKeyResult::default();
                match res {
                    PessimisticLockKeyResult::Empty => {
                        res_pb.set_type(kvrpcpb::PessimisticLockKeyResultType::LockResultNormal)
                    }
                    PessimisticLockKeyResult::Value(v) => {
                        res_pb.set_type(kvrpcpb::PessimisticLockKeyResultType::LockResultNormal);
                        res_pb.set_existence(v.is_some());
                        res_pb.set_value(v.unwrap_or_default());
                    }
                    PessimisticLockKeyResult::Existence(e) => {
                        res_pb.set_type(kvrpcpb::PessimisticLockKeyResultType::LockResultNormal);
                        res_pb.set_existence(e);
                    }
                    PessimisticLockKeyResult::LockedWithConflict { value, conflict_ts } => {
                        res_pb.set_type(
                            kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict,
                        );
                        res_pb.set_existence(value.is_some());
                        res_pb.set_value(value.unwrap_or_default());
                        res_pb.set_locked_with_conflict_ts(conflict_ts.into_inner());
                    }
                    PessimisticLockKeyResult::Waiting => unreachable!(),
                    PessimisticLockKeyResult::Failed(e) => {
                        if error.is_none() {
                            error = Some(e)
                        }
                        res_pb.set_type(kvrpcpb::PessimisticLockKeyResultType::LockResultFailed);
                    }
                }
                res_pb
            })
            .collect();
        (res, error)
    }

    pub fn into_legacy_values_and_not_founds(self) -> (Vec<Value>, Vec<bool>) {
        if self.0.is_empty() {
            return (vec![], vec![]);
        }

        match &self.0[0] {
            PessimisticLockKeyResult::Empty => {
                self.0.into_iter().for_each(|res| res.assert_empty());
                (vec![], vec![])
            }
            PessimisticLockKeyResult::Existence(_) => {
                let not_founds = self.0.into_iter().map(|x| !x.unwrap_existence()).collect();
                (vec![], not_founds)
            }
            PessimisticLockKeyResult::Value(_) => {
                let mut not_founds = Vec::with_capacity(self.0.len());
                let mut values = Vec::with_capacity(self.0.len());
                self.0.into_iter().for_each(|x| {
                    let v = x.unwrap_value();
                    match v {
                        Some(v) => {
                            not_founds.push(false);
                            values.push(v);
                        }
                        None => {
                            not_founds.push(true);
                            values.push(vec![]);
                        }
                    }
                });
                (values, not_founds)
            }
            _ => unreachable!(),
        }
    }

    pub fn estimate_resp_size(&self) -> u64 {
        AsRef::<Vec<PessimisticLockKeyResult>>::as_ref(&self.0)
            .iter()
            .map(|res| {
                match res {
                    PessimisticLockKeyResult::Empty => 1,
                    PessimisticLockKeyResult::Value(v) => v.as_ref().map_or(0, |v| v.len() as u64),
                    PessimisticLockKeyResult::Existence(_) => {
                        2 // type + bool
                    }
                    PessimisticLockKeyResult::LockedWithConflict {
                        value,
                        conflict_ts: _,
                    } => {
                        10 + value.as_ref().map_or(0, |v| v.len() as u64) // 10 stands for type + bool + conflict_ts
                    }
                    PessimisticLockKeyResult::Waiting => {
                        1 // for test only 
                    }
                    PessimisticLockKeyResult::Failed(_) => {
                        1 // type, ignoring error message
                    }
                }
            })
            .sum::<u64>()
    }
}

#[derive(Debug, PartialEq)]
pub enum SecondaryLocksStatus {
    Locked(Vec<kvrpcpb::LockInfo>),
    Committed(TimeStamp),
    RolledBack,
}

impl SecondaryLocksStatus {
    pub fn push(&mut self, lock: kvrpcpb::LockInfo) {
        match self {
            SecondaryLocksStatus::Locked(v) => v.push(lock),
            _ => panic!("unexpected SecondaryLocksStatus"),
        }
    }
}

macro_rules! storage_callback {
    ($($variant: ident ( $cb_ty: ty ) $result_variant: pat => $result: expr,)*) => {
        pub enum StorageCallback {
            $($variant(Callback<$cb_ty>),)*
        }

        impl StorageCallback {
            /// Delivers the process result of a command to the storage callback.
            pub fn execute(self, pr: ProcessResult) {
                match self {
                    $(StorageCallback::$variant(cb) => match pr {
                        $result_variant => cb(Ok($result)),
                        ProcessResult::Failed { err } => cb(Err(err)),
                        _ => panic!("process result mismatch"),
                    },)*
                }
            }
        }

        $(impl StorageCallbackType for $cb_ty {
            fn callback(cb: Callback<Self>) -> StorageCallback {
                StorageCallback::$variant(cb)
            }
        })*
    }
}

storage_callback! {
    Boolean(()) ProcessResult::Res => (),
    Booleans(Vec<Result<()>>) ProcessResult::MultiRes { results } => results,
    MvccInfoByKey(MvccInfo) ProcessResult::MvccKey { mvcc } => mvcc,
    MvccInfoByStartTs(Option<(Key, MvccInfo)>) ProcessResult::MvccStartTs { mvcc } => mvcc,
    Locks(Vec<kvrpcpb::LockInfo>) ProcessResult::Locks { locks } => locks,
    TxnStatus(TxnStatus) ProcessResult::TxnStatus { txn_status } => txn_status,
    Prewrite(PrewriteResult) ProcessResult::PrewriteResult { result } => result,
    PessimisticLock(Result<PessimisticLockResults>) ProcessResult::PessimisticLockRes { res } => res,
    SecondaryLocksStatus(SecondaryLocksStatus) ProcessResult::SecondaryLocksStatus { status } => status,
    RawCompareAndSwap((Option<Value>, bool)) ProcessResult::RawCompareAndSwapRes { previous_value, succeed } => (previous_value, succeed),
}

pub trait StorageCallbackType: Sized {
    fn callback(cb: Callback<Self>) -> StorageCallback;
}
