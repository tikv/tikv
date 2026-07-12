// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::Cow, mem::size_of};

use bitflags::bitflags;
use byteorder::ReadBytesExt;
use collections::HashMap;
use kvproto::kvrpcpb::{IsolationLevel, LockInfo, Op, WriteConflictReason};
use tikv_util::{
    Either,
    codec::{
        bytes::{self, BytesEncoder},
        number::{self, MAX_VAR_I64_LEN, MAX_VAR_U64_LEN, NumberEncoder},
    },
    memory::HeapSize,
};

use crate::{
    Error, ErrorInner, LastChange, Result,
    timestamp::{TimeStamp, TsSet},
    types::{Key, Mutation, SHORT_VALUE_PREFIX, Value},
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockType {
    Put,
    Delete,
    Lock,
    Pessimistic,
    Shared,
}

const FLAG_PUT: u8 = b'P';
const FLAG_DELETE: u8 = b'D';
const FLAG_LOCK: u8 = b'L';
const FLAG_PESSIMISTIC: u8 = b'S';
const FLAG_SHARED: u8 = b'H';

const FOR_UPDATE_TS_PREFIX: u8 = b'f';
const TXN_SIZE_PREFIX: u8 = b't';
const MIN_COMMIT_TS_PREFIX: u8 = b'c';
const ASYNC_COMMIT_PREFIX: u8 = b'a';
const ROLLBACK_TS_PREFIX: u8 = b'r';
const LAST_CHANGE_PREFIX: u8 = b'l';
const TXN_SOURCE_PREFIX: u8 = b's';
const _RESERVED_PREFIX: u8 = b'T'; // Reserved for future use.
const PESSIMISTIC_LOCK_WITH_CONFLICT_PREFIX: u8 = b'F';
const GENERATION_PREFIX: u8 = b'g';
const SHARED_LOCK_TXNS_INFO_PREFIX: u8 = b'h';
const SHARED_LOCK_FLAGS_PREFIX: u8 = b'i';

impl LockType {
    pub fn from_mutation(mutation: &Mutation) -> Option<LockType> {
        match *mutation {
            Mutation::Put(..) | Mutation::Insert(..) => Some(LockType::Put),
            Mutation::Delete(..) => Some(LockType::Delete),
            Mutation::Lock(..) | Mutation::SharedLock(..) => Some(LockType::Lock),
            Mutation::CheckNotExists(..) => None,
        }
    }

    fn from_u8(b: u8) -> Option<LockType> {
        match b {
            FLAG_PUT => Some(LockType::Put),
            FLAG_DELETE => Some(LockType::Delete),
            FLAG_LOCK => Some(LockType::Lock),
            FLAG_PESSIMISTIC => Some(LockType::Pessimistic),
            FLAG_SHARED => Some(LockType::Shared),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            LockType::Put => FLAG_PUT,
            LockType::Delete => FLAG_DELETE,
            LockType::Lock => FLAG_LOCK,
            LockType::Pessimistic => FLAG_PESSIMISTIC,
            LockType::Shared => FLAG_SHARED,
        }
    }
}

pub type LockOrSharedLocks = Either<Lock, SharedLocks>;

#[derive(PartialEq, Clone)]
pub struct Lock {
    pub lock_type: LockType,
    pub primary: Vec<u8>,
    pub ts: TimeStamp,
    pub ttl: u64,
    pub short_value: Option<Value>,
    // If for_update_ts != 0, this lock belongs to a pessimistic transaction
    pub for_update_ts: TimeStamp,
    pub txn_size: u64,
    pub min_commit_ts: TimeStamp,
    pub use_async_commit: bool,
    // This field is only valid for in-memory locks and does not need to be persisted because:
    //   1. the lock should be converted to a write directly when 1pc succeeds.
    //   2. the field should be reverted to false (default value) when 1pc fails.
    pub use_one_pc: bool,
    // Only valid when `use_async_commit` is true, and the lock is primary. Do not set
    // `secondaries` for secondaries.
    pub secondaries: Vec<Vec<u8>>,
    // In some rare cases, a protected rollback may happen when there's already another
    // transaction's lock on the key. In this case, if the other transaction uses calculated
    // timestamp as commit_ts, the protected rollback record may be overwritten. Checking Write CF
    // while committing is relatively expensive. So the solution is putting the ts of the rollback
    // to the lock.
    pub rollback_ts: Vec<TimeStamp>,

    /// The position of the last actual write (PUT or DELETE), used to skip
    /// consecutive LOCK records when reading.
    pub last_change: LastChange,
    /// The source of this txn. It is used by ticdc, if the value is 0 ticdc
    /// will sync the kv change event to downstream, if it is not 0, ticdc
    /// may ignore this change event.
    ///
    /// We use `u64` to reserve more space for future use. For now, the upper
    /// application is limited to setting this value under `0x80`,
    /// so there will no more cost to change it to `u64`.
    pub txn_source: u64,
    /// The lock is locked with conflict using fair lock mode.
    pub is_locked_with_conflict: bool,
    /// The generation of the lock, used in pipelined DML.
    pub generation: u64,
}

impl std::fmt::Debug for Lock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut secondary_keys = std::vec::Vec::with_capacity(self.secondaries.len());
        for key in self.secondaries.iter() {
            secondary_keys.push(log_wrappers::Value::key(key))
        }
        f.debug_struct("Lock")
            .field("lock_type", &self.lock_type)
            .field("primary_key", &log_wrappers::Value::key(&self.primary))
            .field("start_ts", &self.ts)
            .field("ttl", &self.ttl)
            .field(
                "short_value",
                &log_wrappers::Value::value(self.short_value.as_ref().unwrap_or(&b"".to_vec())),
            )
            .field("for_update_ts", &self.for_update_ts)
            .field("txn_size", &self.txn_size)
            .field("min_commit_ts", &self.min_commit_ts)
            .field("use_async_commit", &self.use_async_commit)
            .field("secondaries", &secondary_keys)
            .field("rollback_ts", &self.rollback_ts)
            .field("last_change", &self.last_change)
            .field("txn_source", &self.txn_source)
            .field("is_locked_with_conflict", &self.is_locked_with_conflict)
            .field("generation", &self.generation)
            .finish()
    }
}

impl HeapSize for Lock {
    fn approximate_heap_size(&self) -> usize {
        self.primary.approximate_heap_size()
            + self.short_value.approximate_heap_size()
            + self.secondaries.approximate_heap_size()
            + self.rollback_ts.approximate_heap_size()
    }
}

impl Lock {
    pub fn new(
        lock_type: LockType,
        primary: Vec<u8>,
        ts: TimeStamp,
        ttl: u64,
        short_value: Option<Value>,
        for_update_ts: TimeStamp,
        txn_size: u64,
        min_commit_ts: TimeStamp,
        is_locked_with_conflict: bool,
    ) -> Self {
        Self {
            lock_type,
            primary,
            ts,
            ttl,
            short_value,
            for_update_ts,
            txn_size,
            min_commit_ts,
            use_async_commit: false,
            use_one_pc: false,
            secondaries: Vec::default(),
            rollback_ts: Vec::default(),
            last_change: LastChange::default(),
            txn_source: 0,
            is_locked_with_conflict,
            generation: 0,
        }
    }

    #[must_use]
    pub fn use_async_commit(mut self, secondaries: Vec<Vec<u8>>) -> Self {
        self.use_async_commit = true;
        self.secondaries = secondaries;
        self
    }

    #[must_use]
    pub fn with_rollback_ts(mut self, rollback_ts: Vec<TimeStamp>) -> Self {
        self.rollback_ts = rollback_ts;
        self
    }

    #[must_use]
    pub fn set_last_change(mut self, last_change: LastChange) -> Self {
        self.last_change = last_change;
        self
    }

    #[inline]
    #[must_use]
    pub fn set_txn_source(mut self, source: u64) -> Self {
        self.txn_source = source;
        self
    }

    #[inline]
    #[must_use]
    pub fn with_generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        assert!(
            self.lock_type != LockType::Shared,
            "use SharedLocks to encode shared locks"
        );
        let mut b = Vec::with_capacity(self.pre_allocate_size());
        b.push(self.lock_type.to_u8());
        b.encode_compact_bytes(&self.primary).unwrap();
        b.encode_var_u64(self.ts.into_inner()).unwrap();
        b.encode_var_u64(self.ttl).unwrap();
        if let Some(ref v) = self.short_value {
            b.push(SHORT_VALUE_PREFIX);
            b.push(v.len() as u8);
            b.extend_from_slice(v);
        }
        if !self.for_update_ts.is_zero() {
            b.push(FOR_UPDATE_TS_PREFIX);
            b.encode_u64(self.for_update_ts.into_inner()).unwrap();
        }
        if self.txn_size > 0 {
            b.push(TXN_SIZE_PREFIX);
            b.encode_u64(self.txn_size).unwrap();
        }
        if !self.min_commit_ts.is_zero() {
            b.push(MIN_COMMIT_TS_PREFIX);
            b.encode_u64(self.min_commit_ts.into_inner()).unwrap();
        }
        if self.use_async_commit {
            b.push(ASYNC_COMMIT_PREFIX);
            b.encode_var_u64(self.secondaries.len() as _).unwrap();
            for k in &self.secondaries {
                b.encode_compact_bytes(k).unwrap();
            }
        }
        if !self.rollback_ts.is_empty() {
            b.push(ROLLBACK_TS_PREFIX);
            b.encode_var_u64(self.rollback_ts.len() as _).unwrap();
            for ts in &self.rollback_ts {
                b.encode_u64(ts.into_inner()).unwrap();
            }
        }
        if matches!(
            self.last_change,
            LastChange::NotExist | LastChange::Exist { .. }
        ) {
            let (last_change_ts, versions) = self.last_change.to_parts();
            b.push(LAST_CHANGE_PREFIX);
            b.encode_u64(last_change_ts.into_inner()).unwrap();
            b.encode_var_u64(versions).unwrap();
        }
        if self.txn_source != 0 {
            b.push(TXN_SOURCE_PREFIX);
            b.encode_var_u64(self.txn_source).unwrap();
        }
        if self.is_locked_with_conflict {
            b.push(PESSIMISTIC_LOCK_WITH_CONFLICT_PREFIX);
        }
        if self.generation > 0 {
            b.push(GENERATION_PREFIX);
            b.encode_u64(self.generation).unwrap();
        }
        b
    }

    fn pre_allocate_size(&self) -> usize {
        let mut size = 1 + MAX_VAR_I64_LEN + self.primary.len() + MAX_VAR_U64_LEN * 2;
        if let Some(v) = &self.short_value {
            size += 2 + v.len();
        }
        if !self.for_update_ts.is_zero() {
            size += 1 + size_of::<u64>();
        }
        if self.txn_size > 0 {
            size += 1 + size_of::<u64>();
        }
        if !self.min_commit_ts.is_zero() {
            size += 1 + size_of::<u64>();
        }
        if self.use_async_commit {
            size += 1
                + MAX_VAR_U64_LEN
                + self
                    .secondaries
                    .iter()
                    .map(|k| MAX_VAR_I64_LEN + k.len())
                    .sum::<usize>();
        }
        if !self.rollback_ts.is_empty() {
            size += 1 + MAX_VAR_U64_LEN + size_of::<u64>() * self.rollback_ts.len();
        }
        if matches!(
            self.last_change,
            LastChange::NotExist | LastChange::Exist { .. }
        ) {
            size += 1 + size_of::<u64>() + MAX_VAR_U64_LEN;
        }
        if self.txn_source != 0 {
            size += 1 + MAX_VAR_U64_LEN;
        }
        if self.is_locked_with_conflict {
            size += 1;
        }
        if self.generation > 0 {
            size += 1 + size_of::<u64>();
        }
        size
    }
}

pub fn decode_lock_type(b: &[u8]) -> Result<LockType> {
    if b.is_empty() {
        return Err(Error::from(ErrorInner::BadFormatLock));
    }
    let lock_type = LockType::from_u8(b[0]).ok_or(ErrorInner::BadFormatLock)?;
    Ok(lock_type)
}

pub fn decode_lock_start_ts(b: &[u8]) -> Result<TimeStamp> {
    match decode_lock_type(b)? {
        LockType::Shared => Err(Error::from(ErrorInner::BadFormatLock)),
        _ => {
            let mut b = &b[1..];
            let _ = bytes::decode_compact_bytes(&mut b)?;
            let ts = number::decode_var_u64(&mut b)?.into();
            Ok(ts)
        }
    }
}

/// Checks whether the lock conflicts with the given `ts`. If `ts ==
/// TimeStamp::max()`, the primary lock will be ignored.
fn check_ts_conflict_si(
    lock_or_shared_locks: Cow<'_, LockOrSharedLocks>,
    key: &Key,
    ts: TimeStamp,
    bypass_locks: &TsSet,
    is_replica_read: bool,
) -> Result<()> {
    let lock = match lock_or_shared_locks.as_ref() {
        Either::Left(lock) => lock,
        Either::Right(_) => return Ok(()), // Ignore SharedLocks
    };

    if lock.ts > ts || lock.lock_type == LockType::Lock || lock.is_pessimistic_lock() {
        // Ignore lock when lock.ts > ts or lock's type is Lock, Shared, or Pessimistic
        return Ok(());
    }

    if lock.min_commit_ts > ts {
        // Ignore lock when min_commit_ts > ts
        return Ok(());
    }

    if bypass_locks.contains(lock.ts) {
        return Ok(());
    }

    let raw_key = key.to_raw()?;

    // Disable replica read for autocommit max ts read, to avoid breaking
    // linearizability. See https://github.com/pingcap/tidb/issues/43583 for details.
    if ts == TimeStamp::max() && is_replica_read {
        return Err(Error::from(ErrorInner::KeyIsLocked(
            lock_or_shared_locks
                .into_owned()
                .left()
                .unwrap()
                .into_lock_info(raw_key),
        )));
    }

    if ts == TimeStamp::max()
        && raw_key == lock.primary
        && !lock.use_async_commit
        && !lock.use_one_pc
    {
        // When `ts == TimeStamp::max()` (which means to get latest committed version
        // for primary key), and current key is the primary key, we ignore
        // this lock.
        return Ok(());
    }

    // There is a pending lock. Client should wait or clean it.
    Err(Error::from(ErrorInner::KeyIsLocked(
        lock_or_shared_locks
            .into_owned()
            .left()
            .unwrap()
            .into_lock_info(raw_key),
    )))
}

// Check if lock could be bypassed for isolation level `RcCheckTs`.
fn check_ts_conflict_rc_check_ts(
    lock_or_shared_locks: Cow<'_, LockOrSharedLocks>,
    key: &Key,
    ts: TimeStamp,
    bypass_locks: &TsSet,
) -> Result<()> {
    let lock = match lock_or_shared_locks.as_ref() {
        Either::Left(lock) => lock,
        Either::Right(_) => return Ok(()), // Ignore SharedLocks
    };

    if lock.lock_type == LockType::Lock || lock.is_pessimistic_lock() {
        // Ignore lock when the lock's type is Lock, Shared or Pessimistic.
        return Ok(());
    }

    // The lock is resolved already.
    if bypass_locks.contains(lock.ts) {
        return Ok(());
    }

    // Return conflict error.
    Err(Error::from(ErrorInner::WriteConflict {
        start_ts: ts,
        conflict_start_ts: lock.ts,
        conflict_commit_ts: Default::default(),
        key: key.to_raw()?,
        primary: lock.primary.to_vec(),
        reason: WriteConflictReason::RcCheckTs,
    }))
}

pub fn check_ts_conflict(
    lock_or_shared_locks: Cow<'_, LockOrSharedLocks>,
    key: &Key,
    ts: TimeStamp,
    bypass_locks: &TsSet,
    iso_level: IsolationLevel,
) -> Result<()> {
    match iso_level {
        IsolationLevel::Si => {
            check_ts_conflict_si(lock_or_shared_locks, key, ts, bypass_locks, false)
        }
        IsolationLevel::RcCheckTs => {
            check_ts_conflict_rc_check_ts(lock_or_shared_locks, key, ts, bypass_locks)
        }
        _ => {
            let _ = lock_or_shared_locks;
            Ok(())
        }
    }
}

pub fn check_ts_conflict_for_replica_read(
    lock_or_shared_locks: Cow<'_, LockOrSharedLocks>,
    key: &Key,
    ts: TimeStamp,
    bypass_locks: &TsSet,
    iso_level: IsolationLevel,
) -> Result<()> {
    match iso_level {
        IsolationLevel::Si => {
            check_ts_conflict_si(lock_or_shared_locks, key, ts, bypass_locks, true)
        }
        IsolationLevel::RcCheckTs => unreachable!(),
        _ => {
            let _ = lock_or_shared_locks;
            Ok(())
        }
    }
}

pub fn parse_lock(b: &[u8]) -> Result<LockOrSharedLocks> {
    match decode_lock_type(b)? {
        LockType::Shared => Ok(Either::Right(SharedLocks::parse(b)?)),
        _ => Ok(Either::Left(Lock::parse(b)?)),
    }
}

impl Lock {
    pub fn parse(mut b: &[u8]) -> Result<Lock> {
        if b.is_empty() {
            return Err(Error::from(ErrorInner::BadFormatLock));
        }
        let lock_type = LockType::from_u8(b.read_u8()?).ok_or(ErrorInner::BadFormatLock)?;
        assert!(lock_type != LockType::Shared);
        let primary = bytes::decode_compact_bytes(&mut b)?;
        let ts = number::decode_var_u64(&mut b)?.into();
        let ttl = if b.is_empty() {
            0
        } else {
            number::decode_var_u64(&mut b)?
        };

        if b.is_empty() {
            return Ok(Lock::new(
                lock_type,
                primary,
                ts,
                ttl,
                None,
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
                false,
            ));
        }

        let mut short_value = None;
        let mut for_update_ts = TimeStamp::zero();
        let mut txn_size: u64 = 0;
        let mut min_commit_ts = TimeStamp::zero();
        let mut use_async_commit = false;
        let mut secondaries = Vec::new();
        let mut rollback_ts = Vec::new();
        let mut last_change_ts = TimeStamp::zero();
        let mut estimated_versions_to_last_change = 0;
        let mut txn_source = 0;
        let mut is_locked_with_conflict = false;
        let mut generation = 0;
        while !b.is_empty() {
            match b.read_u8()? {
                SHORT_VALUE_PREFIX => {
                    let len = b.read_u8()?;
                    if b.len() < len as usize {
                        panic!(
                            "content len [{}] shorter than short value len [{}]",
                            b.len(),
                            len,
                        );
                    }
                    short_value = Some(b[..len as usize].to_vec());
                    b = &b[len as usize..];
                }
                FOR_UPDATE_TS_PREFIX => for_update_ts = number::decode_u64(&mut b)?.into(),
                TXN_SIZE_PREFIX => txn_size = number::decode_u64(&mut b)?,
                MIN_COMMIT_TS_PREFIX => min_commit_ts = number::decode_u64(&mut b)?.into(),
                ASYNC_COMMIT_PREFIX => {
                    use_async_commit = true;
                    let len = number::decode_var_u64(&mut b)? as _;
                    secondaries = (0..len)
                        .map(|_| bytes::decode_compact_bytes(&mut b).map_err(Into::into))
                        .collect::<Result<_>>()?;
                }
                ROLLBACK_TS_PREFIX => {
                    let len = number::decode_var_u64(&mut b)? as usize;
                    // Allocate one more place to avoid reallocation when pushing a new timestamp
                    // to it.
                    rollback_ts = Vec::with_capacity(len + 1);
                    for _ in 0..len {
                        rollback_ts.push(number::decode_u64(&mut b)?.into());
                    }
                }
                LAST_CHANGE_PREFIX => {
                    last_change_ts = number::decode_u64(&mut b)?.into();
                    estimated_versions_to_last_change = number::decode_var_u64(&mut b)?;
                }
                TXN_SOURCE_PREFIX => {
                    txn_source = number::decode_var_u64(&mut b)?;
                }
                PESSIMISTIC_LOCK_WITH_CONFLICT_PREFIX => {
                    is_locked_with_conflict = true;
                }
                GENERATION_PREFIX => {
                    generation = number::decode_u64(&mut b)?;
                }
                _ => {
                    // To support forward compatibility, all fields should be serialized in order
                    // and stop parsing if meets an unknown byte.
                    break;
                }
            }
        }
        let mut lock = Lock::new(
            lock_type,
            primary,
            ts,
            ttl,
            short_value,
            for_update_ts,
            txn_size,
            min_commit_ts,
            is_locked_with_conflict,
        )
        .set_last_change(LastChange::from_parts(
            last_change_ts,
            estimated_versions_to_last_change,
        ))
        .set_txn_source(txn_source)
        .with_rollback_ts(rollback_ts)
        .with_generation(generation);
        if use_async_commit {
            lock = lock.use_async_commit(secondaries);
        }
        Ok(lock)
    }

    pub fn into_lock_info(self, raw_key: Vec<u8>) -> LockInfo {
        let mut info = LockInfo::default();
        info.set_primary_lock(self.primary);
        info.set_lock_version(self.ts.into_inner());
        info.set_key(raw_key);
        info.set_lock_ttl(self.ttl);
        info.set_txn_size(self.txn_size);
        let lock_type = match self.lock_type {
            LockType::Put => Op::Put,
            LockType::Delete => Op::Del,
            LockType::Lock => Op::Lock,
            LockType::Pessimistic => Op::PessimisticLock,
            // Lock struct should not have LockType::Shared; use SharedLocks instead.
            LockType::Shared => unreachable!("use SharedLocks::into_lock_info for shared locks"),
        };
        info.set_lock_type(lock_type);
        info.set_lock_for_update_ts(self.for_update_ts.into_inner());
        info.set_use_async_commit(self.use_async_commit);
        info.set_min_commit_ts(self.min_commit_ts.into_inner());
        info.set_secondaries(self.secondaries.into());
        // The client does not care about last_change_ts, versions_to_last_version and
        // txn_source.
        info
    }

    pub fn is_pessimistic_txn(&self) -> bool {
        !self.for_update_ts.is_zero()
    }

    pub fn is_pessimistic_lock(&self) -> bool {
        self.lock_type == LockType::Pessimistic
    }

    pub fn is_pessimistic_lock_with_conflict(&self) -> bool {
        self.is_pessimistic_lock() && self.is_locked_with_conflict
    }
}

bitflags! {
    #[derive(Default)]
    struct SharedLocksFlags: u8 {
        // Indicates whether the shared locks are shrink-only, which means no
        // new shared lock can be added. In other words, a shrink-only
        // `SharedLocks` also blocks new incoming shared locks. This is a
        // one-way change, a shrink-only `SharedLocks` cannot be changed back to
        // a non-shrink-only one.
        const SHRINK_ONLY = 0b0000_0001;
    }
}

#[derive(PartialEq, Clone, Debug, Default)]
pub struct SharedLocks {
    flags: SharedLocksFlags,
    txn_info_segments: HashMap<TimeStamp, Either<Vec<u8>, Lock>>,
}

impl SharedLocks {
    pub fn new() -> Self {
        Default::default()
    }

    fn with_txn_infos(
        mut self,
        txn_info_segments: HashMap<TimeStamp, Either<Vec<u8>, Lock>>,
    ) -> Self {
        self.txn_info_segments = txn_info_segments;
        self
    }

    fn with_flags(mut self, flags: SharedLocksFlags) -> Self {
        self.flags = flags;
        self
    }

    pub fn parse(mut b: &[u8]) -> Result<Self> {
        if b.is_empty() {
            return Err(Error::from(ErrorInner::BadFormatLock));
        }
        let lock_type = LockType::from_u8(b.read_u8()?).ok_or(ErrorInner::BadFormatLock)?;
        assert!(lock_type == LockType::Shared);
        if b.is_empty() {
            return Ok(Self::new());
        }

        let mut flags = SharedLocksFlags::empty();
        let mut segments = HashMap::default();
        while !b.is_empty() {
            match b.read_u8()? {
                SHARED_LOCK_FLAGS_PREFIX => {
                    let flags_byte = b.read_u8()?;
                    flags =
                        SharedLocksFlags::from_bits(flags_byte).ok_or(ErrorInner::BadFormatLock)?;
                }
                SHARED_LOCK_TXNS_INFO_PREFIX => {
                    let len = number::decode_var_u64(&mut b)? as usize;
                    segments.reserve(len + 1); // some schedulers may append a new lock, reserve for it.
                    for _ in 0..len {
                        let lock_bytes = bytes::decode_compact_bytes(&mut b)?;
                        let lock_ts = decode_lock_start_ts(&lock_bytes)?;
                        segments.insert(lock_ts, Either::Left(lock_bytes));
                    }
                }
                _ => {
                    // To support forward compatibility, all fields should be serialized in order
                    // and stop parsing if meets an unknown byte.
                    break;
                }
            }
        }

        Ok(Self::new().with_flags(flags).with_txn_infos(segments))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.txn_info_segments.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.txn_info_segments.is_empty()
    }

    // Decode all shared-lock segments so tests can rely on parsed locks.
    #[cfg(test)]
    pub fn parse_all(&mut self) {
        for (_ts, either) in self.txn_info_segments.iter_mut() {
            if let Either::Left(encoded) = either {
                let lock = Lock::parse(encoded).expect("failed to parse shared lock txn info");
                *either = Either::Right(lock);
            }
        }
    }

    #[inline]
    pub fn is_shrink_only(&self) -> bool {
        self.flags.contains(SharedLocksFlags::SHRINK_ONLY)
    }

    #[inline]
    pub fn set_shrink_only(&mut self) {
        self.flags.insert(SharedLocksFlags::SHRINK_ONLY);
    }

    #[inline]
    #[must_use]
    pub fn contains_start_ts(&self, start_ts: TimeStamp) -> bool {
        self.txn_info_segments.contains_key(&start_ts)
    }

    #[inline]
    pub fn iter_ts(&self) -> impl Iterator<Item = &TimeStamp> {
        self.txn_info_segments.keys()
    }

    /// Returns the shared lock for `ts`, if any.
    ///
    /// When decoding a shared lock, each txn-info segment is kept as raw bytes
    /// (`Either::Left`) to avoid eagerly parsing all segments. This method
    /// performs lazy parsing: the first time a `ts` is accessed, it parses
    /// the bytes into a `Lock` and caches it in-place by replacing
    /// the entry with `Either::Right(Lock)`. Subsequent calls return the cached
    /// `Lock` without reparsing.
    pub fn get_lock(&mut self, ts: &TimeStamp) -> Result<Option<&Lock>> {
        if let Some(either) = self.txn_info_segments.get_mut(ts) {
            if let Either::Left(encoded) = either {
                let lock = Lock::parse(encoded)?;
                *either = Either::Right(lock);
            }
            match either {
                Either::Right(lock) => Ok(Some(lock)),
                Either::Left(_) => unreachable!(),
            }
        } else {
            Ok(None)
        }
    }

    pub fn remove_lock(&mut self, ts: &TimeStamp) -> Result<Option<Lock>> {
        if let Some(either) = self.txn_info_segments.remove(ts) {
            match either {
                Either::Left(encoded) => Ok(Some(Lock::parse(&encoded)?)),
                Either::Right(lock) => Ok(Some(lock)),
            }
        } else {
            Ok(None)
        }
    }

    pub fn insert_lock(&mut self, lock: Lock) -> Result<()> {
        let lock_type = lock.lock_type;
        assert!(matches!(lock_type, LockType::Lock | LockType::Pessimistic));
        if self.is_shrink_only() {
            return Err(ErrorInner::InvalidOperation(format!(
                "cannot insert new lock #{} into shrink-only SharedLocks",
                lock.ts.into_inner()
            ))
            .into());
        } else if self.contains_start_ts(lock.ts) {
            return Err(ErrorInner::InvalidOperation(format!(
                "lock #{} already exists in SharedLocks",
                lock.ts.into_inner()
            ))
            .into());
        }
        self.txn_info_segments.insert(lock.ts, Either::Right(lock));
        Ok(())
    }

    pub fn update_lock(&mut self, lock: Lock) -> Result<()> {
        let lock_type = lock.lock_type;
        assert!(matches!(lock_type, LockType::Lock | LockType::Pessimistic));
        if !self.contains_start_ts(lock.ts) {
            return Err(ErrorInner::InvalidOperation(format!(
                "lock #{} does not exist in SharedLocks",
                lock.ts.into_inner()
            ))
            .into());
        }
        self.txn_info_segments.insert(lock.ts, Either::Right(lock));
        Ok(())
    }

    /// Filters shared locks, keeping only those for which `f` returns true.
    pub fn filter_shared_locks<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&Lock) -> bool,
    {
        let mut to_remove = Vec::new();

        for (ts, either) in self.txn_info_segments.iter_mut() {
            let keep = match either {
                Either::Right(lock) => f(lock),
                Either::Left(encoded) => {
                    let lock = Lock::parse(encoded)?;
                    let keep = f(&lock);
                    *either = Either::Right(lock);
                    keep
                }
            };

            if !keep {
                to_remove.push(*ts);
            }
        }

        for ts in to_remove {
            self.txn_info_segments.remove(&ts);
        }
        Ok(())
    }

    /// Truncates the SharedLocks to contain at most `n` locks.
    /// Keeps locks with smallest timestamps for deterministic behavior.
    pub fn truncate_to(&mut self, n: usize) {
        if self.len() <= n {
            return;
        }
        let mut all_ts: Vec<TimeStamp> = self.txn_info_segments.keys().cloned().collect();
        all_ts.sort();
        for ts in all_ts.into_iter().skip(n) {
            self.txn_info_segments.remove(&ts);
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(self.pre_allocate_size());
        b.push(LockType::Shared.to_u8());
        if !self.flags.is_empty() {
            b.push(SHARED_LOCK_FLAGS_PREFIX);
            b.push(self.flags.bits());
        }
        b.push(SHARED_LOCK_TXNS_INFO_PREFIX);
        b.encode_var_u64(self.len() as u64).unwrap();
        for seg in self.txn_info_segments.values() {
            match seg {
                Either::Left(v) => b.encode_compact_bytes(v).unwrap(),
                Either::Right(l) => b.encode_compact_bytes(&l.to_bytes()).unwrap(),
            };
        }
        b
    }

    fn pre_allocate_size(&self) -> usize {
        1 // lock type
            + if !self.flags.is_empty() {
                2 // SHARED_LOCK_FLAGS_PREFIX + flags byte
            } else {
                0
            }
            + 1 // SHARED_LOCK_TXNS_INFO_PREFIX
            + MAX_VAR_U64_LEN // the size of shared locks
            + self
                .txn_info_segments
                .values()
                .map(|lock| {
                    MAX_VAR_I64_LEN
                        + match lock {
                            Either::Left(v) => v.len(),
                            Either::Right(l) => l.pre_allocate_size(),
                        }
                })
                .sum::<usize>()
    }

    pub fn into_lock_info(self, raw_key: Vec<u8>) -> LockInfo {
        let mut info = LockInfo::default();
        info.lock_type = Op::SharedLock;
        let shared_locks: Vec<_> = self
            .txn_info_segments
            .values()
            .map(|lock| match lock {
                Either::Left(encoded) => Lock::parse(encoded).unwrap(),
                Either::Right(lock) => lock.clone(),
            })
            .map(|lock| lock.into_lock_info(raw_key.clone()))
            .collect();
        info.set_shared_lock_infos(shared_locks.into());
        info.set_key(raw_key);
        info
    }
}

impl HeapSize for SharedLocks {
    fn approximate_heap_size(&self) -> usize {
        self.txn_info_segments.approximate_heap_size()
    }
}

/// A specialized lock only for pessimistic lock. This saves memory for cases
/// that only pessimistic locks exist.
#[derive(Clone, PartialEq)]
pub struct PessimisticLock {
    /// The primary key in raw format.
    pub primary: Box<[u8]>,
    pub start_ts: TimeStamp,
    pub ttl: u64,
    pub for_update_ts: TimeStamp,
    pub min_commit_ts: TimeStamp,

    pub last_change: LastChange,
    pub is_locked_with_conflict: bool,
}

impl PessimisticLock {
    pub fn to_lock(&self) -> Lock {
        Lock::new(
            LockType::Pessimistic,
            self.primary.to_vec(),
            self.start_ts,
            self.ttl,
            None,
            self.for_update_ts,
            0,
            self.min_commit_ts,
            self.is_locked_with_conflict,
        )
        .set_last_change(self.last_change.clone())
    }

    // Same with `to_lock` but does not copy the primary key.
    pub fn into_lock(self) -> Lock {
        Lock::new(
            LockType::Pessimistic,
            Vec::from(self.primary),
            self.start_ts,
            self.ttl,
            None,
            self.for_update_ts,
            0,
            self.min_commit_ts,
            self.is_locked_with_conflict,
        )
        .set_last_change(self.last_change)
    }

    pub fn memory_size(&self) -> usize {
        self.primary.len() + size_of::<Self>()
    }
}

impl std::fmt::Debug for PessimisticLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PessimisticLock")
            .field("primary_key", &log_wrappers::Value::key(&self.primary))
            .field("start_ts", &self.start_ts)
            .field("ttl", &self.ttl)
            .field("for_update_ts", &self.for_update_ts)
            .field("min_commit_ts", &self.min_commit_ts)
            .field("last_change", &self.last_change)
            .field("is_locked_with_conflict", &self.is_locked_with_conflict)
            .finish()
    }
}

/// TxnLock is a wrapper for in-memory pessimistic locks and storage locks.
#[derive(PartialEq, Clone, Debug)]
pub enum TxnLockRef<'a> {
    InMemory(&'a PessimisticLock),
    Persisted(&'a Lock),
}

impl TxnLockRef<'_> {
    pub fn get_start_ts(&self) -> TimeStamp {
        match self {
            TxnLockRef::InMemory(pessimistic_lock) => pessimistic_lock.start_ts,
            TxnLockRef::Persisted(lock) => lock.ts,
        }
    }

    pub fn get_for_update_ts(&self) -> TimeStamp {
        match self {
            TxnLockRef::InMemory(pessimistic_lock) => pessimistic_lock.for_update_ts,
            TxnLockRef::Persisted(lock) => lock.for_update_ts,
        }
    }

    pub fn is_pessimistic_lock(&self) -> bool {
        match self {
            TxnLockRef::InMemory(_) => true,
            TxnLockRef::Persisted(lock) => lock.is_pessimistic_lock(),
        }
    }

    pub fn get_lock_type(&self) -> LockType {
        match self {
            TxnLockRef::InMemory(_) => LockType::Pessimistic,
            TxnLockRef::Persisted(lock) => lock.lock_type,
        }
    }
}

impl<'a> From<&'a PessimisticLock> for TxnLockRef<'a> {
    fn from(in_memory_pessimistic_lock: &'a PessimisticLock) -> Self {
        Self::InMemory(in_memory_pessimistic_lock)
    }
}

impl<'a> From<&'a Lock> for TxnLockRef<'a> {
    fn from(lock: &'a Lock) -> Self {
        Self::Persisted(lock)
    }
}

pub trait LockInfoExt {
    fn is_shared_lock(&self) -> bool;
    fn iter_locks(&self) -> impl Iterator<Item = &LockInfo>;
}

impl LockInfoExt for LockInfo {
    fn is_shared_lock(&self) -> bool {
        self.lock_type == Op::SharedLock
    }

    fn iter_locks(&self) -> impl Iterator<Item = &LockInfo> {
        if self.is_shared_lock() {
            Either::Left(self.get_shared_lock_infos().iter())
        } else {
            Either::Right(std::iter::once(self))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_type() {
        let (key, value) = (b"key", b"value");
        let mut tests = vec![
            (
                Mutation::make_put(Key::from_raw(key), value.to_vec()),
                LockType::Put,
                FLAG_PUT,
            ),
            (
                Mutation::make_delete(Key::from_raw(key)),
                LockType::Delete,
                FLAG_DELETE,
            ),
            (
                Mutation::make_lock(Key::from_raw(key)),
                LockType::Lock,
                FLAG_LOCK,
            ),
            (
                Mutation::make_shared_lock(Key::from_raw(key)),
                LockType::Lock,
                FLAG_LOCK,
            ),
        ];
        for (i, (mutation, lock_type, flag)) in tests.drain(..).enumerate() {
            let lt = LockType::from_mutation(&mutation).unwrap();
            assert_eq!(
                lt, lock_type,
                "#{}, expect from_mutation({:?}) returns {:?}, but got {:?}",
                i, mutation, lock_type, lt
            );
            let f = lock_type.to_u8();
            assert_eq!(
                f, flag,
                "#{}, expect {:?}.to_u8() returns {:?}, but got {:?}",
                i, lock_type, flag, f
            );
            let lt = LockType::from_u8(flag).unwrap();
            assert_eq!(
                lt, lock_type,
                "#{}, expect from_u8({:?}) returns {:?}, but got {:?})",
                i, flag, lock_type, lt
            );
        }

        let lock_type = LockType::Shared;
        let f = lock_type.to_u8();
        assert_eq!(f, FLAG_SHARED);
        let lt = LockType::from_u8(f).unwrap();
        assert_eq!(lt, lock_type);
    }

    #[test]
    fn test_lock() {
        // Test `Lock::to_bytes()` and `parse_lock()` works as a pair.
        let mut locks: Vec<Either<Lock, SharedLocks>> = vec![
            Either::Left(Lock::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
                false,
            )),
            Either::Left(Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
                false,
            )),
            Either::Left(Lock::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                10.into(),
                0,
                TimeStamp::zero(),
                false,
            )),
            Either::Left(Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                10.into(),
                0,
                TimeStamp::zero(),
                false,
            )),
            Either::Left(Lock::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                TimeStamp::zero(),
                16,
                TimeStamp::zero(),
                false,
            )),
            Either::Left(Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                TimeStamp::zero(),
                16,
                TimeStamp::zero(),
                false,
            )),
            Either::Left(Lock::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                10.into(),
                16,
                TimeStamp::zero(),
                false,
            )),
            Either::Left(Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                10.into(),
                0,
                TimeStamp::zero(),
                false,
            )),
            Either::Left(Lock::new(
                LockType::Put,
                b"pkpkpk".to_vec(),
                111.into(),
                222,
                None,
                333.into(),
                444,
                555.into(),
                false,
            )),
            Either::Left(
                Lock::new(
                    LockType::Put,
                    b"pk".to_vec(),
                    111.into(),
                    222,
                    Some(b"short_value".to_vec()),
                    333.into(),
                    444,
                    555.into(),
                    false,
                )
                .use_async_commit(vec![]),
            ),
            Either::Left(
                Lock::new(
                    LockType::Put,
                    b"pk".to_vec(),
                    111.into(),
                    222,
                    Some(b"short_value".to_vec()),
                    333.into(),
                    444,
                    555.into(),
                    false,
                )
                .use_async_commit(vec![b"k".to_vec()]),
            ),
            Either::Left(
                Lock::new(
                    LockType::Put,
                    b"pk".to_vec(),
                    111.into(),
                    222,
                    Some(b"short_value".to_vec()),
                    333.into(),
                    444,
                    555.into(),
                    false,
                )
                .use_async_commit(vec![
                    b"k1".to_vec(),
                    b"kkkkk2".to_vec(),
                    b"k3k3k3k3k3k3".to_vec(),
                    b"k".to_vec(),
                ]),
            ),
            Either::Left(
                Lock::new(
                    LockType::Put,
                    b"pk".to_vec(),
                    111.into(),
                    222,
                    Some(b"short_value".to_vec()),
                    333.into(),
                    444,
                    555.into(),
                    false,
                )
                .use_async_commit(vec![
                    b"k1".to_vec(),
                    b"kkkkk2".to_vec(),
                    b"k3k3k3k3k3k3".to_vec(),
                    b"k".to_vec(),
                ])
                .with_rollback_ts(vec![12.into(), 24.into(), 13.into()]),
            ),
            Either::Left(
                Lock::new(
                    LockType::Put,
                    b"pk".to_vec(),
                    111.into(),
                    222,
                    Some(b"short_value".to_vec()),
                    333.into(),
                    444,
                    555.into(),
                    false,
                )
                .with_rollback_ts(vec![12.into(), 24.into(), 13.into()]),
            ),
            Either::Left(
                Lock::new(
                    LockType::Lock,
                    b"pk".to_vec(),
                    1.into(),
                    10,
                    None,
                    6.into(),
                    16,
                    8.into(),
                    false,
                )
                .set_last_change(LastChange::NotExist),
            ),
            Either::Left(
                Lock::new(
                    LockType::Lock,
                    b"pk".to_vec(),
                    1.into(),
                    10,
                    None,
                    6.into(),
                    16,
                    8.into(),
                    false,
                )
                .set_last_change(LastChange::make_exist(4.into(), 2))
                .set_txn_source(1)
                .with_generation(10),
            ),
            Either::Right(SharedLocks::new().with_txn_infos({
                let mut segments = HashMap::default();
                let seg1_ts: TimeStamp = 11.into();
                let seg1 = Lock::new(
                    LockType::Pessimistic,
                    b"seg1".to_vec(),
                    seg1_ts,
                    22,
                    None,
                    TimeStamp::zero(),
                    0,
                    TimeStamp::zero(),
                    false,
                );
                segments.insert(seg1_ts, Either::Right(seg1));
                let seg2_ts: TimeStamp = 33.into();
                let seg2 = Lock::new(
                    LockType::Pessimistic,
                    b"seg2".to_vec(),
                    seg2_ts,
                    44,
                    Some(b"v".to_vec()),
                    TimeStamp::zero(),
                    0,
                    TimeStamp::zero(),
                    false,
                );
                segments.insert(seg2_ts, Either::Right(seg2));
                segments
            })),
        ];
        for (i, lock) in locks.drain(..).enumerate() {
            let v = match &lock {
                Either::Left(lock) => lock.to_bytes(),
                Either::Right(shared_locks) => shared_locks.to_bytes(),
            };

            let l =
                parse_lock(&v[..]).unwrap_or_else(|e| panic!("#{} parse_lock() err: {:?}", i, e));

            match (l, lock) {
                (Either::Left(l), Either::Left(lock)) => {
                    assert_eq!(l, lock, "#{} expect {:?}, but got {:?}", i, lock, l);
                    assert!(lock.pre_allocate_size() >= v.len());
                }
                (Either::Right(mut l), Either::Right(lock)) => {
                    l.parse_all();
                    assert_eq!(l, lock, "#{} expect {:?}, but got {:?}", i, lock, l);
                    assert!(lock.pre_allocate_size() >= v.len());
                }
                (l, lock) => panic!("#{} mismatch: parsed={:?}, expected={:?}", i, l, lock),
            }
        }

        // Test `parse_lock()` handles incorrect input.
        parse_lock(b"").unwrap_err();

        let lock = Lock::new(
            LockType::Lock,
            b"pk".to_vec(),
            1.into(),
            10,
            Some(b"short_value".to_vec()),
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );
        let mut v = lock.to_bytes();
        parse_lock(&v[..4]).unwrap_err();
        // Test `parse_lock()` ignores unknown bytes.
        v.extend(b"unknown");
        let l = parse_lock(&v).unwrap();
        assert_eq!(l, Either::Left(lock));
    }

    #[test]
    fn test_check_ts_conflict() {
        let key = Key::from_raw(b"foo");
        let mut lock = Lock::new(
            LockType::Put,
            vec![],
            100.into(),
            3,
            None,
            TimeStamp::zero(),
            1,
            TimeStamp::zero(),
            false,
        );

        let empty = Default::default();

        // Ignore the lock if read ts is less than the lock version
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            50.into(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap();

        // Returns the lock if read ts >= lock version
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            110.into(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap_err();

        // Ignore locks that occurs in the `bypass_locks` set.
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            110.into(),
            &TsSet::from_u64s(vec![109]),
            IsolationLevel::Si,
        )
        .unwrap_err();
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            110.into(),
            &TsSet::from_u64s(vec![110]),
            IsolationLevel::Si,
        )
        .unwrap_err();
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            110.into(),
            &TsSet::from_u64s(vec![100]),
            IsolationLevel::Si,
        )
        .unwrap();
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            110.into(),
            &TsSet::from_u64s(vec![99, 101, 102, 100, 80]),
            IsolationLevel::Si,
        )
        .unwrap();

        // Ignore the lock if it is Lock or Pessimistic.
        lock.lock_type = LockType::Lock;
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            110.into(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap();
        lock.lock_type = LockType::Pessimistic;
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            110.into(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap();

        // Ignore the primary lock when reading the latest committed version by setting
        // u64::MAX as ts
        lock.lock_type = LockType::Put;
        lock.primary = b"foo".to_vec();
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            TimeStamp::max(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap();

        // Should not ignore the primary lock of an async commit transaction even if
        // setting u64::MAX as ts
        let async_commit_lock = lock.clone().use_async_commit(vec![]);
        check_ts_conflict(
            Cow::Owned(Either::Left(async_commit_lock)),
            &key,
            TimeStamp::max(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap_err();

        // Should not ignore the secondary lock even though reading the latest version
        lock.primary = b"bar".to_vec();
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            TimeStamp::max(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap_err();

        // Ignore the lock if read ts is less than min_commit_ts
        lock.min_commit_ts = 150.into();
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            140.into(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap();
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            150.into(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap_err();
        check_ts_conflict(
            Cow::Owned(Either::Left(lock.clone())),
            &key,
            160.into(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap_err();

        check_ts_conflict(
            Cow::Owned(Either::Right(SharedLocks::new())),
            &key,
            160.into(),
            &empty,
            IsolationLevel::Si,
        )
        .unwrap();
    }

    #[test]
    fn test_check_ts_conflict_rc_check_ts() {
        let k1 = Key::from_raw(b"k1");
        let mut lock = Lock::new(
            LockType::Put,
            vec![],
            100.into(),
            3,
            None,
            100.into(),
            1,
            TimeStamp::zero(),
            false,
        );

        let empty = Default::default();

        // Ignore locks that occurs in the `bypass_locks` set.
        check_ts_conflict_rc_check_ts(
            Cow::Owned(Either::Left(lock.clone())),
            &k1,
            50.into(),
            &TsSet::from_u64s(vec![100]),
        )
        .unwrap();

        // Ignore locks if the lock type are Pessimistic or Lock.
        lock.lock_type = LockType::Pessimistic;
        check_ts_conflict_rc_check_ts(
            Cow::Owned(Either::Left(lock.clone())),
            &k1,
            50.into(),
            &empty,
        )
        .unwrap();
        lock.lock_type = LockType::Lock;
        check_ts_conflict_rc_check_ts(
            Cow::Owned(Either::Left(lock.clone())),
            &k1,
            50.into(),
            &empty,
        )
        .unwrap();

        // Report error even if read ts is less than the lock version.
        lock.lock_type = LockType::Put;
        check_ts_conflict_rc_check_ts(
            Cow::Owned(Either::Left(lock.clone())),
            &k1,
            50.into(),
            &empty,
        )
        .unwrap_err();
        check_ts_conflict_rc_check_ts(
            Cow::Owned(Either::Left(lock.clone())),
            &k1,
            110.into(),
            &empty,
        )
        .unwrap_err();

        // Report error if for other lock types.
        lock.lock_type = LockType::Delete;
        check_ts_conflict_rc_check_ts(Cow::Owned(Either::Left(lock)), &k1, 50.into(), &empty)
            .unwrap_err();
    }

    #[test]
    fn test_customize_debug() {
        let mut lock = Lock::new(
            LockType::Put,
            b"pk".to_vec(),
            100.into(),
            3,
            Option::from(b"short_value".to_vec()),
            101.into(),
            10,
            127.into(),
            false,
        )
        .use_async_commit(vec![
            b"secondary_k1".to_vec(),
            b"secondary_kkkkk2".to_vec(),
            b"secondary_k3k3k3k3k3k3".to_vec(),
            b"secondary_k4".to_vec(),
        ])
        .set_last_change(LastChange::make_exist(80.into(), 4));

        assert_eq!(
            format!("{:?}", lock),
            "Lock { \
            lock_type: Put, \
            primary_key: 706B, \
            start_ts: TimeStamp(100), \
            ttl: 3, \
            short_value: 73686F72745F76616C7565, \
            for_update_ts: TimeStamp(101), \
            txn_size: 10, \
            min_commit_ts: TimeStamp(127), \
            use_async_commit: true, \
            secondaries: [7365636F6E646172795F6B31, 7365636F6E646172795F6B6B6B6B6B32, \
            7365636F6E646172795F6B336B336B336B336B336B33, 7365636F6E646172795F6B34], \
            rollback_ts: [], \
            last_change: Exist { last_change_ts: TimeStamp(80), estimated_versions_to_last_change: 4 }, \
            txn_source: 0, \
            is_locked_with_conflict: false, \
            generation: 0 \
            }"
        );
        log_wrappers::set_redact_info_log(log_wrappers::RedactOption::On);
        let redact_result = format!("{:?}", lock);
        log_wrappers::set_redact_info_log(log_wrappers::RedactOption::Off);
        assert_eq!(
            redact_result,
            "Lock { \
            lock_type: Put, \
            primary_key: ?, \
            start_ts: TimeStamp(100), \
            ttl: 3, \
            short_value: ?, \
            for_update_ts: TimeStamp(101), \
            txn_size: 10, \
            min_commit_ts: TimeStamp(127), \
            use_async_commit: true, \
            secondaries: [?, ?, ?, ?], \
            rollback_ts: [], \
            last_change: Exist { last_change_ts: TimeStamp(80), estimated_versions_to_last_change: 4 }, \
            txn_source: 0, \
            is_locked_with_conflict: false, \
            generation: 0 \
            }"
        );

        log_wrappers::set_redact_info_log(log_wrappers::RedactOption::Marker);
        let redact_result = format!("{:?}", lock);
        log_wrappers::set_redact_info_log(log_wrappers::RedactOption::Off);
        assert_eq!(
            redact_result,
            "Lock { \
            lock_type: Put, \
            primary_key: ‹706B›, \
            start_ts: TimeStamp(100), \
            ttl: 3, \
            short_value: ‹73686F72745F76616C7565›, \
            for_update_ts: TimeStamp(101), \
            txn_size: 10, \
            min_commit_ts: TimeStamp(127), \
            use_async_commit: true, \
            secondaries: [‹7365636F6E646172795F6B31›, ‹7365636F6E646172795F6B6B6B6B6B32›, \
            ‹7365636F6E646172795F6B336B336B336B336B336B33›, ‹7365636F6E646172795F6B34›], \
            rollback_ts: [], \
            last_change: Exist { last_change_ts: TimeStamp(80), estimated_versions_to_last_change: 4 }, \
            txn_source: 0, \
            is_locked_with_conflict: false, \
            generation: 0 \
            }"
        );

        lock.short_value = None;
        lock.secondaries = Vec::default();
        lock.generation = 10;
        assert_eq!(
            format!("{:?}", lock),
            "Lock { \
            lock_type: Put, \
            primary_key: 706B, \
            start_ts: TimeStamp(100), \
            ttl: 3, \
            short_value: , \
            for_update_ts: TimeStamp(101), \
            txn_size: 10, \
            min_commit_ts: TimeStamp(127), \
            use_async_commit: true, \
            secondaries: [], \
            rollback_ts: [], \
            last_change: Exist { last_change_ts: TimeStamp(80), estimated_versions_to_last_change: 4 }, \
            txn_source: 0, \
            is_locked_with_conflict: false, \
            generation: 10 \
            }"
        );
        log_wrappers::set_redact_info_log(log_wrappers::RedactOption::On);
        let redact_result = format!("{:?}", lock);
        log_wrappers::set_redact_info_log(log_wrappers::RedactOption::Off);
        assert_eq!(
            redact_result,
            "Lock { \
            lock_type: Put, \
            primary_key: ?, \
            start_ts: TimeStamp(100), \
            ttl: 3, \
            short_value: ?, \
            for_update_ts: TimeStamp(101), \
            txn_size: 10, \
            min_commit_ts: TimeStamp(127), \
            use_async_commit: true, \
            secondaries: [], \
            rollback_ts: [], \
            last_change: Exist { last_change_ts: TimeStamp(80), estimated_versions_to_last_change: 4 }, \
            txn_source: 0, \
            is_locked_with_conflict: false, \
            generation: 10 \
            }"
        );
    }

    #[test]
    fn test_pessimistic_lock_to_lock() {
        let pessimistic_lock = PessimisticLock {
            primary: b"primary".to_vec().into_boxed_slice(),
            start_ts: 5.into(),
            ttl: 1000,
            for_update_ts: 10.into(),
            min_commit_ts: 20.into(),
            last_change: LastChange::make_exist(8.into(), 2),
            is_locked_with_conflict: false,
        };
        let expected_lock = Lock {
            lock_type: LockType::Pessimistic,
            primary: b"primary".to_vec(),
            ts: 5.into(),
            ttl: 1000,
            short_value: None,
            for_update_ts: 10.into(),
            txn_size: 0,
            min_commit_ts: 20.into(),
            use_async_commit: false,
            use_one_pc: false,
            secondaries: vec![],
            rollback_ts: vec![],
            last_change: LastChange::make_exist(8.into(), 2),
            txn_source: 0,
            is_locked_with_conflict: false,
            generation: 0,
        };
        assert_eq!(pessimistic_lock.to_lock(), expected_lock);
        assert_eq!(pessimistic_lock.into_lock(), expected_lock);
    }

    #[test]
    fn test_pessimistic_lock_customize_debug() {
        let pessimistic_lock = PessimisticLock {
            primary: b"primary".to_vec().into_boxed_slice(),
            start_ts: 5.into(),
            ttl: 1000,
            for_update_ts: 10.into(),
            min_commit_ts: 20.into(),
            last_change: LastChange::make_exist(8.into(), 2),
            is_locked_with_conflict: false,
        };
        assert_eq!(
            format!("{:?}", pessimistic_lock),
            "PessimisticLock { primary_key: 7072696D617279, start_ts: TimeStamp(5), ttl: 1000, \
            for_update_ts: TimeStamp(10), min_commit_ts: TimeStamp(20), \
            last_change: Exist { last_change_ts: TimeStamp(8), estimated_versions_to_last_change: 2 }\
            , is_locked_with_conflict: false }"
        );
        log_wrappers::set_redact_info_log(log_wrappers::RedactOption::On);
        let redact_result = format!("{:?}", pessimistic_lock);
        log_wrappers::set_redact_info_log(log_wrappers::RedactOption::Off);
        assert_eq!(
            redact_result,
            "PessimisticLock { primary_key: ?, start_ts: TimeStamp(5), ttl: 1000, \
            for_update_ts: TimeStamp(10), min_commit_ts: TimeStamp(20), \
            last_change: Exist { last_change_ts: TimeStamp(8), estimated_versions_to_last_change: 2 }\
            , is_locked_with_conflict: false }"
        );
    }

    #[test]
    fn test_pessimistic_lock_memory_size() {
        let lock = PessimisticLock {
            primary: b"primary".to_vec().into_boxed_slice(),
            start_ts: 5.into(),
            ttl: 1000,
            for_update_ts: 10.into(),
            min_commit_ts: 20.into(),
            last_change: LastChange::make_exist(8.into(), 2),
            is_locked_with_conflict: false,
        };
        // 7 bytes for primary key, 16 bytes for Box<[u8]>, 4 x 8-byte integers, 1
        // enum (8 + 2 * 8) and a bool.
        assert_eq!(lock.memory_size(), 7 + 16 + 5 * 8 + 24);
    }

    #[test]
    fn test_decode_lock_type() {
        fn lock_bytes(lock_type: LockType) -> Vec<u8> {
            Lock::new(
                lock_type,
                b"primary".to_vec(),
                42.into(),
                3600,
                None,
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
                false,
            )
            .to_bytes()
        }

        let shared_bytes = SharedLocks::new().to_bytes();

        assert!(matches!(
            decode_lock_type(&lock_bytes(LockType::Put)),
            Ok(LockType::Put)
        ));
        assert!(matches!(
            decode_lock_type(&lock_bytes(LockType::Delete)),
            Ok(LockType::Delete)
        ));
        assert!(matches!(
            decode_lock_type(&lock_bytes(LockType::Lock)),
            Ok(LockType::Lock)
        ));
        assert!(matches!(
            decode_lock_type(&lock_bytes(LockType::Pessimistic)),
            Ok(LockType::Pessimistic)
        ));
        assert!(matches!(
            decode_lock_type(&shared_bytes),
            Ok(LockType::Shared)
        ));
        decode_lock_type(&[]).unwrap_err();
    }

    #[test]
    fn test_decode_lock_start_ts() {
        let ts: TimeStamp = 123.into();
        let lock = Lock::new(
            LockType::Put,
            b"primary_key".to_vec(),
            ts,
            10,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );
        let bytes = lock.to_bytes();

        assert_eq!(decode_lock_start_ts(&bytes).unwrap(), ts);

        let incomplete = vec![LockType::Put.to_u8()];
        decode_lock_start_ts(&incomplete).unwrap_err();
        decode_lock_start_ts(&[]).unwrap_err();
    }

    #[test]
    fn test_new_in_shared_mode_initial_state() {
        let shared_locks = SharedLocks::new();

        assert_eq!(shared_locks.len(), 0);
        let bytes = shared_locks.to_bytes();
        assert!(matches!(decode_lock_type(&bytes), Ok(LockType::Shared)));
        decode_lock_start_ts(&bytes).unwrap_err();
        assert!(matches!(parse_lock(&bytes).unwrap(), Either::Right(_)));
    }

    #[test]
    fn test_put_and_get_shared_lock_txn() {
        let mut shared_locks = SharedLocks::new();

        let txn1_ts: TimeStamp = 5.into();
        let txn1_lock = Lock::new(
            LockType::Pessimistic,
            b"txn1".to_vec(),
            txn1_ts,
            0,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );

        let txn2_ts: TimeStamp = 7.into();
        let txn2_pessimistic_lock = Lock::new(
            LockType::Pessimistic,
            b"txn2".to_vec(),
            txn2_ts,
            0,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );
        let txn2_prewrite_lock = Lock::new(
            LockType::Lock,
            b"txn2".to_vec(),
            txn2_ts,
            0,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );

        shared_locks.insert_lock(txn1_lock).unwrap();
        shared_locks.insert_lock(txn2_pessimistic_lock).unwrap();
        shared_locks.update_lock(txn2_prewrite_lock).unwrap();

        assert_eq!(shared_locks.len(), 2);
        assert_eq!(
            shared_locks.get_lock(&txn1_ts).unwrap().unwrap().ts,
            txn1_ts
        );
        assert_eq!(
            shared_locks.get_lock(&txn1_ts).unwrap().unwrap().lock_type,
            LockType::Pessimistic
        );
        assert_eq!(
            shared_locks.get_lock(&txn2_ts).unwrap().unwrap().ts,
            txn2_ts
        );
        assert_eq!(
            shared_locks.get_lock(&txn2_ts).unwrap().unwrap().lock_type,
            LockType::Lock
        );

        let found = shared_locks.get_lock(&txn1_ts).unwrap().unwrap();
        assert_eq!(found.primary, b"txn1".to_vec());
        assert_eq!(found.ts, txn1_ts);
        assert_eq!(found.lock_type, LockType::Pessimistic);

        let missing_ts: TimeStamp = 42.into();
        assert!(shared_locks.get_lock(&missing_ts).unwrap().is_none());
    }

    #[test]
    fn test_shared_locks_shrink_only_flag_basic() {
        let mut shared_locks = SharedLocks::new();

        assert!(!shared_locks.is_shrink_only());

        let lock_ts: TimeStamp = 10.into();
        let lock = Lock::new(
            LockType::Pessimistic,
            b"primary".to_vec(),
            lock_ts,
            100,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );
        shared_locks.insert_lock(lock).unwrap();

        shared_locks.set_shrink_only();
        assert!(shared_locks.is_shrink_only());

        let bytes = shared_locks.to_bytes();

        let parsed = parse_lock(&bytes).unwrap();
        match parsed {
            Either::Right(parsed_shared_locks) => {
                assert!(parsed_shared_locks.is_shrink_only());
                assert_eq!(parsed_shared_locks.len(), 1);
            }
            _ => panic!("expected SharedLocks"),
        }
    }

    #[test]
    fn test_insert_lock_to_shrink_only_fails() {
        let mut shared_locks = SharedLocks::new();

        let lock = Lock::new(
            LockType::Pessimistic,
            b"txn1".to_vec(),
            5.into(),
            100,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );

        // Add a lock before setting shrink_only
        shared_locks.insert_lock(lock).unwrap();

        // Set shrink_only and try to add another lock
        shared_locks.set_shrink_only();
        let new_lock = Lock::new(
            LockType::Pessimistic,
            b"txn2".to_vec(),
            7.into(),
            100,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );
        let result = shared_locks.insert_lock(new_lock);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "invalid operation: cannot insert new lock #7 into shrink-only SharedLocks"
        );
    }

    #[test]
    fn test_insert_lock_duplicate_fails() {
        let mut shared_locks = SharedLocks::new();
        let lock_ts: TimeStamp = 10.into();

        let lock = Lock::new(
            LockType::Pessimistic,
            b"primary".to_vec(),
            lock_ts,
            100,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );

        // First insert should succeed
        shared_locks.insert_lock(lock.clone()).unwrap();

        // Second insert with same ts should fail
        let result = shared_locks.insert_lock(lock);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "invalid operation: lock #10 already exists in SharedLocks"
        );
    }

    #[test]
    fn test_update_lock_success() {
        let mut shared_locks = SharedLocks::new();
        let lock_ts: TimeStamp = 10.into();

        let pessimistic_lock = Lock::new(
            LockType::Pessimistic,
            b"primary".to_vec(),
            lock_ts,
            100,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );

        shared_locks.insert_lock(pessimistic_lock.clone()).unwrap();

        let prewrite_lock = Lock::new(
            LockType::Lock,
            b"primary".to_vec(),
            lock_ts,
            100,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );
        shared_locks.update_lock(prewrite_lock).unwrap();

        // Verify the lock type
        let found = shared_locks.get_lock(&lock_ts).unwrap().unwrap();
        assert_eq!(found.lock_type, LockType::Lock);
    }

    #[test]
    fn test_update_lock_nonexistent_fails() {
        let mut shared_locks = SharedLocks::new();
        let lock_ts: TimeStamp = 10.into();

        let lock = Lock::new(
            LockType::Pessimistic,
            b"primary".to_vec(),
            lock_ts,
            100,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        );

        // Try to update non-existent lock
        let result = shared_locks.update_lock(lock);
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.unwrap_err()),
            "invalid operation: lock #10 does not exist in SharedLocks"
        );
    }
}
