// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crate::timestamp::{TimeStamp, TsSet};
use crate::types::{Key, Mutation, Value, SHORT_VALUE_MAX_LEN, SHORT_VALUE_PREFIX};
use crate::{Error, ErrorInner, Result};
use byteorder::ReadBytesExt;
use derive_new::new;
use kvproto::kvrpcpb::{LockInfo, Op};
use tikv_util::codec::bytes::{self, BytesEncoder};
use tikv_util::codec::number::{self, NumberEncoder, MAX_VAR_U64_LEN};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockType {
    Put,
    Delete,
    Lock,
    Pessimistic,
}

const FLAG_PUT: u8 = b'P';
const FLAG_DELETE: u8 = b'D';
const FLAG_LOCK: u8 = b'L';
const FLAG_PESSIMISTIC: u8 = b'S';

const FOR_UPDATE_TS_PREFIX: u8 = b'f';
const TXN_SIZE_PREFIX: u8 = b't';
const MIN_COMMIT_TS_PREFIX: u8 = b'c';

impl LockType {
    pub fn from_mutation(mutation: &Mutation) -> Option<LockType> {
        match *mutation {
            Mutation::Put(_) | Mutation::Insert(_) => Some(LockType::Put),
            Mutation::Delete(_) => Some(LockType::Delete),
            Mutation::Lock(_) => Some(LockType::Lock),
            Mutation::CheckNotExists(_) => None,
        }
    }

    fn from_u8(b: u8) -> Option<LockType> {
        match b {
            FLAG_PUT => Some(LockType::Put),
            FLAG_DELETE => Some(LockType::Delete),
            FLAG_LOCK => Some(LockType::Lock),
            FLAG_PESSIMISTIC => Some(LockType::Pessimistic),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            LockType::Put => FLAG_PUT,
            LockType::Delete => FLAG_DELETE,
            LockType::Lock => FLAG_LOCK,
            LockType::Pessimistic => FLAG_PESSIMISTIC,
        }
    }
}

#[derive(new, PartialEq, Clone, Debug)]
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
}

impl Lock {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(
            1 + MAX_VAR_U64_LEN + self.primary.len() + MAX_VAR_U64_LEN + SHORT_VALUE_MAX_LEN + 2,
        );
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
        b
    }

    pub fn parse(mut b: &[u8]) -> Result<Lock> {
        if b.is_empty() {
            return Err(Error::from(ErrorInner::BadFormatLock));
        }
        let lock_type = LockType::from_u8(b.read_u8()?).ok_or(ErrorInner::BadFormatLock)?;
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
            ));
        }

        let mut short_value = None;
        let mut for_update_ts = TimeStamp::zero();
        let mut txn_size: u64 = 0;
        let mut min_commit_ts = TimeStamp::zero();
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
                flag => panic!("invalid flag [{}] in lock", flag),
            }
        }
        Ok(Lock::new(
            lock_type,
            primary,
            ts,
            ttl,
            short_value,
            for_update_ts,
            txn_size,
            min_commit_ts,
        ))
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
        };
        info.set_lock_type(lock_type);
        info.set_lock_for_update_ts(self.for_update_ts.into_inner());
        info
    }

    /// Checks whether the lock conflicts with the given `ts`. If `ts == TimeStamp::max()`, the primary lock will be ignored.
    pub fn check_ts_conflict(self, key: &Key, ts: TimeStamp, bypass_locks: &TsSet) -> Result<()> {
        if self.ts > ts
            || self.lock_type == LockType::Lock
            || self.lock_type == LockType::Pessimistic
        {
            // Ignore lock when lock.ts > ts or lock's type is Lock or Pessimistic
            return Ok(());
        }

        if self.min_commit_ts > ts {
            // Ignore lock when min_commit_ts > ts
            return Ok(());
        }

        if bypass_locks.contains(self.ts) {
            return Ok(());
        }

        let raw_key = key.to_raw()?;

        if ts == TimeStamp::max() && raw_key == self.primary {
            // When `ts == TimeStamp::max()` (which means to get latest committed version for
            // primary key), and current key is the primary key, we ignore this lock.
            return Ok(());
        }

        // There is a pending lock. Client should wait or clean it.
        Err(Error::from(ErrorInner::KeyIsLocked(
            self.into_lock_info(raw_key),
        )))
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
                Mutation::Put((Key::from_raw(key), value.to_vec())),
                LockType::Put,
                FLAG_PUT,
            ),
            (
                Mutation::Delete(Key::from_raw(key)),
                LockType::Delete,
                FLAG_DELETE,
            ),
            (
                Mutation::Lock(Key::from_raw(key)),
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
    }

    #[test]
    fn test_lock() {
        // Test `Lock::to_bytes()` and `Lock::parse()` works as a pair.
        let mut locks = vec![
            Lock::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
            ),
            Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                TimeStamp::zero(),
                0,
                TimeStamp::zero(),
            ),
            Lock::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                10.into(),
                0,
                TimeStamp::zero(),
            ),
            Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                10.into(),
                0,
                TimeStamp::zero(),
            ),
            Lock::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                TimeStamp::zero(),
                16,
                TimeStamp::zero(),
            ),
            Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                TimeStamp::zero(),
                16,
                TimeStamp::zero(),
            ),
            Lock::new(
                LockType::Put,
                b"pk".to_vec(),
                1.into(),
                10,
                None,
                10.into(),
                16,
                TimeStamp::zero(),
            ),
            Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1.into(),
                10,
                Some(b"short_value".to_vec()),
                10.into(),
                0,
                TimeStamp::zero(),
            ),
            Lock::new(
                LockType::Put,
                b"pkpkpk".to_vec(),
                111.into(),
                222,
                None,
                333.into(),
                444,
                555.into(),
            ),
        ];
        for (i, lock) in locks.drain(..).enumerate() {
            let v = lock.to_bytes();
            let l = Lock::parse(&v[..]).unwrap_or_else(|e| panic!("#{} parse() err: {:?}", i, e));
            assert_eq!(l, lock, "#{} expect {:?}, but got {:?}", i, lock, l);
        }

        // Test `Lock::parse()` handles incorrect input.
        assert!(Lock::parse(b"").is_err());

        let lock = Lock::new(
            LockType::Lock,
            b"pk".to_vec(),
            1.into(),
            10,
            Some(b"short_value".to_vec()),
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
        );
        let v = lock.to_bytes();
        assert!(Lock::parse(&v[..4]).is_err());
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
        );

        let empty = Default::default();

        // Ignore the lock if read ts is less than the lock version
        lock.clone()
            .check_ts_conflict(&key, 50.into(), &empty)
            .unwrap();

        // Returns the lock if read ts >= lock version
        lock.clone()
            .check_ts_conflict(&key, 110.into(), &empty)
            .unwrap_err();

        // Ignore locks that occurs in the `bypass_locks` set.
        lock.clone()
            .check_ts_conflict(&key, 110.into(), &TsSet::from_u64s(vec![109]))
            .unwrap_err();
        lock.clone()
            .check_ts_conflict(&key, 110.into(), &TsSet::from_u64s(vec![110]))
            .unwrap_err();
        lock.clone()
            .check_ts_conflict(&key, 110.into(), &TsSet::from_u64s(vec![100]))
            .unwrap();
        lock.clone()
            .check_ts_conflict(
                &key,
                110.into(),
                &TsSet::from_u64s(vec![99, 101, 102, 100, 80]),
            )
            .unwrap();

        // Ignore the lock if it is Lock or Pessimistic.
        lock.lock_type = LockType::Lock;
        lock.clone()
            .check_ts_conflict(&key, 110.into(), &empty)
            .unwrap();
        lock.lock_type = LockType::Pessimistic;
        lock.clone()
            .check_ts_conflict(&key, 110.into(), &empty)
            .unwrap();

        // Ignore the primary lock when reading the latest committed version by setting u64::MAX as ts
        lock.lock_type = LockType::Put;
        lock.primary = b"foo".to_vec();
        lock.clone()
            .check_ts_conflict(&key, TimeStamp::max(), &empty)
            .unwrap();

        // Should not ignore the secondary lock even though reading the latest version
        lock.primary = b"bar".to_vec();
        lock.clone()
            .check_ts_conflict(&key, TimeStamp::max(), &empty)
            .unwrap_err();

        // Ignore the lock if read ts is less than min_commit_ts
        lock.min_commit_ts = 150.into();
        lock.clone()
            .check_ts_conflict(&key, 140.into(), &empty)
            .unwrap();
        lock.clone()
            .check_ts_conflict(&key, 150.into(), &empty)
            .unwrap_err();
        lock.check_ts_conflict(&key, 160.into(), &empty)
            .unwrap_err();
    }
}
