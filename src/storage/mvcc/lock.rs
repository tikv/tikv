// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::super::types::Value;
use crate::storage::{
    Mutation, FOR_UPDATE_TS_PREFIX, SHORT_VALUE_MAX_LEN, SHORT_VALUE_PREFIX, TXN_SIZE_PREFIX,
};
use byteorder::ReadBytesExt;
use tikv_util::codec::bytes::{self, BytesEncoder};
use tikv_util::codec::number::{self, NumberEncoder, MAX_VAR_U64_LEN};
use std::io;

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

impl LockType {
    pub fn from_mutation(mutation: &Mutation) -> LockType {
        match *mutation {
            Mutation::Put(_) | Mutation::Insert(_) => LockType::Put,
            Mutation::Delete(_) => LockType::Delete,
            Mutation::Lock(_) => LockType::Lock,
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

#[derive(PartialEq, Debug)]
pub struct Lock {
    pub lock_type: LockType,
    pub primary: Vec<u8>,
    pub ts: u64,
    pub ttl: u64,
    pub short_value: Option<Value>,
    // If for_update_ts != 0, this lock belongs to a pessimistic transaction
    pub for_update_ts: u64,
    pub txn_size: u64,
}

impl Lock {
    pub fn new(
        lock_type: LockType,
        primary: Vec<u8>,
        ts: u64,
        ttl: u64,
        short_value: Option<Value>,
        for_update_ts: u64,
        txn_size: u64,
    ) -> Lock {
        Lock {
            lock_type,
            primary,
            ts,
            ttl,
            short_value,
            for_update_ts,
            txn_size,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(
            1 + MAX_VAR_U64_LEN + self.primary.len() + MAX_VAR_U64_LEN + SHORT_VALUE_MAX_LEN + 2,
        );
        b.push(self.lock_type.to_u8());
        b.encode_compact_bytes(&self.primary).unwrap();
        b.encode_var_u64(self.ts).unwrap();
        b.encode_var_u64(self.ttl).unwrap();
        if let Some(ref v) = self.short_value {
            b.push(SHORT_VALUE_PREFIX);
            b.push(v.len() as u8);
            b.extend_from_slice(v);
        }
        if self.for_update_ts > 0 {
            b.push(FOR_UPDATE_TS_PREFIX);
            b.encode_u64(self.for_update_ts).unwrap();
        }
        if self.txn_size > 0 {
            b.push(TXN_SIZE_PREFIX);
            b.encode_u64(self.txn_size).unwrap();
        }
        b
    }

    pub fn parse(mut b: &[u8]) -> Result<Lock> {
        if b.is_empty() {
            return Err(Error::BadFormatLock);
        }
        let lock_type = LockType::from_u8(b.read_u8()?).ok_or(Error::BadFormatLock)?;
        let primary = bytes::decode_compact_bytes(&mut b)?;
        let ts = number::decode_var_u64(&mut b)?;
        let ttl = if b.is_empty() {
            0
        } else {
            number::decode_var_u64(&mut b)?
        };

        if b.is_empty() {
            return Ok(Lock::new(lock_type, primary, ts, ttl, None, 0, 0));
        }

        let mut short_value = None;
        let mut for_update_ts = 0;
        let mut txn_size: u64 = 0;
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
                FOR_UPDATE_TS_PREFIX => for_update_ts = number::decode_u64(&mut b)?,
                TXN_SIZE_PREFIX => txn_size = number::decode_u64(&mut b)?,
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
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Key, Mutation};

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
            let lt = LockType::from_mutation(&mutation);
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
            Lock::new(LockType::Put, b"pk".to_vec(), 1, 10, None, 0, 0),
            Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1,
                10,
                Some(b"short_value".to_vec()),
                0,
                0,
            ),
            Lock::new(LockType::Put, b"pk".to_vec(), 1, 10, None, 10, 0),
            Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1,
                10,
                Some(b"short_value".to_vec()),
                10,
                0,
            ),
            Lock::new(LockType::Put, b"pk".to_vec(), 1, 10, None, 0, 16),
            Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1,
                10,
                Some(b"short_value".to_vec()),
                0,
                16,
            ),
            Lock::new(LockType::Put, b"pk".to_vec(), 1, 10, None, 10, 16),
            Lock::new(
                LockType::Delete,
                b"pk".to_vec(),
                1,
                10,
                Some(b"short_value".to_vec()),
                10,
                0,
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
            1,
            10,
            Some(b"short_value".to_vec()),
            0,
            0,
        );
        let v = lock.to_bytes();
        assert!(Lock::parse(&v[..4]).is_err());
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
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
        BadFormatLock { description("bad format lock data") }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
