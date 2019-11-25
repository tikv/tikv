// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::super::types::Value;
use super::lock::LockType;
use super::{Error, ErrorInner, Result, TimeStamp};
use crate::storage::{SHORT_VALUE_MAX_LEN, VALUE_PREFIX};
use codec::prelude::NumberDecoder;
use tikv_util::codec::number::{self, NumberEncoder, MAX_VAR_U64_LEN};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriteType {
    Put,
    Delete,
    Lock,
    Rollback,
}

const FLAG_PUT: u8 = b'P';
const FLAG_DELETE: u8 = b'D';
const FLAG_LOCK: u8 = b'L';
const FLAG_ROLLBACK: u8 = b'R';

/// The short value for rollback records which are protected from being collapsed.
const PROTECTED_ROLLBACK_SHORT_VALUE: &[u8] = b"p";

impl WriteType {
    pub fn from_lock_type(tp: LockType) -> Option<WriteType> {
        match tp {
            LockType::Put => Some(WriteType::Put),
            LockType::Delete => Some(WriteType::Delete),
            LockType::Lock => Some(WriteType::Lock),
            LockType::Pessimistic => None,
        }
    }

    pub fn from_u8(b: u8) -> Option<WriteType> {
        match b {
            FLAG_PUT => Some(WriteType::Put),
            FLAG_DELETE => Some(WriteType::Delete),
            FLAG_LOCK => Some(WriteType::Lock),
            FLAG_ROLLBACK => Some(WriteType::Rollback),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            WriteType::Put => FLAG_PUT,
            WriteType::Delete => FLAG_DELETE,
            WriteType::Lock => FLAG_LOCK,
            WriteType::Rollback => FLAG_ROLLBACK,
        }
    }
}

#[derive(PartialEq, Clone)]
pub struct Write {
    pub write_type: WriteType,
    pub start_ts: TimeStamp,
    pub commit_ts: TimeStamp,
    pub value: Option<Value>,
}

impl std::fmt::Debug for Write {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Write")
            .field("write_type", &self.write_type)
            .field("start_ts", &self.start_ts)
            .field("commit_ts", &self.commit_ts)
            .field(
                "value",
                &self
                    .value
                    .as_ref()
                    .map(|v| hex::encode_upper(v))
                    .unwrap_or_else(|| "None".to_owned()),
            )
            .finish()
    }
}

impl Write {
    pub fn new(
        write_type: WriteType,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
        value: Option<Value>,
    ) -> Write {
        Write {
            write_type,
            start_ts,
            commit_ts,
            value,
        }
    }

    #[inline]
    pub fn new_rollback(start_ts: TimeStamp, protected: bool) -> Write {
        let value = if protected {
            Some(PROTECTED_ROLLBACK_SHORT_VALUE.to_vec())
        } else {
            None
        };

        Write {
            write_type: WriteType::Rollback,
            start_ts,
            commit_ts: start_ts,
            value,
        }
    }

    #[inline]
    pub fn parse_type(mut b: &[u8]) -> Result<WriteType> {
        let write_type_bytes = b
            .read_u8()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?;
        WriteType::from_u8(write_type_bytes).ok_or_else(|| Error::from(ErrorInner::BadFormatWrite))
    }

    #[inline]
    pub fn as_ref(&self) -> WriteRef<'_> {
        WriteRef {
            write_type: self.write_type,
            start_ts: self.start_ts,
            commit_ts: self.commit_ts,
            value: self.value.as_ref().map(|v| v.as_slice()),
        }
    }

    #[inline]
    pub fn take_value(&mut self) -> Option<Value> {
        self.value.take()
    }
}

#[derive(PartialEq, Clone)]
pub struct WriteRef<'a> {
    pub write_type: WriteType,
    pub start_ts: TimeStamp,
    pub commit_ts: TimeStamp,
    pub value: Option<&'a [u8]>,
}

impl std::fmt::Debug for WriteRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Write")
            .field("write_type", &self.write_type)
            .field("start_ts", &self.start_ts)
            .field("commit_ts", &self.commit_ts)
            .field(
                "value",
                &self
                    .value
                    .as_ref()
                    .map(|v| hex::encode_upper(v))
                    .unwrap_or_else(|| "None".to_owned()),
            )
            .finish()
    }
}

impl WriteRef<'_> {
    pub fn parse(mut b: &[u8]) -> Result<WriteRef<'_>> {
        let write_type_bytes = b
            .read_u8()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?;
        let write_type = WriteType::from_u8(write_type_bytes)
            .ok_or_else(|| Error::from(ErrorInner::BadFormatWrite))?;
        let start_ts = b
            .read_u64()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?
            .into();
        let commit_ts = b
            .read_u64()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?
            .into();
        if b.is_empty() {
            return Ok(WriteRef {
                write_type,
                start_ts,
                commit_ts,
                value: None,
            });
        }

        let flag = b
            .read_u8()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?;
        assert_eq!(flag, VALUE_PREFIX, "invalid flag [{}] in write", flag);

        let len = b
            .read_var_u64()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?;
        if len as usize != b.len() {
            panic!(
                "short value len [{}] not equal to content len [{}]",
                len,
                b.len()
            );
        }

        Ok(WriteRef {
            write_type,
            start_ts,
            commit_ts,
            value: Some(b),
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(1 + MAX_VAR_U64_LEN * 2 + SHORT_VALUE_MAX_LEN + 2);
        b.push(self.write_type.to_u8());
        b.encode_u64(self.start_ts.into_inner()).unwrap();
        b.encode_u64(self.commit_ts.into_inner()).unwrap();
        if let Some(v) = self.value {
            b.push(VALUE_PREFIX);
            b.encode_var_u64(v.len() as u64).unwrap();
            b.extend_from_slice(v);
        }
        b
    }

    #[inline]
    pub fn is_protected(&self) -> bool {
        self.write_type == WriteType::Rollback
            && self
                .value
                .as_ref()
                .map(|v| *v == PROTECTED_ROLLBACK_SHORT_VALUE)
                .unwrap_or_default()
    }

    #[inline]
    pub fn to_owned(&self) -> Write {
        Write::new(
            self.write_type,
            self.start_ts,
            self.commit_ts,
            self.value.map(|v| v.to_owned()),
        )
    }

    #[inline]
    pub fn copy_value(&self) -> Option<Value> {
        self.value.map(|v| v.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::super::LockType;
    use super::*;

    #[test]
    fn test_write_type() {
        let mut tests = vec![
            (Some(LockType::Put), WriteType::Put, FLAG_PUT),
            (Some(LockType::Delete), WriteType::Delete, FLAG_DELETE),
            (Some(LockType::Lock), WriteType::Lock, FLAG_LOCK),
            (None, WriteType::Rollback, FLAG_ROLLBACK),
        ];
        for (i, (lock_type, write_type, flag)) in tests.drain(..).enumerate() {
            if let Some(lock_type) = lock_type {
                let wt = WriteType::from_lock_type(lock_type).unwrap();
                assert_eq!(
                    wt, write_type,
                    "#{}, expect from_lock_type({:?}) returns {:?}, but got {:?}",
                    i, lock_type, write_type, wt
                );
            }
            let f = write_type.to_u8();
            assert_eq!(
                f, flag,
                "#{}, expect {:?}.to_u8() returns {:?}, but got {:?}",
                i, write_type, flag, f
            );
            let wt = WriteType::from_u8(flag).unwrap();
            assert_eq!(
                wt, write_type,
                "#{}, expect from_u8({:?}) returns {:?}, but got {:?}",
                i, flag, write_type, wt
            );
        }
    }

    #[test]
    fn test_write() {
        // Test `Write::to_bytes()` and `Write::parse()` works as a pair.
        let mut writes = vec![
            Write::new(WriteType::Put, 0.into(), 1.into(), None),
            Write::new(
                WriteType::Delete,
                0.into(),
                1.into(),
                Some(b"value".to_vec()),
            ),
        ];
        for (i, write) in writes.drain(..).enumerate() {
            let v = write.as_ref().to_bytes();
            let w = WriteRef::parse(&v[..])
                .unwrap_or_else(|e| panic!("#{} parse() err: {:?}", i, e))
                .to_owned();
            assert_eq!(w, write, "#{} expect {:?}, but got {:?}", i, write, w);
            assert_eq!(Write::parse_type(&v).unwrap(), w.write_type);
        }

        // Test `Write::parse()` handles incorrect input.
        assert!(WriteRef::parse(b"").is_err());

        let lock = Write::new(WriteType::Lock, 1.into(), 2.into(), Some(b"value".to_vec()));
        let v = lock.as_ref().to_bytes();
        assert!(WriteRef::parse(&v[..1]).is_err());
        assert_eq!(Write::parse_type(&v).unwrap(), lock.write_type);
    }

    #[test]
    fn test_is_protected() {
        assert!(Write::new_rollback(1.into(), true).as_ref().is_protected());
        assert!(!Write::new_rollback(2.into(), false).as_ref().is_protected());
        assert!(!Write::new(
            WriteType::Put,
            3.into(),
            3.into(),
            Some(PROTECTED_ROLLBACK_SHORT_VALUE.to_vec())
        )
        .as_ref()
        .is_protected());
    }
}
