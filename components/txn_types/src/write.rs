// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crate::lock::LockType;
use crate::timestamp::TimeStamp;
use crate::types::{Value, SHORT_VALUE_MAX_LEN, SHORT_VALUE_PREFIX};
use crate::{Error, Result};
use codec::prelude::NumberDecoder;
use tikv_util::codec::number::{NumberEncoder, MAX_VAR_U64_LEN};

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
    pub short_value: Option<Value>,
}

impl std::fmt::Debug for Write {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Write")
            .field("write_type", &self.write_type)
            .field("start_ts", &self.start_ts)
            .field(
                "short_value",
                &self
                    .short_value
                    .as_ref()
                    .map(|v| hex::encode_upper(v))
                    .unwrap_or_else(|| "None".to_owned()),
            )
            .finish()
    }
}

impl Write {
    /// Creates a new `Write` record.
    #[inline]
    pub fn new(write_type: WriteType, start_ts: TimeStamp, short_value: Option<Value>) -> Write {
        Write {
            write_type,
            start_ts,
            short_value,
        }
    }

    #[inline]
    pub fn new_rollback(start_ts: TimeStamp, protected: bool) -> Write {
        let short_value = if protected {
            Some(PROTECTED_ROLLBACK_SHORT_VALUE.to_vec())
        } else {
            None
        };

        Write {
            write_type: WriteType::Rollback,
            start_ts,
            short_value,
        }
    }

    #[inline]
    pub fn parse_type(mut b: &[u8]) -> Result<WriteType> {
        let write_type_bytes = b
            .read_u8()
            .map_err(|_| Error::from(Error::BadFormatWrite))?;
        WriteType::from_u8(write_type_bytes).ok_or_else(|| Error::from(Error::BadFormatWrite))
    }

    #[inline]
    pub fn as_ref(&self) -> WriteRef<'_> {
        WriteRef {
            write_type: self.write_type,
            start_ts: self.start_ts,
            short_value: self.short_value.as_ref().map(|v| v.as_slice()),
        }
    }
}

#[derive(PartialEq, Clone)]
pub struct WriteRef<'a> {
    pub write_type: WriteType,
    pub start_ts: TimeStamp,
    pub short_value: Option<&'a [u8]>,
}

impl WriteRef<'_> {
    pub fn parse(mut b: &[u8]) -> Result<WriteRef<'_>> {
        let write_type_bytes = b
            .read_u8()
            .map_err(|_| Error::from(Error::BadFormatWrite))?;
        let write_type = WriteType::from_u8(write_type_bytes)
            .ok_or_else(|| Error::from(Error::BadFormatWrite))?;
        let start_ts = b
            .read_var_u64()
            .map_err(|_| Error::from(Error::BadFormatWrite))?
            .into();
        if b.is_empty() {
            return Ok(WriteRef {
                write_type,
                start_ts,
                short_value: None,
            });
        }

        let flag = b
            .read_u8()
            .map_err(|_| Error::from(Error::BadFormatWrite))?;
        assert_eq!(flag, SHORT_VALUE_PREFIX, "invalid flag [{}] in write", flag);

        let len = b
            .read_u8()
            .map_err(|_| Error::from(Error::BadFormatWrite))?;
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
            short_value: Some(b),
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(1 + MAX_VAR_U64_LEN + SHORT_VALUE_MAX_LEN + 2);
        b.push(self.write_type.to_u8());
        b.encode_var_u64(self.start_ts.into_inner()).unwrap();
        if let Some(v) = self.short_value {
            b.push(SHORT_VALUE_PREFIX);
            b.push(v.len() as u8);
            b.extend_from_slice(v);
        }
        b
    }

    #[inline]
    pub fn is_protected(&self) -> bool {
        self.write_type == WriteType::Rollback
            && self
                .short_value
                .as_ref()
                .map(|v| *v == PROTECTED_ROLLBACK_SHORT_VALUE)
                .unwrap_or_default()
    }

    #[inline]
    pub fn to_owned(&self) -> Write {
        Write::new(
            self.write_type,
            self.start_ts,
            self.short_value.map(|v| v.to_owned()),
        )
    }
}

#[cfg(test)]
mod tests {
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
            Write::new(WriteType::Put, 0.into(), Some(b"short_value".to_vec())),
            Write::new(WriteType::Delete, (1 << 20).into(), None),
            Write::new_rollback((1 << 40).into(), true),
            Write::new(WriteType::Rollback, (1 << 41).into(), None),
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

        let lock = Write::new(WriteType::Lock, 1.into(), Some(b"short_value".to_vec()));
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
            Some(PROTECTED_ROLLBACK_SHORT_VALUE.to_vec())
        )
        .as_ref()
        .is_protected());
    }
}
