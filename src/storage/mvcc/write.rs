// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use super::super::types::Value;
use super::lock::LockType;
use crate::storage::{SHORT_VALUE_MAX_LEN, SHORT_VALUE_PREFIX};
use byteorder::ReadBytesExt;
use tikv_util::codec::number::{self, NumberEncoder, MAX_VAR_U64_LEN};
use std::io;

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
    pub start_ts: u64,
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
    pub fn new(write_type: WriteType, start_ts: u64, short_value: Option<Value>) -> Write {
        Write {
            write_type,
            start_ts,
            short_value,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(1 + MAX_VAR_U64_LEN + SHORT_VALUE_MAX_LEN + 2);
        b.push(self.write_type.to_u8());
        b.encode_var_u64(self.start_ts).unwrap();
        if let Some(ref v) = self.short_value {
            b.push(SHORT_VALUE_PREFIX);
            b.push(v.len() as u8);
            b.extend_from_slice(v);
        }
        b
    }

    pub fn parse(mut b: &[u8]) -> Result<Write> {
        if b.is_empty() {
            return Err(Error::BadFormatWrite);
        }
        let write_type = WriteType::from_u8(b.read_u8()?).ok_or(Error::BadFormatWrite)?;
        let start_ts = number::decode_var_u64(&mut b)?;
        if b.is_empty() {
            return Ok(Write::new(write_type, start_ts, None));
        }

        let flag = b.read_u8()?;
        assert_eq!(flag, SHORT_VALUE_PREFIX, "invalid flag [{}] in write", flag);

        let len = b.read_u8()?;
        if len as usize != b.len() {
            panic!(
                "short value len [{}] not equal to content len [{}]",
                len,
                b.len()
            );
        }
        Ok(Write::new(write_type, start_ts, Some(b.to_vec())))
    }

    pub fn parse_type(mut b: &[u8]) -> Result<WriteType> {
        WriteType::from_u8(b.read_u8()?).ok_or(Error::BadFormatWrite)
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
        BadFormatWrite { description("bad format write data") }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

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
            Write::new(WriteType::Put, 0, None),
            Write::new(WriteType::Delete, 0, Some(b"short_value".to_vec())),
        ];
        for (i, write) in writes.drain(..).enumerate() {
            let v = write.to_bytes();
            let w = Write::parse(&v[..]).unwrap_or_else(|e| panic!("#{} parse() err: {:?}", i, e));
            assert_eq!(w, write, "#{} expect {:?}, but got {:?}", i, write, w);
            assert_eq!(Write::parse_type(&v).unwrap(), w.write_type);
        }

        // Test `Write::parse()` handles incorrect input.
        assert!(Write::parse(b"").is_err());

        let lock = Write::new(WriteType::Lock, 1, Some(b"short_value".to_vec()));
        let v = lock.to_bytes();
        assert!(Write::parse(&v[..1]).is_err());
        assert_eq!(Write::parse_type(&v).unwrap(), lock.write_type);
    }
}
