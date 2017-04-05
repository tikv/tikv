// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use byteorder::ReadBytesExt;
use storage::{Mutation, SHORT_VALUE_MAX_LEN, SHORT_VALUE_PREFIX};
use util::codec::number::{NumberEncoder, NumberDecoder, MAX_VAR_U64_LEN};
use util::codec::bytes::{BytesEncoder, CompactBytesDecoder};
use super::{Error, Result};
use super::super::types::Value;

#[derive(Debug,Clone,Copy,PartialEq)]
pub enum LockType {
    Put,
    Delete,
    Lock,
}

const FLAG_PUT: u8 = b'P';
const FLAG_DELETE: u8 = b'D';
const FLAG_LOCK: u8 = b'L';

impl LockType {
    pub fn from_mutation(mutation: &Mutation) -> LockType {
        match *mutation {
            Mutation::Put(_) => LockType::Put,
            Mutation::Delete(_) => LockType::Delete,
            Mutation::Lock(_) => LockType::Lock,
        }
    }

    fn from_u8(b: u8) -> Option<LockType> {
        match b {
            FLAG_PUT => Some(LockType::Put),
            FLAG_DELETE => Some(LockType::Delete),
            FLAG_LOCK => Some(LockType::Lock),
            _ => None,
        }
    }

    fn to_u8(&self) -> u8 {
        match *self {
            LockType::Put => FLAG_PUT,
            LockType::Delete => FLAG_DELETE,
            LockType::Lock => FLAG_LOCK,
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
}

impl Lock {
    pub fn new(lock_type: LockType,
               primary: Vec<u8>,
               ts: u64,
               ttl: u64,
               short_value: Option<Value>)
               -> Lock {
        Lock {
            lock_type: lock_type,
            primary: primary,
            ts: ts,
            ttl: ttl,
            short_value: short_value,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(1 + MAX_VAR_U64_LEN + self.primary.len() + MAX_VAR_U64_LEN +
                                       SHORT_VALUE_MAX_LEN +
                                       2);
        b.push(self.lock_type.to_u8());
        b.encode_compact_bytes(&self.primary).unwrap();
        b.encode_var_u64(self.ts).unwrap();
        b.encode_var_u64(self.ttl).unwrap();
        if let Some(ref v) = self.short_value {
            b.push(SHORT_VALUE_PREFIX);
            b.push(v.len() as u8);
            b.extend_from_slice(v);
        }
        b
    }

    pub fn parse(mut b: &[u8]) -> Result<Lock> {
        if b.is_empty() {
            return Err(Error::BadFormatLock);
        }
        let lock_type = try!(LockType::from_u8(try!(b.read_u8())).ok_or(Error::BadFormatLock));
        let primary = try!(b.decode_compact_bytes());
        let ts = try!(b.decode_var_u64());
        let ttl = if b.is_empty() {
            0
        } else {
            try!(b.decode_var_u64())
        };

        if b.is_empty() {
            return Ok(Lock::new(lock_type, primary, ts, ttl, None));
        }

        let flag = try!(b.read_u8());
        assert_eq!(flag,
                   SHORT_VALUE_PREFIX,
                   "invalid flag [{:?}] in write",
                   flag);

        let len = try!(b.read_u8());
        if len as usize != b.len() {
            panic!("short value len [{}] not equal to content len [{}]",
                   len,
                   b.len());
        }

        Ok(Lock::new(lock_type, primary, ts, ttl, Some(b.to_vec())))
    }
}

#[cfg(test)]
mod tests {
    use storage::{make_key, Mutation};
    use super::*;

    #[test]
    fn test_lock_type() {
        let (key, value) = (b"key", b"value");
        let mut tests =
            vec![(Mutation::Put((make_key(key), value.to_vec())), LockType::Put, FLAG_PUT),
                 (Mutation::Delete(make_key(key)), LockType::Delete, FLAG_DELETE),
                 (Mutation::Lock(make_key(key)), LockType::Lock, FLAG_LOCK)];
        for (i, (mutation, lock_type, flag)) in tests.drain(..).enumerate() {
            let lt = LockType::from_mutation(&mutation);
            if lt != lock_type {
                panic!("#{}, expect from_mutation({:?}) returns {:?}, but got {:?}",
                       i,
                       mutation,
                       lock_type,
                       lt);
            }
            let f = lock_type.to_u8();
            if f != flag {
                panic!("#{}, expect {:?}.to_u8() returns {:?}, but got {:?}",
                       i,
                       lock_type,
                       flag,
                       f);
            }
            let lt = LockType::from_u8(flag).unwrap();
            if lt != lock_type {
                panic!("#{}, expect from_u8({:?}) returns {:?}, but got {:?})",
                       i,
                       flag,
                       lock_type,
                       lt);
            }
        }
    }

    #[test]
    fn test_lock() {
        let mut locks = vec![Lock::new(LockType::Put, b"pk".to_vec(), 1, 10, None),
                             Lock::new(LockType::Delete,
                                       b"pk".to_vec(),
                                       1,
                                       10,
                                       Some(b"short".to_vec()))];
        for (i, lock) in locks.drain(..).enumerate() {
            let v = lock.to_bytes();
            match Lock::parse(&v[..]) {
                Ok(l) => {
                    if l != lock {
                        panic!("#{} expect {:?}, but got {:?}", i, lock, l);
                    }
                }
                Err(e) => {
                    panic!("#{} parse() err: {:?}", i, e);
                }
            }
        }
    }
}
