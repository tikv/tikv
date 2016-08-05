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
use storage::Mutation;
use util::codec::number::{NumberEncoder, NumberDecoder};
use util::codec::bytes::{BytesEncoder, CompactBytesDecoder};
use super::{Error, Result};

#[derive(Debug,Clone,Copy)]
pub enum LockType {
    Put,
    Delete,
    Lock,
}

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
            b'P' => Some(LockType::Put),
            b'D' => Some(LockType::Delete),
            b'L' => Some(LockType::Lock),
            _ => None,
        }
    }

    fn to_u8(&self) -> u8 {
        match *self {
            LockType::Put => b'P',
            LockType::Delete => b'D',
            LockType::Lock => b'L',
        }
    }
}

pub struct Lock {
    pub lock_type: LockType,
    pub primary: Vec<u8>,
    pub ts: u64,
}

impl Lock {
    pub fn new(lock_type: LockType, primary: Vec<u8>, ts: u64) -> Lock {
        Lock {
            lock_type: lock_type,
            primary: primary,
            ts: ts,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = vec![];
        b.push(self.lock_type.to_u8());
        b.encode_compact_bytes(&self.primary).unwrap();
        b.encode_var_u64(self.ts).unwrap();
        b
    }

    pub fn parse(mut b: &[u8]) -> Result<Lock> {
        if b.len() == 0 {
            return Err(Error::BadFormatLock);
        }
        let lock_type = try!(LockType::from_u8(try!(b.read_u8())).ok_or(Error::BadFormatLock));
        let primary = try!(b.decode_compact_bytes());
        let ts = try!(b.decode_var_u64());
        Ok(Lock::new(lock_type, primary, ts))
    }
}
