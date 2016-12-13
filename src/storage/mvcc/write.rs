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
use util::codec::number::{NumberEncoder, NumberDecoder, MAX_VAR_U64_LEN};
use storage::{SHORT_VALUE_PREFIX, SHORT_VALUE_MAX_LEN};
use super::lock::LockType;
use super::{Error, Result};
use super::super::types::Value;

#[derive(Debug,Clone,Copy,PartialEq)]
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
    pub fn from_lock_type(tp: LockType) -> WriteType {
        match tp {
            LockType::Put => WriteType::Put,
            LockType::Delete => WriteType::Delete,
            LockType::Lock => WriteType::Lock,
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

    fn to_u8(&self) -> u8 {
        match *self {
            WriteType::Put => FLAG_PUT,
            WriteType::Delete => FLAG_DELETE,
            WriteType::Lock => FLAG_LOCK,
            WriteType::Rollback => FLAG_ROLLBACK,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Write {
    pub write_type: WriteType,
    pub start_ts: u64,
    pub short_value: Option<Value>,
}

impl Write {
    pub fn new(write_type: WriteType, start_ts: u64, short_value: Option<Value>) -> Write {
        Write {
            write_type: write_type,
            start_ts: start_ts,
            short_value: short_value,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(1 + MAX_VAR_U64_LEN + SHORT_VALUE_MAX_LEN);
        b.push(self.write_type.to_u8());
        b.encode_var_u64(self.start_ts).unwrap();
        if let Some(ref v) = self.short_value {
            b.push(SHORT_VALUE_PREFIX);
            b.extend_from_slice(v);
        }
        b
    }

    pub fn parse(mut b: &[u8]) -> Result<Write> {
        if b.len() == 0 {
            return Err(Error::BadFormatWrite);
        }
        let write_type = try!(WriteType::from_u8(try!(b.read_u8())).ok_or(Error::BadFormatWrite));
        let start_ts = try!(b.decode_var_u64());
        let short_value = if b.len() > 0 {
            if try!(b.read_u8()) == SHORT_VALUE_PREFIX {
                Some(b.to_vec())
            } else {
                panic!("invalid write");
            }
        } else {
            None
        };
        Ok(Write::new(write_type, start_ts, short_value))
    }
}
