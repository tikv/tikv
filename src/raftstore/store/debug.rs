// Copyright 2017 PingCAP, Inc.
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

use std::{error, result};
use kvproto::debugpb::*;
use rocksdb::DB;

use raftstore::store::keys;
use raftstore::store::Engines;
use raftstore::store::engine::Peekable;
use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};

pub type Result<T> = result::Result<T, Error>;

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        InvalidArgument(msg: String) {
            description(msg)
            display("Invalid Argument {:?}", msg)
        }
        NotFound(msg: String) {
            description(msg)
            display("Not Found {:?}", msg)
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
    }
}

#[derive(Clone)]
pub struct Debug {
    engines: Engines,
}

impl Debug {
    pub fn new(engines: Engines) -> Debug {
        Debug { engines }
    }

    pub fn get(&self, cf: CF, key_encoded: &[u8]) -> Result<Vec<u8>> {
        let db = try!(self.get_db(cf));
        let cf = try!(cf_to_str(cf));
        match db.get_value_cf(cf, &keys::data_key(key_encoded)) {
            Ok(Some(v)) => Ok((&v).to_vec()),
            Ok(None) => Err(Error::NotFound(
                format!("get none value for encoded key {:?}", key_encoded,),
            )),
            Err(e) => Err(box_err!(e)),
        }
    }

    fn get_db(&self, cf: CF) -> Result<&DB> {
        match cf {
            CF::DEFAULT | CF::WRITE | CF::LOCK => Ok(&self.engines.kv_engine),
            CF::RAFT => Ok(&self.engines.raft_engine),
            _ => Err(Error::InvalidArgument("invalid cf".to_owned())),
        }
    }
}

pub fn str_to_cf(cf: &str) -> Result<CF> {
    match cf {
        CF_DEFAULT => Ok(CF::DEFAULT),
        CF_WRITE => Ok(CF::WRITE),
        CF_LOCK => Ok(CF::LOCK),
        CF_RAFT => Ok(CF::RAFT),
        _ => Err(Error::InvalidArgument("invalid cf".to_owned())),
    }
}

pub fn cf_to_str(cf: CF) -> Result<&'static str> {
    match cf {
        CF::DEFAULT => Ok(CF_DEFAULT),
        CF::WRITE => Ok(CF_WRITE),
        CF::LOCK => Ok(CF_LOCK),
        CF::RAFT => Ok(CF_RAFT),
        _ => Err(Error::InvalidArgument("invalid cf".to_owned())),
    }
}
