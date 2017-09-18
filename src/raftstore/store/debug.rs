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

use kvproto::debugpb::*;
use rocksdb::DB;

use raftstore::store::keys;
use raftstore::store::Engines;
use raftstore::store::engine::Peekable;
use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};

use super::super::errors::Result;

pub fn cf_to_str(cf: CF) -> Result<&'static str> {
    match cf {
        CF::DEFAULT => Ok(CF_DEFAULT),
        CF::WRITE => Ok(CF_WRITE),
        CF::LOCK => Ok(CF_LOCK),
        CF::RAFT => Ok(CF_RAFT),
        _ => Err(box_err!("invalid cf".to_owned())),
    }
}

pub fn get_db<'a>(engines: &'a Engines, cf: &str) -> Result<&'a DB> {
    match cf {
        CF_DEFAULT | CF_WRITE | CF_LOCK => Ok(&engines.kv_engine),
        CF_RAFT => Ok(&engines.raft_engine),
        _ => Err(box_err!("invalid cf".to_owned())),
    }
}

pub fn get_value(db: &DB, cf: &str, key_encoded: &[u8]) -> Result<Option<Vec<u8>>> {
    match db.get_value_cf(cf, &keys::data_key(key_encoded)) {
        Ok(Some(v)) => Ok(Some((&v).to_vec())),
        Ok(None) => Ok(None),
        Err(e) => Err(box_err!(e)),
    }
}
