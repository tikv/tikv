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
pub struct Debugger {
    engines: Engines,
}

impl Debugger {
    pub fn new(engines: Engines) -> Debugger {
        Debugger { engines }
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
            CF::DEFAULT | CF::WRITE | CF::LOCK | CF::RAFT => Ok(&self.engines.kv_engine),
            _ => Err(Error::InvalidArgument("invalid cf".to_owned())),
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::{ColumnFamilyOptions, DBOptions, Writable};
    use kvproto::debugpb::*;
    use tempdir::TempDir;

    use util::rocksdb::{self as rocksdb_util, CFOptions};
    use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use super::*;

    #[test]
    fn test_cf_to_str() {
        let cases = vec![
            (CF::DEFAULT, CF_DEFAULT),
            (CF::WRITE, CF_WRITE),
            (CF::LOCK, CF_LOCK),
            (CF::RAFT, CF_RAFT),
        ];
        for (cf, s) in cases {
            assert_eq!(cf_to_str(cf).unwrap(), s);
        }

        cf_to_str(CF::INVALID).unwrap_err();
    }

    fn new_debug() -> Debugger {
        let tmp = TempDir::new("test_debug").unwrap();
        let path = tmp.path().to_str().unwrap();
        let engine = Arc::new(
            rocksdb_util::new_engine_opt(
                path,
                DBOptions::new(),
                vec![
                    CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_WRITE, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_LOCK, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_RAFT, ColumnFamilyOptions::new()),
                ],
            ).unwrap(),
        );

        let engines = Engines::new(engine.clone(), engine);
        Debugger::new(engines)
    }

    #[test]
    fn test_get_db() {
        let debug = new_debug();
        let cases = vec![CF::DEFAULT, CF::WRITE, CF::LOCK, CF::RAFT];
        for cf in cases {
            debug.get_db(cf).unwrap();
        }
        debug.get_db(CF::INVALID).unwrap_err();
    }

    #[test]
    fn test_get() {
        let debug = new_debug();
        let engine = &debug.engines.kv_engine;
        let (k, v) = (b"k", b"v");
        engine.put(keys::data_key(k).as_slice(), v).unwrap();
        assert_eq!(
            &*engine.get(keys::data_key(k).as_slice()).unwrap().unwrap(),
            v
        );

        assert_eq!(debug.get(CF::DEFAULT, k).unwrap().as_slice(), v);
        match debug.get(CF::DEFAULT, keys::data_key(k).as_slice()) {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }
}
