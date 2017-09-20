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
use rocksdb::DB as RocksDb;

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

    pub fn get(&self, db: DB, cf: &str, key: &[u8]) -> Result<Vec<u8>> {
        let db = try!(self.get_db(db));
        let cf = try!(validate_cf(cf));
        match db.get_value_cf(cf, key) {
            Ok(Some(v)) => Ok((&v).to_vec()),
            Ok(None) => Err(Error::NotFound(
                format!("get none value for key {:?}", key,),
            )),
            Err(e) => Err(box_err!(e)),
        }
    }

    fn get_db(&self, db: DB) -> Result<&RocksDb> {
        match db {
            DB::KV => Ok(&self.engines.kv_engine),
            DB::RAFT => Ok(&self.engines.raft_engine),
            _ => Err(Error::InvalidArgument("invalid db".to_owned())),
        }
    }
}

pub fn validate_cf(cf: &str) -> Result<&str> {
    match cf {
        CF_DEFAULT | CF_WRITE | CF_LOCK | CF_RAFT => Ok(cf),
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
    fn test_validate_cf() {
        let cases = vec![CF_DEFAULT, CF_WRITE, CF_LOCK, CF_RAFT];
        for cf in cases {
            assert_eq!(validate_cf(cf).unwrap(), cf);
        }

        validate_cf("foo").unwrap_err();
    }

    fn new_debugger() -> Debugger {
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
        let debugger = new_debugger();
        let cases = vec![DB::KV, DB::RAFT];
        for db in cases {
            debugger.get_db(db).unwrap();
        }
        debugger.get_db(DB::INVALID).unwrap_err();
    }

    #[test]
    fn test_get() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv_engine;
        let (k, v) = (b"k", b"v");
        engine.put(k, v).unwrap();
        assert_eq!(&*engine.get(k).unwrap().unwrap(), v);

        assert_eq!(debugger.get(DB::KV, CF_DEFAULT, k).unwrap().as_slice(), v);
        match debugger.get(DB::KV, CF_DEFAULT, b"foo") {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }
}
