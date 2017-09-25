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
use kvproto::raft_serverpb;
use kvproto::eraftpb;

use raftstore::store::{keys, Engines};
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
        try!(validate_db_and_cf(db, cf));
        let db = match db {
            DB::KV => &self.engines.kv_engine,
            DB::RAFT => &self.engines.raft_engine,
            _ => unreachable!(),
        };
        match db.get_value_cf(cf, key) {
            Ok(Some(v)) => Ok(v.to_vec()),
            Ok(None) => Err(Error::NotFound(
                format!("value for key {:?} in db {:?}", key, db),
            )),
            Err(e) => Err(box_err!(e)),
        }
    }

    pub fn region_info(
        &self,
        region_id: u64,
    ) -> Result<
        (
            Option<raft_serverpb::RaftLocalState>,
            Option<raft_serverpb::RaftApplyState>,
            Option<raft_serverpb::RegionLocalState>,
        ),
    > {
        let raft_state_key = keys::raft_state_key(region_id);
        let raft_state = box_try!(
            self.engines
                .raft_engine
                .get_msg::<raft_serverpb::RaftLocalState>(&raft_state_key)
        );

        let apply_state_key = keys::apply_state_key(region_id);
        let apply_state = box_try!(
            self.engines
                .kv_engine
                .get_msg_cf::<raft_serverpb::RaftApplyState>(CF_RAFT, &apply_state_key)
        );

        let region_state_key = keys::region_state_key(region_id);
        let region_state = box_try!(
            self.engines
                .kv_engine
                .get_msg_cf::<raft_serverpb::RegionLocalState>(CF_RAFT, &region_state_key)
        );

        match (raft_state, apply_state, region_state) {
            (None, None, None) => Err(Error::NotFound(format!("info for region {}", region_id))),
            (raft_state, apply_state, region_state) => Ok((raft_state, apply_state, region_state)),
        }
    }

    pub fn raft_log(&self, region_id: u64, log_index: u64) -> Result<eraftpb::Entry> {
        let key = keys::raft_log_key(region_id, log_index);
        match self.engines.raft_engine.get_msg(&key) {
            Ok(Some(entry)) => Ok(entry),
            Ok(None) => Err(Error::NotFound(format!(
                "raft log for region {} at index {}",
                region_id,
                log_index
            ))),
            Err(e) => Err(box_err!(e)),
        }
    }
}

pub fn validate_db_and_cf(db: DB, cf: &str) -> Result<()> {
    match (db, cf) {
        (DB::KV, CF_DEFAULT) |
        (DB::KV, CF_WRITE) |
        (DB::KV, CF_LOCK) |
        (DB::KV, CF_RAFT) |
        (DB::RAFT, CF_DEFAULT) => Ok(()),
        _ => Err(Error::InvalidArgument(
            format!("invalid cf {:?} for db {:?}", cf, db),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::{ColumnFamilyOptions, DBOptions, Writable};
    use kvproto::debugpb::*;
    use tempdir::TempDir;

    use raftstore::store::engine::Mutable;
    use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use util::rocksdb::{self as rocksdb_util, CFOptions};
    use super::*;

    #[test]
    fn test_validate_db_and_cf() {
        let valid_cases = vec![
            (DB::KV, CF_DEFAULT),
            (DB::KV, CF_WRITE),
            (DB::KV, CF_LOCK),
            (DB::KV, CF_RAFT),
            (DB::RAFT, CF_DEFAULT),
        ];
        for (db, cf) in valid_cases {
            validate_db_and_cf(db, cf).unwrap();
        }

        let invalid_cases = vec![
            (DB::RAFT, CF_WRITE),
            (DB::RAFT, CF_LOCK),
            (DB::RAFT, CF_RAFT),
            (DB::INVALID, CF_DEFAULT),
            (DB::INVALID, "BAD_CF"),
        ];
        for (db, cf) in invalid_cases {
            validate_db_and_cf(db, cf).unwrap_err();
        }
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

    #[test]
    fn test_raft_log() {
        let debugger = new_debugger();
        let engine = &debugger.engines.raft_engine;
        let (region_id, log_index) = (1, 1);
        let key = keys::raft_log_key(region_id, log_index);
        let mut entry = eraftpb::Entry::new();
        entry.set_term(1);
        entry.set_index(1);
        entry.set_entry_type(eraftpb::EntryType::EntryNormal);
        entry.set_data(vec![42]);
        engine.put_msg(key.as_slice(), &entry).unwrap();
        assert_eq!(
            engine
                .get_msg::<eraftpb::Entry>(key.as_slice())
                .unwrap()
                .unwrap(),
            entry
        );

        assert_eq!(debugger.raft_log(region_id, log_index).unwrap(), entry);
        match debugger.raft_log(region_id + 1, log_index + 1) {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }

    #[test]
    fn test_region_info() {
        let debugger = new_debugger();
        let raft_engine = &debugger.engines.raft_engine;
        let kv_engine = &debugger.engines.kv_engine;
        let raft_cf = kv_engine.cf_handle(CF_RAFT).unwrap();
        let region_id = 1;

        let raft_state_key = keys::raft_state_key(region_id);
        let mut raft_state = raft_serverpb::RaftLocalState::new();
        raft_state.set_last_index(42);
        raft_engine.put_msg(&raft_state_key, &raft_state).unwrap();
        assert_eq!(
            raft_engine
                .get_msg::<raft_serverpb::RaftLocalState>(&raft_state_key)
                .unwrap()
                .unwrap(),
            raft_state
        );

        let apply_state_key = keys::apply_state_key(region_id);
        let mut apply_state = raft_serverpb::RaftApplyState::new();
        apply_state.set_applied_index(42);
        kv_engine
            .put_msg_cf(raft_cf, &apply_state_key, &apply_state)
            .unwrap();
        assert_eq!(
            kv_engine
                .get_msg_cf::<raft_serverpb::RaftApplyState>(CF_RAFT, &apply_state_key)
                .unwrap()
                .unwrap(),
            apply_state
        );

        let region_state_key = keys::region_state_key(region_id);
        let mut region_state = raft_serverpb::RegionLocalState::new();
        region_state.set_state(raft_serverpb::PeerState::Tombstone);
        kv_engine
            .put_msg_cf(raft_cf, &region_state_key, &region_state)
            .unwrap();
        assert_eq!(
            kv_engine
                .get_msg_cf::<raft_serverpb::RegionLocalState>(CF_RAFT, &region_state_key)
                .unwrap()
                .unwrap(),
            region_state
        );

        assert_eq!(
            debugger.region_info(region_id).unwrap(),
            (Some(raft_state), Some(apply_state), Some(region_state))
        );
        match debugger.region_info(region_id + 1) {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }
}
