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

use std::{error, mem, result};

use protobuf::RepeatedField;

use rocksdb::{DBIterator, Kv, SeekKey, DB};
use kvproto::kvrpcpb::{LockInfo, MvccInfo, Op, ValueInfo, WriteInfo};
use kvproto::debugpb::DB as DBType;
use kvproto::{eraftpb, raft_serverpb};

use raftstore::store::{keys, Engines, Iterable, Peekable};
use raftstore::store::engine::IterOption;
use storage::{is_short_value, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use storage::types::Key;
use storage::mvcc::{Lock, Write, WriteType};
use util::rocksdb::{compact_range, get_cf_handle};

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

    fn get_db_from_type(&self, db: DBType) -> Result<&DB> {
        match db {
            DBType::KV => Ok(&self.engines.kv_engine),
            DBType::RAFT => Ok(&self.engines.raft_engine),
            _ => Err(box_err!("invalid DBType type")),
        }
    }

    pub fn get(&self, db: DBType, cf: &str, key: &[u8]) -> Result<Vec<u8>> {
        validate_db_and_cf(db, cf)?;
        let db = self.get_db_from_type(db)?;
        match db.get_value_cf(cf, key) {
            Ok(Some(v)) => Ok(v.to_vec()),
            Ok(None) => Err(Error::NotFound(
                format!("value for key {:?} in db {:?}", key, db),
            )),
            Err(e) => Err(box_err!(e)),
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

    pub fn region_size<T: AsRef<str>>(
        &self,
        region_id: u64,
        cfs: Vec<T>,
    ) -> Result<Vec<(T, usize)>> {
        let region_state_key = keys::region_state_key(region_id);
        match self.engines
            .kv_engine
            .get_msg_cf::<raft_serverpb::RegionLocalState>(CF_RAFT, &region_state_key)
        {
            Ok(Some(region_state)) => {
                let region = region_state.get_region();
                let start_key = &keys::data_key(region.get_start_key());
                let end_key = &keys::data_end_key(region.get_end_key());
                let mut sizes = vec![];
                for cf in cfs {
                    let mut size = 0;
                    box_try!(self.engines.kv_engine.scan_cf(
                        cf.as_ref(),
                        start_key,
                        end_key,
                        false,
                        &mut |_, v| {
                            size += v.len();
                            Ok(true)
                        }
                    ));
                    sizes.push((cf, size));
                }
                Ok(sizes)
            }
            Ok(None) => Err(Error::NotFound(format!("none region {:?}", region_id))),
            Err(e) => Err(box_err!(e)),
        }
    }

    pub fn scan_mvcc(&self, start: &[u8], end: &[u8], limit: u64) -> Result<MvccInfoIterator> {
        if end.is_empty() && limit == 0 {
            return Err(Error::InvalidArgument("no limit and to_key".to_owned()));
        }
        MvccInfoIterator::new(&self.engines.kv_engine, start, end, limit)
    }

    /// Compact the cf[start..end) in the db.
    pub fn compact(&self, db: DBType, cf: &str, start: &[u8], end: &[u8]) -> Result<()> {
        validate_db_and_cf(db, cf)?;
        let db = self.get_db_from_type(db)?;
        let handle = box_try!(get_cf_handle(db, cf));
        let start = if start.is_empty() { None } else { Some(start) };
        let end = if end.is_empty() { None } else { Some(end) };
        compact_range(db, handle, start, end, false);
        Ok(())
    }
}

pub struct MvccInfoIterator<'a> {
    limit: u64,

    lock_iter: DBIterator<'a>,
    default_iter: DBIterator<'a>,
    write_iter: DBIterator<'a>,

    // Current key and value vector when poping default and write.
    default_cur: (Vec<u8>, Vec<Kv>),
    write_cur: (Vec<u8>, Vec<Kv>),

    // Collect key-prefix and MvccInfo respectively.
    // The indices in arrays: 0 for lock, 1 for default and 2 for write.
    mvcc_infos: Vec<(Vec<u8>, MvccInfo, [bool; 3])>,
    count: u64,

    // 3 iter children are finished or not.
    finished: [bool; 3],
    // keys yield by 3 iter children latest.
    cur_keys: [Option<Vec<u8>>; 3],
}

impl<'a> MvccInfoIterator<'a> {
    fn new(db: &'a DB, from: &[u8], to: &[u8], limit: u64) -> Result<Self> {
        let gen_iter = |cf: &str| -> Result<_> {
            let to = if to.is_empty() { None } else { Some(to) };
            let iter_option = IterOption::new(to.map(Vec::from), false);
            let mut iter = box_try!(db.new_iterator_cf(cf, iter_option));
            iter.seek(SeekKey::from(from));
            Ok(iter)
        };
        Ok(MvccInfoIterator {
            limit: limit,
            lock_iter: try!(gen_iter(CF_LOCK)),
            default_iter: try!(gen_iter(CF_DEFAULT)),
            write_iter: try!(gen_iter(CF_WRITE)),
            default_cur: (Vec::new(), Vec::new()),
            write_cur: (Vec::new(), Vec::new()),
            mvcc_infos: Vec::new(),
            count: 0,
            finished: [false; 3],
            cur_keys: [None, None, None],
        })
    }

    fn next_lock(&mut self) -> Result<Option<(Vec<u8>, LockInfo)>> {
        let mut iter = &mut self.lock_iter;
        if let Some((key, value)) = <&mut DBIterator as Iterator>::next(&mut iter) {
            let lock = box_try!(Lock::parse(&value));
            let mut lock_info = LockInfo::default();
            lock_info.set_primary_lock(lock.primary);
            lock_info.set_lock_version(lock.ts);
            lock_info.set_lock_ttl(lock.ttl);
            lock_info.set_key(key.clone());
            return Ok(Some((key, lock_info)));
        };
        Ok(None)
    }

    fn next_default(&mut self) -> Result<Option<(Vec<u8>, Vec<ValueInfo>)>> {
        let mut iter = &mut self.default_iter;
        let next = try!(Self::next_grouped(&mut iter, &mut self.default_cur));
        if let Some((prefix, vec_kv)) = next {
            let mut values = Vec::with_capacity(vec_kv.len());
            for (key, value) in vec_kv {
                let mut value_info = ValueInfo::default();
                value_info.set_is_short_value(is_short_value(&value));
                value_info.set_value(value);
                let encoded_key = Key::from_encoded(keys::origin_key(&key).to_owned());
                value_info.set_ts(box_try!(encoded_key.decode_ts()));
                values.push(value_info);
            }
            return Ok(Some((prefix, values)));
        }
        Ok(None)
    }

    fn next_write(&mut self) -> Result<Option<(Vec<u8>, Vec<WriteInfo>)>> {
        let mut iter = &mut self.write_iter;
        let next = try!(Self::next_grouped(&mut iter, &mut self.write_cur));
        if let Some((prefix, vec_kv)) = next {
            let mut writes = Vec::with_capacity(vec_kv.len());
            for (key, value) in vec_kv {
                let write = box_try!(Write::parse(&value));
                let mut write_info = WriteInfo::default();
                write_info.set_start_ts(write.start_ts);
                match write.write_type {
                    WriteType::Put => write_info.set_field_type(Op::Put),
                    WriteType::Delete => write_info.set_field_type(Op::Del),
                    WriteType::Lock => write_info.set_field_type(Op::Lock),
                    WriteType::Rollback => write_info.set_field_type(Op::Rollback),
                }
                let encoded_key = Key::from_encoded(keys::origin_key(&key).to_owned());
                write_info.set_commit_ts(box_try!(encoded_key.decode_ts()));
                writes.push(write_info);
            }
            return Ok(Some((prefix, writes)));
        }
        Ok(None)
    }

    fn next_grouped(
        mut iter: &mut DBIterator,
        cur: &mut (Vec<u8>, Vec<Kv>),
    ) -> Result<Option<(Vec<u8>, Vec<Kv>)>> {
        let (mut cur_prefix, mut cur_kv) = (Vec::new(), Vec::new());
        mem::swap(&mut cur_prefix, &mut cur.0);
        mem::swap(&mut cur_kv, &mut cur.1);
        while let Some((key, value)) = <&mut DBIterator as Iterator>::next(&mut iter) {
            if !cur_prefix.is_empty() {
                if key.starts_with(&cur_prefix) {
                    cur_kv.push((key, value));
                } else {
                    let new_prefix = box_try!(truncate_data_key(&key));
                    let new_kv = vec![(key, value)];
                    *cur = (new_prefix, new_kv);
                    break;
                }
            } else {
                cur_prefix = box_try!(truncate_data_key(&key));
                cur_kv.push((key, value));
            }
        }
        if !cur_prefix.is_empty() {
            Ok(Some((cur_prefix, cur_kv)))
        } else {
            Ok(None)
        }
    }

    fn collect<F: FnOnce(&mut MvccInfo)>(&mut self, key: Vec<u8>, i: usize, update: F) {
        let occupied = self.mvcc_infos
            .iter()
            .enumerate()
            .find(|&(_, elem)| elem.0 == key)
            .map(|(i, _)| i);
        if let Some(offset) = occupied {
            update(&mut self.mvcc_infos[offset].1);
            self.mvcc_infos[offset].2[i] = true;
        } else {
            let mut mvcc_info = (key, MvccInfo::default(), [false; 3]);
            update(&mut mvcc_info.1);
            mvcc_info.2[i] = true;
            self.mvcc_infos.push(mvcc_info);
        }
    }

    fn pull(&mut self) -> Result<()> {
        if !self.finished[0] && self.cur_keys[0].is_none() {
            if let Some((prefix, lock)) = try!(self.next_lock()) {
                self.collect(prefix.clone(), 0, move |mvcc| mvcc.set_lock(lock));
                self.cur_keys[0] = Some(prefix);
            } else {
                self.finished[0] = true;
            }
        }
        if !self.finished[1] && self.cur_keys[1].is_none() {
            if let Some((prefix, values)) = try!(self.next_default()) {
                let values = RepeatedField::from_vec(values);
                self.collect(prefix.clone(), 1, move |mvcc| mvcc.set_values(values));
                self.cur_keys[1] = Some(prefix);
            } else {
                self.finished[1] = true;
            }
        }
        if !self.finished[2] && self.cur_keys[2].is_none() {
            if let Some((prefix, writes)) = try!(self.next_write()) {
                let writes = RepeatedField::from_vec(writes);
                self.collect(prefix.clone(), 2, move |mvcc| mvcc.set_writes(writes));
                self.cur_keys[2] = Some(prefix);
            } else {
                self.finished[2] = true;
            }
        }
        self.mvcc_infos.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(())
    }
}

impl<'a> Iterator for MvccInfoIterator<'a> {
    type Item = Result<(Vec<u8>, MvccInfo)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.limit != 0 && self.count >= self.limit {
            return None;
        }
        if let Err(e) = self.pull() {
            return Some(Err(e));
        }
        if !self.mvcc_infos.is_empty() {
            let (key, mvcc_info, gots) = self.mvcc_infos.remove(0);
            for (i, got) in gots.iter().enumerate() {
                if *got && self.cur_keys[i].as_ref().map(Vec::as_slice) == Some(&key) {
                    self.cur_keys[i] = None;
                }
            }
            self.count += 1;
            return Some(Ok((key, mvcc_info)));
        }
        if !self.finished.iter().all(|t| *t) || !self.cur_keys.iter().all(|t| t.is_none()) {
            return Some(Err(box_err!("MvccInfoIterator fail, it's a bug")));
        }
        None
    }
}

fn truncate_data_key(data_key: &[u8]) -> Result<Vec<u8>> {
    let k = Key::from_encoded(keys::origin_key(data_key).to_owned());
    let k = box_try!(k.truncate_ts());
    Ok(keys::data_key(k.encoded()))
}

pub fn validate_db_and_cf(db: DBType, cf: &str) -> Result<()> {
    match (db, cf) {
        (DBType::KV, CF_DEFAULT) |
        (DBType::KV, CF_WRITE) |
        (DBType::KV, CF_LOCK) |
        (DBType::KV, CF_RAFT) |
        (DBType::RAFT, CF_DEFAULT) => Ok(()),
        _ => Err(Error::InvalidArgument(
            format!("invalid cf {:?} for db {:?}", cf, db),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::{ColumnFamilyOptions, DBOptions, Writable};
    use kvproto::metapb;
    use tempdir::TempDir;

    use raftstore::store::engine::Mutable;
    use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use storage::mvcc::{Lock, LockType};
    use util::rocksdb::{self as rocksdb_util, CFOptions};
    use super::*;

    #[test]
    fn test_validate_db_and_cf() {
        let valid_cases = vec![
            (DBType::KV, CF_DEFAULT),
            (DBType::KV, CF_WRITE),
            (DBType::KV, CF_LOCK),
            (DBType::KV, CF_RAFT),
            (DBType::RAFT, CF_DEFAULT),
        ];
        for (db, cf) in valid_cases {
            validate_db_and_cf(db, cf).unwrap();
        }

        let invalid_cases = vec![
            (DBType::RAFT, CF_WRITE),
            (DBType::RAFT, CF_LOCK),
            (DBType::RAFT, CF_RAFT),
            (DBType::INVALID, CF_DEFAULT),
            (DBType::INVALID, "BAD_CF"),
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

        let got = debugger.get(DBType::KV, CF_DEFAULT, k).unwrap();
        assert_eq!(&got, v);

        match debugger.get(DBType::KV, CF_DEFAULT, b"foo") {
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


    #[test]
    fn test_region_size() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv_engine;

        let region_id = 1;
        let region_state_key = keys::region_state_key(region_id);
        let mut region = metapb::Region::new();
        region.set_id(region_id);
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"zz".to_vec());
        let mut state = raft_serverpb::RegionLocalState::new();
        state.set_region(region);
        let cf_raft = engine.cf_handle(CF_RAFT).unwrap();
        engine
            .put_msg_cf(cf_raft, &region_state_key, &state)
            .unwrap();

        let cfs = vec![CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE];
        let (k, v) = (keys::data_key(b"k"), b"v");
        for cf in &cfs {
            let cf_handle = engine.cf_handle(cf).unwrap();
            engine.put_cf(cf_handle, k.as_slice(), v).unwrap();
        }

        let sizes = debugger.region_size(region_id, cfs.clone()).unwrap();
        assert_eq!(sizes.len(), 4);
        for (cf, size) in sizes {
            cfs.iter().find(|&&c| c == cf).unwrap();
            assert!(size > 0);
        }
    }

    #[test]
    fn test_scan_mvcc() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv_engine;

        let cf_default_data = vec![(b"k1", b"v", 5), (b"k2", b"x", 10), (b"k3", b"y", 15)];
        for &(prefix, value, ts) in &cf_default_data {
            let encoded_key = Key::from_raw(prefix).append_ts(ts);
            let key = keys::data_key(encoded_key.encoded().as_slice());
            engine.put(key.as_slice(), value).unwrap();
        }

        let lock_cf = engine.cf_handle(CF_LOCK).unwrap();
        let cf_lock_data = vec![
            (b"k1", LockType::Put, b"v", 5),
            (b"k4", LockType::Lock, b"x", 10),
            (b"k5", LockType::Delete, b"y", 15),
        ];
        for &(prefix, tp, value, version) in &cf_lock_data {
            let encoded_key = Key::from_raw(prefix);
            let key = keys::data_key(encoded_key.encoded().as_slice());
            let lock = Lock::new(tp, value.to_vec(), version, 0, None);
            let value = lock.to_bytes();
            engine
                .put_cf(lock_cf, key.as_slice(), value.as_slice())
                .unwrap();
        }

        let write_cf = engine.cf_handle(CF_WRITE).unwrap();
        let cf_write_data = vec![
            (b"k2", WriteType::Put, 5, 10),
            (b"k6", WriteType::Lock, 20, 30),
            (b"k7", WriteType::Rollback, 35, 40),
        ];
        for &(prefix, tp, start_ts, commit_ts) in &cf_write_data {
            let encoded_key = Key::from_raw(prefix).append_ts(commit_ts);
            let key = keys::data_key(encoded_key.encoded().as_slice());
            let write = Write::new(tp, start_ts, None);
            let value = write.to_bytes();
            engine
                .put_cf(write_cf, key.as_slice(), value.as_slice())
                .unwrap();
        }

        let mut count = 0;
        for key_and_mvcc in debugger.scan_mvcc(b"z", &[], 10).unwrap() {
            assert!(key_and_mvcc.is_ok());
            count += 1;
        }
        assert_eq!(count, 7);
    }
}
