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

use std::thread;
use std::collections::btree_map::{BTreeMap, Entry as BTreeMapEntry};
use std::{error, result};
use std::sync::mpsc::{channel, Receiver};

use protobuf::RepeatedField;
use grpc::Error as GrpcError;

use rocksdb::{DB as RocksDB, DBIterator, Kv, SeekKey};
use kvproto::kvrpcpb::{LockInfo, MvccInfo, Op, ValueInfo, WriteInfo};
use kvproto::debugpb::*;
use kvproto::{eraftpb, raft_serverpb};

use raftstore::errors::Error as RaftstoreError;
use raftstore::store::{keys, Engines, Iterable, Peekable};
use raftstore::store::engine::IterOption;
use storage::{is_short_value, CfName, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use storage::types::Key;
use storage::mvcc::{Lock, Write, WriteType};

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
            from(e: RaftstoreError) -> (Box::new(e))
            from(e: GrpcError) -> (Box::new(e))
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

    pub fn scan_mvcc<D, E>(&self, mut req: ScanMvccRequest, deal: &mut D) -> Result<()>
    where
        Error: ::std::convert::From<E>,
        D: FnMut(Vec<u8>, MvccInfo) -> ::std::result::Result<(), E>,
    {
        let from_key = req.take_from_key();
        let limit = req.get_limit();
        let to_key = Some(req.take_to_key())
            .into_iter()
            .filter(|k| !k.is_empty())
            .next();
        if to_key.is_none() && limit == 0 {
            return Err(Error::InvalidArgument("no limit and to_key".to_owned()));
        }

        let lock_rx = self.mvcc_kvs(CF_LOCK, from_key.clone(), to_key.clone());
        let default_rx = self.mvcc_kvs_grouped(CF_DEFAULT, from_key.clone(), to_key.clone());
        let write_rx = self.mvcc_kvs_grouped(CF_WRITE, from_key.clone(), to_key.clone());

        let mut mvcc_infos: BTreeMap<Vec<u8>, (MvccInfo, bool, bool, bool)> = BTreeMap::new();
        let (mut want_lock, mut want_default, mut want_write) = (true, true, true);
        let (mut m_l, mut m_d, mut m_w) = (None, None, None);
        let mut count = 0;
        loop {
            if !want_lock && !want_default && !want_write {
                break;
            }
            if want_lock {
                if let Ok(kv) = lock_rx.recv() {
                    let key = keys::origin_key(&kv.0).to_vec();
                    let lock = box_try!(Lock::parse(&kv.1));
                    let mut lock_info = LockInfo::default();
                    lock_info.set_primary_lock(lock.primary);
                    lock_info.set_lock_version(lock.ts);
                    lock_info.set_key(kv.0.to_vec());
                    lock_info.set_lock_ttl(lock.ttl);
                    match mvcc_infos.entry(key.clone()) {
                        BTreeMapEntry::Vacant(ent) => {
                            let mut mvcc_info = MvccInfo::default();
                            mvcc_info.set_lock(lock_info);
                            ent.insert((mvcc_info, true, false, false));
                        }
                        BTreeMapEntry::Occupied(mut ent) => {
                            let mut mvcc_and_flags = ent.get_mut();
                            mvcc_and_flags.0.set_lock(lock_info);
                            mvcc_and_flags.1 = true;
                        }
                    }
                    m_l = Some(key);
                } else {
                    m_l = Some(vec![0xffu8]);
                }
                want_lock = false;
            }
            if want_default {
                if let Ok(vec_kv) = default_rx.recv() {
                    let key = box_try!(truncate_data_key(&vec_kv[0].0)).encoded().to_vec();
                    let mut values = RepeatedField::new();
                    for kv in vec_kv {
                        let key = Key::from_encoded(keys::origin_key(&kv.0).to_owned());
                        let mut value_info = ValueInfo::default();
                        value_info.set_is_short_value(is_short_value(&kv.1));
                        value_info.set_value(kv.1);
                        value_info.set_ts(box_try!(key.decode_ts()));
                        values.push(value_info);
                    }
                    match mvcc_infos.entry(key.clone()) {
                        BTreeMapEntry::Vacant(ent) => {
                            let mut mvcc_info = MvccInfo::default();
                            mvcc_info.set_values(values);
                            ent.insert((mvcc_info, false, true, false));
                        }
                        BTreeMapEntry::Occupied(mut ent) => {
                            let mut mvcc_and_flags = ent.get_mut();
                            mvcc_and_flags.0.set_values(values);
                            mvcc_and_flags.2 = true;
                        }
                    }
                    m_d = Some(key);
                } else {
                    m_d = Some(vec![0xffu8]);
                }
                want_default = false;
            }
            if want_write {
                if let Ok(vec_kv) = write_rx.recv() {
                    let key = box_try!(truncate_data_key(&vec_kv[0].0)).encoded().to_vec();
                    let mut writes = RepeatedField::new();
                    for kv in vec_kv {
                        let key = Key::from_encoded(keys::origin_key(&kv.0).to_owned());
                        let write = box_try!(Write::parse(&kv.1));
                        let mut write_info = WriteInfo::default();
                        write_info.set_start_ts(write.start_ts);
                        match write.write_type {
                            WriteType::Put => write_info.set_field_type(Op::Put),
                            WriteType::Delete => write_info.set_field_type(Op::Del),
                            WriteType::Lock => write_info.set_field_type(Op::Lock),
                            WriteType::Rollback => write_info.set_field_type(Op::Rollback),
                        }
                        write_info.set_commit_ts(box_try!(key.decode_ts()));
                        writes.push(write_info);
                    }
                    match mvcc_infos.entry(key.clone()) {
                        BTreeMapEntry::Vacant(ent) => {
                            let mut mvcc_info = MvccInfo::default();
                            mvcc_info.set_writes(writes);
                            ent.insert((mvcc_info, false, false, true));
                        }
                        BTreeMapEntry::Occupied(mut ent) => {
                            let mut mvcc_and_flags = ent.get_mut();
                            mvcc_and_flags.0.set_writes(writes);
                            mvcc_and_flags.3 = true;
                        }
                    }
                    m_w = Some(key);
                } else {
                    m_w = Some(vec![0xffu8]);
                }
                want_write = false;
            }
            for key in mvcc_infos.keys().cloned().collect::<Vec<_>>() {
                let mvcc_and_flags = mvcc_infos.remove(&key).unwrap();
                let g_l = mvcc_and_flags.1 || m_l.as_ref().map(|k| &key <= k).unwrap_or(true);
                let g_d = mvcc_and_flags.2 || m_d.as_ref().map(|k| &key <= k).unwrap_or(true);
                let g_w = mvcc_and_flags.3 || m_w.as_ref().map(|k| &key <= k).unwrap_or(true);
                if g_l && g_d && g_w {
                    try!(deal(key, mvcc_and_flags.0).map_err(Error::from));
                    count += 1;
                    if limit != 0 && count >= limit {
                        break;
                    }
                    if count >= limit {}
                    if mvcc_and_flags.1 {
                        want_lock = true;
                    }
                    if mvcc_and_flags.2 {
                        want_default = true;
                    }
                    if mvcc_and_flags.3 {
                        want_write = true;
                    }
                } else {
                    mvcc_infos.insert(key, mvcc_and_flags);
                    break;
                }
            }
        }
        Ok(())
    }

    fn mvcc_kvs(&self, cf: CfName, from: Vec<u8>, to: Option<Vec<u8>>) -> Receiver<Kv> {
        let (tx, rx) = channel::<Kv>();
        let db = self.engines.kv_engine.clone();
        thread::Builder::new()
            .name(thd_name!(format!("scan_mvcc_{}", cf)))
            .spawn(move || -> Result<()> {
                for kv in &mut try!(gen_mvcc_iter(db.as_ref(), cf, from, to)) {
                    box_try!(tx.send(kv));
                }
                Ok(())
            })
            .unwrap();
        rx
    }

    fn mvcc_kvs_grouped(
        &self,
        cf: CfName,
        from: Vec<u8>,
        to: Option<Vec<u8>>,
    ) -> Receiver<Vec<Kv>> {
        let (tx, rx) = channel::<Vec<Kv>>();
        let db = self.engines.kv_engine.clone();
        thread::Builder::new()
            .name(thd_name!(format!("scan_mvcc_{}", cf)))
            .spawn(move || -> Result<()> {
                let (mut cur, mut cur_key): (_, Option<Key>) = (Vec::new(), None);
                for kv in &mut try!(gen_mvcc_iter(db.as_ref(), cf, from, to)) {
                    if let Some(key) = cur_key {
                        if keys::origin_key(&kv.0).starts_with(key.encoded()) {
                            cur_key = Some(key);
                            cur.push(kv);
                        } else {
                            box_try!(tx.send(cur));
                            cur_key = Some(box_try!(truncate_data_key(&kv.0)));
                            cur = vec![kv];
                        }
                    } else {
                        cur_key = Some(box_try!(truncate_data_key(&kv.0)));
                        cur.push(kv);
                    }
                }
                if !cur.is_empty() {
                    box_try!(tx.send(cur));
                }
                Ok(())
            })
            .unwrap();
        rx
    }
}

fn truncate_data_key(data_key: &[u8]) -> Result<Key> {
    let k = Key::from_encoded(keys::origin_key(data_key).to_owned());
    k.truncate_ts().map_err(|e| box_err!(e))
}

fn gen_mvcc_iter(
    db: &RocksDB,
    cf: CfName,
    from: Vec<u8>,
    to: Option<Vec<u8>>,
) -> Result<DBIterator> {
    let iter_option = IterOption::new(to, false);
    let mut iter = try!(db.new_iterator_cf(cf, iter_option).map_err(Error::from));
    iter.seek(SeekKey::from(from.as_ref()));
    Ok(iter)
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

        let k = keys::data_key(b"meta_lock");
        let v = Lock::new(LockType::Put, b"pk".to_vec(), 1, 10, None).to_bytes();
        let cf_handle = engine.cf_handle(CF_LOCK).unwrap();
        engine.put_cf(cf_handle, k.as_slice(), &v).unwrap();

        let mut scan_mvcc_req = ScanMvccRequest::new();
        scan_mvcc_req.set_from_key(b"m".to_vec());
        scan_mvcc_req.set_to_key(b"n".to_vec());

        let mut count = 0;
        debugger
            .scan_mvcc(scan_mvcc_req, &mut |_, _| -> Result<()> {
                count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(count, 1);
    }
}
