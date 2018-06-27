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

use std::cell::RefCell;
use std::cmp::Ordering;
use std::iter::FromIterator;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::{error, result};
use util::collections::HashSet;

use protobuf::{self, Message, RepeatedField};

use kvproto::debugpb::{DB as DBType, MODULE};
use kvproto::kvrpcpb::{MvccInfo, MvccLock, MvccValue, MvccWrite, Op};
use kvproto::metapb::Region;
use kvproto::raft_serverpb::*;
use raft::eraftpb::Entry;
use rocksdb::{Kv, SeekKey, Writable, WriteBatch, WriteOptions, DB};

use raft::{self, quorum, RawNode};
use raftstore::store::engine::{IterOption, Mutable};
use raftstore::store::util as raftstore_util;
use raftstore::store::{
    init_apply_state, init_raft_state, write_initial_apply_state, write_initial_raft_state,
    write_peer_state,
};
use raftstore::store::{keys, CacheQueryStats, Engines, Iterable, Peekable, PeerStorage};
use storage::mvcc::{Lock, LockType, Write, WriteType};
use storage::types::{truncate_ts, Key};
use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use util::config::ReadableSize;
use util::escape;
use util::properties::MvccProperties;
use util::rocksdb::{compact_range, get_cf_handle};
use util::worker::Worker;

pub type Result<T> = result::Result<T, Error>;
type DBIterator = ::rocksdb::DBIterator<Arc<DB>>;

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

#[derive(PartialEq, Debug, Default)]
pub struct RegionInfo {
    pub raft_local_state: Option<RaftLocalState>,
    pub raft_apply_state: Option<RaftApplyState>,
    pub region_local_state: Option<RegionLocalState>,
}

impl RegionInfo {
    fn new(
        raft_local: Option<RaftLocalState>,
        raft_apply: Option<RaftApplyState>,
        region_local: Option<RegionLocalState>,
    ) -> Self {
        RegionInfo {
            raft_local_state: raft_local,
            raft_apply_state: raft_apply,
            region_local_state: region_local,
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

    pub fn get_engine(&self) -> &Engines {
        &self.engines
    }

    /// Get all regions holding region meta data from raft CF in KV storage.
    pub fn get_all_meta_regions(&self) -> Result<Vec<u64>> {
        let db = &self.engines.kv_engine;
        let cf = CF_RAFT;
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let mut regions = Vec::with_capacity(128);
        box_try!(db.scan_cf(cf, start_key, end_key, false, |key, _| {
            let (id, suffix) = keys::decode_region_meta_key(key)?;
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }
            regions.push(id);
            Ok(true)
        }));
        Ok(regions)
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
            Ok(None) => Err(Error::NotFound(format!(
                "value for key {:?} in db {:?}",
                key, db
            ))),
            Err(e) => Err(box_err!(e)),
        }
    }

    pub fn raft_log(&self, region_id: u64, log_index: u64) -> Result<Entry> {
        let key = keys::raft_log_key(region_id, log_index);
        match self.engines.raft_engine.get_msg(&key) {
            Ok(Some(entry)) => Ok(entry),
            Ok(None) => Err(Error::NotFound(format!(
                "raft log for region {} at index {}",
                region_id, log_index
            ))),
            Err(e) => Err(box_err!(e)),
        }
    }

    pub fn region_info(&self, region_id: u64) -> Result<RegionInfo> {
        let raft_state_key = keys::raft_state_key(region_id);
        let raft_state = box_try!(
            self.engines
                .raft_engine
                .get_msg::<RaftLocalState>(&raft_state_key)
        );

        let apply_state_key = keys::apply_state_key(region_id);
        let apply_state = box_try!(
            self.engines
                .kv_engine
                .get_msg_cf::<RaftApplyState>(CF_RAFT, &apply_state_key)
        );

        let region_state_key = keys::region_state_key(region_id);
        let region_state = box_try!(
            self.engines
                .kv_engine
                .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
        );

        match (raft_state, apply_state, region_state) {
            (None, None, None) => Err(Error::NotFound(format!("info for region {}", region_id))),
            (raft_state, apply_state, region_state) => {
                Ok(RegionInfo::new(raft_state, apply_state, region_state))
            }
        }
    }

    pub fn region_size<T: AsRef<str>>(
        &self,
        region_id: u64,
        cfs: Vec<T>,
    ) -> Result<Vec<(T, usize)>> {
        let region_state_key = keys::region_state_key(region_id);
        match self
            .engines
            .kv_engine
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
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
                        |_, v| {
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

    /// Scan MVCC Infos for given range `[start, end)`.
    pub fn scan_mvcc(&self, start: &[u8], end: &[u8], limit: u64) -> Result<MvccInfoIterator> {
        if !start.starts_with(b"z") || (!end.is_empty() && !end.starts_with(b"z")) {
            return Err(Error::InvalidArgument(
                "start and end should start with \"z\"".to_owned(),
            ));
        }
        if end.is_empty() && limit == 0 {
            return Err(Error::InvalidArgument("no limit and to_key".to_owned()));
        }
        MvccInfoIterator::new(&self.engines.kv_engine, start, end, limit)
    }

    /// Compact the cf[start..end) in the db.
    pub fn compact(
        &self,
        db: DBType,
        cf: &str,
        start: &[u8],
        end: &[u8],
        threads: u32,
    ) -> Result<()> {
        validate_db_and_cf(db, cf)?;
        let db = self.get_db_from_type(db)?;
        let handle = box_try!(get_cf_handle(db, cf));
        let start = if start.is_empty() { None } else { Some(start) };
        let end = if end.is_empty() { None } else { Some(end) };
        info!("Debugger starts manual comapct on {:?}.{}", db, cf);
        compact_range(db, handle, start, end, false, threads);
        info!("Debugger finishs manual comapct on {:?}.{}", db, cf);
        Ok(())
    }

    /// Set regions to tombstone by manual, and apply other status(such as
    /// peers, version, and key range) from `region` which comes from PD normally.
    pub fn set_region_tombstone(&self, regions: Vec<Region>) -> Result<Vec<(u64, Error)>> {
        let store_id = self.get_store_id()?;
        let db = &self.engines.kv_engine;
        let wb = WriteBatch::new();

        let mut errors = Vec::with_capacity(regions.len());
        for region in regions {
            let region_id = region.get_id();
            if let Err(e) = set_region_tombstone(db.as_ref(), store_id, region, &wb) {
                errors.push((region_id, e));
            }
        }

        if errors.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            box_try!(db.write_opt(wb, &write_opts));
        }
        Ok(errors)
    }

    pub fn recover_regions(&self, regions: Vec<Region>) -> Result<Vec<(u64, Error)>> {
        let db = &self.engines.kv_engine;

        let mut errors = Vec::with_capacity(regions.len());
        for region in regions {
            let region_id = region.get_id();
            if let Err(e) = self.recover_region(db, region) {
                errors.push((region_id, e));
            }
        }

        Ok(errors)
    }

    fn recover_region(&self, db: &Arc<DB>, region: Region) -> Result<()> {
        let wb = WriteBatch::new();

        let mut mvcc_checker = box_try!(MvccChecker::new(
            Arc::clone(db),
            region.get_start_key(),
            region.get_end_key()
        ));
        mvcc_checker.check_mvcc(&wb)?;

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        box_try!(db.write_opt(wb, &write_opts));

        warn!(
            "total fix default: {}, lock: {}, write: {}",
            mvcc_checker.default_fix_count,
            mvcc_checker.lock_fix_count,
            mvcc_checker.write_fix_count
        );

        Ok(())
    }

    pub fn bad_regions(&self) -> Result<Vec<(u64, Error)>> {
        let mut res = Vec::new();

        let from = keys::REGION_META_MIN_KEY.to_owned();
        let to = keys::REGION_META_MAX_KEY.to_owned();
        let readopts = IterOption::new(Some(from.clone()), Some(to), false).build_read_opts();
        let handle = box_try!(get_cf_handle(&self.engines.kv_engine, CF_RAFT));
        let mut iter = DBIterator::new_cf(Arc::clone(&self.engines.kv_engine), handle, readopts);
        iter.seek(SeekKey::from(from.as_ref()));

        let fake_snap_worker = Worker::new("fake snap worker");

        let check_value = |value: Vec<u8>| -> Result<()> {
            let local_state = box_try!(protobuf::parse_from_bytes::<RegionLocalState>(&value));
            match local_state.get_state() {
                PeerState::Tombstone | PeerState::Applying => return Ok(()),
                _ => {}
            }

            let region = local_state.get_region();
            let store_id = self.get_store_id()?;

            let peer_id = raftstore_util::find_peer(region, store_id)
                .map(|peer| peer.get_id())
                .ok_or_else(|| {
                    Error::Other("RegionLocalState doesn't contains peer itself".into())
                })?;

            let raft_state = box_try!(init_raft_state(&self.engines.raft_engine, region));
            let apply_state = box_try!(init_apply_state(&self.engines.kv_engine, region));
            if raft_state.get_last_index() < apply_state.get_applied_index() {
                return Err(Error::Other("last index < applied index".into()));
            }

            let tag = format!("[region {}] {}", region.get_id(), peer_id);
            let peer_storage = box_try!(PeerStorage::new(
                Arc::clone(&self.engines.kv_engine),
                Arc::clone(&self.engines.raft_engine),
                region,
                fake_snap_worker.scheduler(),
                tag.clone(),
                Rc::new(RefCell::new(CacheQueryStats::default())),
            ));

            let raft_cfg = raft::Config {
                id: peer_id,
                peers: vec![],
                election_tick: 10,
                heartbeat_tick: 2,
                max_size_per_msg: ReadableSize::mb(1).0,
                max_inflight_msgs: 256,
                applied: apply_state.get_applied_index(),
                check_quorum: true,
                tag,
                skip_bcast_commit: true,
                ..Default::default()
            };

            box_try!(RawNode::new(&raft_cfg, peer_storage, vec![]));
            Ok(())
        };

        for (key, value) in &mut iter {
            if let Ok((region_id, suffix)) = keys::decode_region_meta_key(&key) {
                if suffix != keys::REGION_STATE_SUFFIX {
                    continue;
                }
                if let Err(e) = check_value(value) {
                    res.push((region_id, e));
                }
            }
        }
        Ok(res)
    }

    pub fn remove_failed_stores(
        &self,
        store_ids: Vec<u64>,
        region_ids: Option<Vec<u64>>,
    ) -> Result<()> {
        let store_id = self.get_store_id()?;
        if store_ids.iter().any(|&s| s == store_id) {
            let msg = format!("Store {} in the failed list", store_id);
            return Err(Error::Other(msg.into()));
        }
        let wb = WriteBatch::new();
        let handle = box_try!(get_cf_handle(self.engines.kv_engine.as_ref(), CF_RAFT));
        let store_ids = HashSet::<u64>::from_iter(store_ids);

        {
            let remove_stores = |key: &[u8], value: &[u8]| {
                let (_, suffix_type) = box_try!(keys::decode_region_meta_key(key));
                if suffix_type != keys::REGION_STATE_SUFFIX {
                    return Ok(());
                }

                let mut region_state = RegionLocalState::new();
                box_try!(region_state.merge_from_bytes(value));
                if region_state.get_state() == PeerState::Tombstone {
                    return Ok(());
                }

                let mut new_peers = region_state.get_region().get_peers().to_owned();
                new_peers.retain(|peer| !store_ids.contains(&peer.get_store_id()));
                let new_peers_len = new_peers.len();
                let old_peers_len = region_state.get_region().get_peers().len();

                if new_peers_len < quorum(old_peers_len) {
                    let region_id = region_state.get_region().get_id();
                    let old_peers = region_state.mut_region().take_peers();
                    info!(
                        "region {} change peers from {:?}, to {:?}",
                        region_id, old_peers, new_peers
                    );
                    // We need to leave epoch untouched to avoid inconsistency.
                    region_state
                        .mut_region()
                        .set_peers(RepeatedField::from_vec(new_peers));
                    box_try!(wb.put_msg_cf(handle, key, &region_state));
                }
                Ok(())
            };

            if let Some(region_ids) = region_ids {
                let kv = &self.engines.kv_engine;
                for region_id in region_ids {
                    let key = keys::region_state_key(region_id);
                    if let Some(value) = box_try!(kv.get_value_cf(CF_RAFT, &key)) {
                        box_try!(remove_stores(&key, &value));
                    } else {
                        let msg = format!("No such region {} on the store", region_id);
                        return Err(Error::Other(msg.into()));
                    }
                }
            } else {
                box_try!(self.engines.kv_engine.scan_cf(
                    CF_RAFT,
                    keys::REGION_META_MIN_KEY,
                    keys::REGION_META_MAX_KEY,
                    false,
                    |key, value| remove_stores(key, value).map(|_| true)
                ));
            }
        }

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        box_try!(self.engines.kv_engine.write_opt(wb, &write_opts));
        Ok(())
    }

    pub fn recreate_region(&self, region: Region) -> Result<()> {
        let region_id = region.get_id();
        let kv = self.engines.kv_engine.as_ref();
        let raft = self.engines.raft_engine.as_ref();

        let kv_wb = WriteBatch::new();
        let raft_wb = WriteBatch::new();
        let kv_handle = box_try!(get_cf_handle(kv, CF_RAFT));

        if region.get_start_key() >= region.get_end_key() && !region.get_end_key().is_empty() {
            return Err(box_err!("Bad region: {:?}", region));
        }

        box_try!(self.engines.kv_engine.scan_cf(
            CF_RAFT,
            keys::REGION_META_MIN_KEY,
            keys::REGION_META_MAX_KEY,
            false,
            |key, value| {
                let (_, suffix_type) = box_try!(keys::decode_region_meta_key(key));
                if suffix_type != keys::REGION_STATE_SUFFIX {
                    return Ok(true);
                }

                let mut region_state = RegionLocalState::new();
                box_try!(region_state.merge_from_bytes(value));
                if region_state.get_state() == PeerState::Tombstone {
                    return Ok(true);
                }
                let exists_region = region_state.get_region();

                if !region_overlap(exists_region, &region) {
                    return Ok(true);
                }

                if exists_region.get_start_key() == region.get_start_key()
                    && exists_region.get_end_key() == region.get_end_key()
                {
                    Err(box_err!("region still exists {:?}", region))
                } else {
                    Err(box_err!("region overlap with {:?}", exists_region))
                }
            },
        ));

        // RegionLocalState.
        let mut region_state = RegionLocalState::new();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region);
        let key = keys::region_state_key(region_id);
        if box_try!(kv.get_msg_cf::<RegionLocalState>(CF_RAFT, &key)).is_some() {
            return Err(Error::Other(
                "Store already has the RegionLocalState".into(),
            ));
        }
        box_try!(kv_wb.put_msg_cf(kv_handle, &key, &region_state));

        // RaftApplyState.
        let key = keys::apply_state_key(region_id);
        if box_try!(kv.get_msg_cf::<RaftApplyState>(CF_RAFT, &key)).is_some() {
            return Err(Error::Other("Store already has the RaftApplyState".into()));
        }
        box_try!(write_initial_apply_state(kv, &kv_wb, region_id));

        // RaftLocalState.
        let key = keys::raft_state_key(region_id);
        if box_try!(raft.get_msg::<RaftLocalState>(&key)).is_some() {
            return Err(Error::Other("Store already has the RaftLocalState".into()));
        }
        box_try!(write_initial_raft_state(&raft_wb, region_id));

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        box_try!(kv.write_opt(kv_wb, &write_opts));
        box_try!(raft.write_opt(raft_wb, &write_opts));
        Ok(())
    }

    pub fn get_store_id(&self) -> Result<u64> {
        let db = &self.engines.kv_engine;
        db.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)
            .map_err(|e| box_err!(e))
            .and_then(|ident| match ident {
                Some(ident) => Ok(ident.get_store_id()),
                None => Err(Error::NotFound("No store ident key".to_owned())),
            })
    }

    pub fn modify_tikv_config(
        &self,
        module: MODULE,
        config_name: &str,
        config_value: &str,
    ) -> Result<()> {
        let db = match module {
            MODULE::KVDB => DBType::KV,
            MODULE::RAFTDB => DBType::RAFT,
            _ => return Err(Error::NotFound(format!("unsupported module: {:?}", module))),
        };
        let rocksdb = self.get_db_from_type(db)?;
        let vec: Vec<&str> = config_name.split('.').collect();
        if vec.len() == 1 {
            box_try!(rocksdb.set_db_options(&[(config_name, config_value)]));
        } else if vec.len() == 2 {
            let cf = vec[0];
            let config_name = vec[1];
            validate_db_and_cf(db, cf)?;

            let handle = box_try!(get_cf_handle(rocksdb, cf));
            // currently we can't modify block_cache_size via set_options_cf
            if config_name == "block_cache_size" {
                let opt = rocksdb.get_options_cf(handle);
                let capacity = ReadableSize::from_str(config_value);
                if capacity.is_err() {
                    return Err(Error::InvalidArgument(format!(
                        "bad block cache size: {:?}",
                        capacity.unwrap_err()
                    )));
                }
                box_try!(opt.set_block_cache_capacity(capacity.unwrap().0));
            } else {
                let mut opt = Vec::new();
                opt.push((config_name, config_value));
                box_try!(rocksdb.set_options_cf(handle, &opt));
            }
        } else {
            return Err(Error::InvalidArgument(format!(
                "bad argument: {}",
                config_name
            )));
        }
        Ok(())
    }

    fn get_region_state(&self, region_id: u64) -> Result<RegionLocalState> {
        let region_state_key = keys::region_state_key(region_id);
        let region_state = box_try!(
            self.engines
                .kv_engine
                .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
        );
        match region_state {
            Some(v) => Ok(v),
            None => Err(Error::NotFound(format!("region {}", region_id))),
        }
    }

    pub fn get_region_properties(&self, region_id: u64) -> Result<Vec<(String, String)>> {
        let region_state = self.get_region_state(region_id)?;
        let region = region_state.get_region();
        let db = &self.engines.kv_engine;

        let mut num_entries = 0;
        let mut mvcc_properties = MvccProperties::new();
        let collection = box_try!(raftstore_util::get_region_properties_cf(
            db, CF_WRITE, region
        ));
        for (_, v) in &*collection {
            num_entries += v.num_entries();
            let mvcc = box_try!(MvccProperties::decode(v.user_collected_properties()));
            mvcc_properties.add(&mvcc);
        }

        let res = [
            ("num_files", collection.len() as u64),
            ("num_entries", num_entries),
            ("num_deletes", num_entries - mvcc_properties.num_versions),
            ("mvcc.min_ts", mvcc_properties.min_ts),
            ("mvcc.max_ts", mvcc_properties.max_ts),
            ("mvcc.num_rows", mvcc_properties.num_rows),
            ("mvcc.num_puts", mvcc_properties.num_puts),
            ("mvcc.num_versions", mvcc_properties.num_versions),
            ("mvcc.max_row_versions", mvcc_properties.max_row_versions),
        ].iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Ok(res)
    }
}

pub struct MvccChecker {
    db: Arc<DB>,
    lock_iter: DBIterator,
    default_iter: DBIterator,
    write_iter: DBIterator,
    lock_fix_count: usize,
    default_fix_count: usize,
    write_fix_count: usize,
}

impl MvccChecker {
    fn new(db: Arc<DB>, start_key: &[u8], end_key: &[u8]) -> Result<Self> {
        let start_key = keys::data_key(start_key);
        let end_key = keys::data_end_key(end_key);
        let gen_iter = |cf: &str| -> Result<_> {
            let from = start_key.clone();
            let to = end_key.clone();
            let readopts = IterOption::new(Some(from.clone()), Some(to), false).build_read_opts();
            let handle = box_try!(get_cf_handle(db.as_ref(), cf));
            let mut iter = DBIterator::new_cf(Arc::clone(&db), handle, readopts);
            iter.seek(SeekKey::Start);
            Ok(iter)
        };

        Ok(MvccChecker {
            db: Arc::clone(&db),
            write_iter: gen_iter(CF_WRITE)?,
            lock_iter: gen_iter(CF_LOCK)?,
            default_iter: gen_iter(CF_DEFAULT)?,
            lock_fix_count: 0,
            default_fix_count: 0,
            write_fix_count: 0,
        })
    }

    fn min_key(key: Option<Vec<u8>>, iter: &DBIterator, f: fn(&[u8]) -> &[u8]) -> Option<Vec<u8>> {
        let iter_key = if iter.valid() {
            Some(f(keys::origin_key(iter.key())).to_vec())
        } else {
            None
        };
        match (key, iter_key) {
            (Some(a), Some(b)) => if a < b {
                Some(a)
            } else {
                Some(b)
            },
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    pub fn check_mvcc(&mut self, wb: &WriteBatch) -> Result<()> {
        loop {
            // Find min key in the 3 CFs.
            let mut key = MvccChecker::min_key(None, &self.default_iter, truncate_ts);
            key = MvccChecker::min_key(key, &self.lock_iter, |k| k);
            key = MvccChecker::min_key(key, &self.write_iter, truncate_ts);

            match key {
                Some(key) => self.check_mvcc_key(wb, key.as_ref())?,
                None => return Ok(()),
            }
        }
    }

    fn check_mvcc_key(&mut self, wb: &WriteBatch, key: &[u8]) -> Result<()> {
        let (mut default, mut write, mut lock) = (None, None, None);
        let (mut next_default, mut next_write, mut next_lock) = (true, true, true);
        loop {
            if next_default {
                default = self.next_default(key)?;
                next_default = false;
            }
            if next_write {
                write = self.next_write(key)?;
                next_write = false;
            }
            if next_lock {
                lock = self.next_lock(key)?;
                next_lock = false;
            }

            // If lock exists, check whether the records in DEFAULT and WRITE
            // match it.
            if let Some(ref l) = lock {
                // All write records' ts should be less than lock's ts.
                if let Some((commit_ts, _)) = write {
                    if l.ts <= commit_ts {
                        warn!(
                            "LOCK ts is less than WRITE ts, key: {}, lock_ts: {}, commit_ts: {}",
                            escape(key),
                            l.ts,
                            commit_ts
                        );
                        self.delete(wb, CF_LOCK, key, None)?;
                        self.lock_fix_count += 1;
                        next_lock = true;
                        continue;
                    }
                }

                // If the lock's type is PUT and contains no short value, there
                // should be a corresponding default record.
                if l.lock_type == LockType::Put && l.short_value.is_none() {
                    match default {
                        Some(start_ts) if start_ts == l.ts => {
                            next_default = true;
                        }
                        _ => {
                            warn!(
                                "no corresponding DEFAULT record for LOCK, key: {}, lock_ts: {}",
                                escape(key),
                                l.ts
                            );
                            self.delete(wb, CF_LOCK, key, None)?;
                            self.lock_fix_count += 1;
                        }
                    }
                }
                next_lock = true;
                continue;
            }

            // For none-put write or write with short_value, no DEFAULT record
            // is needed.
            if let Some((_, ref w)) = write {
                if w.write_type != WriteType::Put || w.short_value.is_some() {
                    next_write = true;
                    continue;
                }
            }

            // The start_ts of DEFAULT and WRITE should be matched.
            match (default, &write) {
                (Some(start_ts), &Some((_, ref w))) if start_ts == w.start_ts => {
                    next_default = true;
                    next_write = true;
                    continue;
                }
                (Some(start_ts), &Some((_, ref w))) if start_ts < w.start_ts => next_write = true,
                (Some(start_ts), &Some((_, ref w))) if start_ts > w.start_ts => next_default = true,
                (Some(_), &Some(_)) => {} // Won't happen.
                (None, &Some(_)) => next_write = true,
                (Some(_), &None) => next_default = true,
                (None, &None) => return Ok(()),
            }

            if next_default {
                warn!(
                    "orphan DEFAULT record, key: {}, start_ts: {}",
                    escape(key),
                    default.unwrap()
                );
                self.delete(wb, CF_DEFAULT, key, default)?;
                self.default_fix_count += 1;
            }

            if next_write {
                if let Some((commit_ts, ref w)) = write {
                    warn!(
                        "no corresponding DEFAULT record for WRITE, key: {}, start_ts: {}, commit_ts: {}",
                        escape(key),
                        w.start_ts,
                        commit_ts
                    );
                    self.delete(wb, CF_WRITE, key, Some(commit_ts))?;
                    self.write_fix_count += 1;
                }
            }
        }
    }

    fn next_lock(&mut self, key: &[u8]) -> Result<Option<Lock>> {
        if self.lock_iter.valid() && keys::origin_key(self.lock_iter.key()) == key {
            let lock = box_try!(Lock::parse(self.lock_iter.value()));
            self.lock_iter.next();
            return Ok(Some(lock));
        }
        Ok(None)
    }

    fn next_default(&mut self, key: &[u8]) -> Result<Option<u64>> {
        if self.default_iter.valid()
            && truncate_ts(keys::origin_key(self.default_iter.key())) == key
        {
            let record_key = Key::from_encoded(keys::origin_key(self.default_iter.key()).to_vec());
            let start_ts = box_try!(record_key.decode_ts());
            self.default_iter.next();
            return Ok(Some(start_ts));
        }
        Ok(None)
    }

    fn next_write(&mut self, key: &[u8]) -> Result<Option<(u64, Write)>> {
        if self.write_iter.valid() && truncate_ts(keys::origin_key(self.write_iter.key())) == key {
            let record_key = Key::from_encoded(keys::origin_key(self.write_iter.key()).to_vec());
            let write = box_try!(Write::parse(self.write_iter.value()));
            let commit_ts = box_try!(record_key.decode_ts());
            self.write_iter.next();
            return Ok(Some((commit_ts, write)));
        }
        Ok(None)
    }

    fn delete(&mut self, wb: &WriteBatch, cf: &str, key: &[u8], ts: Option<u64>) -> Result<()> {
        let handle = box_try!(get_cf_handle(self.db.as_ref(), cf));
        let key = match ts {
            Some(ts) => Key::from_encoded(key.to_vec()).append_ts(ts),
            None => Key::from_encoded(key.to_vec()),
        };
        box_try!(wb.delete_cf(handle, &keys::data_key(key.encoded())));
        Ok(())
    }
}

fn region_overlap(r1: &Region, r2: &Region) -> bool {
    let (start_key_1, start_key_2) = (r1.get_start_key(), r2.get_start_key());
    let (end_key_1, end_key_2) = (r1.get_end_key(), r2.get_end_key());
    (start_key_1 < end_key_2 || end_key_2.is_empty())
        && (start_key_2 < end_key_1 || end_key_1.is_empty())
}

pub struct MvccInfoIterator {
    limit: u64,
    count: u64,
    lock_iter: DBIterator,
    default_iter: DBIterator,
    write_iter: DBIterator,
}

impl MvccInfoIterator {
    fn new(db: &Arc<DB>, from: &[u8], to: &[u8], limit: u64) -> Result<Self> {
        if !keys::validate_data_key(from) {
            return Err(Error::InvalidArgument(format!(
                "from non-mvcc area {:?}",
                from
            )));
        }

        let gen_iter = |cf: &str| -> Result<_> {
            let to = if to.is_empty() { None } else { Some(to) };
            let readopts = IterOption::new(None, to.map(Vec::from), false).build_read_opts();
            let handle = box_try!(get_cf_handle(db.as_ref(), cf));
            let mut iter = DBIterator::new_cf(Arc::clone(db), handle, readopts);
            iter.seek(SeekKey::from(from));
            Ok(iter)
        };
        Ok(MvccInfoIterator {
            limit,
            count: 0,
            lock_iter: gen_iter(CF_LOCK)?,
            default_iter: gen_iter(CF_DEFAULT)?,
            write_iter: gen_iter(CF_WRITE)?,
        })
    }

    fn next_lock(&mut self) -> Result<Option<(Vec<u8>, MvccLock)>> {
        let mut iter = &mut self.lock_iter;
        if let Some((key, value)) = <&mut DBIterator as Iterator>::next(&mut iter) {
            let lock = box_try!(Lock::parse(&value));
            let mut lock_info = MvccLock::default();
            match lock.lock_type {
                LockType::Put => lock_info.set_field_type(Op::Put),
                LockType::Delete => lock_info.set_field_type(Op::Del),
                LockType::Lock => lock_info.set_field_type(Op::Lock),
            }
            lock_info.set_start_ts(lock.ts);
            lock_info.set_primary(lock.primary);
            lock_info.set_short_value(lock.short_value.unwrap_or_default());
            return Ok(Some((key, lock_info)));
        };
        Ok(None)
    }

    fn next_default(&mut self) -> Result<Option<(Vec<u8>, RepeatedField<MvccValue>)>> {
        if let Some((prefix, vec_kv)) = Self::next_grouped(&mut self.default_iter) {
            let mut values = Vec::with_capacity(vec_kv.len());
            for (key, value) in vec_kv {
                let mut value_info = MvccValue::default();
                let encoded_key = Key::from_encoded(keys::origin_key(&key).to_owned());
                value_info.set_start_ts(box_try!(encoded_key.decode_ts()));
                value_info.set_value(value);
                values.push(value_info);
            }
            return Ok(Some((prefix, RepeatedField::from_vec(values))));
        }
        Ok(None)
    }

    fn next_write(&mut self) -> Result<Option<(Vec<u8>, RepeatedField<MvccWrite>)>> {
        if let Some((prefix, vec_kv)) = Self::next_grouped(&mut self.write_iter) {
            let mut writes = Vec::with_capacity(vec_kv.len());
            for (key, value) in vec_kv {
                let write = box_try!(Write::parse(&value));
                let mut write_info = MvccWrite::default();
                match write.write_type {
                    WriteType::Put => write_info.set_field_type(Op::Put),
                    WriteType::Delete => write_info.set_field_type(Op::Del),
                    WriteType::Lock => write_info.set_field_type(Op::Lock),
                    WriteType::Rollback => write_info.set_field_type(Op::Rollback),
                }
                write_info.set_start_ts(write.start_ts);
                let encoded_key = Key::from_encoded(keys::origin_key(&key).to_owned());
                write_info.set_commit_ts(box_try!(encoded_key.decode_ts()));
                write_info.set_short_value(write.short_value.unwrap_or_default());
                writes.push(write_info);
            }
            return Ok(Some((prefix, RepeatedField::from_vec(writes))));
        }
        Ok(None)
    }

    fn next_grouped(iter: &mut DBIterator) -> Option<(Vec<u8>, Vec<Kv>)> {
        if iter.valid() {
            let prefix = truncate_ts(iter.key()).to_vec();
            let mut kvs = vec![(iter.key().to_vec(), iter.value().to_vec())];
            while iter.next() && iter.key().starts_with(&prefix) {
                kvs.push((iter.key().to_vec(), iter.value().to_vec()));
            }
            return Some((prefix, kvs));
        }
        None
    }

    fn next_item(&mut self) -> Result<Option<(Vec<u8>, MvccInfo)>> {
        if self.limit != 0 && self.count >= self.limit {
            return Ok(None);
        }

        let mut mvcc_info = MvccInfo::new();
        let mut min_prefix = Vec::new();

        let (lock_ok, writes_ok) = match (self.lock_iter.valid(), self.write_iter.valid()) {
            (false, false) => return Ok(None),
            (true, true) => {
                let prefix1 = self.lock_iter.key();
                let prefix2 = truncate_ts(self.write_iter.key());
                match prefix1.cmp(prefix2) {
                    Ordering::Less => (true, false),
                    Ordering::Equal => (true, true),
                    _ => (false, true),
                }
            }
            valid_pair => valid_pair,
        };

        if lock_ok {
            if let Some((prefix, lock)) = self.next_lock()? {
                mvcc_info.set_lock(lock);
                min_prefix = prefix;
            }
        }
        if writes_ok {
            if let Some((prefix, writes)) = self.next_write()? {
                mvcc_info.set_writes(writes);
                min_prefix = prefix;
            }
        }
        if self.default_iter.valid() {
            match truncate_ts(self.default_iter.key()).cmp(&min_prefix) {
                Ordering::Equal => if let Some((_, values)) = self.next_default()? {
                    mvcc_info.set_values(values);
                },
                Ordering::Greater => {}
                _ => {
                    let err_msg = format!(
                        "scan_mvcc CF_DEFAULT corrupt: want {}, got {}",
                        escape(&min_prefix),
                        escape(truncate_ts(self.default_iter.key()))
                    );
                    return Err(box_err!(err_msg));
                }
            }
        }
        self.count += 1;
        Ok(Some((min_prefix, mvcc_info)))
    }
}

impl Iterator for MvccInfoIterator {
    type Item = Result<(Vec<u8>, MvccInfo)>;

    fn next(&mut self) -> Option<Result<(Vec<u8>, MvccInfo)>> {
        match self.next_item() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub fn validate_db_and_cf(db: DBType, cf: &str) -> Result<()> {
    match (db, cf) {
        (DBType::KV, CF_DEFAULT)
        | (DBType::KV, CF_WRITE)
        | (DBType::KV, CF_LOCK)
        | (DBType::KV, CF_RAFT)
        | (DBType::RAFT, CF_DEFAULT) => Ok(()),
        _ => Err(Error::InvalidArgument(format!(
            "invalid cf {:?} for db {:?}",
            cf, db
        ))),
    }
}

fn set_region_tombstone(db: &DB, store_id: u64, region: Region, wb: &WriteBatch) -> Result<()> {
    let id = region.get_id();
    let key = keys::region_state_key(id);

    let region_state = db
        .get_msg_cf::<RegionLocalState>(CF_RAFT, &key)
        .map_err(|e| box_err!(e))
        .and_then(|s| s.ok_or_else(|| Error::Other("Can't find RegionLocalState".into())))?;
    if region_state.get_state() == PeerState::Tombstone {
        return Ok(());
    }

    let peer_id = region_state
        .get_region()
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .map(|p| p.get_id())
        .ok_or_else(|| Error::Other("RegionLocalState doesn't contains the peer itself".into()))?;

    let old_conf_ver = region_state.get_region().get_region_epoch().get_conf_ver();
    let new_conf_ver = region.get_region_epoch().get_conf_ver();
    if new_conf_ver <= old_conf_ver {
        return Err(box_err!("invalid conf_ver"));
    }

    // If the store is not in peers, or it's still in but its peer_id
    // has changed, we know the peer is marked as tombstone success.
    let scheduled = region
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == store_id)
        .map_or(true, |p| p.get_id() != peer_id);
    if !scheduled {
        return Err(box_err!("The peer is still in target peers"));
    }

    box_try!(write_peer_state(
        db,
        wb,
        &region,
        PeerState::Tombstone,
        None
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;
    use std::sync::Arc;

    use kvproto::metapb::{Peer, Region};
    use raft::eraftpb::EntryType;
    use rocksdb::{ColumnFamilyOptions, DBOptions, Writable};
    use tempdir::TempDir;

    use super::*;
    use raftstore::store::engine::Mutable;
    use storage::mvcc::{Lock, LockType};
    use storage::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use util::rocksdb::{self as rocksdb_util, new_engine_opt, CFOptions};

    fn init_region_state(engine: &DB, region_id: u64, stores: &[u64]) -> Region {
        let cf_raft = engine.cf_handle(CF_RAFT).unwrap();
        let mut region = Region::new();
        region.set_id(region_id);
        for (i, &store_id) in stores.iter().enumerate() {
            let mut peer = Peer::new();
            peer.set_id(i as u64);
            peer.set_store_id(store_id);
            region.mut_peers().push(peer);
        }
        let mut region_state = RegionLocalState::new();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region.clone());
        let key = keys::region_state_key(region_id);
        engine.put_msg_cf(cf_raft, &key, &region_state).unwrap();
        region
    }

    fn get_region_state(engine: &DB, region_id: u64) -> RegionLocalState {
        let key = keys::region_state_key(region_id);
        engine
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &key)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn test_region_overlap() {
        let new_region = |start: &[u8], end: &[u8]| -> Region {
            let mut region = Region::default();
            region.set_start_key(start.to_owned());
            region.set_end_key(end.to_owned());
            region
        };

        // For normal case.
        assert!(region_overlap(
            &new_region(b"a", b"z"),
            &new_region(b"b", b"y")
        ));
        assert!(region_overlap(
            &new_region(b"a", b"n"),
            &new_region(b"m", b"z")
        ));
        assert!(!region_overlap(
            &new_region(b"a", b"m"),
            &new_region(b"n", b"z")
        ));

        // For the first or last region.
        assert!(region_overlap(
            &new_region(b"m", b""),
            &new_region(b"a", b"n")
        ));
        assert!(region_overlap(
            &new_region(b"a", b"n"),
            &new_region(b"m", b"")
        ));
        assert!(region_overlap(
            &new_region(b"", b""),
            &new_region(b"m", b"")
        ));
        assert!(!region_overlap(
            &new_region(b"a", b"m"),
            &new_region(b"n", b"")
        ));
    }

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

        let engines = Engines::new(Arc::clone(&engine), engine);
        Debugger::new(engines)
    }

    impl Debugger {
        fn set_store_id(&self, store_id: u64) {
            let mut ident = StoreIdent::new();
            ident.set_store_id(store_id);
            let db = &self.engines.kv_engine;
            db.put_msg(keys::STORE_IDENT_KEY, &ident).unwrap();
        }
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
        let mut entry = Entry::new();
        entry.set_term(1);
        entry.set_index(1);
        entry.set_entry_type(EntryType::EntryNormal);
        entry.set_data(vec![42]);
        engine.put_msg(&key, &entry).unwrap();
        assert_eq!(engine.get_msg::<Entry>(&key).unwrap().unwrap(), entry);

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
        let mut raft_state = RaftLocalState::new();
        raft_state.set_last_index(42);
        raft_engine.put_msg(&raft_state_key, &raft_state).unwrap();
        assert_eq!(
            raft_engine
                .get_msg::<RaftLocalState>(&raft_state_key)
                .unwrap()
                .unwrap(),
            raft_state
        );

        let apply_state_key = keys::apply_state_key(region_id);
        let mut apply_state = RaftApplyState::new();
        apply_state.set_applied_index(42);
        kv_engine
            .put_msg_cf(raft_cf, &apply_state_key, &apply_state)
            .unwrap();
        assert_eq!(
            kv_engine
                .get_msg_cf::<RaftApplyState>(CF_RAFT, &apply_state_key)
                .unwrap()
                .unwrap(),
            apply_state
        );

        let region_state_key = keys::region_state_key(region_id);
        let mut region_state = RegionLocalState::new();
        region_state.set_state(PeerState::Tombstone);
        kv_engine
            .put_msg_cf(raft_cf, &region_state_key, &region_state)
            .unwrap();
        assert_eq!(
            kv_engine
                .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
                .unwrap()
                .unwrap(),
            region_state
        );

        assert_eq!(
            debugger.region_info(region_id).unwrap(),
            RegionInfo::new(Some(raft_state), Some(apply_state), Some(region_state))
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
        let mut region = Region::new();
        region.set_id(region_id);
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"zz".to_vec());
        let mut state = RegionLocalState::new();
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
            (b"k3", WriteType::Put, 15, 20),
            (b"k6", WriteType::Lock, 25, 30),
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

        // Test scan with bad start, end or limit.
        assert!(debugger.scan_mvcc(b"z", b"", 0).is_err());
        assert!(debugger.scan_mvcc(b"z", b"x", 3).is_err());
    }

    #[test]
    fn test_tombstone_regions() {
        let debugger = new_debugger();
        debugger.set_store_id(11);
        let engine = debugger.engines.kv_engine.as_ref();

        // region 1 with peers at stores 11, 12, 13.
        let region_1 = init_region_state(engine, 1, &[11, 12, 13]);
        // Got the target region from pd, which doesn't contains the store.
        let mut target_region_1 = region_1.clone();
        target_region_1.mut_peers().remove(0);
        target_region_1.mut_region_epoch().set_conf_ver(100);

        // region 2 with peers at stores 11, 12, 13.
        let region_2 = init_region_state(engine, 2, &[11, 12, 13]);
        // Got the target region from pd, which has different peer_id.
        let mut target_region_2 = region_2.clone();
        target_region_2.mut_peers()[0].set_id(100);
        target_region_2.mut_region_epoch().set_conf_ver(100);

        // region 3 with peers at stores 21, 22, 23.
        let region_3 = init_region_state(engine, 3, &[21, 22, 23]);
        // Got the target region from pd but the peers are not changed.
        let mut target_region_3 = region_3.clone();
        target_region_3.mut_region_epoch().set_conf_ver(100);

        // Test with bad target region. No region state in rocksdb should be changed.
        let target_regions = vec![
            target_region_1.clone(),
            target_region_2.clone(),
            target_region_3,
        ];
        let errors = debugger.set_region_tombstone(target_regions).unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, 3);
        assert_eq!(get_region_state(engine, 1).take_region(), region_1);
        assert_eq!(get_region_state(engine, 2).take_region(), region_2);

        // After set_region_tombstone success, all region should be adjusted.
        let target_regions = vec![target_region_1, target_region_2];
        let errors = debugger.set_region_tombstone(target_regions).unwrap();
        assert!(errors.is_empty());
        for &region_id in &[1, 2] {
            let state = get_region_state(engine, region_id).get_state();
            assert_eq!(state, PeerState::Tombstone);
        }
    }

    #[test]
    fn test_remove_failed_stores() {
        let debugger = new_debugger();
        debugger.set_store_id(100);
        let engine = debugger.engines.kv_engine.as_ref();

        // region 1 with peers at stores 11, 12, 13 and 14.
        init_region_state(engine, 1, &[11, 12, 13, 14]);
        // region 2 with peers at stores 21, 22, 23.
        init_region_state(engine, 2, &[21, 22, 23]);

        // Only remove specified stores from region 1.
        debugger
            .remove_failed_stores(vec![13, 14, 21, 23], Some(vec![1]))
            .unwrap();

        // 13 and 14 should be removed from region 1.
        let region_state = get_region_state(engine, 1);
        assert_eq!(region_state.get_region().get_peers().len(), 2);
        // 21 and 23 shouldn't be removed from region 2.
        let region_state = get_region_state(engine, 2);
        assert_eq!(region_state.get_region().get_peers().len(), 3);

        // Remove specified stores from all regions.
        debugger.remove_failed_stores(vec![11, 23], None).unwrap();

        // 11 should be removed from region 1.
        let region_state = get_region_state(engine, 1);
        assert_eq!(region_state.get_region().get_peers().len(), 1);

        // 23 shouldn't be removed from region 2 because there's still a quorom.
        let region_state = get_region_state(engine, 2);
        assert_eq!(region_state.get_region().get_peers().len(), 3);

        // Should fail when the store itself is in the failed list.
        init_region_state(engine, 3, &[100, 31, 32, 33]);
        assert!(debugger.remove_failed_stores(vec![100], None).is_err());
    }

    #[test]
    fn test_bad_regions() {
        let debugger = new_debugger();
        let kv_engine = debugger.engines.kv_engine.as_ref();
        let raft_engine = debugger.engines.raft_engine.as_ref();
        let store_id = 1; // It's a fake id.

        let wb1 = WriteBatch::new();
        let handle1 = get_cf_handle(raft_engine, CF_DEFAULT).unwrap();

        let wb2 = WriteBatch::new();
        let handle2 = get_cf_handle(kv_engine, CF_RAFT).unwrap();

        {
            let mock_region_state = |region_id: u64, peers: &[u64]| {
                let region_state_key = keys::region_state_key(region_id);
                let mut region_state = RegionLocalState::new();
                region_state.set_state(PeerState::Normal);
                {
                    let region = region_state.mut_region();
                    region.set_id(region_id);
                    let peers = peers.iter().enumerate().map(|(i, &sid)| {
                        let mut peer = Peer::new();
                        peer.id = i as u64;
                        peer.store_id = sid;
                        peer
                    });
                    region.set_peers(RepeatedField::from_iter(peers));
                }
                wb2.put_msg_cf(handle2, &region_state_key, &region_state)
                    .unwrap();
            };
            let mock_raft_state = |region_id: u64, last_index: u64, commit_index: u64| {
                let raft_state_key = keys::raft_state_key(region_id);
                let mut raft_state = RaftLocalState::new();
                raft_state.set_last_index(last_index);
                raft_state.mut_hard_state().set_commit(commit_index);
                wb1.put_msg_cf(handle1, &raft_state_key, &raft_state)
                    .unwrap();
            };
            let mock_apply_state = |region_id: u64, apply_index: u64| {
                let raft_apply_key = keys::apply_state_key(region_id);
                let mut apply_state = RaftApplyState::new();
                apply_state.set_applied_index(apply_index);
                wb2.put_msg_cf(handle2, &raft_apply_key, &apply_state)
                    .unwrap();
            };

            for &region_id in &[10, 11, 12] {
                mock_region_state(region_id, &[store_id]);
            }

            // last index < commit index
            mock_raft_state(10, 100, 110);

            // commit index < last index < apply index, or commit index < apply index < last index.
            mock_raft_state(11, 100, 90);
            mock_apply_state(11, 110);
            mock_raft_state(12, 100, 90);
            mock_apply_state(12, 95);

            // region state doesn't contains the peer itself.
            mock_region_state(13, &[]);
        }

        raft_engine.write_opt(wb1, &WriteOptions::new()).unwrap();
        kv_engine.write_opt(wb2, &WriteOptions::new()).unwrap();

        let bad_regions = debugger.bad_regions().unwrap();
        assert_eq!(bad_regions.len(), 4);
        for (i, (region_id, _)) in bad_regions.into_iter().enumerate() {
            assert_eq!(region_id, (10 + i) as u64);
        }
    }

    #[test]
    fn test_modify_tikv_config() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv_engine;

        let db_opts = engine.get_db_options();
        assert_eq!(db_opts.get_max_background_jobs(), 2);
        debugger
            .modify_tikv_config(MODULE::KVDB, "max_background_jobs", "8")
            .unwrap();
        let db_opts = engine.get_db_options();
        assert_eq!(db_opts.get_max_background_jobs(), 8);

        let cf = engine.cf_handle(CF_DEFAULT).unwrap();
        let cf_opts = engine.get_options_cf(cf);
        assert_eq!(cf_opts.get_disable_auto_compactions(), false);
        debugger
            .modify_tikv_config(MODULE::KVDB, "default.disable_auto_compactions", "true")
            .unwrap();
        let cf_opts = engine.get_options_cf(cf);
        assert_eq!(cf_opts.get_disable_auto_compactions(), true);
    }

    #[test]
    fn test_recreate_region() {
        let debugger = new_debugger();
        let engine = debugger.engines.kv_engine.as_ref();

        let metadata = vec![("", "g"), ("g", "m"), ("m", "")];

        for (region_id, (start, end)) in metadata.into_iter().enumerate() {
            let region_id = region_id as u64;
            let cf_raft = engine.cf_handle(CF_RAFT).unwrap();
            let mut region = Region::new();
            region.set_id(region_id);
            region.set_start_key(start.to_owned().into_bytes());
            region.set_end_key(end.to_owned().into_bytes());

            let mut region_state = RegionLocalState::new();
            region_state.set_state(PeerState::Normal);
            region_state.set_region(region);
            let key = keys::region_state_key(region_id);
            engine.put_msg_cf(cf_raft, &key, &region_state).unwrap();
        }

        let remove_region_state = |region_id: u64| {
            let key = keys::region_state_key(region_id);
            let cf_raft = engine.cf_handle(CF_RAFT).unwrap();
            engine.delete_cf(cf_raft, &key).unwrap();
        };

        let mut region = Region::new();
        region.set_id(100);

        region.set_start_key(b"k".to_vec());
        region.set_end_key(b"z".to_vec());
        assert!(debugger.recreate_region(region.clone()).is_err());

        remove_region_state(1);
        remove_region_state(2);
        assert!(debugger.recreate_region(region.clone()).is_ok());
        assert_eq!(get_region_state(engine, 100).get_region(), &region);

        region.set_start_key(b"z".to_vec());
        region.set_end_key(b"".to_vec());
        assert!(debugger.recreate_region(region).is_err());
    }

    #[test]
    fn test_mvcc_checker() {
        let (mut default, mut lock, mut write) = (vec![], vec![], vec![]);
        enum Expect {
            Keep,
            Remove,
        };
        // test check CF_LOCK.
        default.extend(vec![
            // key, start_ts, check
            (b"k4", 100, Expect::Keep),
            (b"k5", 100, Expect::Keep),
        ]);
        lock.extend(vec![
            // key, start_ts, lock_type, short_value, check
            (b"k1", 100, LockType::Put, false, Expect::Remove), // k1: remove orphan lock.
            (b"k2", 100, LockType::Delete, false, Expect::Keep), // k2: Delete doesn't need default.
            (b"k3", 100, LockType::Put, true, Expect::Keep), // k3: short value doesn't need default.
            (b"k4", 100, LockType::Put, false, Expect::Keep), // k4: corresponding default exists.
            (b"k5", 100, LockType::Put, false, Expect::Remove), // k5: duplicated lock and write.
        ]);
        write.extend(vec![
            // key, start_ts, commit_ts, write_type, short_value, check
            (b"k5", 100, 101, WriteType::Put, false, Expect::Keep),
        ]);

        // test match CF_DEFAULT and CF_WRITE.
        default.extend(vec![
            // key, start_ts
            (b"k6", 96, Expect::Remove), // extra default.
            (b"k6", 94, Expect::Keep),   // ok.
            (b"k6", 90, Expect::Remove), // Delete should not have default.
            (b"k6", 88, Expect::Remove), // Default is redundant if write has short value.
        ]);
        write.extend(vec![
            // key, start_ts, commit_ts, write_type, short_value
            (b"k6", 100, 101, WriteType::Put, true, Expect::Keep), // short value doesn't need default.
            (b"k6", 99, 99, WriteType::Rollback, false, Expect::Keep), // rollback doesn't need default.
            (b"k6", 97, 98, WriteType::Delete, false, Expect::Keep), // delete doesn't need default.
            (b"k6", 94, 94, WriteType::Put, false, Expect::Keep),    // ok.
            (b"k6", 92, 93, WriteType::Put, false, Expect::Remove),  // extra write.
            (b"k6", 90, 91, WriteType::Delete, false, Expect::Keep),
            (b"k6", 88, 89, WriteType::Put, true, Expect::Keep),
        ]);

        // Combine problems together.
        default.extend(vec![
            // key, start_ts
            (b"k7", 98, Expect::Remove), // default without lock or write.
            (b"k7", 90, Expect::Remove), // orphan default.
        ]);
        lock.extend(vec![
            // key, start_ts, lock_type, short_value, check
            (b"k7", 100, LockType::Put, false, Expect::Remove), // duplicated lock and write.
        ]);
        write.extend(vec![
            // key, start_ts, commit_ts, write_type, short_value
            (b"k7", 99, 100, WriteType::Put, false, Expect::Remove), // write without default.
            (b"k7", 96, 97, WriteType::Put, true, Expect::Keep),
        ]);

        // Out of range.
        default.extend(vec![
            // key, start_ts
            (b"k8", 100, Expect::Keep),
        ]);
        lock.extend(vec![
            // key, start_ts, lock_type, short_value, check
            (b"k8", 101, LockType::Put, false, Expect::Keep),
        ]);
        write.extend(vec![
            // key, start_ts, commit_ts, write_type, short_value
            (b"k8", 102, 103, WriteType::Put, false, Expect::Keep),
        ]);

        let mut kv = vec![];
        for (key, ts, expect) in default {
            kv.push((
                CF_DEFAULT,
                Key::from_raw(key).append_ts(ts),
                b"v".to_vec(),
                expect,
            ));
        }
        for (key, ts, tp, short_value, expect) in lock {
            let v = if short_value {
                Some(b"v".to_vec())
            } else {
                None
            };
            let lock = Lock::new(tp, vec![], ts, 0, v);
            kv.push((CF_LOCK, Key::from_raw(key), lock.to_bytes(), expect));
        }
        for (key, start_ts, commit_ts, tp, short_value, expect) in write {
            let v = if short_value {
                Some(b"v".to_vec())
            } else {
                None
            };
            let write = Write::new(tp, start_ts, v);
            kv.push((
                CF_WRITE,
                Key::from_raw(key).append_ts(commit_ts),
                write.to_bytes(),
                expect,
            ));
        }

        let path = TempDir::new("test_mvcc_checker").expect("");
        let path_str = path.path().to_str().unwrap();
        let cfs_opts = ALL_CFS
            .into_iter()
            .map(|cf| CFOptions::new(cf, ColumnFamilyOptions::new()))
            .collect();
        let db = Arc::new(new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap());
        // Write initial KVs.
        let wb = WriteBatch::new();
        for &(cf, ref k, ref v, _) in &kv {
            wb.put_cf(
                get_cf_handle(&db, cf).unwrap(),
                &keys::data_key(k.encoded()),
                v,
            ).unwrap();
        }
        db.write(wb).unwrap();
        // Fix problems.
        let mut checker = MvccChecker::new(Arc::clone(&db), b"k", b"k8").unwrap();
        let wb = WriteBatch::new();
        checker.check_mvcc(&wb).unwrap();
        db.write(wb).unwrap();
        // Check result.
        for (cf, k, _, expect) in kv {
            let data =
                db.get_cf(
                    get_cf_handle(&db, cf).unwrap(),
                    &keys::data_key(k.encoded()),
                ).unwrap();
            match expect {
                Expect::Keep => assert!(data.is_some()),
                Expect::Remove => assert!(data.is_none()),
            }
        }
    }
}
