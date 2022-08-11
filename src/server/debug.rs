// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    iter::FromIterator,
    path::Path,
    result,
    sync::Arc,
    thread::{Builder as ThreadBuilder, JoinHandle},
};

use collections::HashSet;
use engine_rocks::{
    raw::{CompactOptions, DBBottommostLevelCompaction, DB},
    util::get_cf_handle,
    Compat, RocksEngine, RocksEngineIterator, RocksMvccProperties, RocksWriteBatch,
};
use engine_traits::{
    Engines, IterOptions, Iterable, Iterator as EngineIterator, Mutable, MvccProperties, Peekable,
    RaftEngine, Range, RangePropertiesExt, SeekKey, SyncMutable, WriteBatch, WriteBatchExt,
    WriteOptions, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE,
};
use kvproto::{
    debugpb::{self, Db as DBType},
    metapb::{PeerRole, Region},
    raft_serverpb::*,
};
use protobuf::Message;
use raft::{self, eraftpb::Entry, RawNode};
use raftstore::{
    coprocessor::get_region_approximate_middle,
    store::{
        util as raftstore_util, write_initial_apply_state, write_initial_raft_state,
        write_peer_state, PeerStorage,
    },
};
use thiserror::Error;
use tikv_util::{
    config::ReadableSize, keybuilder::KeyBuilder, sys::thread::StdThreadBuildWrapper,
    worker::Worker,
};
use txn_types::Key;

pub use crate::storage::mvcc::MvccInfoIterator;
use crate::{
    config::ConfigController,
    server::reset_to_version::ResetToVersionManager,
    storage::mvcc::{Lock, LockType, TimeStamp, Write, WriteRef, WriteType},
};

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument {0:?}")]
    InvalidArgument(String),

    #[error("Not Found {0:?}")]
    NotFound(String),

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

/// Describes the meta information of a Region.
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

/// A thin wrapper of `DBBottommostLevelCompaction`.
#[derive(Copy, Clone, Debug)]
pub struct BottommostLevelCompaction(pub DBBottommostLevelCompaction);

impl<'a> From<Option<&'a str>> for BottommostLevelCompaction {
    fn from(v: Option<&'a str>) -> Self {
        let b = match v {
            Some("skip") => DBBottommostLevelCompaction::Skip,
            Some("force") => DBBottommostLevelCompaction::Force,
            _ => DBBottommostLevelCompaction::IfHaveCompactionFilter,
        };
        BottommostLevelCompaction(b)
    }
}

impl From<debugpb::BottommostLevelCompaction> for BottommostLevelCompaction {
    fn from(v: debugpb::BottommostLevelCompaction) -> Self {
        let b = match v {
            debugpb::BottommostLevelCompaction::Skip => DBBottommostLevelCompaction::Skip,
            debugpb::BottommostLevelCompaction::Force => DBBottommostLevelCompaction::Force,
            debugpb::BottommostLevelCompaction::IfHaveCompactionFilter => {
                DBBottommostLevelCompaction::IfHaveCompactionFilter
            }
        };
        BottommostLevelCompaction(b)
    }
}

impl From<BottommostLevelCompaction> for debugpb::BottommostLevelCompaction {
    fn from(bottommost: BottommostLevelCompaction) -> debugpb::BottommostLevelCompaction {
        match bottommost.0 {
            DBBottommostLevelCompaction::Skip => debugpb::BottommostLevelCompaction::Skip,
            DBBottommostLevelCompaction::Force => debugpb::BottommostLevelCompaction::Force,
            DBBottommostLevelCompaction::IfHaveCompactionFilter => {
                debugpb::BottommostLevelCompaction::IfHaveCompactionFilter
            }
        }
    }
}

#[derive(Clone)]
pub struct Debugger<ER: RaftEngine> {
    engines: Engines<RocksEngine, ER>,
    reset_to_version_manager: ResetToVersionManager,
    cfg_controller: ConfigController,
}

impl<ER: RaftEngine> Debugger<ER> {
    pub fn new(
        engines: Engines<RocksEngine, ER>,
        cfg_controller: ConfigController,
    ) -> Debugger<ER> {
        let reset_to_version_manager = ResetToVersionManager::new(engines.kv.clone());
        Debugger {
            engines,
            reset_to_version_manager,
            cfg_controller,
        }
    }

    pub fn get_engine(&self) -> &Engines<RocksEngine, ER> {
        &self.engines
    }

    /// Get all regions holding region meta data from raft CF in KV storage.
    pub fn get_all_regions_in_store(&self) -> Result<Vec<u64>> {
        let db = &self.engines.kv;
        let cf = CF_RAFT;
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let mut regions = Vec::with_capacity(128);
        box_try!(db.scan_cf(cf, start_key, end_key, false, |key, _| {
            let (id, suffix) = box_try!(keys::decode_region_meta_key(key));
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }
            regions.push(id);
            Ok(true)
        }));
        regions.sort_unstable();
        Ok(regions)
    }

    fn get_db_from_type(&self, db: DBType) -> Result<&Arc<DB>> {
        match db {
            DBType::Kv => Ok(self.engines.kv.as_inner()),
            DBType::Raft => Err(box_err!("Get raft db is not allowed")),
            _ => Err(box_err!("invalid DBType type")),
        }
    }

    pub fn get(&self, db: DBType, cf: &str, key: &[u8]) -> Result<Vec<u8>> {
        validate_db_and_cf(db, cf)?;
        let db = self.get_db_from_type(db)?;
        match db.c().get_value_cf(cf, key) {
            Ok(Some(v)) => Ok(v.to_vec()),
            Ok(None) => Err(Error::NotFound(format!(
                "value for key {:?} in db {:?}",
                key, db
            ))),
            Err(e) => Err(box_err!(e)),
        }
    }

    pub fn raft_log(&self, region_id: u64, log_index: u64) -> Result<Entry> {
        if let Some(e) = box_try!(self.engines.raft.get_entry(region_id, log_index)) {
            return Ok(e);
        }
        Err(Error::NotFound(format!(
            "raft log for region {} at index {}",
            region_id, log_index
        )))
    }

    pub fn region_info(&self, region_id: u64) -> Result<RegionInfo> {
        let raft_state = box_try!(self.engines.raft.get_raft_state(region_id));

        let apply_state_key = keys::apply_state_key(region_id);
        let apply_state = box_try!(
            self.engines
                .kv
                .get_msg_cf::<RaftApplyState>(CF_RAFT, &apply_state_key)
        );

        let region_state_key = keys::region_state_key(region_id);
        let region_state = box_try!(
            self.engines
                .kv
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
            .kv
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
        {
            Ok(Some(region_state)) => {
                let region = region_state.get_region();
                let start_key = &keys::data_key(region.get_start_key());
                let end_key = &keys::data_end_key(region.get_end_key());
                let mut sizes = vec![];
                for cf in cfs {
                    let mut size = 0;
                    box_try!(self.engines.kv.scan_cf(
                        cf.as_ref(),
                        start_key,
                        end_key,
                        false,
                        |k, v| {
                            size += k.len() + v.len();
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
    pub fn scan_mvcc(
        &self,
        start: &[u8],
        end: &[u8],
        limit: u64,
    ) -> Result<MvccInfoIterator<RocksEngineIterator>> {
        if end.is_empty() && limit == 0 {
            return Err(Error::InvalidArgument("no limit and to_key".to_owned()));
        }
        MvccInfoIterator::new(
            |cf, opts| {
                let kv = &self.engines.kv;
                kv.iterator_cf_opt(cf, opts).map_err(|e| box_err!(e))
            },
            if start.is_empty() { None } else { Some(start) },
            if end.is_empty() { None } else { Some(end) },
            limit as usize,
        )
        .map_err(|e| box_err!(e))
    }

    /// Scan raw keys for given range `[start, end)` in given cf.
    pub fn raw_scan(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        cf: &str,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let db = &self.engines.kv;
        let end = if !end.is_empty() {
            Some(KeyBuilder::from_vec(end.to_vec(), 0, 0))
        } else {
            None
        };
        let iter_opt =
            IterOptions::new(Some(KeyBuilder::from_vec(start.to_vec(), 0, 0)), end, false);
        let mut iter = box_try!(db.iterator_cf_opt(cf, iter_opt));
        if !iter.seek_to_first().unwrap() {
            return Ok(vec![]);
        }

        let mut res = vec![(iter.key().to_vec(), iter.value().to_vec())];
        while res.len() < limit && iter.next().unwrap() {
            res.push((iter.key().to_vec(), iter.value().to_vec()));
        }

        Ok(res)
    }

    /// Compact the cf[start..end) in the db.
    pub fn compact(
        &self,
        db: DBType,
        cf: &str,
        start: &[u8],
        end: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) -> Result<()> {
        validate_db_and_cf(db, cf)?;
        let db = self.get_db_from_type(db)?;
        let handle = box_try!(get_cf_handle(db, cf));
        let start = if start.is_empty() { None } else { Some(start) };
        let end = if end.is_empty() { None } else { Some(end) };
        info!("Debugger starts manual compact"; "db" => ?db, "cf" => cf);
        let mut opts = CompactOptions::new();
        opts.set_max_subcompactions(threads as i32);
        opts.set_exclusive_manual_compaction(false);
        opts.set_bottommost_level_compaction(bottommost.0);
        db.compact_range_cf_opt(handle, &opts, start, end);
        info!("Debugger finishes manual compact"; "db" => ?db, "cf" => cf);
        Ok(())
    }

    /// Set regions to tombstone by manual, and apply other status(such as
    /// peers, version, and key range) from `region` which comes from PD normally.
    pub fn set_region_tombstone(&self, regions: Vec<Region>) -> Result<Vec<(u64, Error)>> {
        let store_id = self.get_store_ident()?.get_store_id();
        let db = &self.engines.kv;
        let mut wb = db.write_batch();

        let mut errors = Vec::with_capacity(regions.len());
        for region in regions {
            let region_id = region.get_id();
            if let Err(e) = set_region_tombstone(db.as_inner(), store_id, region, &mut wb) {
                errors.push((region_id, e));
            }
        }

        if errors.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            box_try!(wb.write_opt(&write_opts));
        }
        Ok(errors)
    }

    pub fn set_region_tombstone_by_id(&self, regions: Vec<u64>) -> Result<Vec<(u64, Error)>> {
        let db = &self.engines.kv;
        let mut wb = db.write_batch();
        let mut errors = Vec::with_capacity(regions.len());
        for region_id in regions {
            let key = keys::region_state_key(region_id);
            let region_state = match db.get_msg_cf::<RegionLocalState>(CF_RAFT, &key) {
                Ok(Some(state)) => state,
                Ok(None) => {
                    let error = box_err!("{} region local state not exists", region_id);
                    errors.push((region_id, error));
                    continue;
                }
                Err(_) => {
                    let error = box_err!("{} gets region local state fail", region_id);
                    errors.push((region_id, error));
                    continue;
                }
            };
            if region_state.get_state() == PeerState::Tombstone {
                info!("skip {} because it's already tombstone", region_id);
                continue;
            }
            let region = &region_state.get_region();
            write_peer_state(&mut wb, region, PeerState::Tombstone, None).unwrap();
        }

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        wb.write_opt(&write_opts).unwrap();
        Ok(errors)
    }

    pub fn recover_regions(
        &self,
        regions: Vec<Region>,
        read_only: bool,
    ) -> Result<Vec<(u64, Error)>> {
        let db = &self.engines.kv;

        let mut errors = Vec::with_capacity(regions.len());
        for region in regions {
            let region_id = region.get_id();
            if let Err(e) = recover_mvcc_for_range(
                db.as_inner(),
                region.get_start_key(),
                region.get_end_key(),
                read_only,
                0,
            ) {
                errors.push((region_id, e));
            }
        }

        Ok(errors)
    }

    pub fn recover_all(&self, threads: usize, read_only: bool) -> Result<()> {
        let db = self.engines.kv.clone();

        info!("Calculating split keys...");
        let split_keys = divide_db(db.as_inner(), threads)
            .unwrap()
            .into_iter()
            .map(|k| {
                let k = Key::from_encoded(keys::origin_key(&k).to_vec())
                    .truncate_ts()
                    .unwrap();
                k.as_encoded().clone()
            });

        let mut range_borders = vec![b"".to_vec()];
        range_borders.extend(split_keys);
        range_borders.push(b"".to_vec());

        let mut handles = Vec::new();

        for thread_index in 0..range_borders.len() - 1 {
            let db = db.clone();
            let start_key = range_borders[thread_index].clone();
            let end_key = range_borders[thread_index + 1].clone();

            let props = tikv_util::thread_group::current_properties();
            let thread = ThreadBuilder::new()
                .name(format!("mvcc-recover-thread-{}", thread_index))
                .spawn_wrapper(move || {
                    tikv_util::thread_group::set_properties(props);
                    tikv_alloc::add_thread_memory_accessor();
                    info!(
                        "thread {}: started on range [{}, {})",
                        thread_index,
                        log_wrappers::Value::key(&start_key),
                        log_wrappers::Value::key(&end_key)
                    );

                    let result = recover_mvcc_for_range(
                        db.as_inner(),
                        &start_key,
                        &end_key,
                        read_only,
                        thread_index,
                    );
                    tikv_alloc::remove_thread_memory_accessor();
                    result
                })
                .unwrap();

            handles.push(thread);
        }

        let res = handles
            .into_iter()
            .map(|h: JoinHandle<Result<()>>| h.join())
            .map(|r| {
                if let Err(e) = &r {
                    error!("{:?}", e);
                }
                r
            })
            .all(|r| r.is_ok());
        if res {
            Ok(())
        } else {
            Err(box_err!("Not all threads finished successfully."))
        }
    }

    pub fn bad_regions(&self) -> Result<Vec<(u64, Error)>> {
        let mut res = Vec::new();

        let from = keys::REGION_META_MIN_KEY.to_owned();
        let to = keys::REGION_META_MAX_KEY.to_owned();
        let readopts = IterOptions::new(
            Some(KeyBuilder::from_vec(from.clone(), 0, 0)),
            Some(KeyBuilder::from_vec(to, 0, 0)),
            false,
        );
        let mut iter = box_try!(self.engines.kv.iterator_cf_opt(CF_RAFT, readopts));
        iter.seek(SeekKey::from(from.as_ref())).unwrap();

        let fake_snap_worker = Worker::new("fake-snap-worker").lazy_build("fake-snap");
        let fake_raftlog_fetch_worker =
            Worker::new("fake-raftlog-fetch-worker").lazy_build("fake-raftlog-fetch");

        let check_value = |value: &[u8]| -> Result<()> {
            let mut local_state = RegionLocalState::default();
            box_try!(local_state.merge_from_bytes(value));

            match local_state.get_state() {
                PeerState::Tombstone | PeerState::Applying => return Ok(()),
                _ => {}
            }

            let region = local_state.get_region();
            let store_id = self.get_store_ident()?.get_store_id();

            let peer_id = raftstore_util::find_peer(region, store_id)
                .map(|peer| peer.get_id())
                .ok_or_else(|| {
                    Error::Other("RegionLocalState doesn't contains peer itself".into())
                })?;

            let tag = format!("[region {}] {}", region.get_id(), peer_id);
            let peer_storage = box_try!(PeerStorage::<RocksEngine, ER>::new(
                self.engines.clone(),
                region,
                fake_snap_worker.scheduler(),
                fake_raftlog_fetch_worker.scheduler(),
                peer_id,
                tag,
            ));

            let raft_cfg = raft::Config {
                id: peer_id,
                election_tick: 10,
                heartbeat_tick: 2,
                max_size_per_msg: ReadableSize::mb(1).0,
                max_inflight_msgs: 256,
                check_quorum: true,
                skip_bcast_commit: true,
                ..Default::default()
            };

            box_try!(RawNode::new(
                &raft_cfg,
                peer_storage,
                &slog_global::get_global()
            ));
            Ok(())
        };

        while box_try!(iter.valid()) {
            let (key, value) = (iter.key(), iter.value());
            if let Ok((region_id, suffix)) = keys::decode_region_meta_key(key) {
                if suffix != keys::REGION_STATE_SUFFIX {
                    box_try!(iter.next());
                    continue;
                }
                if let Err(e) = check_value(value) {
                    res.push((region_id, e));
                }
            }
            box_try!(iter.next());
        }
        Ok(res)
    }

    pub fn remove_failed_stores(
        &self,
        store_ids: Vec<u64>,
        region_ids: Option<Vec<u64>>,
        promote_learner: bool,
    ) -> Result<()> {
        let store_id = self.get_store_ident()?.get_store_id();
        if store_ids.iter().any(|&s| s == store_id) {
            let msg = format!("Store {} in the failed list", store_id);
            return Err(Error::Other(msg.into()));
        }
        let mut wb = RocksWriteBatch::new(self.engines.kv.as_inner().clone());
        let store_ids = HashSet::<u64>::from_iter(store_ids);

        {
            let remove_stores = |key: &[u8], value: &[u8], kv_wb: &mut RocksWriteBatch| {
                let (_, suffix_type) = box_try!(keys::decode_region_meta_key(key));
                if suffix_type != keys::REGION_STATE_SUFFIX {
                    return Ok(());
                }

                let mut region_state = RegionLocalState::default();
                box_try!(region_state.merge_from_bytes(value));
                if region_state.get_state() == PeerState::Tombstone {
                    return Ok(());
                }

                let mut new_peers = region_state.get_region().get_peers().to_owned();
                new_peers.retain(|peer| !store_ids.contains(&peer.get_store_id()));

                let region_id = region_state.get_region().get_id();
                let old_peers = region_state.mut_region().take_peers();

                if promote_learner {
                    if new_peers
                        .iter()
                        .filter(|peer| peer.get_role() != PeerRole::Learner)
                        .count()
                        != 0
                    {
                        // no need to promote learner, do nothing
                    } else if new_peers
                        .iter()
                        .filter(|peer| peer.get_role() == PeerRole::Learner)
                        .count()
                        > 1
                    {
                        error!(
                            "failed to promote learner due to multiple learners, skip promote learner";
                            "region_id" => region_id,
                        )
                    } else {
                        for peer in &mut new_peers {
                            match peer.get_role() {
                                PeerRole::Voter
                                | PeerRole::IncomingVoter
                                | PeerRole::DemotingVoter => {}
                                PeerRole::Learner => {
                                    info!(
                                        "promote learner";
                                        "region_id" => region_id,
                                        "peer_id" => peer.get_id(),
                                    );
                                    peer.set_role(PeerRole::Voter);
                                }
                            }
                        }
                    }
                }
                info!(
                    "peers changed";
                    "region_id" => region_id,
                    "old_peers" => ?old_peers,
                    "new_peers" => ?new_peers,
                );
                // We need to leave epoch untouched to avoid inconsistency.
                region_state.mut_region().set_peers(new_peers.into());
                box_try!(kv_wb.put_msg_cf(CF_RAFT, key, &region_state));
                Ok(())
            };

            if let Some(region_ids) = region_ids {
                let kv = &self.engines.kv;
                for region_id in region_ids {
                    let key = keys::region_state_key(region_id);
                    if let Some(value) = box_try!(kv.get_value_cf(CF_RAFT, &key)) {
                        box_try!(remove_stores(&key, &value, &mut wb));
                    } else {
                        let msg = format!("No such region {} on the store", region_id);
                        return Err(Error::Other(msg.into()));
                    }
                }
            } else {
                box_try!(self.engines.kv.scan_cf(
                    CF_RAFT,
                    keys::REGION_META_MIN_KEY,
                    keys::REGION_META_MAX_KEY,
                    false,
                    |key, value| remove_stores(key, value, &mut wb).map(|_| true)
                ));
            }
        }

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        box_try!(wb.write_opt(&write_opts));
        Ok(())
    }

    pub fn drop_unapplied_raftlog(&self, region_ids: Option<Vec<u64>>) -> Result<()> {
        let kv = &self.engines.kv;
        let raft = &self.engines.raft;

        let region_ids = region_ids.unwrap_or(self.get_all_regions_in_store()?);
        for region_id in region_ids {
            let region_state = self.region_info(region_id)?;

            // It's safe to unwrap region_local_state here, because get_all_regions_in_store()
            // guarantees that the region state exists in kvdb.
            if region_state.region_local_state.unwrap().state == PeerState::Tombstone {
                continue;
            }

            let old_raft_local_state = region_state.raft_local_state.ok_or_else(|| {
                Error::Other(format!("No RaftLocalState found for region {}", region_id).into())
            })?;
            let old_raft_apply_state = region_state.raft_apply_state.ok_or_else(|| {
                Error::Other(format!("No RaftApplyState found for region {}", region_id).into())
            })?;

            let applied_index = old_raft_apply_state.applied_index;
            let commit_index = old_raft_apply_state.commit_index;
            let last_index = old_raft_local_state.last_index;

            if last_index == applied_index && commit_index == applied_index {
                continue;
            }

            let new_raft_local_state = RaftLocalState {
                last_index: applied_index,
                ..old_raft_local_state.clone()
            };
            let new_raft_apply_state = RaftApplyState {
                commit_index: applied_index,
                ..old_raft_apply_state.clone()
            };

            info!(
                "dropping unapplied raft log";
                "region_id" => region_id,
                "old_raft_local_state" => ?old_raft_local_state,
                "new_raft_local_state" => ?new_raft_local_state,
                "old_raft_apply_state" => ?old_raft_apply_state,
                "new_raft_apply_state" => ?new_raft_apply_state,
            );

            // flush the changes
            box_try!(kv.put_msg_cf(
                CF_RAFT,
                &keys::apply_state_key(region_id),
                &new_raft_apply_state
            ));
            box_try!(raft.put_raft_state(region_id, &new_raft_local_state));
            let deleted_logs = box_try!(raft.gc(region_id, applied_index + 1, last_index + 1));
            raft.sync().unwrap();
            kv.sync().unwrap();

            info!(
                "dropped unapplied raft log";
                "region_id" => region_id,
                "old_raft_local_state" => ?old_raft_local_state,
                "new_raft_local_state" => ?new_raft_local_state,
                "old_raft_apply_state" => ?old_raft_apply_state,
                "new_raft_apply_state" => ?new_raft_apply_state,
                "deleted logs" => deleted_logs,
            );
        }

        Ok(())
    }

    pub fn recreate_region(&self, region: Region) -> Result<()> {
        let region_id = region.get_id();
        let kv = &self.engines.kv;
        let raft = &self.engines.raft;

        let mut kv_wb = self.engines.kv.write_batch();
        let mut raft_wb = self.engines.raft.log_batch(0);

        if region.get_start_key() >= region.get_end_key() && !region.get_end_key().is_empty() {
            return Err(box_err!("Bad region: {:?}", region));
        }

        box_try!(self.engines.kv.scan_cf(
            CF_RAFT,
            keys::REGION_META_MIN_KEY,
            keys::REGION_META_MAX_KEY,
            false,
            |key, value| {
                let (_, suffix_type) = box_try!(keys::decode_region_meta_key(key));
                if suffix_type != keys::REGION_STATE_SUFFIX {
                    return Ok(true);
                }

                let mut region_state = RegionLocalState::default();
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
        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region);
        let key = keys::region_state_key(region_id);
        if box_try!(kv.get_msg_cf::<RegionLocalState>(CF_RAFT, &key)).is_some() {
            return Err(Error::Other(
                "Store already has the RegionLocalState".into(),
            ));
        }
        box_try!(kv_wb.put_msg_cf(CF_RAFT, &key, &region_state));

        // RaftApplyState.
        let key = keys::apply_state_key(region_id);
        if box_try!(kv.get_msg_cf::<RaftApplyState>(CF_RAFT, &key)).is_some() {
            return Err(Error::Other("Store already has the RaftApplyState".into()));
        }
        box_try!(write_initial_apply_state(&mut kv_wb, region_id));

        // RaftLocalState.
        if box_try!(raft.get_raft_state(region_id)).is_some() {
            return Err(Error::Other("Store already has the RaftLocalState".into()));
        }
        box_try!(write_initial_raft_state(&mut raft_wb, region_id));

        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        box_try!(kv_wb.write_opt(&write_opts));
        box_try!(self.engines.raft.consume(&mut raft_wb, true));
        Ok(())
    }

    pub fn get_store_ident(&self) -> Result<StoreIdent> {
        let db = &self.engines.kv;
        db.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)
            .map_err(|e| box_err!(e))
            .and_then(|ident| match ident {
                Some(ident) => Ok(ident),
                None => Err(Error::NotFound("No store ident key".to_owned())),
            })
    }

    pub fn modify_tikv_config(&self, config_name: &str, config_value: &str) -> Result<()> {
        if let Err(e) = self.cfg_controller.update_config(config_name, config_value) {
            return Err(Error::Other(
                format!("failed to update config, err: {:?}", e).into(),
            ));
        }
        Ok(())
    }

    fn get_region_state(&self, region_id: u64) -> Result<RegionLocalState> {
        let region_state_key = keys::region_state_key(region_id);
        let region_state = box_try!(
            self.engines
                .kv
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
        let start = keys::enc_start_key(region);
        let end = keys::enc_end_key(region);

        let mut res = dump_write_cf_properties(self.engines.kv.as_inner(), &start, &end)?;
        let mut res1 = dump_default_cf_properties(self.engines.kv.as_inner(), &start, &end)?;
        res.append(&mut res1);

        let middle_key = match box_try!(get_region_approximate_middle(&self.engines.kv, region)) {
            Some(data_key) => keys::origin_key(&data_key).to_vec(),
            None => Vec::new(),
        };

        res.push((
            "region.start_key".to_owned(),
            hex::encode(&region.start_key),
        ));
        res.push(("region.end_key".to_owned(), hex::encode(&region.end_key)));
        res.push((
            "region.middle_key_by_approximate_size".to_owned(),
            hex::encode(&middle_key),
        ));

        Ok(res)
    }

    pub fn get_range_properties(&self, start: &[u8], end: &[u8]) -> Result<Vec<(String, String)>> {
        let mut props = dump_write_cf_properties(
            self.engines.kv.as_inner(),
            &keys::data_key(start),
            &keys::data_end_key(end),
        )?;
        let mut props1 = dump_default_cf_properties(
            self.engines.kv.as_inner(),
            &keys::data_key(start),
            &keys::data_end_key(end),
        )?;
        props.append(&mut props1);
        Ok(props)
    }

    pub fn reset_to_version(&self, version: u64) {
        self.reset_to_version_manager.start(version.into());
    }
}

fn dump_default_cf_properties(
    db: &Arc<DB>,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<(String, String)>> {
    let mut num_entries = 0; // number of Rocksdb K/V entries.

    let collection = box_try!(db.c().get_range_properties_cf(CF_DEFAULT, start, end));
    let num_files = collection.len();

    for (_, v) in collection.iter() {
        num_entries += v.num_entries();
    }
    let sst_files = collection
        .iter()
        .map(|(k, _)| {
            Path::new(&*k)
                .file_name()
                .map(|f| f.to_str().unwrap())
                .unwrap_or(&*k)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join(", ");

    let res = vec![
        ("defaultcf.num_entries".to_owned(), num_entries.to_string()),
        ("defaultcf.num_files".to_owned(), num_files.to_string()),
        ("defaultcf.sst_files".to_owned(), sst_files),
    ];
    Ok(res)
}

fn dump_write_cf_properties(
    db: &Arc<DB>,
    start: &[u8],
    end: &[u8],
) -> Result<Vec<(String, String)>> {
    let mut num_entries = 0; // number of Rocksdb K/V entries.

    let collection = box_try!(db.c().get_range_properties_cf(CF_WRITE, start, end));
    let num_files = collection.len();

    let mut mvcc_properties = MvccProperties::new();
    for (_, v) in collection.iter() {
        num_entries += v.num_entries();
        let mvcc = box_try!(RocksMvccProperties::decode(v.user_collected_properties()));
        mvcc_properties.add(&mvcc);
    }

    let sst_files = collection
        .iter()
        .map(|(k, _)| {
            Path::new(&*k)
                .file_name()
                .map(|f| f.to_str().unwrap())
                .unwrap_or(&*k)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join(", ");

    let mut res: Vec<(String, String)> = [
        (
            "mvcc.min_ts",
            if mvcc_properties.min_ts == TimeStamp::max() {
                0
            } else {
                mvcc_properties.min_ts.into_inner()
            },
        ),
        ("mvcc.max_ts", mvcc_properties.max_ts.into_inner()),
        ("mvcc.num_rows", mvcc_properties.num_rows),
        ("mvcc.num_puts", mvcc_properties.num_puts),
        ("mvcc.num_deletes", mvcc_properties.num_deletes),
        ("mvcc.num_versions", mvcc_properties.num_versions),
        ("mvcc.max_row_versions", mvcc_properties.max_row_versions),
    ]
    .iter()
    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
    .collect();

    // Entries and delete marks of RocksDB.
    let num_deletes = num_entries - mvcc_properties.num_versions;
    res.push(("writecf.num_entries".to_owned(), num_entries.to_string()));
    res.push(("writecf.num_deletes".to_owned(), num_deletes.to_string()));

    // count and list of files.
    res.push(("writecf.num_files".to_owned(), num_files.to_string()));
    res.push(("writecf.sst_files".to_owned(), sst_files));

    Ok(res)
}

fn recover_mvcc_for_range(
    db: &Arc<DB>,
    start_key: &[u8],
    end_key: &[u8],
    read_only: bool,
    thread_index: usize,
) -> Result<()> {
    let mut mvcc_checker = box_try!(MvccChecker::new(Arc::clone(db), start_key, end_key));
    mvcc_checker.thread_index = thread_index;

    let wb_limit: usize = 10240;

    loop {
        let mut wb = RocksWriteBatch::new(db.clone());
        mvcc_checker.check_mvcc(&mut wb, Some(wb_limit))?;

        let batch_size = wb.count();

        if !read_only {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            box_try!(wb.write_opt(&write_opts));
        } else {
            info!("thread {}: skip write {} rows", thread_index, batch_size);
        }

        info!(
            "thread {}: total fix default: {}, lock: {}, write: {}",
            thread_index,
            mvcc_checker.default_fix_count,
            mvcc_checker.lock_fix_count,
            mvcc_checker.write_fix_count
        );

        if batch_size < wb_limit {
            info!("thread {} has finished working.", thread_index);
            return Ok(());
        }
    }
}

pub struct MvccChecker {
    lock_iter: RocksEngineIterator,
    default_iter: RocksEngineIterator,
    write_iter: RocksEngineIterator,
    scan_count: usize,
    lock_fix_count: usize,
    default_fix_count: usize,
    write_fix_count: usize,
    pub thread_index: usize,
}

impl MvccChecker {
    fn new(db: Arc<DB>, start_key: &[u8], end_key: &[u8]) -> Result<Self> {
        let start_key = keys::data_key(start_key);
        let end_key = keys::data_end_key(end_key);
        let gen_iter = |cf: &str| -> Result<_> {
            let from = start_key.clone();
            let to = end_key.clone();
            let readopts = IterOptions::new(
                Some(KeyBuilder::from_vec(from, 0, 0)),
                Some(KeyBuilder::from_vec(to, 0, 0)),
                false,
            );
            let mut iter = box_try!(db.c().iterator_cf_opt(cf, readopts));
            iter.seek(SeekKey::Start).unwrap();
            Ok(iter)
        };

        Ok(MvccChecker {
            write_iter: gen_iter(CF_WRITE)?,
            lock_iter: gen_iter(CF_LOCK)?,
            default_iter: gen_iter(CF_DEFAULT)?,
            scan_count: 0,
            lock_fix_count: 0,
            default_fix_count: 0,
            write_fix_count: 0,
            thread_index: 0,
        })
    }

    fn min_key(
        key: Option<Vec<u8>>,
        iter: &RocksEngineIterator,
        f: fn(&[u8]) -> &[u8],
    ) -> Option<Vec<u8>> {
        let iter_key = if iter.valid().unwrap() {
            Some(f(keys::origin_key(iter.key())).to_vec())
        } else {
            None
        };
        match (key, iter_key) {
            (Some(a), Some(b)) => {
                if a < b {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    pub fn check_mvcc(&mut self, wb: &mut RocksWriteBatch, limit: Option<usize>) -> Result<()> {
        loop {
            // Find min key in the 3 CFs.
            let mut key = MvccChecker::min_key(None, &self.default_iter, |k| {
                Key::truncate_ts_for(k).unwrap()
            });
            key = MvccChecker::min_key(key, &self.lock_iter, |k| k);
            key = MvccChecker::min_key(key, &self.write_iter, |k| Key::truncate_ts_for(k).unwrap());

            match key {
                Some(key) => self.check_mvcc_key(wb, key.as_ref())?,
                None => return Ok(()),
            }

            if let Some(limit) = limit {
                if wb.count() >= limit {
                    return Ok(());
                }
            }
        }
    }

    fn check_mvcc_key(&mut self, wb: &mut RocksWriteBatch, key: &[u8]) -> Result<()> {
        self.scan_count += 1;
        if self.scan_count % 1_000_000 == 0 {
            info!(
                "thread {}: scan {} rows",
                self.thread_index, self.scan_count
            );
        }

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
                // All write records's ts should be less than the for_update_ts (if the
                // lock is from a pessimistic transaction) or the ts of the lock.
                let (kind, check_ts) = if l.for_update_ts >= l.ts {
                    ("for_update_ts", l.for_update_ts)
                } else {
                    ("ts", l.ts)
                };

                if let Some((commit_ts, _)) = write {
                    if check_ts <= commit_ts {
                        info!(
                            "thread {}: LOCK {} is less than WRITE ts, key: {}, {}: {}, commit_ts: {}",
                            self.thread_index,
                            kind,
                            log_wrappers::Value::key(key),
                            kind,
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
                            info!(
                                "thread {}: no corresponding DEFAULT record for LOCK, key: {}, lock_ts: {}",
                                self.thread_index,
                                log_wrappers::Value::key(key),
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
                info!(
                    "thread {}: orphan DEFAULT record, key: {}, start_ts: {}",
                    self.thread_index,
                    log_wrappers::Value::key(key),
                    default.unwrap()
                );
                self.delete(wb, CF_DEFAULT, key, default)?;
                self.default_fix_count += 1;
            }

            if next_write {
                if let Some((commit_ts, ref w)) = write {
                    info!(
                        "thread {}: no corresponding DEFAULT record for WRITE, key: {}, start_ts: {}, commit_ts: {}",
                        self.thread_index,
                        log_wrappers::Value::key(key),
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
        if self.lock_iter.valid().unwrap() && keys::origin_key(self.lock_iter.key()) == key {
            let lock = box_try!(Lock::parse(self.lock_iter.value()));
            self.lock_iter.next().unwrap();
            return Ok(Some(lock));
        }
        Ok(None)
    }

    fn next_default(&mut self, key: &[u8]) -> Result<Option<TimeStamp>> {
        if self.default_iter.valid().unwrap()
            && box_try!(Key::truncate_ts_for(keys::origin_key(
                self.default_iter.key()
            ))) == key
        {
            let start_ts = box_try!(Key::decode_ts_from(keys::origin_key(
                self.default_iter.key()
            )));
            self.default_iter.next().unwrap();
            return Ok(Some(start_ts));
        }
        Ok(None)
    }

    fn next_write(&mut self, key: &[u8]) -> Result<Option<(TimeStamp, Write)>> {
        if self.write_iter.valid().unwrap()
            && box_try!(Key::truncate_ts_for(keys::origin_key(
                self.write_iter.key()
            ))) == key
        {
            let write = box_try!(WriteRef::parse(self.write_iter.value())).to_owned();
            let commit_ts = box_try!(Key::decode_ts_from(keys::origin_key(self.write_iter.key())));
            self.write_iter.next().unwrap();
            return Ok(Some((commit_ts, write)));
        }
        Ok(None)
    }

    fn delete(
        &mut self,
        wb: &mut RocksWriteBatch,
        cf: &str,
        key: &[u8],
        ts: Option<TimeStamp>,
    ) -> Result<()> {
        match ts {
            Some(ts) => {
                let key = Key::from_encoded_slice(key).append_ts(ts);
                box_try!(wb.delete_cf(cf, &keys::data_key(key.as_encoded())));
            }
            None => box_try!(wb.delete_cf(cf, &keys::data_key(key))),
        };
        Ok(())
    }
}

fn region_overlap(r1: &Region, r2: &Region) -> bool {
    let (start_key_1, start_key_2) = (r1.get_start_key(), r2.get_start_key());
    let (end_key_1, end_key_2) = (r1.get_end_key(), r2.get_end_key());
    (start_key_1 < end_key_2 || end_key_2.is_empty())
        && (start_key_2 < end_key_1 || end_key_1.is_empty())
}

fn validate_db_and_cf(db: DBType, cf: &str) -> Result<()> {
    match (db, cf) {
        (DBType::Kv, CF_DEFAULT)
        | (DBType::Kv, CF_WRITE)
        | (DBType::Kv, CF_LOCK)
        | (DBType::Kv, CF_RAFT)
        | (DBType::Raft, CF_DEFAULT) => Ok(()),
        _ => Err(Error::InvalidArgument(format!(
            "invalid cf {:?} for db {:?}",
            cf, db
        ))),
    }
}

fn set_region_tombstone(
    db: &Arc<DB>,
    store_id: u64,
    region: Region,
    wb: &mut RocksWriteBatch,
) -> Result<()> {
    let id = region.get_id();
    let key = keys::region_state_key(id);

    let region_state = db
        .c()
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
        return Err(box_err!(
            "invalid conf_ver: please make sure you have removed the peer by PD"
        ));
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

    box_try!(write_peer_state(wb, &region, PeerState::Tombstone, None));
    Ok(())
}

fn divide_db(db: &Arc<DB>, parts: usize) -> raftstore::Result<Vec<Vec<u8>>> {
    // Empty start and end key cover all range.
    let start = keys::data_key(b"");
    let end = keys::data_end_key(b"");
    let range = Range::new(&start, &end);
    Ok(box_try!(
        RocksEngine::from_db(db.clone()).get_range_approximate_split_keys(range, parts - 1)
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine_rocks::{
        raw::{ColumnFamilyOptions, DBOptions},
        raw_util::{new_engine_opt, CFOptions},
        RocksEngine,
    };
    use engine_traits::{Mutable, SyncMutable, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
    use kvproto::{
        kvrpcpb::ApiVersion,
        metapb::{Peer, PeerRole, Region},
    };
    use raft::eraftpb::EntryType;
    use tempfile::Builder;

    use super::*;
    use crate::storage::mvcc::{Lock, LockType};

    fn init_region_state(
        engine: &Arc<DB>,
        region_id: u64,
        stores: &[u64],
        mut learner: usize,
    ) -> Region {
        let mut region = Region::default();
        region.set_id(region_id);
        for (i, &store_id) in stores.iter().enumerate() {
            let mut peer = Peer::default();
            peer.set_id(i as u64);
            peer.set_store_id(store_id);
            if learner > 0 {
                peer.set_role(PeerRole::Learner);
                learner -= 1;
            }
            region.mut_peers().push(peer);
        }
        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region.clone());
        let key = keys::region_state_key(region_id);
        engine.c().put_msg_cf(CF_RAFT, &key, &region_state).unwrap();
        region
    }

    fn init_raft_state(
        kv_engine: &RocksEngine,
        raft_engine: &RocksEngine,
        region_id: u64,
        last_index: u64,
        commit_index: u64,
        applied_index: u64,
    ) {
        let apply_state_key = keys::apply_state_key(region_id);
        let mut apply_state = RaftApplyState::default();
        apply_state.set_applied_index(applied_index);
        apply_state.set_commit_index(commit_index);
        kv_engine
            .put_msg_cf(CF_RAFT, &apply_state_key, &apply_state)
            .unwrap();

        let raft_state_key = keys::raft_state_key(region_id);
        let mut raft_state = RaftLocalState::default();
        raft_state.set_last_index(last_index);
        raft_engine.put_msg(&raft_state_key, &raft_state).unwrap();
    }

    fn get_region_state(engine: &Arc<DB>, region_id: u64) -> RegionLocalState {
        let key = keys::region_state_key(region_id);
        engine
            .c()
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
            &new_region(b"b", b"y"),
        ));
        assert!(region_overlap(
            &new_region(b"a", b"n"),
            &new_region(b"m", b"z"),
        ));
        assert!(!region_overlap(
            &new_region(b"a", b"m"),
            &new_region(b"n", b"z"),
        ));

        // For the first or last region.
        assert!(region_overlap(
            &new_region(b"m", b""),
            &new_region(b"a", b"n"),
        ));
        assert!(region_overlap(
            &new_region(b"a", b"n"),
            &new_region(b"m", b""),
        ));
        assert!(region_overlap(
            &new_region(b"", b""),
            &new_region(b"m", b""),
        ));
        assert!(!region_overlap(
            &new_region(b"a", b"m"),
            &new_region(b"n", b""),
        ));
    }

    #[test]
    fn test_validate_db_and_cf() {
        let valid_cases = vec![
            (DBType::Kv, CF_DEFAULT),
            (DBType::Kv, CF_WRITE),
            (DBType::Kv, CF_LOCK),
            (DBType::Kv, CF_RAFT),
            (DBType::Raft, CF_DEFAULT),
        ];
        for (db, cf) in valid_cases {
            validate_db_and_cf(db, cf).unwrap();
        }

        let invalid_cases = vec![
            (DBType::Raft, CF_WRITE),
            (DBType::Raft, CF_LOCK),
            (DBType::Raft, CF_RAFT),
            (DBType::Invalid, CF_DEFAULT),
            (DBType::Invalid, "BAD_CF"),
        ];
        for (db, cf) in invalid_cases {
            validate_db_and_cf(db, cf).unwrap_err();
        }
    }

    fn new_debugger() -> Debugger<RocksEngine> {
        let tmp = Builder::new().prefix("test_debug").tempdir().unwrap();
        let path = tmp.path().to_str().unwrap();
        let engine = Arc::new(
            engine_rocks::raw_util::new_engine_opt(
                path,
                DBOptions::new(),
                vec![
                    CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_WRITE, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_LOCK, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_RAFT, ColumnFamilyOptions::new()),
                ],
            )
            .unwrap(),
        );

        let engines = Engines::new(
            RocksEngine::from_db(Arc::clone(&engine)),
            RocksEngine::from_db(engine),
        );
        Debugger::new(engines, ConfigController::default())
    }

    impl Debugger<RocksEngine> {
        fn set_store_id(&self, store_id: u64) {
            let mut ident = self.get_store_ident().unwrap_or_default();
            ident.set_store_id(store_id);
            let db = &self.engines.kv;
            db.put_msg(keys::STORE_IDENT_KEY, &ident).unwrap();
        }

        fn set_cluster_id(&self, cluster_id: u64) {
            let mut ident = self.get_store_ident().unwrap_or_default();
            ident.set_cluster_id(cluster_id);
            let db = &self.engines.kv;
            db.put_msg(keys::STORE_IDENT_KEY, &ident).unwrap();
        }

        fn set_store_api_version(&self, api_version: ApiVersion) {
            let mut ident = self.get_store_ident().unwrap_or_default();
            ident.set_api_version(api_version);
            let db = &self.engines.kv;
            db.put_msg(keys::STORE_IDENT_KEY, &ident).unwrap();
        }
    }

    #[test]
    fn test_get() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv;
        let (k, v) = (b"k", b"v");
        engine.put(k, v).unwrap();
        assert_eq!(&*engine.get_value(k).unwrap().unwrap(), v);

        let got = debugger.get(DBType::Kv, CF_DEFAULT, k).unwrap();
        assert_eq!(&got, v);

        match debugger.get(DBType::Kv, CF_DEFAULT, b"foo") {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }

    #[test]
    fn test_raft_log() {
        let debugger = new_debugger();
        let engine = &debugger.engines.raft;
        let (region_id, log_index) = (1, 1);
        let key = keys::raft_log_key(region_id, log_index);
        let mut entry = Entry::default();
        entry.set_term(1);
        entry.set_index(1);
        entry.set_entry_type(EntryType::EntryNormal);
        entry.set_data(vec![42].into());
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
        let raft_engine = &debugger.engines.raft;
        let kv_engine = &debugger.engines.kv;
        let region_id = 1;

        let raft_state_key = keys::raft_state_key(region_id);
        let mut raft_state = RaftLocalState::default();
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
        let mut apply_state = RaftApplyState::default();
        apply_state.set_applied_index(42);
        kv_engine
            .put_msg_cf(CF_RAFT, &apply_state_key, &apply_state)
            .unwrap();
        assert_eq!(
            kv_engine
                .get_msg_cf::<RaftApplyState>(CF_RAFT, &apply_state_key)
                .unwrap()
                .unwrap(),
            apply_state
        );

        let region_state_key = keys::region_state_key(region_id);
        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Tombstone);
        kv_engine
            .put_msg_cf(CF_RAFT, &region_state_key, &region_state)
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
        let engine = &debugger.engines.kv;

        let region_id = 1;
        let region_state_key = keys::region_state_key(region_id);
        let mut region = Region::default();
        region.set_id(region_id);
        region.set_start_key(b"a".to_vec());
        region.set_end_key(b"zz".to_vec());
        let mut state = RegionLocalState::default();
        state.set_region(region);
        engine
            .put_msg_cf(CF_RAFT, &region_state_key, &state)
            .unwrap();

        let cfs = vec![CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE];
        let (k, v) = (keys::data_key(b"k"), b"v");
        for cf in &cfs {
            engine.put_cf(cf, k.as_slice(), v).unwrap();
        }

        let sizes = debugger.region_size(region_id, cfs.clone()).unwrap();
        assert_eq!(sizes.len(), 4);
        for (cf, size) in sizes {
            cfs.iter().find(|&&c| c == cf).unwrap();
            assert_eq!(size, k.len() + v.len());
        }
    }

    #[test]
    fn test_scan_mvcc() {
        let debugger = new_debugger();
        // Test scan with bad start, end or limit.
        assert!(debugger.scan_mvcc(b"z", b"", 0).is_err());
        assert!(debugger.scan_mvcc(b"z", b"x", 3).is_err());
    }

    #[test]
    fn test_tombstone_regions() {
        let debugger = new_debugger();
        debugger.set_store_id(11);
        let engine = &debugger.engines.kv;

        // region 1 with peers at stores 11, 12, 13.
        let region_1 = init_region_state(engine.as_inner(), 1, &[11, 12, 13], 0);
        // Got the target region from pd, which doesn't contains the store.
        let mut target_region_1 = region_1.clone();
        target_region_1.mut_peers().remove(0);
        target_region_1.mut_region_epoch().set_conf_ver(100);

        // region 2 with peers at stores 11, 12, 13.
        let region_2 = init_region_state(engine.as_inner(), 2, &[11, 12, 13], 0);
        // Got the target region from pd, which has different peer_id.
        let mut target_region_2 = region_2.clone();
        target_region_2.mut_peers()[0].set_id(100);
        target_region_2.mut_region_epoch().set_conf_ver(100);

        // region 3 with peers at stores 21, 22, 23.
        let region_3 = init_region_state(engine.as_inner(), 3, &[21, 22, 23], 0);
        // Got the target region from pd but the peers are not changed.
        let mut target_region_3 = region_3;
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
        assert_eq!(
            get_region_state(engine.as_inner(), 1).take_region(),
            region_1
        );
        assert_eq!(
            get_region_state(engine.as_inner(), 2).take_region(),
            region_2
        );

        // After set_region_tombstone success, all region should be adjusted.
        let target_regions = vec![target_region_1, target_region_2];
        let errors = debugger.set_region_tombstone(target_regions).unwrap();
        assert!(errors.is_empty());
        for &region_id in &[1, 2] {
            let state = get_region_state(engine.as_inner(), region_id).get_state();
            assert_eq!(state, PeerState::Tombstone);
        }
    }

    #[test]
    fn test_tombstone_regions_by_id() {
        let debugger = new_debugger();
        debugger.set_store_id(11);
        let engine = &debugger.engines.kv;

        // tombstone region 1 which currently not exists.
        let errors = debugger.set_region_tombstone_by_id(vec![1]).unwrap();
        assert!(!errors.is_empty());

        // region 1 with peers at stores 11, 12, 13.
        init_region_state(engine.as_inner(), 1, &[11, 12, 13], 0);
        let mut expected_state = get_region_state(engine.as_inner(), 1);
        expected_state.set_state(PeerState::Tombstone);

        // tombstone region 1.
        let errors = debugger.set_region_tombstone_by_id(vec![1]).unwrap();
        assert!(errors.is_empty());
        assert_eq!(get_region_state(engine.as_inner(), 1), expected_state);

        // tombstone region 1 again.
        let errors = debugger.set_region_tombstone_by_id(vec![1]).unwrap();
        assert!(errors.is_empty());
        assert_eq!(get_region_state(engine.as_inner(), 1), expected_state);
    }

    #[test]
    fn test_remove_failed_stores() {
        let debugger = new_debugger();
        debugger.set_store_id(100);
        let engine = &debugger.engines.kv;

        let get_region_stores = |engine: &Arc<DB>, region_id: u64| {
            get_region_state(engine, region_id)
                .get_region()
                .get_peers()
                .iter()
                .map(|p| p.get_store_id())
                .collect::<Vec<_>>()
        };

        let get_region_learner = |engine: &Arc<DB>, region_id: u64| {
            get_region_state(engine, region_id)
                .get_region()
                .get_peers()
                .iter()
                .filter(|p| p.get_role() == PeerRole::Learner)
                .count()
        };

        // region 1 with peers at stores 11, 12, 13 and 14.
        init_region_state(engine.as_inner(), 1, &[11, 12, 13, 14], 0);
        // region 2 with peers at stores 21, 22 and 23.
        init_region_state(engine.as_inner(), 2, &[21, 22, 23], 0);

        // Only remove specified stores from region 1.
        debugger
            .remove_failed_stores(vec![13, 14, 21, 23], Some(vec![1]), false)
            .unwrap();

        // 13 and 14 should be removed from region 1.
        assert_eq!(get_region_stores(engine.as_inner(), 1), &[11, 12]);
        // 21 and 23 shouldn't be removed from region 2.
        assert_eq!(get_region_stores(engine.as_inner(), 2), &[21, 22, 23]);

        // Remove specified stores from all regions.
        debugger
            .remove_failed_stores(vec![11, 23], None, false)
            .unwrap();

        assert_eq!(get_region_stores(engine.as_inner(), 1), &[12]);
        assert_eq!(get_region_stores(engine.as_inner(), 2), &[21, 22]);

        // Should fail when the store itself is in the failed list.
        init_region_state(engine.as_inner(), 3, &[100, 31, 32, 33], 0);
        debugger
            .remove_failed_stores(vec![100], None, false)
            .unwrap_err();

        // no learner, promote learner does nothing
        init_region_state(engine.as_inner(), 4, &[41, 42, 43, 44], 0);
        debugger.remove_failed_stores(vec![44], None, true).unwrap();
        assert_eq!(get_region_stores(engine.as_inner(), 4), &[41, 42, 43]);
        assert_eq!(get_region_learner(engine.as_inner(), 4), 0);

        // promote learner
        init_region_state(engine.as_inner(), 5, &[51, 52, 53, 54], 1);
        debugger
            .remove_failed_stores(vec![52, 53, 54], None, true)
            .unwrap();
        assert_eq!(get_region_stores(engine.as_inner(), 5), &[51]);
        assert_eq!(get_region_learner(engine.as_inner(), 5), 0);

        // no need to promote learner
        init_region_state(engine.as_inner(), 6, &[61, 62, 63, 64], 1);
        debugger.remove_failed_stores(vec![64], None, true).unwrap();
        assert_eq!(get_region_stores(engine.as_inner(), 6), &[61, 62, 63]);
        assert_eq!(get_region_learner(engine.as_inner(), 6), 1);
    }

    #[test]
    fn test_drop_unapplied_raftlog() {
        let debugger = new_debugger();
        debugger.set_store_id(100);
        let kv_engine = &debugger.engines.kv;
        let raft_engine = &debugger.engines.raft;

        init_region_state(kv_engine.as_inner(), 1, &[100, 101], 1);
        init_region_state(kv_engine.as_inner(), 2, &[100, 103], 1);
        init_raft_state(kv_engine, raft_engine, 1, 100, 90, 80);
        init_raft_state(kv_engine, raft_engine, 2, 80, 80, 80);

        let region_info_2_before = debugger.region_info(2).unwrap();

        // Drop raftlog on all regions
        debugger.drop_unapplied_raftlog(None).unwrap();

        let region_info_1 = debugger.region_info(1).unwrap();
        let region_info_2 = debugger.region_info(2).unwrap();

        assert_eq!(
            region_info_1.raft_local_state.as_ref().unwrap().last_index,
            80
        );
        assert_eq!(
            region_info_1
                .raft_apply_state
                .as_ref()
                .unwrap()
                .applied_index,
            80
        );
        assert_eq!(
            region_info_1
                .raft_apply_state
                .as_ref()
                .unwrap()
                .commit_index,
            80
        );
        assert_eq!(region_info_2, region_info_2_before);
    }

    #[test]
    fn test_bad_regions() {
        let debugger = new_debugger();
        let kv_engine = &debugger.engines.kv;
        let raft_engine = &debugger.engines.raft;
        let store_id = 1; // It's a fake id.

        let mut wb1 = raft_engine.write_batch();
        let cf1 = CF_DEFAULT;

        let mut wb2 = kv_engine.write_batch();
        let cf2 = CF_RAFT;

        {
            let mock_region_state = |wb: &mut RocksWriteBatch, region_id: u64, peers: &[u64]| {
                let region_state_key = keys::region_state_key(region_id);
                let mut region_state = RegionLocalState::default();
                region_state.set_state(PeerState::Normal);
                {
                    let region = region_state.mut_region();
                    region.set_id(region_id);
                    let peers = peers
                        .iter()
                        .enumerate()
                        .map(|(i, &sid)| Peer {
                            id: i as u64,
                            store_id: sid,
                            ..Default::default()
                        })
                        .collect::<Vec<_>>();
                    region.set_peers(peers.into());
                }
                wb.put_msg_cf(cf2, &region_state_key, &region_state)
                    .unwrap();
            };
            let mock_raft_state =
                |wb: &mut RocksWriteBatch, region_id: u64, last_index: u64, commit_index: u64| {
                    let raft_state_key = keys::raft_state_key(region_id);
                    let mut raft_state = RaftLocalState::default();
                    raft_state.set_last_index(last_index);
                    raft_state.mut_hard_state().set_commit(commit_index);
                    wb.put_msg_cf(cf1, &raft_state_key, &raft_state).unwrap();
                };
            let mock_apply_state = |wb: &mut RocksWriteBatch, region_id: u64, apply_index: u64| {
                let raft_apply_key = keys::apply_state_key(region_id);
                let mut apply_state = RaftApplyState::default();
                apply_state.set_applied_index(apply_index);
                wb.put_msg_cf(cf2, &raft_apply_key, &apply_state).unwrap();
            };

            for &region_id in &[10, 11, 12] {
                mock_region_state(&mut wb2, region_id, &[store_id]);
            }

            // last index < commit index
            mock_raft_state(&mut wb1, 10, 100, 110);

            // commit index < last index < apply index, or commit index < apply index < last index.
            mock_raft_state(&mut wb1, 11, 100, 90);
            mock_apply_state(&mut wb2, 11, 110);
            mock_raft_state(&mut wb1, 12, 100, 90);
            mock_apply_state(&mut wb2, 12, 95);

            // region state doesn't contains the peer itself.
            mock_region_state(&mut wb2, 13, &[]);
        }

        wb1.write_opt(&WriteOptions::new()).unwrap();
        wb2.write_opt(&WriteOptions::new()).unwrap();

        let bad_regions = debugger.bad_regions().unwrap();
        assert_eq!(bad_regions.len(), 4);
        for (i, (region_id, _)) in bad_regions.into_iter().enumerate() {
            assert_eq!(region_id, (10 + i) as u64);
        }
    }

    #[test]
    fn test_recreate_region() {
        let debugger = new_debugger();
        let engine = &debugger.engines.kv;

        let metadata = vec![("", "g"), ("g", "m"), ("m", "")];

        for (region_id, (start, end)) in metadata.into_iter().enumerate() {
            let region_id = region_id as u64;
            let mut region = Region::default();
            region.set_id(region_id);
            region.set_start_key(start.to_owned().into_bytes());
            region.set_end_key(end.to_owned().into_bytes());

            let mut region_state = RegionLocalState::default();
            region_state.set_state(PeerState::Normal);
            region_state.set_region(region);
            let key = keys::region_state_key(region_id);
            engine.put_msg_cf(CF_RAFT, &key, &region_state).unwrap();
        }

        let remove_region_state = |region_id: u64| {
            let key = keys::region_state_key(region_id);
            engine.delete_cf(CF_RAFT, &key).unwrap();
        };

        let mut region = Region::default();
        region.set_id(100);

        region.set_start_key(b"k".to_vec());
        region.set_end_key(b"z".to_vec());
        assert!(debugger.recreate_region(region.clone()).is_err());

        remove_region_state(1);
        remove_region_state(2);
        assert!(debugger.recreate_region(region.clone()).is_ok());
        assert_eq!(
            get_region_state(engine.as_inner(), 100).get_region(),
            &region
        );

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
        }
        // test check CF_LOCK.
        default.extend(vec![
            // key, start_ts, check
            (b"k4", 100, Expect::Keep),
            (b"k5", 100, Expect::Keep),
        ]);
        lock.extend(vec![
            // key, start_ts, for_update_ts, lock_type, short_value, check
            (b"k1", 100, 0, LockType::Put, false, Expect::Remove), // k1: remove orphan lock.
            (b"k2", 100, 0, LockType::Delete, false, Expect::Keep), // k2: Delete doesn't need default.
            (b"k3", 100, 0, LockType::Put, true, Expect::Keep), // k3: short value doesn't need default.
            (b"k4", 100, 0, LockType::Put, false, Expect::Keep), // k4: corresponding default exists.
            (b"k5", 100, 0, LockType::Put, false, Expect::Remove), // k5: duplicated lock and write.
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
            // key, start_ts, for_update_ts, lock_type, short_value, check
            (b"k7", 100, 0, LockType::Put, false, Expect::Remove), // duplicated lock and write.
        ]);
        write.extend(vec![
            // key, start_ts, commit_ts, write_type, short_value
            (b"k7", 99, 100, WriteType::Put, false, Expect::Remove), // write without default.
            (b"k7", 96, 97, WriteType::Put, true, Expect::Keep),
        ]);

        // Locks from pessimistic transactions
        default.extend(vec![
            // key, start_ts
            (b"k8", 100, Expect::Keep),
            (b"k9", 100, Expect::Keep),
        ]);
        lock.extend(vec![
            // key, start_ts, for_update_ts, lock_type, short_value, check
            (b"k8", 90, 105, LockType::Pessimistic, false, Expect::Remove), // newer writes exist
            (b"k9", 90, 115, LockType::Put, true, Expect::Keep), // prewritten lock from a pessimistic txn
        ]);
        write.extend(vec![
            // key, start_ts, commit_ts, write_type, short_value
            (b"k8", 100, 110, WriteType::Put, false, Expect::Keep),
            (b"k9", 100, 110, WriteType::Put, false, Expect::Keep),
        ]);

        // Out of range.
        default.extend(vec![
            // key, start_ts
            (b"l0", 100, Expect::Keep),
        ]);
        lock.extend(vec![
            // key, start_ts, for_update_ts, lock_type, short_value, check
            (b"l0", 101, 0, LockType::Put, false, Expect::Keep),
        ]);
        write.extend(vec![
            // key, start_ts, commit_ts, write_type, short_value
            (b"l0", 102, 103, WriteType::Put, false, Expect::Keep),
        ]);

        let mut kv = vec![];
        for (key, ts, expect) in default {
            kv.push((
                CF_DEFAULT,
                Key::from_raw(key).append_ts(ts.into()),
                b"v".to_vec(),
                expect,
            ));
        }
        for (key, ts, for_update_ts, tp, short_value, expect) in lock {
            let v = if short_value {
                Some(b"v".to_vec())
            } else {
                None
            };
            let lock = Lock::new(
                tp,
                vec![],
                ts.into(),
                0,
                v,
                for_update_ts.into(),
                0,
                TimeStamp::zero(),
            );
            kv.push((CF_LOCK, Key::from_raw(key), lock.to_bytes(), expect));
        }
        for (key, start_ts, commit_ts, tp, short_value, expect) in write {
            let v = if short_value {
                Some(b"v".to_vec())
            } else {
                None
            };
            let write = Write::new(tp, start_ts.into(), v);
            kv.push((
                CF_WRITE,
                Key::from_raw(key).append_ts(commit_ts.into()),
                write.as_ref().to_bytes(),
                expect,
            ));
        }

        let path = Builder::new()
            .prefix("test_mvcc_checker")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, ColumnFamilyOptions::new()))
            .collect();
        let db = Arc::new(new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap());
        // Write initial KVs.
        let mut wb = db.c().write_batch();
        for &(cf, ref k, ref v, _) in &kv {
            wb.put_cf(cf, &keys::data_key(k.as_encoded()), v).unwrap();
        }
        wb.write().unwrap();
        // Fix problems.
        let mut checker = MvccChecker::new(Arc::clone(&db), b"k", b"l").unwrap();
        let mut wb = db.c().write_batch();
        checker.check_mvcc(&mut wb, None).unwrap();
        wb.write().unwrap();
        // Check result.
        for (cf, k, _, expect) in kv {
            let data = db
                .get_cf(
                    get_cf_handle(&db, cf).unwrap(),
                    &keys::data_key(k.as_encoded()),
                )
                .unwrap();
            match expect {
                Expect::Keep => assert!(data.is_some()),
                Expect::Remove => assert!(data.is_none()),
            }
        }
    }

    #[test]
    fn test_debug_raw_scan() {
        let keys: &[&[u8]] = &[
            b"a",
            b"a1",
            b"a2",
            b"a2\x00",
            b"a2\x00\x00",
            b"b",
            b"b1",
            b"b2",
            b"b2\x00",
            b"b2\x00\x00",
            b"c",
            b"c1",
            b"c2",
            b"c2\x00",
            b"c2\x00\x00",
        ];

        let debugger = new_debugger();

        let mut wb = debugger.engines.kv.write_batch();
        for key in keys {
            let data_key = keys::data_key(key);
            let value = key.to_vec();
            wb.put(&data_key, &value).unwrap();
        }
        wb.write().unwrap();

        let check = |result: Result<_>, expected: &[&[u8]]| {
            assert_eq!(
                result.unwrap(),
                expected
                    .iter()
                    .map(|k| (keys::data_key(k), k.to_vec()))
                    .collect::<Vec<_>>()
            );
        };

        check(debugger.raw_scan(b"z", &[b'z' + 1], 100, CF_DEFAULT), keys);
        check(debugger.raw_scan(b"za", b"zz", 100, CF_DEFAULT), keys);
        check(debugger.raw_scan(b"za1", b"za1", 100, CF_DEFAULT), &[]);
        check(
            debugger.raw_scan(b"za1", b"za2\x00\x00", 100, CF_DEFAULT),
            &keys[1..4],
        );
        check(
            debugger.raw_scan(b"za2\x00", b"za2\x00\x00", 100, CF_DEFAULT),
            &keys[3..4],
        );
        check(
            debugger.raw_scan(b"zb\x00", b"zb2\x00\x00", 100, CF_DEFAULT),
            &keys[6..9],
        );
        check(debugger.raw_scan(b"za1", b"zz", 1, CF_DEFAULT), &keys[1..2]);
        check(debugger.raw_scan(b"za1", b"zz", 3, CF_DEFAULT), &keys[1..4]);
        check(
            debugger.raw_scan(b"za1", b"zb2\x00\x00", 8, CF_DEFAULT),
            &keys[1..9],
        );
    }

    #[test]
    fn test_store_region() {
        let debugger = new_debugger();
        let store_id: u64 = 42;
        let cluster_id: u64 = 4242;
        debugger.set_store_id(store_id);
        debugger.set_cluster_id(cluster_id);
        debugger.set_store_api_version(ApiVersion::V2);
        assert_eq!(
            store_id,
            debugger
                .get_store_ident()
                .expect("get store id")
                .get_store_id()
        );
        assert_eq!(
            cluster_id,
            debugger
                .get_store_ident()
                .expect("get cluster id")
                .get_cluster_id()
        );

        assert_eq!(
            ApiVersion::V2,
            debugger
                .get_store_ident()
                .expect("get api version")
                .get_api_version()
        )
    }
}
