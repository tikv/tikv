// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread::JoinHandle};

use engine_rocks::{
    raw::CompactOptions, util::get_cf_handle, RocksEngine, RocksEngineIterator, RocksStatistics,
};
use engine_traits::{
    CachedTablet, Iterable, MiscExt, Peekable, RaftEngine, RaftLogBatch, TabletContext,
    TabletRegistry, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE,
};
use futures::future::Future;
use keys::{data_key, enc_end_key, enc_start_key, DATA_MAX_KEY, DATA_PREFIX_KEY};
use kvproto::{
    debugpb::Db as DbType,
    kvrpcpb::MvccInfo,
    metapb,
    raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent},
};
use nom::AsBytes;
use raft::{prelude::Entry, RawNode};
use raftstore::{
    coprocessor::{get_region_approximate_middle, get_region_approximate_size},
    store::util::check_key_in_region,
};
use raftstore_v2::Storage;
use slog::o;
use tikv_util::{
    config::ReadableSize, store::find_peer, sys::thread::StdThreadBuildWrapper, worker::Worker,
};

use super::debug::{recover_mvcc_for_range, BottommostLevelCompaction, Debugger, RegionInfo};
use crate::{
    config::ConfigController,
    server::debug::{dump_default_cf_properties, dump_write_cf_properties, Error, Result},
    storage::mvcc::{MvccInfoCollector, MvccInfoScanner},
};

// `key1` and `key2` should both be start_key or end_key.
fn smaller_key<'a>(key1: &'a [u8], key2: &'a [u8], is_end_key: bool) -> &'a [u8] {
    if is_end_key && key1.is_empty() {
        return key2;
    }
    if is_end_key && key2.is_empty() {
        return key1;
    }
    if key1 < key2 {
        return key1;
    }
    key2
}

// `key1` and `key2` should both be start_key or end_key.
fn larger_key<'a>(key1: &'a [u8], key2: &'a [u8], is_end_key: bool) -> &'a [u8] {
    if is_end_key && key1.is_empty() {
        return key1;
    }
    if is_end_key && key2.is_empty() {
        return key2;
    }
    if key1 < key2 {
        return key2;
    }
    key1
}

// return the region containing the seek_key or the next region if not existed
fn seek_region(
    seek_key: &[u8],
    sorted_region_states: &[RegionLocalState],
) -> Option<RegionLocalState> {
    if sorted_region_states.is_empty() {
        return None;
    }

    let idx = match sorted_region_states
        .binary_search_by(|state| state.get_region().get_start_key().cmp(seek_key))
    {
        Ok(idx) => return Some(sorted_region_states[idx].clone()),
        Err(idx) => idx,
    };

    // idx == 0 means seek_key is less than the first region's start key
    if idx == 0 {
        return Some(sorted_region_states[idx].clone());
    }

    let region_state = &sorted_region_states[idx - 1];
    if check_key_in_region(seek_key, region_state.get_region()).is_err() {
        return sorted_region_states.get(idx).cloned();
    }

    Some(region_state.clone())
}

pub struct MvccInfoIteratorV2 {
    scanner: Option<MvccInfoScanner<RocksEngineIterator, MvccInfoCollector>>,
    tablet_reg: TabletRegistry<RocksEngine>,
    sorted_region_states: Vec<RegionLocalState>,
    cur_region: metapb::Region,
    start: Vec<u8>,
    end: Vec<u8>,
    limit: usize,
    count: usize,
}

impl MvccInfoIteratorV2 {
    pub fn new(
        sorted_region_states: Vec<RegionLocalState>,
        tablet_reg: TabletRegistry<RocksEngine>,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Self> {
        let seek_key = if start.is_empty() {
            start
        } else {
            &start[DATA_PREFIX_KEY.len()..]
        };

        if let Some(mut first_region_state) = seek_region(seek_key, &sorted_region_states) {
            let mut tablet_cache = get_tablet_cache(
                &tablet_reg,
                first_region_state.get_region().get_id(),
                Some(first_region_state.clone()),
            )?;

            let tablet = tablet_cache.latest().unwrap();
            let region_start_key = enc_start_key(first_region_state.get_region());
            let region_end_key = enc_end_key(first_region_state.get_region());
            let iter_start = larger_key(start, &region_start_key, false);
            let iter_end = smaller_key(end, &region_end_key, true);
            assert!(!iter_start.is_empty() && !iter_start.is_empty());
            let scanner = Some(
                MvccInfoScanner::new(
                    |cf, opts| tablet.iterator_opt(cf, opts).map_err(|e| box_err!(e)),
                    Some(iter_start),
                    Some(iter_end),
                    MvccInfoCollector::default(),
                )
                .map_err(|e| -> Error { box_err!(e) })?,
            );

            Ok(MvccInfoIteratorV2 {
                scanner,
                tablet_reg,
                sorted_region_states,
                cur_region: first_region_state.take_region(),
                start: start.to_vec(),
                end: end.to_vec(),
                limit,
                count: 0,
            })
        } else {
            Ok(MvccInfoIteratorV2 {
                scanner: None,
                tablet_reg,
                sorted_region_states,
                cur_region: metapb::Region::default(),
                start: start.to_vec(),
                end: end.to_vec(),
                limit,
                count: 0,
            })
        }
    }
}

impl Iterator for MvccInfoIteratorV2 {
    type Item = raftstore::Result<(Vec<u8>, MvccInfo)>;

    fn next(&mut self) -> Option<raftstore::Result<(Vec<u8>, MvccInfo)>> {
        if self.scanner.is_none() || (self.limit != 0 && self.count >= self.limit) {
            return None;
        }

        loop {
            match self.scanner.as_mut().unwrap().next_item() {
                Ok(Some(item)) => {
                    self.count += 1;
                    return Some(Ok(item));
                }
                Ok(None) => {
                    let cur_end_key = self.cur_region.get_end_key();
                    if cur_end_key.is_empty() {
                        return None;
                    }

                    let next_region_state = seek_region(cur_end_key, &self.sorted_region_states);
                    if next_region_state.is_none() {
                        self.scanner = None;
                        return None;
                    }

                    let next_region_state = next_region_state.unwrap();
                    if &self.cur_region == next_region_state.get_region() {
                        return None;
                    }
                    self.cur_region = next_region_state.get_region().clone();
                    let mut tablet_cache = get_tablet_cache(
                        &self.tablet_reg,
                        next_region_state.get_region().get_id(),
                        Some(next_region_state.clone()),
                    )
                    .unwrap();
                    let tablet = tablet_cache.latest().unwrap();
                    let region_start_key = enc_start_key(&self.cur_region);
                    let region_end_key = enc_end_key(&self.cur_region);
                    let iter_start = larger_key(&self.start, &region_start_key, false);
                    let iter_end = smaller_key(&self.end, &region_end_key, true);
                    assert!(!iter_start.is_empty() && !iter_start.is_empty());
                    self.scanner = Some(
                        MvccInfoScanner::new(
                            |cf, opts| tablet.iterator_opt(cf, opts).map_err(|e| box_err!(e)),
                            Some(iter_start),
                            Some(iter_end),
                            MvccInfoCollector::default(),
                        )
                        .unwrap(),
                    );
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// Debugger for raftstore-v2
#[derive(Clone)]
pub struct DebuggerImplV2<ER: RaftEngine> {
    tablet_reg: TabletRegistry<RocksEngine>,
    raft_engine: ER,
    kv_statistics: Option<Arc<RocksStatistics>>,
    raft_statistics: Option<Arc<RocksStatistics>>,
    cfg_controller: ConfigController,
}

impl<ER: RaftEngine> DebuggerImplV2<ER> {
    pub fn new(
        tablet_reg: TabletRegistry<RocksEngine>,
        raft_engine: ER,
        cfg_controller: ConfigController,
    ) -> Self {
        println!("Debugger for raftstore-v2 is used");
        DebuggerImplV2 {
            tablet_reg,
            raft_engine,
            cfg_controller,
            kv_statistics: None,
            raft_statistics: None,
        }
    }

    pub fn recover_regions(
        &self,
        regions: Vec<metapb::Region>,
        read_only: bool,
    ) -> Result<Vec<(u64, Error)>> {
        let mut errors = Vec::with_capacity(regions.len());
        for region in regions {
            let region_id = region.get_id();
            let region_state = box_try!(self.raft_engine.get_region_state(region_id, u64::MAX))
                .ok_or_else(|| Error::NotFound(format!("Not found region {:?}", region_id)))?;
            if region_state.get_state() != PeerState::Normal {
                info!(
                    "skip region";
                    "region_id" => region_id,
                    "peer_state" => ?region_state.get_state(),
                );
                continue;
            }

            let mut tablet_cache =
                get_tablet_cache(&self.tablet_reg, region_id, Some(region_state))?;
            let tablet = tablet_cache.latest().unwrap();

            if let Err(e) = recover_mvcc_for_range(
                tablet,
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
        info!("Calculating split keys...");

        let region_groups =
            deivde_regions_for_concurrency(&self.raft_engine, &self.tablet_reg, threads as u64)?;

        let mut handles = Vec::new();
        for (thread_index, region_group) in region_groups.into_iter().enumerate() {
            let props = tikv_util::thread_group::current_properties();

            let mut tablets = vec![];
            for r in &region_group {
                let mut cache = get_tablet_cache(&self.tablet_reg, r.get_id(), None).unwrap();
                tablets.push(cache.latest().unwrap().clone());
            }
            let thread = std::thread::Builder::new()
                .name(format!("mvcc-recover-thread-{}", thread_index))
                .spawn_wrapper(move || {
                    tikv_util::thread_group::set_properties(props);

                    let mut results = vec![];
                    for (region, tablet) in region_group.into_iter().zip(tablets) {
                        info!(
                            "mvcc recover";
                            "thread_index" => thread_index,
                            "region" => ?region,
                        );
                        results.push(recover_mvcc_for_range(
                            &tablet,
                            region.get_start_key(),
                            region.get_end_key(),
                            read_only,
                            thread_index,
                        ));
                    }

                    tikv_alloc::remove_thread_memory_accessor();
                    results
                })
                .unwrap();

            handles.push(thread);
        }

        let res = handles
            .into_iter()
            .map(|h: JoinHandle<Vec<Result<()>>>| h.join())
            .map(|results| {
                if let Err(e) = &results {
                    error!("{:?}", e);
                } else {
                    for r in results.as_ref().unwrap() {
                        if let Err(e) = r {
                            error!("{:?}", e);
                        }
                    }
                }
                results
            })
            .all(|results| {
                if results.is_err() {
                    return false;
                }
                for r in &results.unwrap() {
                    if !r.is_ok() {
                        return false;
                    }
                }
                true
            });

        if res {
            Ok(())
        } else {
            Err(box_err!("Not all threads finished successfully."))
        }
    }

    pub fn bad_regions(&self) -> Result<Vec<(u64, Error)>> {
        let store_id = self.get_store_ident()?.get_store_id();
        let mut res = Vec::new();
        let fake_read_worker = Worker::new("fake-read-worker").lazy_build("fake-read-worker");

        let logger = slog_global::borrow_global().new(o!());
        let check_region_state = |region_id: u64| -> Result<()> {
            let region_state =
                box_try!(self.raft_engine.get_region_state(region_id, u64::MAX)).unwrap();
            match region_state.get_state() {
                PeerState::Tombstone | PeerState::Applying => return Ok(()),
                _ => {}
            }

            let region = region_state.get_region();
            let peer_id = find_peer(region, store_id)
                .map(|peer| peer.get_id())
                .ok_or_else(|| {
                    Error::Other(
                        format!(
                            "RegionLocalState doesn't contains peer itself, {:?}",
                            region_state
                        )
                        .into(),
                    )
                })?;

            let logger = logger.new(o!("region_id" => region_id, "peer_id" => peer_id));
            let storage = box_try!(Storage::<RocksEngine, ER>::new(
                region_id,
                store_id,
                self.raft_engine.clone(),
                fake_read_worker.scheduler(),
                &logger
            ))
            .unwrap();

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

            box_try!(RawNode::new(&raft_cfg, storage, &logger));
            Ok(())
        };

        box_try!(
            self.raft_engine
                .for_each_raft_group::<raftstore::Error, _>(&mut |region_id| {
                    if let Err(e) = check_region_state(region_id) {
                        res.push((region_id, e));
                    }

                    Ok(())
                })
        );

        Ok(res)
    }

    /// Set regions to tombstone by manual, and apply other status(such as
    /// peers, version, and key range) from `region` which comes from PD
    /// normally.
    pub fn set_region_tombstone(&self, regions: Vec<metapb::Region>) -> Result<Vec<(u64, Error)>> {
        let store_id = self.get_store_ident()?.get_store_id();
        let mut lb = self.raft_engine.log_batch(regions.len());

        let mut errors = Vec::with_capacity(regions.len());
        for region in regions {
            let region_id = region.get_id();
            if let Err(e) = set_region_tombstone(&self.raft_engine, store_id, region, &mut lb) {
                errors.push((region_id, e));
            }
        }

        if errors.is_empty() {
            box_try!(self.raft_engine.consume(&mut lb, true));
        }

        Ok(errors)
    }

    pub fn set_region_tombstone_by_id(&self, regions: Vec<u64>) -> Result<Vec<(u64, Error)>> {
        let mut lb = self.raft_engine.log_batch(regions.len());
        let mut errors = Vec::with_capacity(regions.len());
        for region_id in regions {
            let mut region_state = match self
                .raft_engine
                .get_region_state(region_id, u64::MAX)
                .map_err(|e| box_err!(e))
                .and_then(|s| s.ok_or_else(|| Error::Other("Can't find RegionLocalState".into())))
            {
                Ok(region_state) => region_state,
                Err(e) => {
                    errors.push((region_id, e));
                    continue;
                }
            };

            let apply_state = match self
                .raft_engine
                .get_apply_state(region_id, u64::MAX)
                .map_err(|e| box_err!(e))
                .and_then(|s| s.ok_or_else(|| Error::Other("Can't find RaftApplyState".into())))
            {
                Ok(apply_state) => apply_state,
                Err(e) => {
                    errors.push((region_id, e));
                    continue;
                }
            };

            if region_state.get_state() == PeerState::Tombstone {
                info!("skip {} because it's already tombstone", region_id);
                continue;
            }
            region_state.set_state(PeerState::Tombstone);
            box_try!(lb.put_region_state(
                region_id,
                apply_state.get_applied_index(),
                &region_state
            ));
        }

        if errors.is_empty() {
            box_try!(self.raft_engine.consume(&mut lb, true));
        }
        Ok(errors)
    }

    pub fn drop_unapplied_raftlog(&self, region_ids: Option<Vec<u64>>) -> Result<()> {
        let raft_engine = &self.raft_engine;
        let region_ids = region_ids.unwrap_or(self.get_all_regions_in_store()?);
        for region_id in region_ids {
            let region_state = self.region_info(region_id)?;
            // It's safe to unwrap region_local_state here, because
            // get_all_regions_in_store() guarantees that the region state
            // exists in kvdb.
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
            let mut lb = raft_engine.log_batch(10);
            box_try!(lb.put_apply_state(region_id, applied_index, &new_raft_apply_state));
            box_try!(lb.put_raft_state(region_id, &new_raft_local_state));
            box_try!(raft_engine.gc(region_id, applied_index + 1, last_index + 1, &mut lb));
            box_try!(raft_engine.consume(&mut lb, true));

            info!(
                "dropped unapplied raft log";
                "region_id" => region_id,
                "old_raft_local_state" => ?old_raft_local_state,
                "new_raft_local_state" => ?new_raft_local_state,
                "old_raft_apply_state" => ?old_raft_apply_state,
                "new_raft_apply_state" => ?new_raft_apply_state,
            );
        }

        Ok(())
    }
}

fn set_region_tombstone<ER: RaftEngine>(
    raft_engine: &ER,
    store_id: u64,
    region: metapb::Region,
    lb: &mut <ER as RaftEngine>::LogBatch,
) -> Result<()> {
    let id = region.get_id();

    let mut region_state = raft_engine
        .get_region_state(id, u64::MAX)
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

    let apply_state = raft_engine
        .get_apply_state(id, u64::MAX)
        .map_err(|e| box_err!(e))
        .and_then(|s| s.ok_or_else(|| Error::Other("Can't find RaftApplyState".into())))?;
    region_state.set_state(PeerState::Tombstone);
    region_state.set_region(region);
    box_try!(lb.put_region_state(id, apply_state.get_applied_index(), &region_state));

    Ok(())
}

impl<ER: RaftEngine> Debugger for DebuggerImplV2<ER> {
    fn get(&self, db: DbType, cf: &str, key: &[u8]) -> Result<Vec<u8>> {
        validate_db_and_cf(db, cf)?;
        let region_state =
            find_region_state_by_key(&self.raft_engine, &key[DATA_PREFIX_KEY.len()..])?;
        let mut tablet_cache = get_tablet_cache(
            &self.tablet_reg,
            region_state.get_region().get_id(),
            Some(region_state),
        )?;
        let tablet = tablet_cache.latest().unwrap();
        match tablet.get_value_cf(cf, key) {
            Ok(Some(v)) => Ok(v.to_vec()),
            Ok(None) => Err(Error::NotFound(format!(
                "value for key {:?} in db {:?}",
                key, db
            ))),
            Err(e) => Err(box_err!(e)),
        }
    }

    fn raft_log(&self, region_id: u64, log_index: u64) -> Result<Entry> {
        if let Some(log) = box_try!(self.raft_engine.get_entry(region_id, log_index)) {
            return Ok(log);
        }
        Err(Error::NotFound(format!(
            "raft log for region {} at index {}",
            region_id, log_index
        )))
    }

    fn region_info(&self, region_id: u64) -> Result<RegionInfo> {
        let persisted_applied = box_try!(self.raft_engine.get_flushed_index(region_id, CF_RAFT))
            .ok_or_else(|| Error::NotFound(format!("info for region {}", region_id)))?;
        let raft_state = box_try!(self.raft_engine.get_raft_state(region_id));

        // We used persisted_applied to acquire the apply state. It may not be the
        // lastest apply state but it's the real persisted one which means the tikv will
        // acquire this one during start.
        let apply_state = box_try!(
            self.raft_engine
                .get_apply_state(region_id, persisted_applied)
        )
        .map(|mut apply_state| {
            // the persisted_applied is the raft log replay start point, so it's the real
            // persisted applied_index
            apply_state.applied_index = persisted_applied;
            apply_state
        });

        let region_state = box_try!(
            self.raft_engine
                .get_region_state(region_id, persisted_applied)
        );

        match (raft_state, apply_state, region_state) {
            (None, None, None) => Err(Error::NotFound(format!("info for region {}", region_id))),
            (raft_state, apply_state, region_state) => {
                Ok(RegionInfo::new(raft_state, apply_state, region_state))
            }
        }
    }

    fn region_size<T: AsRef<str>>(&self, region_id: u64, cfs: Vec<T>) -> Result<Vec<(T, usize)>> {
        match self.raft_engine.get_region_state(region_id, u64::MAX) {
            Ok(Some(region_state)) => {
                let region = region_state.get_region();
                let state = region_state.get_state();
                let start_key = &keys::data_key(region.get_start_key());
                let end_key = &keys::data_end_key(region.get_end_key());
                let mut sizes = vec![];
                let mut tablet_cache =
                    get_tablet_cache(&self.tablet_reg, region.id, Some(region_state))?;
                let Some(tablet) = tablet_cache.latest() else {
                    return Err(Error::NotFound(format!(
                        "tablet not found, region_id={:?}, peer_state={:?}",
                        region_id, state
                    )));
                };
                for cf in cfs {
                    let mut size = 0;
                    box_try!(tablet.scan(cf.as_ref(), start_key, end_key, false, |k, v| {
                        size += k.len() + v.len();
                        Ok(true)
                    }));
                    sizes.push((cf, size));
                }
                Ok(sizes)
            }
            Ok(None) => Err(Error::NotFound(format!("none region {:?}", region_id))),
            Err(e) => Err(box_err!(e)),
        }
    }

    fn scan_mvcc(
        &self,
        start: &[u8],
        end: &[u8],
        limit: u64,
    ) -> Result<impl Iterator<Item = raftstore::Result<(Vec<u8>, MvccInfo)>> + Send> {
        if end.is_empty() && limit == 0 {
            return Err(Error::InvalidArgument("no limit and to_key".to_owned()));
        }
        if !end.is_empty() && start > end {
            return Err(Error::InvalidArgument(
                "start key should not be larger than end key".to_owned(),
            ));
        }

        let mut region_states = get_all_active_region_states(&self.raft_engine);

        region_states.sort_by(|r1, r2| {
            r1.get_region()
                .get_start_key()
                .cmp(r2.get_region().get_start_key())
        });

        MvccInfoIteratorV2::new(
            region_states,
            self.tablet_reg.clone(),
            start,
            end,
            limit as usize,
        )
    }

    fn compact(
        &self,
        db: DbType,
        cf: &str,
        start: &[u8],
        end: &[u8],
        threads: u32,
        bottommost: BottommostLevelCompaction,
    ) -> Result<()> {
        validate_db_and_cf(db, cf)?;
        if db == DbType::Raft {
            return Err(box_err!("Get raft db is not allowed"));
        }
        let compactions = find_region_states_by_key_range(&self.raft_engine, start, end);
        for (region_id, start_key, end_key, region_state) in compactions {
            let mut tablet_cache =
                get_tablet_cache(&self.tablet_reg, region_id, Some(region_state))?;
            let talbet = tablet_cache.latest().unwrap();
            info!("Debugger starts manual compact"; "talbet" => ?talbet, "cf" => cf);
            let mut opts = CompactOptions::new();
            opts.set_max_subcompactions(threads as i32);
            opts.set_exclusive_manual_compaction(false);
            opts.set_bottommost_level_compaction(bottommost.0);
            let handle = box_try!(get_cf_handle(talbet.as_inner(), cf));
            talbet.as_inner().compact_range_cf_opt(
                handle,
                &opts,
                start_key.as_ref().map(|k| k.as_bytes()),
                end_key.as_ref().map(|k| k.as_bytes()),
            );
            info!("Debugger finishes manual compact"; "db" => ?db, "cf" => cf);
        }

        Ok(())
    }

    fn get_all_regions_in_store(&self) -> Result<Vec<u64>> {
        let mut region_ids = vec![];
        let raft_engine = &self.raft_engine;
        self.raft_engine
            .for_each_raft_group::<raftstore::Error, _>(&mut |region_id| {
                let region_state = raft_engine
                    .get_region_state(region_id, u64::MAX)
                    .unwrap()
                    .unwrap();
                if region_state.state == PeerState::Tombstone {
                    return Ok(());
                }
                region_ids.push(region_id);
                Ok(())
            })
            .unwrap();
        region_ids.sort_unstable();
        Ok(region_ids)
    }

    fn dump_kv_stats(&self) -> Result<String> {
        let mut kv_str = String::new();
        self.tablet_reg.for_each_opened_tablet(|_, cached| {
            if let Some(tablet) = cached.latest() {
                let str = MiscExt::dump_stats(tablet).unwrap();
                kv_str.push_str(&str);
            }
            true
        });
        if let Some(s) = self.kv_statistics.as_ref()
            && let Some(s) = s.to_string()
        {
            kv_str.push_str(&s);
        }
        Ok(kv_str)
    }

    fn dump_raft_stats(&self) -> Result<String> {
        let mut raft_str = box_try!(RaftEngine::dump_stats(&self.raft_engine));
        if let Some(s) = self.raft_statistics.as_ref()
            && let Some(s) = s.to_string()
        {
            raft_str.push_str(&s);
        }
        Ok(raft_str)
    }

    fn modify_tikv_config(&self, config_name: &str, config_value: &str) -> Result<()> {
        if let Err(e) = self.cfg_controller.update_config(config_name, config_value) {
            return Err(Error::Other(
                format!("failed to update config, err: {:?}", e).into(),
            ));
        }
        Ok(())
    }

    fn get_store_ident(&self) -> Result<StoreIdent> {
        self.raft_engine
            .get_store_ident()
            .map_err(|e| Error::EngineTrait(e))
            .and_then(|ident| match ident {
                Some(ident) => Ok(ident),
                None => Err(Error::NotFound("No store ident key".to_owned())),
            })
    }

    fn get_region_properties(&self, region_id: u64) -> Result<Vec<(String, String)>> {
        let region_state = match self.raft_engine.get_region_state(region_id, u64::MAX) {
            Ok(Some(region_state)) => region_state,
            Ok(None) => return Err(Error::NotFound(format!("none region {:?}", region_id))),
            Err(e) => return Err(Error::EngineTrait(e)),
        };

        let state = region_state.get_state();
        if state == PeerState::Tombstone {
            return Err(Error::NotFound(format!(
                "region {:?} is tombstone",
                region_id
            )));
        }
        let region = region_state.get_region().clone();
        let start = keys::enc_start_key(&region);
        let end = keys::enc_end_key(&region);

        let mut tablet_cache = get_tablet_cache(&self.tablet_reg, region.id, Some(region_state))?;
        let Some(tablet) = tablet_cache.latest() else {
            return Err(Error::NotFound(format!(
                "tablet not found, region_id={:?}, peer_state={:?}",
                region_id, state
            )));
        };
        let mut res = dump_write_cf_properties(tablet, &start, &end)?;
        let mut res1 = dump_default_cf_properties(tablet, &start, &end)?;
        res.append(&mut res1);

        let middle_key = match box_try!(get_region_approximate_middle(tablet, &region)) {
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
            hex::encode(middle_key),
        ));

        Ok(res)
    }

    fn reset_to_version(&self, _version: u64) {
        unimplemented!()
    }

    fn set_kv_statistics(&mut self, s: Option<Arc<RocksStatistics>>) {
        self.kv_statistics = s;
    }

    fn set_raft_statistics(&mut self, s: Option<Arc<RocksStatistics>>) {
        self.raft_statistics = s;
    }

    fn key_range_flashback_to_version(
        &self,
        _version: u64,
        _region_id: u64,
        _start_key: &[u8],
        _end_key: &[u8],
        _start_ts: u64,
        _commit_ts: u64,
    ) -> impl Future<Output = Result<()>> + Send {
        async move { unimplemented!() }
    }

    fn get_range_properties(&self, start: &[u8], end: &[u8]) -> Result<Vec<(String, String)>> {
        let mut props = vec![];
        let start = &keys::data_key(start);
        let end = &keys::data_end_key(end);
        let regions = find_region_states_by_key_range(&self.raft_engine, start, end);
        for (region_id, start_key, end_key, region_state) in regions {
            let mut tablet_cache =
                get_tablet_cache(&self.tablet_reg, region_id, Some(region_state)).unwrap();
            let talbet = tablet_cache.latest().unwrap();
            let mut prop = dump_write_cf_properties(
                talbet,
                start_key.as_ref().map(|k| (k.as_bytes())).unwrap_or(start),
                end_key.as_ref().map(|k| k.as_bytes()).unwrap_or(end),
            )
            .unwrap();
            props.append(&mut prop);
            let mut prop = dump_default_cf_properties(
                talbet,
                start_key.as_ref().map(|k| k.as_bytes()).unwrap_or(start),
                end_key.as_ref().map(|k| k.as_bytes()).unwrap_or(end),
            )?;
            props.append(&mut prop);
        }
        Ok(props)
    }
}

fn validate_db_and_cf(db: DbType, cf: &str) -> Result<()> {
    match (db, cf) {
        (DbType::Kv, CF_DEFAULT)
        | (DbType::Kv, CF_WRITE)
        | (DbType::Kv, CF_LOCK)
        | (DbType::Raft, CF_DEFAULT) => Ok(()),
        _ => Err(Error::InvalidArgument(format!(
            "invalid cf {:?} for db {:?}",
            cf, db
        ))),
    }
}

// Return the overlap range (without data prefix) of the `range` in region or
// None if they are exclusive
// Note: generally, range should start with `DATA_PREFIX_KEY`, but they can also
// be empty in case of compacting whole cluster for example.
// Note: the range end being `DATA_PREFIX_KEY` and `DATA_MAX_KEY` both means the
// largest key
fn range_in_region<'a>(
    range: (&'a [u8], &'a [u8]),
    region: &'a metapb::Region,
) -> Option<(&'a [u8], &'a [u8])> {
    let range_start = if !range.0.is_empty() {
        range.0
    } else {
        DATA_PREFIX_KEY
    };

    let range_end = if !range.1.is_empty() && range.1 != DATA_MAX_KEY {
        range.1
    } else {
        DATA_PREFIX_KEY
    };
    if range_start == DATA_PREFIX_KEY && range_end == DATA_PREFIX_KEY {
        return Some((region.get_start_key(), region.get_end_key()));
    } else if range_start == DATA_PREFIX_KEY {
        assert!(range_end.starts_with(DATA_PREFIX_KEY));
        if region.get_start_key() < &range_end[DATA_PREFIX_KEY.len()..] {
            return Some((
                region.get_start_key(),
                smaller_key(
                    &range_end[DATA_PREFIX_KEY.len()..],
                    region.get_end_key(),
                    true,
                ),
            ));
        }
        None
    } else if range_end == DATA_PREFIX_KEY {
        assert!(range_start.starts_with(DATA_PREFIX_KEY));
        if &range_start[DATA_PREFIX_KEY.len()..] < region.get_end_key()
            || region.get_end_key().is_empty()
        {
            return Some((
                larger_key(
                    &range_start[DATA_PREFIX_KEY.len()..],
                    region.get_start_key(),
                    false,
                ),
                region.get_end_key(),
            ));
        }
        None
    } else {
        assert!(range_start.starts_with(DATA_PREFIX_KEY));
        assert!(range_end.starts_with(DATA_PREFIX_KEY));
        let start_key = larger_key(
            &range_start[DATA_PREFIX_KEY.len()..],
            region.get_start_key(),
            false,
        );
        let end_key = smaller_key(
            &range_end[DATA_PREFIX_KEY.len()..],
            region.get_end_key(),
            true,
        );
        if start_key < end_key {
            return Some((start_key, end_key));
        }
        None
    }
}

fn find_region_states_by_key_range<ER: RaftEngine>(
    raft_engine: &ER,
    start: &[u8],
    end: &[u8],
) -> Vec<(u64, Option<Vec<u8>>, Option<Vec<u8>>, RegionLocalState)> {
    let mut regions = vec![];
    raft_engine
        .for_each_raft_group::<raftstore::Error, _>(&mut |region_id| {
            let region_state = raft_engine
                .get_region_state(region_id, u64::MAX)
                .unwrap()
                .unwrap();
            if region_state.state != PeerState::Normal {
                return Ok(());
            }

            if let Some((start_key, end_key)) =
                range_in_region((start, end), region_state.get_region())
            {
                let start = if start_key.is_empty() {
                    None
                } else {
                    Some(data_key(start_key))
                };
                let end = if end_key.is_empty() {
                    None
                } else {
                    Some(data_key(end_key))
                };
                regions.push((region_id, start, end, region_state));
            };

            Ok(())
        })
        .unwrap();
    regions
}

fn find_region_state_by_key<ER: RaftEngine>(
    raft_engine: &ER,
    key: &[u8],
) -> Result<RegionLocalState> {
    let mut region_ids = vec![];
    raft_engine
        .for_each_raft_group::<raftstore::Error, _>(&mut |region_id| {
            region_ids.push(region_id);
            Ok(())
        })
        .unwrap();

    for region_id in region_ids {
        if let Ok(Some(region_state)) = raft_engine.get_region_state(region_id, u64::MAX) {
            let region = region_state.get_region();
            if check_key_in_region(key, region).is_ok() {
                if region_state.get_state() != PeerState::Normal {
                    break;
                }
                return Ok(region_state);
            }
        }
    }

    Err(Error::NotFound(format!(
        "Not found region containing {:?}",
        key
    )))
}

fn get_tablet_cache(
    tablet_reg: &TabletRegistry<RocksEngine>,
    region_id: u64,
    state: Option<RegionLocalState>,
) -> Result<CachedTablet<RocksEngine>> {
    if let Some(tablet_cache) = tablet_reg.get(region_id) {
        Ok(tablet_cache)
    } else {
        let region_state = state.unwrap();
        let ctx = TabletContext::new(region_state.get_region(), Some(region_state.tablet_index));
        match tablet_reg.load(ctx, false) {
            Ok(tablet_cache) => Ok(tablet_cache),
            Err(e) => {
                println!(
                    "tablet load failed, region_state {:?}",
                    region_state.get_state()
                );
                return Err(box_err!(e));
            }
        }
    }
}

fn get_all_active_region_states<ER: RaftEngine>(raft_engine: &ER) -> Vec<RegionLocalState> {
    let mut region_states = vec![];
    raft_engine
        .for_each_raft_group::<raftstore::Error, _>(&mut |region_id| {
            let region_state = raft_engine
                .get_region_state(region_id, u64::MAX)
                .unwrap()
                .unwrap();
            if region_state.state != PeerState::Tombstone {
                region_states.push(region_state);
            }
            Ok(())
        })
        .unwrap();

    region_states
}

// This method devide all regions into `threads` of groups where each group has
// similar data volume (estimated by region size) so that we use `threads` of
// threads to execute them concurrently.
// Note: we cannot guarantee that we can divde them into exactly `threads` of
// groups for some cases, ex: [0, 0, 0, 0, 0, 100], we can at most return two
// groups for this.
fn deivde_regions_for_concurrency<ER: RaftEngine>(
    raft_engine: &ER,
    registry: &TabletRegistry<RocksEngine>,
    threads: u64,
) -> Result<Vec<Vec<metapb::Region>>> {
    let region_states = get_all_active_region_states(raft_engine);

    if threads == 1 {
        return Ok(vec![
            region_states
                .into_iter()
                .map(|mut r| r.take_region())
                .collect(),
        ]);
    }

    let mut regions_groups = vec![];
    let mut region_sizes = vec![];
    let mut total_size = 0;
    for region_state in region_states {
        let mut tablet_cache = get_tablet_cache(
            registry,
            region_state.get_region().get_id(),
            Some(region_state.clone()),
        )?;
        let tablet = tablet_cache.latest().unwrap();
        let region_size = box_try!(get_region_approximate_size(
            tablet,
            region_state.get_region(),
            0
        ));
        region_sizes.push((region_size, region_state));
        total_size += region_size;
    }
    region_sizes.sort_by(|a, b| a.0.cmp(&b.0));

    let group_size = (total_size + threads - 1) / threads;
    let mut cur_group = vec![];
    let mut cur_size = 0;
    for (region_size, mut region_state) in region_sizes.into_iter() {
        cur_group.push(region_state.take_region());
        cur_size += region_size;
        if cur_size >= group_size {
            cur_size = 0;
            regions_groups.push(cur_group);
            cur_group = vec![];
        }
    }
    if !cur_group.is_empty() {
        regions_groups.push(cur_group);
    }

    assert!(regions_groups.len() <= threads as usize);
    Ok(regions_groups)
}

#[cfg(any(test, feature = "testexport"))]
pub fn new_debugger(path: &std::path::Path) -> DebuggerImplV2<raft_log_engine::RaftLogEngine> {
    use crate::{config::TikvConfig, server::KvEngineFactoryBuilder};

    let mut cfg = TikvConfig::default();
    cfg.storage.data_dir = path.to_str().unwrap().to_string();
    cfg.raft_store.raftdb_path = cfg.infer_raft_db_path(None).unwrap();
    cfg.raft_engine.mut_config().dir = cfg.infer_raft_engine_path(None).unwrap();
    let cache = cfg.storage.block_cache.build_shared_cache();
    let env = cfg.build_shared_rocks_env(None, None).unwrap();

    let factory = KvEngineFactoryBuilder::new(env, &cfg, cache, None).build();
    let reg = TabletRegistry::new(Box::new(factory), path).unwrap();

    let raft_engine =
        raft_log_engine::RaftLogEngine::new(cfg.raft_engine.config(), None, None).unwrap();

    DebuggerImplV2::new(reg, raft_engine, ConfigController::default())
}

#[cfg(test)]
mod tests {
    use collections::HashMap;
    use engine_traits::{
        RaftEngineReadOnly, RaftLogBatch, SyncMutable, ALL_CFS, CF_DEFAULT, CF_LOCK, CF_WRITE,
        DATA_CFS,
    };
    use kvproto::{
        metapb::{self, Peer, PeerRole},
        raft_serverpb::*,
    };
    use raft::prelude::EntryType;
    use raftstore::store::RAFT_INIT_LOG_INDEX;
    use tikv_util::store::new_peer;

    use super::*;

    const INITIAL_APPLY_INDEX: u64 = 5;

    impl<ER: RaftEngine> DebuggerImplV2<ER> {
        fn set_store_id(&self, store_id: u64) {
            let mut ident = self.get_store_ident().unwrap_or_default();
            ident.set_store_id(store_id);
            let mut lb = self.raft_engine.log_batch(3);
            lb.put_store_ident(&ident).unwrap();
            self.raft_engine.consume(&mut lb, true).unwrap();
        }
    }

    fn init_region_state<ER: RaftEngine>(
        raft_engine: &ER,
        region_id: u64,
        stores: &[u64],
        mut learner: usize,
    ) -> metapb::Region {
        let mut region = metapb::Region::default();
        region.set_id(region_id);
        for (i, &store_id) in stores.iter().enumerate() {
            let mut peer = metapb::Peer::default();
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
        let mut lb = raft_engine.log_batch(3);
        lb.put_region_state(region_id, INITIAL_APPLY_INDEX, &region_state)
            .unwrap();
        raft_engine.consume(&mut lb, true).unwrap();
        region
    }

    fn init_raft_state<ER: RaftEngine>(
        raft_engine: &ER,
        region_id: u64,
        last_index: u64,
        commit_index: u64,
        applied_index: u64,
        admin_flush: Option<u64>,
    ) {
        let mut lb = raft_engine.log_batch(10);
        let mut apply_state = RaftApplyState::default();
        apply_state.set_applied_index(applied_index);
        apply_state.set_commit_index(commit_index);
        lb.put_apply_state(region_id, applied_index, &apply_state)
            .unwrap();

        let mut raft_state = RaftLocalState::default();
        raft_state.set_last_index(last_index);
        lb.put_raft_state(region_id, &raft_state).unwrap();

        if let Some(admin_flush) = admin_flush {
            lb.put_flushed_index(region_id, CF_RAFT, 5, admin_flush)
                .unwrap();
        }
        raft_engine.consume(&mut lb, true).unwrap();
    }

    #[test]
    fn test_get() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        let raft_engine = &debugger.raft_engine;
        let region_id = 1;

        let mut region = metapb::Region::default();
        region.set_id(region_id);
        region.set_start_key(b"k10".to_vec());
        region.set_end_key(b"k20".to_vec());
        let mut state = RegionLocalState::default();
        state.set_region(region.clone());
        state.set_tablet_index(5);

        let ctx = TabletContext::new(&region, Some(5));
        let mut tablet_cache = debugger.tablet_reg.load(ctx, true).unwrap();
        let tablet = tablet_cache.latest().unwrap();

        let mut wb = raft_engine.log_batch(10);
        wb.put_region_state(region_id, 10, &state).unwrap();
        raft_engine.consume(&mut wb, true).unwrap();

        let cfs = vec![CF_DEFAULT, CF_LOCK, CF_WRITE];
        let (k, v) = (keys::data_key(b"k15"), b"v");
        for cf in &cfs {
            tablet.put_cf(cf, k.as_slice(), v).unwrap();
        }

        for cf in &cfs {
            let got = debugger.get(DbType::Kv, cf, &k).unwrap();
            assert_eq!(&got, v);
        }

        match debugger.get(DbType::Kv, CF_DEFAULT, b"k15") {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }

        let mut wb = raft_engine.log_batch(10);
        state.set_state(PeerState::Tombstone);
        wb.put_region_state(region_id, 10, &state).unwrap();
        raft_engine.consume(&mut wb, true).unwrap();
        for cf in &cfs {
            debugger.get(DbType::Kv, cf, &k).unwrap_err();
        }
    }

    #[test]
    fn test_raft_log() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        let raft_engine = &debugger.raft_engine;
        let (region_id, log_index) = (1, 1);

        let mut entry = Entry::default();
        entry.set_term(1);
        entry.set_index(1);
        entry.set_entry_type(EntryType::EntryNormal);
        entry.set_data(vec![42].into());
        let mut wb = raft_engine.log_batch(10);
        RaftLogBatch::append(&mut wb, region_id, None, vec![entry.clone()]).unwrap();
        raft_engine.consume(&mut wb, true).unwrap();

        assert_eq!(debugger.raft_log(region_id, log_index).unwrap(), entry);
        match debugger.raft_log(region_id + 1, log_index + 1) {
            Err(Error::NotFound(_)) => (),
            _ => panic!("expect Error::NotFound(_)"),
        }
    }

    #[test]
    fn test_region_info() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        let raft_engine = &debugger.raft_engine;
        let region_id = 1;

        let mut wb = raft_engine.log_batch(10);
        let mut raft_state = RaftLocalState::default();
        raft_state.set_last_index(42);
        RaftLogBatch::put_raft_state(&mut wb, region_id, &raft_state).unwrap();

        let mut apply_state = RaftApplyState::default();
        apply_state.set_applied_index(42);
        RaftLogBatch::put_apply_state(&mut wb, region_id, 42, &apply_state).unwrap();

        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Tombstone);
        RaftLogBatch::put_region_state(&mut wb, region_id, 42, &region_state).unwrap();

        wb.put_flushed_index(region_id, CF_RAFT, 5, 42).unwrap();

        // This will not be read
        let mut apply_state2 = RaftApplyState::default();
        apply_state2.set_applied_index(100);
        RaftLogBatch::put_apply_state(&mut wb, region_id, 100, &apply_state2).unwrap();

        raft_engine.consume(&mut wb, true).unwrap();

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
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        let raft_engine = &debugger.raft_engine;
        let region_id = 1;

        let mut region = metapb::Region::default();
        region.set_id(region_id);
        region.set_start_key(b"k10".to_vec());
        region.set_end_key(b"k20".to_vec());
        let mut state = RegionLocalState::default();
        state.set_region(region.clone());
        state.set_tablet_index(5);

        let ctx = TabletContext::new(&region, Some(5));
        let mut tablet_cache = debugger.tablet_reg.load(ctx, true).unwrap();
        let tablet = tablet_cache.latest().unwrap();

        let mut wb = raft_engine.log_batch(10);
        wb.put_region_state(region_id, 10, &state).unwrap();
        raft_engine.consume(&mut wb, true).unwrap();

        let cfs = vec![CF_DEFAULT, CF_LOCK, CF_WRITE];
        let (k, v) = (keys::data_key(b"k15"), b"v");
        for cf in &cfs {
            tablet.put_cf(cf, k.as_slice(), v).unwrap();
        }

        let sizes = debugger.region_size(region_id, cfs.clone()).unwrap();
        assert_eq!(sizes.len(), 3);
        for (cf, size) in sizes {
            cfs.iter().find(|&&c| c == cf).unwrap();
            assert_eq!(size, k.len() + v.len());
        }

        // test for region that has not been trimmed
        let (k, v) = (keys::data_key(b"k05"), b"v");
        let k1 = keys::data_key(b"k25");
        for cf in &cfs {
            tablet.put_cf(cf, k.as_slice(), v).unwrap();
            tablet.put_cf(cf, k1.as_slice(), v).unwrap();
        }

        let sizes = debugger.region_size(region_id, cfs.clone()).unwrap();
        assert_eq!(sizes.len(), 3);
        for (cf, size) in sizes {
            cfs.iter().find(|&&c| c == cf).unwrap();
            assert_eq!(size, k.len() + v.len());
        }

        state.set_state(PeerState::Tombstone);
        let mut wb = raft_engine.log_batch(10);
        wb.put_region_state(region_id, 10, &state).unwrap();
        raft_engine.consume(&mut wb, true).unwrap();
        debugger.tablet_reg.remove(region_id);
        debugger.region_size(region_id, cfs.clone()).unwrap_err();
    }

    #[test]
    fn test_compact() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        let compact = |db, cf| debugger.compact(db, cf, &[0], &[0xFF], 1, Some("skip").into());
        compact(DbType::Kv, CF_DEFAULT).unwrap();
        compact(DbType::Kv, CF_LOCK).unwrap();
        compact(DbType::Kv, CF_WRITE).unwrap();
        compact(DbType::Raft, CF_DEFAULT).unwrap_err();
    }

    #[test]
    fn test_range_in_region() {
        let mut region = metapb::Region::default();
        region.set_start_key(b"k01".to_vec());
        region.set_end_key(b"k10".to_vec());

        let ranges = vec![
            ("", "", "k01", "k10"),
            ("z", "z", "k01", "k10"),
            ("zk00", "", "k01", "k10"),
            ("zk00", "z", "k01", "k10"),
            ("", "zk11", "k01", "k10"),
            ("z", "zk11", "k01", "k10"),
            ("zk02", "zk07", "k02", "k07"),
            ("zk00", "zk07", "k01", "k07"),
            ("zk02", "zk11", "k02", "k10"),
            ("zk02", "{", "k02", "k10"),
        ];

        for (range_start, range_end, expect_start, expect_end) in ranges {
            assert_eq!(
                (expect_start.as_bytes(), expect_end.as_bytes()),
                range_in_region((range_start.as_bytes(), range_end.as_bytes()), &region).unwrap()
            );
        }

        let ranges = vec![("zk05", "zk02"), ("zk11", ""), ("", "zk00")];
        for (range_start, range_end) in ranges {
            assert!(
                range_in_region((range_start.as_bytes(), range_end.as_bytes()), &region).is_none()
            );
        }

        region.set_start_key(b"".to_vec());
        region.set_end_key(b"k10".to_vec());

        let ranges = vec![
            ("", "", "", "k10"),
            ("z", "z", "", "k10"),
            ("zk00", "", "k00", "k10"),
            ("zk00", "z", "k00", "k10"),
            ("", "zk11", "", "k10"),
            ("z", "zk11", "", "k10"),
            ("zk02", "zk07", "k02", "k07"),
            ("zk02", "zk11", "k02", "k10"),
            ("zk02", "{", "k02", "k10"),
        ];

        for (range_start, range_end, expect_start, expect_end) in ranges {
            assert_eq!(
                (expect_start.as_bytes(), expect_end.as_bytes()),
                range_in_region((range_start.as_bytes(), range_end.as_bytes()), &region).unwrap()
            );
        }

        let ranges = vec![("zk05", "zk02"), ("zk11", "")];
        for (range_start, range_end) in ranges {
            assert!(
                range_in_region((range_start.as_bytes(), range_end.as_bytes()), &region).is_none()
            );
        }

        region.set_start_key(b"k01".to_vec());
        region.set_end_key(b"".to_vec());

        let ranges = vec![
            ("", "", "k01", ""),
            ("z", "z", "k01", ""),
            ("zk00", "", "k01", ""),
            ("zk00", "z", "k01", ""),
            ("", "zk11", "k01", "k11"),
            ("z", "zk11", "k01", "k11"),
            ("zk02", "zk07", "k02", "k07"),
            ("zk02", "zk11", "k02", "k11"),
            ("zk02", "{", "k02", ""),
        ];

        for (range_start, range_end, expect_start, expect_end) in ranges {
            assert_eq!(
                (expect_start.as_bytes(), expect_end.as_bytes()),
                range_in_region((range_start.as_bytes(), range_end.as_bytes()), &region).unwrap()
            );
        }

        let ranges = vec![("zk05", "zk02"), ("", "zk00")];
        for (range_start, range_end) in ranges {
            assert!(
                range_in_region((range_start.as_bytes(), range_end.as_bytes()), &region).is_none()
            );
        }
    }

    #[test]
    fn test_divide_regions_even() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());

        let mut lb = debugger.raft_engine.log_batch(30);
        for i in 0..20 {
            let mut region = metapb::Region::default();
            region.set_peers(vec![new_peer(1, i + 1)].into());
            region.set_id(i + 1);
            let ctx = TabletContext::new(&region, Some(5));
            let mut cache = debugger.tablet_reg.load(ctx, true).unwrap();
            let tablet = cache.latest().unwrap();
            for j in 0..10 {
                // (6 + 3) * 10
                let k = format!("zk{:04}", i * 100 + j);
                tablet.put(k.as_bytes(), b"val").unwrap();
            }
            tablet.flush_cfs(DATA_CFS, true).unwrap();

            let mut region_state = RegionLocalState::default();
            region_state.set_region(region);
            region_state.set_tablet_index(5);
            lb.put_region_state(i + 1, 5, &region_state).unwrap();
        }
        debugger.raft_engine.consume(&mut lb, true).unwrap();

        let groups =
            deivde_regions_for_concurrency(&debugger.raft_engine, &debugger.tablet_reg, 4).unwrap();
        assert_eq!(groups.len(), 4);
        for g in groups {
            assert_eq!(g.len(), 5);
        }

        let groups =
            deivde_regions_for_concurrency(&debugger.raft_engine, &debugger.tablet_reg, 3).unwrap();
        assert_eq!(groups[0].len(), 7);
        assert_eq!(groups[1].len(), 7);
        assert_eq!(groups[2].len(), 6);
    }

    #[test]
    fn test_divide_regions_uneven() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());

        let mut lb = debugger.raft_engine.log_batch(30);
        let mut region_sizes = HashMap::default();
        let mut total_size = 0;
        let mut max_region_size = 0;
        for i in 0..20 {
            let mut region = metapb::Region::default();
            region.set_peers(vec![new_peer(1, i + 1)].into());
            region.set_id(i + 1);
            let ctx = TabletContext::new(&region, Some(5));
            let mut cache = debugger.tablet_reg.load(ctx, true).unwrap();
            let tablet = cache.latest().unwrap();
            for j in 0..=i {
                let k = format!("zk{:04}", i * 100 + j);
                tablet.put(k.as_bytes(), b"val").unwrap();
            }

            let group_size = (6 + 3) * (i + 1);
            max_region_size = group_size;
            total_size += group_size;
            region_sizes.insert(i + 1, group_size);
            tablet.flush_cfs(DATA_CFS, true).unwrap();

            let mut region_state = RegionLocalState::default();
            region_state.set_region(region);
            region_state.set_tablet_index(5);
            lb.put_region_state(i + 1, 5, &region_state).unwrap();
        }
        debugger.raft_engine.consume(&mut lb, true).unwrap();

        let check_group = |groups: Vec<Vec<metapb::Region>>, group_size_threshold| {
            let count = groups.iter().fold(0, |count, group| count + group.len());
            assert_eq!(count, 20);
            for (i, group) in groups.iter().enumerate() {
                let mut current_group_size = 0;
                for region in group {
                    current_group_size += *region_sizes.get(&region.get_id()).unwrap();
                }
                // All groups should have total size > `group_size_threshold` except for the
                // last region.
                if i != groups.len() - 1 {
                    assert!(
                        current_group_size >= group_size_threshold
                            && current_group_size < group_size_threshold + max_region_size
                    );
                }
            }
        };

        let groups =
            deivde_regions_for_concurrency(&debugger.raft_engine, &debugger.tablet_reg, 4).unwrap();
        let group_size_threshold = total_size / 4;
        check_group(groups, group_size_threshold);

        let groups =
            deivde_regions_for_concurrency(&debugger.raft_engine, &debugger.tablet_reg, 7).unwrap();
        let group_size_threshold = total_size / 7;
        check_group(groups, group_size_threshold);
    }

    #[test]
    fn test_bad_regions() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        let store_id = 1;
        debugger.set_store_id(store_id);

        let mut lb = debugger.raft_engine.log_batch(30);

        let put_region_state =
            |lb: &mut raft_log_engine::RaftLogBatch, region_id: u64, peers: &[u64]| {
                let mut region_state = RegionLocalState::default();
                region_state.set_state(PeerState::Normal);
                let region = region_state.mut_region();
                region.set_id(region_id);
                let peers = peers
                    .iter()
                    .enumerate()
                    .map(|(_, &sid)| Peer {
                        id: region_id,
                        store_id: sid,
                        ..Default::default()
                    })
                    .collect::<Vec<_>>();
                region.set_peers(peers.into());
                lb.put_region_state(region_id, 5, &region_state).unwrap();
            };

        let put_apply_state =
            |lb: &mut raft_log_engine::RaftLogBatch, region_id: u64, apply_index: u64| {
                let mut apply_state = RaftApplyState::default();
                apply_state.set_applied_index(apply_index);
                lb.put_apply_state(region_id, apply_index, &apply_state)
                    .unwrap();

                for cf in ALL_CFS {
                    lb.put_flushed_index(region_id, cf, 5, apply_index).unwrap();
                }
            };

        let put_raft_state = |lb: &mut raft_log_engine::RaftLogBatch,
                              region_id: u64,
                              last_index: u64,
                              commit_index: u64| {
            let mut raft_state = RaftLocalState::default();
            raft_state.set_last_index(last_index);
            raft_state.mut_hard_state().set_commit(commit_index);
            lb.put_raft_state(region_id, &raft_state).unwrap();
        };

        for &region_id in &[10, 11, 12] {
            put_region_state(&mut lb, region_id, &[store_id]);
        }

        // last index < commit index
        put_raft_state(&mut lb, 10, 100, 110);
        put_apply_state(&mut lb, 10, RAFT_INIT_LOG_INDEX);

        // commit index < last index < apply index, or commit index < apply index < last
        // index.
        put_raft_state(&mut lb, 11, 100, 90);
        put_apply_state(&mut lb, 11, 110);
        put_raft_state(&mut lb, 12, 100, 90);
        put_apply_state(&mut lb, 12, 95);

        // region state doesn't contains the peer itself.
        put_region_state(&mut lb, 13, &[]);

        debugger.raft_engine.consume(&mut lb, true).unwrap();

        let mut bad_regions = debugger.bad_regions().unwrap();
        bad_regions.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(bad_regions.len(), 4);
        for (i, (region_id, _)) in bad_regions.into_iter().enumerate() {
            assert_eq!(region_id, (10 + i) as u64);
        }
    }

    #[test]
    fn test_tombstone_regions() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        debugger.set_store_id(11);
        let mut apply_state = RaftApplyState::default();
        apply_state.set_applied_index(5);
        let mut lb = debugger.raft_engine.log_batch(10);

        // region 1 with peers at stores 11, 12, 13.
        let region_1 = init_region_state(&debugger.raft_engine, 1, &[11, 12, 13], 0);
        lb.put_apply_state(1, 5, &apply_state).unwrap();
        // Got the target region from pd, which doesn't contains the store.
        let mut target_region_1 = region_1.clone();
        target_region_1.mut_peers().remove(0);
        target_region_1.mut_region_epoch().set_conf_ver(100);

        // region 2 with peers at stores 11, 12, 13.
        let region_2 = init_region_state(&debugger.raft_engine, 2, &[11, 12, 13], 0);
        lb.put_apply_state(2, 5, &apply_state).unwrap();
        // Got the target region from pd, which has different peer_id.
        let mut target_region_2 = region_2.clone();
        target_region_2.mut_peers()[0].set_id(100);
        target_region_2.mut_region_epoch().set_conf_ver(100);

        // region 3 with peers at stores 21, 22, 23.
        let region_3 = init_region_state(&debugger.raft_engine, 3, &[21, 22, 23], 0);
        lb.put_apply_state(3, 5, &apply_state).unwrap();
        // Got the target region from pd but the peers are not changed.
        let mut target_region_3 = region_3;
        target_region_3.mut_region_epoch().set_conf_ver(100);

        // region 4 with peers at stores 11, 12, 13.
        let region_4 = init_region_state(&debugger.raft_engine, 4, &[11, 12, 13], 0);
        lb.put_apply_state(4, 5, &apply_state).unwrap();
        // Got the target region from pd but region epoch are not changed.
        let mut target_region_4 = region_4;
        target_region_4.mut_peers()[0].set_id(100);

        // region 5 with peers at stores 11, 12, 13.
        let region_5 = init_region_state(&debugger.raft_engine, 5, &[11, 12, 13], 0);
        lb.put_apply_state(5, 5, &apply_state).unwrap();
        // Got the target region from pd but peer is not scheduled.
        let mut target_region_5 = region_5;
        target_region_5.mut_region_epoch().set_conf_ver(100);

        debugger.raft_engine.consume(&mut lb, true).unwrap();

        let must_meet_error = |region_with_error: metapb::Region| {
            let error_region_id = region_with_error.get_id();
            let regions = vec![
                target_region_1.clone(),
                target_region_2.clone(),
                region_with_error,
            ];
            let errors = debugger.set_region_tombstone(regions).unwrap();
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].0, error_region_id);
            assert_eq!(
                debugger
                    .raft_engine
                    .get_region_state(1, u64::MAX)
                    .unwrap()
                    .unwrap()
                    .take_region(),
                region_1
            );

            assert_eq!(
                debugger
                    .raft_engine
                    .get_region_state(2, u64::MAX)
                    .unwrap()
                    .unwrap()
                    .take_region(),
                region_2
            );
        };

        // Test with bad target region. No region state in rocksdb should be changed.
        must_meet_error(target_region_3);
        must_meet_error(target_region_4);
        must_meet_error(target_region_5);

        // After set_region_tombstone success, all region should be adjusted.
        let target_regions = vec![target_region_1, target_region_2];
        let errors = debugger.set_region_tombstone(target_regions).unwrap();
        assert!(errors.is_empty());
        for &region_id in &[1, 2] {
            let state = debugger
                .raft_engine
                .get_region_state(region_id, u64::MAX)
                .unwrap()
                .unwrap()
                .get_state();
            assert_eq!(state, PeerState::Tombstone);
        }
    }

    #[test]
    fn test_tombstone_regions_by_id() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        debugger.set_store_id(11);
        let mut apply_state = RaftApplyState::default();
        apply_state.set_applied_index(5);
        let mut lb = debugger.raft_engine.log_batch(10);

        // tombstone region 1 which currently not exists.
        let errors = debugger.set_region_tombstone_by_id(vec![1]).unwrap();
        assert!(!errors.is_empty());

        // region 1 with peers at stores 11, 12, 13.
        init_region_state(&debugger.raft_engine, 1, &[11, 12, 13], 0);
        lb.put_apply_state(1, 5, &apply_state).unwrap();
        debugger.raft_engine.consume(&mut lb, true).unwrap();
        let mut expected_state = debugger
            .raft_engine
            .get_region_state(1, u64::MAX)
            .unwrap()
            .unwrap();
        expected_state.set_state(PeerState::Tombstone);

        // tombstone region 1.
        let errors = debugger.set_region_tombstone_by_id(vec![1]).unwrap();
        assert!(errors.is_empty());
        assert_eq!(
            debugger
                .raft_engine
                .get_region_state(1, u64::MAX)
                .unwrap()
                .unwrap(),
            expected_state
        );

        // tombstone region 1 again.
        let errors = debugger.set_region_tombstone_by_id(vec![1]).unwrap();
        assert!(errors.is_empty());
        assert_eq!(
            debugger
                .raft_engine
                .get_region_state(1, u64::MAX)
                .unwrap()
                .unwrap(),
            expected_state
        );
    }

    #[test]
    fn test_drop_unapplied_raftlog() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        let raft_engine = &debugger.raft_engine;

        init_region_state(raft_engine, 1, &[100, 101], 1);
        init_region_state(raft_engine, 2, &[100, 103], 1);
        init_raft_state(raft_engine, 1, 100, 90, 80, Some(80));
        init_raft_state(raft_engine, 2, 80, 80, 80, Some(80));

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

    // It tests that the latest apply state cannot be read as it is invisible
    // on persisted_applied
    #[test]
    fn test_drop_unapplied_raftlog_2() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        let raft_engine = &debugger.raft_engine;

        init_region_state(raft_engine, 1, &[100, 101], 1);
        init_raft_state(raft_engine, 1, 100, 90, 80, Some(80));
        // It will not be read due to less persisted_applied
        init_raft_state(raft_engine, 1, 200, 190, 180, None);

        // Drop raftlog on all regions
        debugger.drop_unapplied_raftlog(None).unwrap();
        let region_info_1 = debugger.region_info(1).unwrap();

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
    }

    #[test]
    fn test_get_all_regions_in_store() {
        let dir = test_util::temp_dir("test-debugger", false);
        let debugger = new_debugger(dir.path());
        let raft_engine = &debugger.raft_engine;

        init_region_state(raft_engine, 1, &[100, 101], 1);
        init_region_state(raft_engine, 3, &[100, 101], 1);
        init_region_state(raft_engine, 4, &[100, 101], 1);

        let mut lb = raft_engine.log_batch(3);

        let mut put_tombsotne_region = |region_id: u64| {
            let mut region = metapb::Region::default();
            region.set_id(region_id);
            let mut region_state = RegionLocalState::default();
            region_state.set_state(PeerState::Tombstone);
            region_state.set_region(region.clone());
            lb.put_region_state(region_id, INITIAL_APPLY_INDEX, &region_state)
                .unwrap();
            raft_engine.consume(&mut lb, true).unwrap();
        };

        put_tombsotne_region(2);
        put_tombsotne_region(5);

        let regions = debugger.get_all_regions_in_store().unwrap();
        assert_eq!(regions, vec![1, 3, 4]);
    }
}
