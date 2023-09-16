// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_rocks::{
    raw::CompactOptions, util::get_cf_handle, RocksEngine, RocksEngineIterator, RocksStatistics,
};
use engine_traits::{
    CachedTablet, Iterable, Peekable, RaftEngine, TabletContext, TabletRegistry, CF_DEFAULT,
    CF_LOCK, CF_WRITE,
};
use keys::{data_key, DATA_MAX_KEY, DATA_PREFIX_KEY};
use kvproto::{
    debugpb::Db as DbType,
    kvrpcpb::MvccInfo,
    metapb,
    raft_serverpb::{PeerState, RegionLocalState, StoreIdent},
};
use nom::AsBytes;
use raft::prelude::Entry;
use raftstore::store::util::check_key_in_region;

use super::debug::{BottommostLevelCompaction, Debugger, RegionInfo};
use crate::{
    config::ConfigController,
    server::debug::{Error, Result},
    storage::mvcc::{MvccInfoCollector, MvccInfoScanner},
};

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
            let scanner = Some(
                MvccInfoScanner::new(
                    |cf, opts| tablet.iterator_opt(cf, opts).map_err(|e| box_err!(e)),
                    if start.is_empty() { None } else { Some(start) },
                    if end.is_empty() { None } else { Some(end) },
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
                    self.scanner = Some(
                        MvccInfoScanner::new(
                            |cf, opts| tablet.iterator_opt(cf, opts).map_err(|e| box_err!(e)),
                            if self.start.is_empty() {
                                None
                            } else {
                                Some(self.start.as_bytes())
                            },
                            if self.end.is_empty() {
                                None
                            } else {
                                Some(self.end.as_bytes())
                            },
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
    _cfg_controller: ConfigController,
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
            _cfg_controller: cfg_controller,
            kv_statistics: None,
            raft_statistics: None,
        }
    }
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
        let raft_state = box_try!(self.raft_engine.get_raft_state(region_id));
        let apply_state = box_try!(self.raft_engine.get_apply_state(region_id, u64::MAX));
        let region_state = box_try!(self.raft_engine.get_region_state(region_id, u64::MAX));

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

<<<<<<< HEAD
        let mut region_states = vec![];
        self.raft_engine
            .for_each_raft_group::<raftstore::Error, _>(&mut |region_id| {
                let region_state = self
                    .raft_engine
                    .get_region_state(region_id, u64::MAX)
                    .unwrap()
                    .unwrap();
                if region_state.state == PeerState::Normal {
                    region_states.push(region_state);
                }
                Ok(())
            })
            .unwrap();
=======
        let mut region_states = get_all_active_region_states(&self.raft_engine);
>>>>>>> 820ed9395b (tikv-ctl v2: get_all_regions_in_store excludes `tombstone` (#15522))

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
        let mut compactions = vec![];
        self.raft_engine
            .for_each_raft_group::<raftstore::Error, _>(&mut |region_id| {
                let region_state = self
                    .raft_engine
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
                    compactions.push((region_id, start, end, region_state));
                };

                Ok(())
            })
            .unwrap();

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
        unimplemented!()
    }

    fn dump_raft_stats(&self) -> Result<String> {
        unimplemented!()
    }

    fn modify_tikv_config(&self, _config_name: &str, _config_value: &str) -> Result<()> {
        unimplemented!()
    }

    fn get_store_ident(&self) -> Result<StoreIdent> {
        unimplemented!()
    }

<<<<<<< HEAD
    fn get_region_properties(&self, _region_id: u64) -> Result<Vec<(String, String)>> {
        unimplemented!()
=======
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
>>>>>>> 820ed9395b (tikv-ctl v2: get_all_regions_in_store excludes `tombstone` (#15522))
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

<<<<<<< HEAD
// `key1` and `key2` should both be start_key or end_key.
fn smaller_key<'a>(key1: &'a [u8], key2: &'a [u8], end_key: bool) -> &'a [u8] {
    if end_key && key1.is_empty() {
        return key2;
    }
    if end_key && key2.is_empty() {
        return key1;
    }
    if key1 < key2 {
        return key1;
    }
    key2
}

// `key1` and `key2` should both be start_key or end_key.
fn larger_key<'a>(key1: &'a [u8], key2: &'a [u8], end_key: bool) -> &'a [u8] {
    if end_key && key1.is_empty() {
        return key1;
=======
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
>>>>>>> 820ed9395b (tikv-ctl v2: get_all_regions_in_store excludes `tombstone` (#15522))
    }
    if end_key && key2.is_empty() {
        return key2;
    }
    if key1 < key2 {
        return key2;
    }
    key1
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use engine_traits::{RaftLogBatch, SyncMutable, CF_DEFAULT, CF_LOCK, CF_WRITE};
    use kvproto::{metapb, raft_serverpb::*};
    use raft::prelude::EntryType;
    use raft_log_engine::RaftLogEngine;

    use super::*;
    use crate::{
        config::TikvConfig,
        server::KvEngineFactoryBuilder,
        storage::{txn::tests::must_prewrite_put, TestEngineBuilder},
    };

    const INITIAL_TABLET_INDEX: u64 = 5;
    const INITIAL_APPLY_INDEX: u64 = 5;

    fn new_debugger(path: &Path) -> DebuggerImplV2<RaftLogEngine> {
        let mut cfg = TikvConfig::default();
        cfg.storage.data_dir = path.to_str().unwrap().to_string();
        cfg.raft_store.raftdb_path = cfg.infer_raft_db_path(None).unwrap();
        cfg.raft_engine.mut_config().dir = cfg.infer_raft_engine_path(None).unwrap();
        let cache = cfg
            .storage
            .block_cache
            .build_shared_cache(cfg.storage.engine);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let factory = KvEngineFactoryBuilder::new(env, &cfg, cache).build();
        let reg = TabletRegistry::new(Box::new(factory), path).unwrap();

        let raft_engine = RaftLogEngine::new(cfg.raft_engine.config(), None, None).unwrap();

        DebuggerImplV2::new(reg, raft_engine, ConfigController::default())
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

    // For simplicity, the format of the key is inline with data in
    // prepare_data_on_disk
    fn extract_key(key: &[u8]) -> &[u8] {
        &key[1..4]
    }

    // Prepare some data
    // Data for each region:
    // Region 1: k00 .. k04
    // Region 2: k05 .. k09
    // Region 3: k10 .. k14
    // Region 4: k15 .. k19  <tombstone>
    // Region 5: k20 .. k24
    // Region 6: k26 .. k28  <range of region and tablet not matched>
    fn prepare_data_on_disk(path: &Path) {
        let mut cfg = TikvConfig::default();
        cfg.storage.data_dir = path.to_str().unwrap().to_string();
        cfg.raft_store.raftdb_path = cfg.infer_raft_db_path(None).unwrap();
        cfg.raft_engine.mut_config().dir = cfg.infer_raft_engine_path(None).unwrap();
        cfg.gc.enable_compaction_filter = false;
        let cache = cfg
            .storage
            .block_cache
            .build_shared_cache(cfg.storage.engine);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let factory = KvEngineFactoryBuilder::new(env, &cfg, cache).build();
        let reg = TabletRegistry::new(Box::new(factory), path).unwrap();

        let raft_engine = RaftLogEngine::new(cfg.raft_engine.config(), None, None).unwrap();
        let mut wb = raft_engine.log_batch(5);
        for i in 0..6 {
            let mut region = metapb::Region::default();
            let start_key = format!("k{:02}", i * 5);
            let end_key = format!("k{:02}", (i + 1) * 5);
            region.set_id(i + 1);
            region.set_start_key(start_key.into_bytes());
            region.set_end_key(end_key.into_bytes());
            let mut region_state = RegionLocalState::default();
            region_state.set_tablet_index(INITIAL_TABLET_INDEX);
            if region.get_id() == 4 {
                region_state.set_state(PeerState::Tombstone);
            } else if region.get_id() == 6 {
                region.set_start_key(b"k26".to_vec());
                region.set_end_key(b"k28".to_vec());
            }
            region_state.set_region(region);

            let tablet_path = reg.tablet_path(i + 1, INITIAL_TABLET_INDEX);
            // Use tikv_kv::RocksEngine instead of loading tablet from registry in order to
            // use prewrite method to prepare mvcc data
            let mut engine = TestEngineBuilder::new().path(tablet_path).build().unwrap();
            for i in i * 5..(i + 1) * 5 {
                let key = format!("zk{:02}", i);
                let val = format!("val{:02}", i);
                // Use prewrite only is enough for preparing mvcc data
                must_prewrite_put(
                    &mut engine,
                    key.as_bytes(),
                    val.as_bytes(),
                    key.as_bytes(),
                    10,
                );
            }

            wb.put_region_state(i + 1, INITIAL_APPLY_INDEX, &region_state)
                .unwrap();
        }
        raft_engine.consume(&mut wb, true).unwrap();
    }

    #[test]
    fn test_scan_mvcc() {
        let dir = test_util::temp_dir("test-debugger", false);
        prepare_data_on_disk(dir.path());
        let debugger = new_debugger(dir.path());
        // Test scan with bad start, end or limit.
        assert!(debugger.scan_mvcc(b"z", b"", 0).is_err());
        assert!(debugger.scan_mvcc(b"z", b"x", 3).is_err());

        let verify_scanner =
            |range, scanner: &mut dyn Iterator<Item = raftstore::Result<(Vec<u8>, MvccInfo)>>| {
                for i in range {
                    let key = format!("k{:02}", i).into_bytes();
                    assert_eq!(key, extract_key(&scanner.next().unwrap().unwrap().0));
                }
            };

        // full scann
        let mut scanner = debugger.scan_mvcc(b"", b"", 100).unwrap();
        verify_scanner(0..15, &mut scanner);
        verify_scanner(20..25, &mut scanner);
        verify_scanner(26..28, &mut scanner);
        assert!(scanner.next().is_none());

        // Range has more elements than limit
        let mut scanner = debugger.scan_mvcc(b"zk01", b"zk09", 5).unwrap();
        verify_scanner(1..6, &mut scanner);
        assert!(scanner.next().is_none());

        // Range has less elements than limit
        let mut scanner = debugger.scan_mvcc(b"zk07", b"zk10", 10).unwrap();
        verify_scanner(7..10, &mut scanner);
        assert!(scanner.next().is_none());

        // Start from the key where no region contains it
        let mut scanner = debugger.scan_mvcc(b"zk16", b"", 100).unwrap();
        verify_scanner(20..25, &mut scanner);
        verify_scanner(26..28, &mut scanner);
        assert!(scanner.next().is_none());

        // Scan a range not existed in the cluster
        let mut scanner = debugger.scan_mvcc(b"zk16", b"zk19", 100).unwrap();
        assert!(scanner.next().is_none());

        // The end key is less than the start_key of the first region
        let mut scanner = debugger.scan_mvcc(b"", b"zj", 100).unwrap();
        assert!(scanner.next().is_none());
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
<<<<<<< HEAD
=======

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
>>>>>>> 820ed9395b (tikv-ctl v2: get_all_regions_in_store excludes `tombstone` (#15522))
}
