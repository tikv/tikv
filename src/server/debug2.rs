// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::{raw::CompactOptions, util::get_cf_handle, RocksEngine, RocksEngineIterator};
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
                if region_state.get_state() != PeerState::Normal {
                    return Err(Error::NotFound(format!(
                        "region {:?} has been deleted",
                        region_id
                    )));
                }
                let region = region_state.get_region();
                let start_key = &keys::data_key(region.get_start_key());
                let end_key = &keys::data_end_key(region.get_end_key());
                let mut sizes = vec![];
                let mut tablet_cache =
                    get_tablet_cache(&self.tablet_reg, region.id, Some(region_state))?;
                let tablet = tablet_cache.latest().unwrap();
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
        self.raft_engine
            .for_each_raft_group::<raftstore::Error, _>(&mut |region_id| {
                region_ids.push(region_id);
                Ok(())
            })
            .unwrap();
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

    fn get_region_properties(&self, _region_id: u64) -> Result<Vec<(String, String)>> {
        unimplemented!()
    }

    fn reset_to_version(&self, _version: u64) {
        unimplemented!()
    }

    fn set_kv_statistics(&mut self, _s: Option<std::sync::Arc<engine_rocks::RocksStatistics>>) {
        unimplemented!()
    }

    fn set_raft_statistics(&mut self, _s: Option<std::sync::Arc<engine_rocks::RocksStatistics>>) {
        unimplemented!()
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
}
