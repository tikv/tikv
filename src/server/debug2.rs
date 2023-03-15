// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::{raw::CompactOptions, util::get_cf_handle, RocksEngine, RocksEngineIterator};
use engine_traits::{
    CachedTablet, Iterable, Peekable, RaftEngine, TabletContext, TabletRegistry, CF_DEFAULT,
    CF_LOCK, CF_WRITE,
};
use keys::{data_key, DATA_PREFIX_KEY};
use kvproto::{debugpb::Db as DbType, kvrpcpb::MvccInfo, metapb, raft_serverpb::RegionLocalState};
use nom::AsBytes;
use raft::prelude::Entry;

use super::debug::{BottommostLevelCompaction, RegionInfo};
use crate::{
    config::ConfigController,
    server::debug::{Error, Result},
    storage::mvcc::{MvccInfoCollector, MvccInfoScanner},
};

pub struct MvccInfoIteratorV2<ER: RaftEngine> {
    scanner: MvccInfoScanner<RocksEngineIterator, MvccInfoCollector>,
    tablet_reg: TabletRegistry<RocksEngine>,
    raft_engine: ER,
    cur_region: metapb::Region,
    start: Vec<u8>,
    end: Vec<u8>,
    limit: usize,
    count: usize,
}

impl<ER: RaftEngine> MvccInfoIteratorV2<ER> {
    pub fn new(
        raft_engine: ER,
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
        let mut first_region_state = find_region_state_by_key(&raft_engine, seek_key)?;

        let mut tablet_cache = get_tablet_cache(
            &tablet_reg,
            first_region_state.get_region().get_id(),
            Some(first_region_state.clone()),
        )?;

        let tablet = tablet_cache.latest().unwrap();
        let scanner = MvccInfoScanner::new(
            |cf, opts| tablet.iterator_opt(cf, opts).map_err(|e| box_err!(e)),
            if start.is_empty() { None } else { Some(start) },
            if end.is_empty() { None } else { Some(end) },
            MvccInfoCollector::default(),
        )
        .map_err(|e| -> Error { box_err!(e) })?;

        Ok(MvccInfoIteratorV2 {
            scanner,
            tablet_reg,
            raft_engine,
            cur_region: first_region_state.take_region(),
            start: start.to_vec(),
            end: end.to_vec(),
            limit,
            count: 0,
        })
    }
}

impl<ER: RaftEngine> Iterator for MvccInfoIteratorV2<ER> {
    type Item = raftstore::Result<(Vec<u8>, MvccInfo)>;

    fn next(&mut self) -> Option<raftstore::Result<(Vec<u8>, MvccInfo)>> {
        if self.limit != 0 && self.count >= self.limit {
            return None;
        }

        loop {
            match self.scanner.next_item() {
                Ok(Some(item)) => {
                    self.count += 1;
                    return Some(Ok(item));
                }
                Ok(None) => {
                    let cur_end_key = self.cur_region.get_end_key();
                    if cur_end_key.is_empty() {
                        return None;
                    }

                    if let Ok(next_region_state) =
                        find_region_state_by_key(&self.raft_engine, cur_end_key)
                    {
                        if &self.cur_region == next_region_state.get_region() {
                            return None;
                        }
                        self.cur_region = next_region_state.get_region().clone();
                        let mut tablet_cache = get_tablet_cache(
                            &self.tablet_reg,
                            next_region_state.get_region().get_id(),
                            Some(next_region_state),
                        )
                        .unwrap();
                        let tablet = tablet_cache.latest().unwrap();
                        self.scanner = MvccInfoScanner::new(
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
                        .unwrap();
                    } else {
                        return None;
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// Debugger for raftstore-v2
#[derive(Clone)]
pub struct DebuggerV2<ER: RaftEngine> {
    tablet_reg: TabletRegistry<RocksEngine>,
    raft_engine: ER,
    _cfg_controller: ConfigController,
}

impl<ER: RaftEngine> DebuggerV2<ER> {
    pub fn new(
        tablet_reg: TabletRegistry<RocksEngine>,
        raft_engine: ER,
        cfg_controller: ConfigController,
    ) -> Self {
        println!("Debugger for raftstore-v2 is used");
        DebuggerV2 {
            tablet_reg,
            raft_engine,
            _cfg_controller: cfg_controller,
        }
    }

    pub fn get_all_regions_in_store(&self) -> Result<Vec<u64>> {
        let mut region_ids = vec![];
        self.raft_engine
            .for_each_raft_group::<raftstore::Error, _>(&mut |region_id| {
                region_ids.push(region_id);
                Ok(())
            })
            .unwrap();
        Ok(region_ids)
    }

    pub fn get(&self, db: DbType, cf: &str, key: &[u8]) -> Result<Vec<u8>> {
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

    pub fn raft_log(&self, region_id: u64, log_index: u64) -> Result<Entry> {
        if let Some(log) = box_try!(self.raft_engine.get_entry(region_id, log_index)) {
            return Ok(log);
        }
        Err(Error::NotFound(format!(
            "raft log for region {} at index {}",
            region_id, log_index
        )))
    }

    pub fn region_info(&self, region_id: u64) -> Result<RegionInfo> {
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

    pub fn region_size<T: AsRef<str>>(
        &self,
        region_id: u64,
        cfs: Vec<T>,
    ) -> Result<Vec<(T, usize)>> {
        match self.raft_engine.get_region_state(region_id, u64::MAX) {
            Ok(Some(region_state)) => {
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

    /// Scan MVCC Infos for given range `[start, end)`.
    pub fn scan_mvcc(
        &self,
        start: &[u8],
        end: &[u8],
        limit: u64,
    ) -> Result<MvccInfoIteratorV2<ER>> {
        if end.is_empty() && limit == 0 {
            return Err(Error::InvalidArgument("no limit and to_key".to_owned()));
        }

        MvccInfoIteratorV2::new(
            self.raft_engine.clone(),
            self.tablet_reg.clone(),
            start,
            end,
            limit as usize,
        )
    }

    /// Compact the cf[start..end) in the db.
    pub fn compact(
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
                if let Some((start_key, end_key)) =
                    range_in_region((start, end), region_state.get_region())
                {
                    let start = if start_key.is_empty() {
                        None
                    } else {
                        Some(data_key(start_key))
                    };
                    let end = if end.is_empty() {
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

// return the overlap range (without data prefix) of the `range` in region or
// None if they are exclusive
fn range_in_region<'a>(
    range: (&'a [u8], &'a [u8]),
    region: &'a metapb::Region,
) -> Option<(&'a [u8], &'a [u8])> {
    if range.0.is_empty() && range.1.is_empty() {
        return Some((region.get_start_key(), region.get_end_key()));
    } else if range.0.is_empty() {
        assert!(range.1.starts_with(DATA_PREFIX_KEY));
        if region.get_start_key() < &range.1[DATA_PREFIX_KEY.len()..] {
            return Some((
                region.get_start_key(),
                smaller_key(&range.1[DATA_PREFIX_KEY.len()..], region.get_end_key()),
            ));
        }
        None
    } else if range.1.is_empty() {
        assert!(range.0.starts_with(DATA_PREFIX_KEY));
        if &range.0[DATA_PREFIX_KEY.len()..] < region.get_end_key()
            || region.get_end_key().is_empty()
        {
            return Some((
                larger_key(&range.0[DATA_PREFIX_KEY.len()..], region.get_start_key()),
                region.get_end_key(),
            ));
        }
        None
    } else {
        assert!(range.0.starts_with(DATA_PREFIX_KEY));
        assert!(range.1.starts_with(DATA_PREFIX_KEY));
        let start_key = larger_key(&range.0[DATA_PREFIX_KEY.len()..], region.get_start_key());
        let end_key = smaller_key(&range.1[DATA_PREFIX_KEY.len()..], region.get_end_key());
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
            if region.get_start_key() <= key
                && (key < region.get_end_key() || region.get_end_key().is_empty())
            {
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
            Err(e) => return Err(box_err!(e)),
        }
    }
}

fn smaller_key<'a>(key1: &'a [u8], key2: &'a [u8]) -> &'a [u8] {
    if key1 < key2 {
        return key1;
    }
    key2
}

fn larger_key<'a>(key1: &'a [u8], key2: &'a [u8]) -> &'a [u8] {
    if key1 < key2 || key2.is_empty() {
        return key2;
    }
    key1
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use engine_test::raft::RaftTestEngine;
    use engine_traits::{RaftLogBatch, SyncMutable, CF_DEFAULT, CF_LOCK, CF_WRITE};
    use kvproto::{metapb, raft_serverpb::*};
    use raft::prelude::EntryType;

    use super::*;
    use crate::{
        config::TikvConfig,
        server::KvEngineFactoryBuilder,
        storage::{txn::tests::must_prewrite_put, TestEngineBuilder},
    };

    const INITIAL_TABLET_INDEX: u64 = 5;
    const INITIAL_APPLY_INDEX: u64 = 5;

    fn new_debugger(path: &Path) -> DebuggerV2<RaftTestEngine> {
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

        let raft_engine = RaftTestEngine::new(cfg.raft_engine.config(), None, None).unwrap();

        DebuggerV2::new(reg, raft_engine, ConfigController::default())
    }

    // For simplicity, the format of the key is inline with data in
    // prepare_data_on_disk
    fn extract_key(key: &[u8]) -> &[u8] {
        &key[1..4]
    }

    // Prepare some data in MVCC format
    // Data for each region:
    // Region 1: k00 .. k04
    // Region 2: k05 .. k09
    // Region 3: k10 .. k14
    fn prepare_data_on_disk(path: &Path) {
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

        let raft_engine = RaftTestEngine::new(cfg.raft_engine.config(), None, None).unwrap();
        let mut wb = raft_engine.log_batch(5);
        for i in 0..3 {
            let mut region = metapb::Region::default();
            let start_key = {
                if i == 0 {
                    String::new()
                } else {
                    format!("k{:02}", i * 5)
                }
            };
            let end_key = {
                if i == 2 {
                    String::new()
                } else {
                    format!("k{:02}", (i + 1) * 5)
                }
            };
            region.set_id(i + 1);
            region.set_start_key(start_key.into_bytes());
            region.set_end_key(end_key.into_bytes());
            let mut region_state = RegionLocalState::default();
            region_state.set_tablet_index(INITIAL_TABLET_INDEX);
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
    }

    #[test]
    fn test_scan_mvcc() {
        let dir = test_util::temp_dir("test-debugger", false);
        prepare_data_on_disk(dir.path());
        let debugger = new_debugger(dir.path());
        // Test scan with bad start, end or limit.
        assert!(debugger.scan_mvcc(b"z", b"", 0).is_err());
        assert!(debugger.scan_mvcc(b"z", b"x", 3).is_err());

        let mut keys = vec![];
        for i in 0..15 {
            keys.push(format!("k{:02}", i).into_bytes());
        }

        // Full scan
        let scanner = debugger.scan_mvcc(b"", b"", 100).unwrap();
        for (i, item) in scanner.enumerate() {
            let (key, _) = item.unwrap();
            assert_eq!(keys[i], extract_key(&key));
        }

        // Range has more elements than limit
        let mut scanner = debugger.scan_mvcc(b"zk01", b"zk09", 5).unwrap();
        for i in 1..6 {
            let (key, _) = scanner.next().unwrap().unwrap();
            assert_eq!(keys[i], extract_key(&key));
        }
        assert!(scanner.next().is_none());

        // Range has less elements than limit
        let mut scanner = debugger.scan_mvcc(b"zk07", b"zk10", 10).unwrap();
        for i in 7..10 {
            let (key, _) = scanner.next().unwrap().unwrap();
            assert_eq!(keys[i], extract_key(&key));
        }
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
}
