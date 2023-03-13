// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashSet;
use engine_rocks::{raw::CompactOptions, util::get_cf_handle, RocksEngine, RocksEngineIterator};
use engine_traits::{
    CachedTablet, Iterable, Peekable, RaftEngine, TabletContext, TabletRegistry, CF_DEFAULT,
    CF_LOCK, CF_WRITE,
};
use keys::{data_key, DATA_PREFIX_KEY};
use kvproto::{metapb, raft_serverpb::RegionLocalState};
use nom::AsBytes;
use raft::prelude::Entry;

use super::debug::{BottommostLevelCompaction, RegionInfo};
pub use crate::storage::mvcc::MvccInfoIterator;
use crate::{
    config::{ConfigController, DbType},
    server::debug::{Error, Result},
};

// Debugger for raftstore-v2
#[derive(Clone)]
pub struct DebuggerV2<ER: RaftEngine> {
    tablet_reg: TabletRegistry<RocksEngine>,
    raft_engine: ER,
    cfg_controller: ConfigController,
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
            cfg_controller,
        }
    }

    pub fn get_all_regions_in_store(&self) -> Result<Vec<u64>> {
        let tablet_root = self.tablet_reg.tablet_root();
        let mut regions = HashSet::default();
        for dir in std::fs::read_dir(tablet_root)
            .unwrap_or_else(|e| panic!("fail to read tablet_root: {:?}", e))
        {
            let dir = dir.unwrap_or_else(|e| panic!("fail to read dir: {:?}", e));
            let (_, id, _) = self.tablet_reg.parse_tablet_name(&dir.path()).unwrap();
            regions.insert(id);
        }
        let mut regions: Vec<u64> = regions.into_iter().collect();
        regions.sort_unstable();
        Ok(regions)
    }

    pub fn get(&self, db: DbType, cf: &str, key: &[u8]) -> Result<Vec<u8>> {
        validate_db_and_cf(db, cf)?;
        let region_state = self.find_region_state_by_key(&key[DATA_PREFIX_KEY.len()..])?;
        let mut tablet_cache =
            self.get_tablet_cache(region_state.get_region().get_id(), Some(region_state))?;
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
                let mut tablet_cache = self.get_tablet_cache(region.id, Some(region_state))?;
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

    fn find_region_state_by_key(&self, key: &[u8]) -> Result<RegionLocalState> {
        let regions = self.get_all_regions_in_store()?;
        for id in regions {
            if let Ok(Some(region_state)) = self.raft_engine.get_region_state(id, u64::MAX) {
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
        &self,
        region_id: u64,
        state: Option<RegionLocalState>,
    ) -> Result<CachedTablet<RocksEngine>> {
        if let Some(tablet_cache) = self.tablet_reg.get(region_id) {
            Ok(tablet_cache)
        } else {
            let region_state = state.unwrap();
            let ctx =
                TabletContext::new(region_state.get_region(), Some(region_state.tablet_index));
            match self.tablet_reg.load(ctx, false) {
                Ok(tablet_cache) => Ok(tablet_cache),
                Err(e) => return Err(box_err!(e)),
            }
        }
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
            }).unwrap();

        for (region_id, start_key, end_key, region_state) in compactions {
            let mut tablet_cache = self.get_tablet_cache(region_id, Some(region_state))?;
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
    use engine_test::raft::RaftTestEngine;
    use engine_traits::{RaftLogBatch, SyncMutable, CF_DEFAULT, CF_LOCK, CF_WRITE};
    use kvproto::{metapb, raft_serverpb::*};
    use raft::prelude::EntryType;

    use super::*;
    use crate::{config::TikvConfig, server::KvEngineFactoryBuilder};

    fn new_debugger() -> DebuggerV2<RaftTestEngine> {
        let dir = test_util::temp_dir("test-debugger", false);

        let mut cfg = TikvConfig::default();
        cfg.storage.data_dir = dir.path().to_str().unwrap().to_string();
        cfg.raft_store.raftdb_path = cfg.infer_raft_db_path(None).unwrap();
        cfg.raft_engine.mut_config().dir = cfg.infer_raft_engine_path(None).unwrap();
        let cache = cfg
            .storage
            .block_cache
            .build_shared_cache(cfg.storage.engine);
        let env = cfg.build_shared_rocks_env(None, None).unwrap();

        let factory = KvEngineFactoryBuilder::new(env, &cfg, cache).build();
        let reg = TabletRegistry::new(Box::new(factory), dir.path()).unwrap();

        let raft_engine = RaftTestEngine::new(cfg.raft_engine.config(), None, None).unwrap();

        DebuggerV2::new(reg, raft_engine, ConfigController::default())
    }

    #[test]
    fn test_get() {
        let debugger = new_debugger();
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
        let debugger = new_debugger();
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
        let debugger = new_debugger();
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
        let debugger = new_debugger();
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
    fn test_compact() {
        let debugger = new_debugger();
        let compact = |db, cf| debugger.compact(db, cf, &[0], &[0xFF], 1, Some("skip").into());
        compact(DbType::Kv, CF_DEFAULT).unwrap();
        compact(DbType::Kv, CF_LOCK).unwrap();
        compact(DbType::Kv, CF_WRITE).unwrap();
        compact(DbType::Raft, CF_DEFAULT).unwrap_err();
    }
}
