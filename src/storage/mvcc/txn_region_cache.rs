use crate::storage::kv::{self, ScanMode, Snapshot, Statistics};
use engine_rocks::{RocksEngine, RocksSnapshot};
use engine_traits::{IterOptions, Snapshot as SnapshotTrait, CF_DEFAULT, CF_WRITE};
use kvproto::metapb;
use raftstore::store::{
    RegionCache, RegionCacheBuilder, RegionCacheBuilderFactory, RegionSnapshot,
};
use raftstore::{Error, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tikv_util::collections::HashMap;
use txn_types::{Key, Value, WriteRef, WriteType};

pub struct TxnRegionCache {
    valid: AtomicBool,
    data: HashMap<Key, Value>,
    region_id: u64,
}

impl TxnRegionCache {
    pub fn new(data: HashMap<Key, Value>, region_id: u64) -> TxnRegionCache {
        TxnRegionCache {
            data,
            region_id,
            valid: AtomicBool::new(true),
        }
    }
}

impl RegionCache for TxnRegionCache {
    fn get(&self, key: &Key) -> Option<Value> {
        self.data.get(key).map(|v| v.clone())
    }
    fn valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }
    fn set_valid(&self, v: bool) {
        self.valid.store(v, Ordering::Release);
    }
    fn region_id(&self) -> u64 {
        self.region_id
    }
}

pub struct TxnRegionCacheBuilder {
    region: metapb::Region,
}

impl RegionCacheBuilder<RocksEngine> for TxnRegionCacheBuilder {
    fn build(&self, snap: RocksSnapshot) -> Result<Arc<dyn RegionCache>> {
        let region_snapshot =
            RegionSnapshot::<RocksEngine>::from_snapshot(snap.into_sync(), self.region.clone());
        self.do_build(region_snapshot)
            .map_err(|_| Error::Other("error exit".into()))
    }

    fn region_id(&self) -> u64 {
        self.region.get_id()
    }
}

impl TxnRegionCacheBuilder {
    fn do_build<S: Snapshot>(&self, snap: S) -> kv::Result<Arc<dyn RegionCache>> {
        let iter_opt = IterOptions::new(None, None, false);
        let mut write_cursor = snap.iter_cf(CF_WRITE, iter_opt, ScanMode::Forward)?;
        let mut stats = Statistics::default();
        //if !write_cursor.seek(self.region.get_start_key(), &mut stats.write) {
        write_cursor.seek_to_first(&mut stats.write);
        let mut data = HashMap::default();
        let mut last_key = vec![];
        while write_cursor.valid()? {
            let key = write_cursor.key(&mut stats.write);
            let (user_key, _) = Key::split_on_ts_for(key).unwrap();
            if user_key.to_vec() == last_key {
                write_cursor.next(&mut stats.write);
                continue;
            }
            let value = write_cursor.value(&mut stats.write);
            let write = WriteRef::parse(value).unwrap();
            if write.write_type == WriteType::Put || write.write_type == WriteType::Delete {
                last_key = user_key.to_owned();
                let user_value = if let Some(v) = write.short_value {
                    Some(v.to_owned())
                } else if write.write_type == WriteType::Put {
                    snap.get_cf(
                        CF_DEFAULT,
                        &Key::from_encoded(last_key.clone()).append_ts(write.start_ts),
                    )?
                } else {
                    None
                };

                if let Some(v) = user_value {
                    data.insert(Key::from_encoded(user_key.to_vec()), v);
                }
            }
            write_cursor.next(&mut stats.write);
        }
        Ok(Arc::new(TxnRegionCache::new(data, self.region.get_id())))
    }
}

pub struct TxnRegionCacheBuilderFactory {}

impl TxnRegionCacheBuilderFactory {
    pub fn new() -> TxnRegionCacheBuilderFactory {
        Self {}
    }
}

impl RegionCacheBuilderFactory<RocksEngine> for TxnRegionCacheBuilderFactory {
    fn create_builder(&self, region: metapb::Region) -> Box<dyn RegionCacheBuilder<RocksEngine>> {
        Box::new(TxnRegionCacheBuilder { region })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::kv::{Engine, TestEngineBuilder};
    use crate::storage::mvcc::tests::*;
    use kvproto::kvrpcpb::Context;

    #[test]
    fn test_basic_builder() {
        let region = metapb::Region::default();
        let builder = TxnRegionCacheBuilder { region };

        let engine = TestEngineBuilder::new().build().unwrap();
        must_prewrite_put(&engine, b"item1", b"value1", b"item1", 2);
        must_prewrite_put(&engine, b"item2", b"value2", b"item2", 2);
        must_prewrite_put(&engine, b"item3", b"value3", b"item3", 2);
        must_commit(&engine, b"item1", 2, 3);
        must_commit(&engine, b"item2", 2, 3);
        must_commit(&engine, b"item3", 2, 3);
        must_prewrite_put(&engine, b"item1", b"value4", b"item1", 4);
        must_commit(&engine, b"item1", 4, 5);

        let ctx = Context::default();
        let snap = engine.snapshot(&ctx).unwrap();
        let cache = builder.do_build(snap).unwrap();
        assert_eq!(
            b"value4".to_vec(),
            cache.get(&Key::from_raw(b"item1")).unwrap()
        );
        assert_eq!(
            b"value2".to_vec(),
            cache.get(&Key::from_raw(b"item2")).unwrap()
        );
        assert_eq!(
            b"value3".to_vec(),
            cache.get(&Key::from_raw(b"item3")).unwrap()
        );
    }
}
