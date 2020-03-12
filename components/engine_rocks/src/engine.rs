// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use engine_traits::{
    Error, IterOptions, Iterable, KvEngine, Mutable, Peekable, ReadOptions, Result,
};
use rocksdb::{DBIterator, Writable, DB};

use crate::db_vector::RocksDBVector;
use crate::options::RocksReadOptions;
use crate::util::get_cf_handle;
use crate::{RocksEngineIterator, RocksSnapshot};

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct RocksEngine(Arc<DB>);

impl RocksEngine {
    pub fn from_db(db: Arc<DB>) -> Self {
        RocksEngine(db)
    }

    pub fn from_ref(db: &Arc<DB>) -> &Self {
        unsafe { &*(db as *const Arc<DB> as *const RocksEngine) }
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        &self.0
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.0.clone()
    }

    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }

        // If path is not an empty directory, we say db exists. If path is not an empty directory
        // but db has not been created, `DB::list_column_families` fails and we can clean up
        // the directory by this indication.
        fs::read_dir(&path).unwrap().next().is_some()
    }
}

impl KvEngine for RocksEngine {
    type Snapshot = RocksSnapshot;

    fn snapshot(&self) -> RocksSnapshot {
        RocksSnapshot::new(self.0.clone())
    }

    fn sync(&self) -> Result<()> {
        self.0.sync_wal().map_err(Error::Engine)
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        let e: &dyn Any = &self.0;
        e.downcast_ref().expect("bad engine downcast")
    }
}

impl Iterable for RocksEngine {
    type Iterator = RocksEngineIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: RocksReadOptions = opts.into();
        Ok(RocksEngineIterator::from_raw(DBIterator::new(
            self.0.clone(),
            opt.into_raw(),
        )))
    }

    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let handle = get_cf_handle(&self.0, cf)?;
        let opt: RocksReadOptions = opts.into();
        Ok(RocksEngineIterator::from_raw(DBIterator::new_cf(
            self.0.clone(),
            handle,
            opt.into_raw(),
        )))
    }
}

impl Peekable for RocksEngine {
    type DBVector = RocksDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<RocksDBVector>> {
        let opt: RocksReadOptions = opts.into();
        let v = self.0.get_opt(key, &opt.into_raw())?;
        Ok(v.map(RocksDBVector::from_raw))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<RocksDBVector>> {
        let opt: RocksReadOptions = opts.into();
        let handle = get_cf_handle(&self.0, cf)?;
        let v = self.0.get_cf_opt(handle, key, &opt.into_raw())?;
        Ok(v.map(RocksDBVector::from_raw))
    }
}

impl Mutable for RocksEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.0.put(key, value).map_err(Error::Engine)
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.0, cf)?;
        self.0.put_cf(handle, key, value).map_err(Error::Engine)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.0.delete(key).map_err(Error::Engine)
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.0, cf)?;
        self.0.delete_cf(handle, key).map_err(Error::Engine)
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.0, cf)?;
        self.0
            .delete_range_cf(handle, begin_key, end_key)
            .map_err(Error::Engine)
    }
}

#[cfg(test)]
mod tests {
    use engine::rocks::util;
    use engine_traits::{Iterable, KvEngine, Mutable, Peekable};
    use kvproto::metapb::Region;
    use std::sync::Arc;
    use tempfile::Builder;

    use crate::{RocksEngine, RocksSnapshot};

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = RocksEngine::from_db(Arc::new(
            util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        ));

        let mut r = Region::default();
        r.set_id(10);

        let key = b"key";
        engine.put_msg(key, &r).unwrap();
        engine.put_msg_cf(cf, key, &r).unwrap();

        let snap = engine.snapshot();

        let mut r1: Region = engine.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_cf: Region = engine.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r1_cf);

        let mut r2: Region = snap.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r2);
        let r2_cf: Region = snap.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r2_cf);

        r.set_id(11);
        engine.put_msg(key, &r).unwrap();
        r1 = engine.get_msg(key).unwrap().unwrap();
        r2 = snap.get_msg(key).unwrap().unwrap();
        assert_ne!(r1, r2);

        let b: Option<Region> = engine.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = RocksEngine::from_db(Arc::new(
            util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        ));

        engine.put(b"k1", b"v1").unwrap();
        engine.put_cf(cf, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get_value(b"k1").unwrap().unwrap(), b"v1");
        assert!(engine.get_value_cf("foo", b"k1").is_err());
        assert_eq!(&*engine.get_value_cf(cf, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = RocksEngine::from_db(Arc::new(
            util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        ));

        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();
        engine.put_cf(cf, b"a1", b"v1").unwrap();
        engine.put_cf(cf, b"a2", b"v22").unwrap();

        let mut data = vec![];
        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v2".to_vec()),
            ]
        );
        data.clear();

        engine
            .scan_cf(cf, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                Ok(true)
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                (b"a1".to_vec(), b"v1".to_vec()),
                (b"a2".to_vec(), b"v22".to_vec()),
            ]
        );
        data.clear();

        let pair = engine.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek(b"a3").unwrap().is_none());
        let pair_cf = engine.seek_cf(cf, b"a1").unwrap().unwrap();
        assert_eq!(pair_cf, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek_cf(cf, b"a3").unwrap().is_none());

        let mut index = 0;
        engine
            .scan(b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                index += 1;
                Ok(index != 1)
            })
            .unwrap();

        assert_eq!(data.len(), 1);

        let snap = RocksSnapshot::new(engine.get_sync_db());

        engine.put(b"a3", b"v3").unwrap();
        assert!(engine.seek(b"a3").unwrap().is_some());

        let pair = snap.seek(b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(snap.seek(b"a3").unwrap().is_none());

        data.clear();

        snap.scan(b"", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 2);
    }
}
