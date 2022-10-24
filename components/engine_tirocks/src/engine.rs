// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs, path::Path, sync::Arc};

use engine_traits::{Code, Error, Result, Status};
use tirocks::{
    db::RawCfHandle,
    option::{ReadOptions, WriteOptions},
    Db, Iterator,
};

use crate::{
    db_vector::RocksPinSlice, engine_iterator, r2e, util, RocksEngineIterator, RocksSnapshot,
};

#[derive(Clone, Debug)]
pub struct RocksEngine {
    db: Arc<Db>,
    // TODO: always enable and remove following flag
    multi_batch_write: bool,
}

impl RocksEngine {
    #[inline]
    pub(crate) fn new(db: Arc<Db>) -> Self {
        RocksEngine {
            multi_batch_write: db.db_options().multi_batch_write(),
            db,
        }
    }

    #[inline]
    pub fn exists(path: impl AsRef<Path>) -> Result<bool> {
        let path = path.as_ref();
        if !path.exists() || !path.is_dir() {
            return Ok(false);
        }
        let current_file_path = path.join("CURRENT");
        if current_file_path.exists() && current_file_path.is_file() {
            return Ok(true);
        }

        // If path is not an empty directory, we say db exists. If path is not an empty
        // directory but db has not been created, `DB::list_column_families` fails and
        // we can clean up the directory by this indication.
        if fs::read_dir(&path).unwrap().next().is_some() {
            Err(Error::Engine(Status::with_code(Code::Corruption)))
        } else {
            Ok(false)
        }
    }

    #[inline]
    pub(crate) fn as_inner(&self) -> &Arc<Db> {
        &self.db
    }

    #[inline]
    pub fn cf(&self, name: &str) -> Result<&RawCfHandle> {
        util::cf_handle(&self.db, name)
    }

    #[inline]
    fn get(
        &self,
        opts: &engine_traits::ReadOptions,
        handle: &RawCfHandle,
        key: &[u8],
    ) -> Result<Option<RocksPinSlice>> {
        let mut opt = ReadOptions::default();
        opt.set_fill_cache(opts.fill_cache());
        // TODO: reuse slice.
        let mut slice = RocksPinSlice::default();
        match self.db.get_pinned(&opt, handle, key, &mut slice.0) {
            Ok(true) => Ok(Some(slice)),
            Ok(false) => Ok(None),
            Err(s) => Err(r2e(s)),
        }
    }

    #[inline]
    fn snapshot(&self) -> RocksSnapshot {
        RocksSnapshot::new(self.db.clone())
    }

    #[inline]
    pub(crate) fn multi_batch_write(&self) -> bool {
        self.multi_batch_write
    }

    #[inline]
    pub(crate) fn approximate_memtable_stats(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<(u64, u64)> {
        let handle = self.cf(cf)?;
        Ok(self
            .as_inner()
            .approximate_mem_table_stats(handle, start, end))
    }

    // TODO: move this function when MiscExt is implemented.
    #[cfg(test)]
    pub(crate) fn flush(&self, cf: &str, wait: bool) -> Result<()> {
        use tirocks::option::FlushOptions;

        let write_handle = self.cf(cf)?;
        self.as_inner()
            .flush(FlushOptions::default().set_wait(wait), write_handle)
            .map_err(r2e)
    }
}

impl engine_traits::Iterable for RocksEngine {
    type Iterator = RocksEngineIterator;

    fn iterator_opt(&self, cf: &str, opts: engine_traits::IterOptions) -> Result<Self::Iterator> {
        let opt = engine_iterator::to_tirocks_opt(opts);
        let handle = self.cf(cf)?;
        Ok(RocksEngineIterator::from_raw(Iterator::new(
            self.db.clone(),
            opt,
            handle,
        )))
    }
}

impl engine_traits::Peekable for RocksEngine {
    type DbVector = RocksPinSlice;

    #[inline]
    fn get_value_opt(
        &self,
        opts: &engine_traits::ReadOptions,
        key: &[u8],
    ) -> Result<Option<RocksPinSlice>> {
        self.get(opts, self.db.default_cf(), key)
    }

    #[inline]
    fn get_value_cf_opt(
        &self,
        opts: &engine_traits::ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<RocksPinSlice>> {
        let handle = self.cf(cf)?;
        self.get(opts, handle, key)
    }
}

impl engine_traits::SyncMutable for RocksEngine {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = self.db.default_cf();
        self.db
            .put(&WriteOptions::default(), handle, key, value)
            .map_err(r2e)
    }

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = self.cf(cf)?;
        self.db
            .put(&WriteOptions::default(), handle, key, value)
            .map_err(r2e)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let handle = self.db.default_cf();
        self.db
            .delete(&WriteOptions::default(), handle, key)
            .map_err(r2e)
    }

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        let handle = self.cf(cf)?;
        self.db
            .delete(&WriteOptions::default(), handle, key)
            .map_err(r2e)
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = self.db.default_cf();
        self.db
            .delete_range(&WriteOptions::default(), handle, begin_key, end_key)
            .map_err(r2e)
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = self.cf(cf)?;
        self.db
            .delete_range(&WriteOptions::default(), handle, begin_key, end_key)
            .map_err(r2e)
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{Iterable, Peekable, SyncMutable, CF_DEFAULT};
    use kvproto::metapb::Region;
    use tempfile::Builder;

    use crate::util;

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = util::new_engine(path.path(), &[CF_DEFAULT, cf]).unwrap();

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
        let engine = util::new_engine(path.path(), &[CF_DEFAULT, cf]).unwrap();

        engine.put(b"k1", b"v1").unwrap();
        engine.put_cf(cf, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get_value(b"k1").unwrap().unwrap(), b"v1");
        engine.get_value_cf("foo", b"k1").unwrap_err();
        assert_eq!(&*engine.get_value_cf(cf, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = util::new_engine(path.path(), &[CF_DEFAULT, cf]).unwrap();

        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();
        engine.put_cf(cf, b"a1", b"v1").unwrap();
        engine.put_cf(cf, b"a2", b"v22").unwrap();

        let mut data = vec![];
        engine
            .scan(CF_DEFAULT, b"", &[0xFF, 0xFF], false, |key, value| {
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
            .scan(cf, b"", &[0xFF, 0xFF], false, |key, value| {
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

        let pair = engine.seek(CF_DEFAULT, b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek(CF_DEFAULT, b"a3").unwrap().is_none());
        let pair_cf = engine.seek(cf, b"a1").unwrap().unwrap();
        assert_eq!(pair_cf, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(engine.seek(cf, b"a3").unwrap().is_none());

        let mut index = 0;
        engine
            .scan(CF_DEFAULT, b"", &[0xFF, 0xFF], false, |key, value| {
                data.push((key.to_vec(), value.to_vec()));
                index += 1;
                Ok(index != 1)
            })
            .unwrap();

        assert_eq!(data.len(), 1);

        let snap = engine.snapshot();

        engine.put(b"a3", b"v3").unwrap();
        assert!(engine.seek(CF_DEFAULT, b"a3").unwrap().is_some());

        let pair = snap.seek(CF_DEFAULT, b"a1").unwrap().unwrap();
        assert_eq!(pair, (b"a1".to_vec(), b"v1".to_vec()));
        assert!(snap.seek(CF_DEFAULT, b"a3").unwrap().is_none());

        data.clear();

        snap.scan(CF_DEFAULT, b"", &[0xFF, 0xFF], false, |key, value| {
            data.push((key.to_vec(), value.to_vec()));
            Ok(true)
        })
        .unwrap();

        assert_eq!(data.len(), 2);
    }
}
