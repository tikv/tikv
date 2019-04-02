// Copyright 2016 PingCAP, Inc.
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

use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;
use std::option::Option;
use std::sync::Arc;

use super::{Error, Iterable, Mutable, Peekable, Result};

pub mod util;

pub use engine_rocksdb::rocksdb_options::UnsafeSnap;
pub use engine_rocksdb::{
    load_latest_options, rocksdb::supported_compression, run_ldb_tool,
    set_external_sst_file_global_seq_no, BlockBasedOptions, CColumnFamilyDescriptor, CFHandle,
    ColumnFamilyOptions, CompactOptions, CompactionJobInfo, CompactionOptions, CompactionPriority,
    DBBottommostLevelCompaction, DBCompactionStyle, DBCompressionType, DBEntryType, DBIterator,
    DBOptions, DBRateLimiterMode, DBRecoveryMode, DBStatisticsHistogramType,
    DBStatisticsTickerType, DBVector, Env, EnvOptions, EventListener, ExternalSstFileInfo,
    FlushJobInfo, HistogramData, IngestExternalFileOptions, IngestionInfo, Kv, PerfContext, Range,
    RateLimiter, ReadOptions, SeekKey, SequentialFile, SliceTransform, SstFileWriter,
    TablePropertiesCollection, TablePropertiesCollector, TablePropertiesCollectorFactory,
    TitanBlobIndex, TitanDBOptions, UserCollectedProperties, Writable, WriteBatch, WriteOptions,
    WriteStallCondition, WriteStallInfo, DB,
};

use super::IterOption;

pub struct Snapshot {
    db: Arc<DB>,
    snap: UnsafeSnap,
}

/// Because snap will be valid whenever db is valid, so it's safe to send
/// it around.
unsafe impl Send for Snapshot {}
unsafe impl Sync for Snapshot {}

impl Snapshot {
    pub fn new(db: Arc<DB>) -> Snapshot {
        unsafe {
            Snapshot {
                snap: db.unsafe_snap(),
                db,
            }
        }
    }

    pub fn into_sync(self) -> SyncSnapshot {
        SyncSnapshot(Arc::new(self))
    }

    pub fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
    }

    pub fn cf_handle(&self, cf: &str) -> Result<&CFHandle> {
        self::util::get_cf_handle(&self.db, cf).map_err(Error::from)
    }

    pub fn get_db(&self) -> Arc<DB> {
        Arc::clone(&self.db)
    }

    pub fn db_iterator(&self, iter_opt: IterOption) -> DBIterator<Arc<DB>> {
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        DBIterator::new(Arc::clone(&self.db), opt)
    }

    pub fn db_iterator_cf(&self, cf: &str, iter_opt: IterOption) -> Result<DBIterator<Arc<DB>>> {
        let handle = self::util::get_cf_handle(&self.db, cf)?;
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(DBIterator::new_cf(Arc::clone(&self.db), handle, opt))
    }
}

impl Debug for Snapshot {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(fmt, "Engine Snapshot Impl")
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        unsafe {
            self.db.release_snap(&self.snap);
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyncSnapshot(Arc<Snapshot>);

impl Deref for SyncSnapshot {
    type Target = Snapshot;

    fn deref(&self) -> &Snapshot {
        &self.0
    }
}

impl SyncSnapshot {
    pub fn new(db: Arc<DB>) -> SyncSnapshot {
        SyncSnapshot(Arc::new(Snapshot::new(db)))
    }

    pub fn clone(&self) -> SyncSnapshot {
        SyncSnapshot(Arc::clone(&self.0))
    }
}

impl Peekable for DB {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBVector>> {
        let v = self.get(key)?;
        Ok(v)
    }

    fn get_value_cf(&self, cf: &str, key: &[u8]) -> Result<Option<DBVector>> {
        let handle = self::util::get_cf_handle(self, cf)?;
        let v = self.get_cf(handle, key)?;
        Ok(v)
    }
}

impl Iterable for DB {
    fn new_iterator(&self, iter_opt: IterOption) -> DBIterator<&DB> {
        self.iter_opt(iter_opt.build_read_opts())
    }

    fn new_iterator_cf(&self, cf: &str, iter_opt: IterOption) -> Result<DBIterator<&DB>> {
        let handle = self::util::get_cf_handle(self, cf)?;
        let readopts = iter_opt.build_read_opts();
        Ok(DBIterator::new_cf(self, handle, readopts))
    }
}

impl Peekable for Snapshot {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBVector>> {
        let mut opt = ReadOptions::new();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_opt(key, &opt)?;
        Ok(v)
    }

    fn get_value_cf(&self, cf: &str, key: &[u8]) -> Result<Option<DBVector>> {
        let handle = self::util::get_cf_handle(&self.db, cf)?;
        let mut opt = ReadOptions::new();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_cf_opt(handle, key, &opt)?;
        Ok(v)
    }
}

impl Iterable for Snapshot {
    fn new_iterator(&self, iter_opt: IterOption) -> DBIterator<&DB> {
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        DBIterator::new(&self.db, opt)
    }

    fn new_iterator_cf(&self, cf: &str, iter_opt: IterOption) -> Result<DBIterator<&DB>> {
        let handle = self::util::get_cf_handle(&self.db, cf)?;
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(DBIterator::new_cf(&self.db, handle, opt))
    }
}

impl Mutable for DB {}
impl Mutable for WriteBatch {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rocks::Writable;
    use kvproto::metapb::Region;
    use std::sync::Arc;
    use tempdir::TempDir;

    #[test]
    fn test_base() {
        let path = TempDir::new("var").unwrap();
        let cf = "cf";
        let engine = Arc::new(
            self::util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        );

        let mut r = Region::new();
        r.set_id(10);

        let key = b"key";
        let handle = self::util::get_cf_handle(&engine, cf).unwrap();
        engine.put_msg(key, &r).unwrap();
        engine.put_msg_cf(handle, key, &r).unwrap();

        let snap = Snapshot::new(Arc::clone(&engine));

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
        let path = TempDir::new("var").unwrap();
        let cf = "cf";
        let engine =
            self::util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap();

        engine.put(b"k1", b"v1").unwrap();
        let handle = engine.cf_handle("cf").unwrap();
        engine.put_cf(handle, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get_value(b"k1").unwrap().unwrap(), b"v1");
        assert!(engine.get_value_cf("foo", b"k1").is_err());
        assert_eq!(&*engine.get_value_cf(cf, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = TempDir::new("var").unwrap();
        let cf = "cf";
        let engine = Arc::new(
            self::util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        );
        let handle = engine.cf_handle(cf).unwrap();

        engine.put(b"a1", b"v1").unwrap();
        engine.put(b"a2", b"v2").unwrap();
        engine.put_cf(handle, b"a1", b"v1").unwrap();
        engine.put_cf(handle, b"a2", b"v22").unwrap();

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

        let snap = Snapshot::new(Arc::clone(&engine));

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
