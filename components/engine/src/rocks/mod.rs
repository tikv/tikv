// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

mod sst;
pub use sst::{SstWriter, SstWriterBuilder};

pub mod util;

mod db;
pub use self::db::*;
mod snapshot;
pub use self::snapshot::{RocksSnapshot as Snapshot, SyncRocksSnapshot as SyncSnapshot};
mod writebatch;
pub use self::writebatch::RocksWriteBatch as WriteBatch;
mod iterator;
pub use self::iterator::RocksIterator as Iterator;
mod options;
pub use self::options::*;

pub use engine_rocksdb::rocksdb_options::UnsafeSnap;
pub use engine_rocksdb::{
    load_latest_options, rocksdb::supported_compression, run_ldb_tool,
    set_external_sst_file_global_seq_no, BlockBasedOptions, CColumnFamilyDescriptor, CFHandle,
    Cache, CompactOptions, CompactionJobInfo, CompactionOptions, CompactionPriority,
    DBBottommostLevelCompaction, DBCompactionStyle, DBCompressionType, DBEntryType, DBIterator,
    DBOptions, DBRateLimiterMode, DBRecoveryMode, DBStatisticsHistogramType,
    DBStatisticsTickerType, DBTitanDBBlobRunMode, DBVector, Env, EnvOptions, EventListener,
    ExternalSstFileInfo, FlushJobInfo, HistogramData, IngestionInfo, Kv, LRUCacheOptions,
    MemoryAllocator, PerfContext, Range, RateLimiter, SequentialFile, SliceTransform,
    TablePropertiesCollection, TablePropertiesCollector, TablePropertiesCollectorFactory,
    TitanBlobIndex, TitanDBOptions, UserCollectedProperties, Writable, WriteBatch as RawWriteBatch,
    WriteStallCondition, WriteStallInfo, DB,
};
pub use engine_rocksdb::{
    ColumnFamilyOptions as RawCFOptions, IngestExternalFileOptions as RawIngestExternalFileOptions,
    ReadOptions as RawReadOptions, SeekKey as RawSeekKey, WriteOptions as RawWriteOptions,
};

#[cfg(test)]
mod tests {
    use super::{KvEngine, Snapshot};
    use crate::rocks::{util, Rocks, Writable};
    use crate::{Iterable, Mutable, Peekable};
    use kvproto::metapb::Region;
    use std::sync::Arc;
    use tempfile::Builder;

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = Rocks(Arc::new(
            util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        ));

        let mut r = Region::default();
        r.set_id(10);

        let key = b"key";
        let handle = util::get_cf_handle(&engine, cf).unwrap();
        engine.put_msg(key, &r).unwrap();
        engine.put_msg_cf(handle, key, &r).unwrap();

        let snap = engine.snapshot();

        let mut r1: Region = util::get_msg(&engine, key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_cf: Region = util::get_msg_cf(&engine, cf, key).unwrap().unwrap();
        assert_eq!(r, r1_cf);

        let mut r2: Region = util::get_msg(&snap, key).unwrap().unwrap();
        assert_eq!(r, r2);
        let r2_cf: Region = util::get_msg_cf(&snap, cf, key).unwrap().unwrap();
        assert_eq!(r, r2_cf);

        r.set_id(11);
        engine.put_msg(key, &r).unwrap();
        r1 = util::get_msg(&engine, key).unwrap().unwrap();
        r2 = util::get_msg(&snap, key).unwrap().unwrap();
        assert_ne!(r1, r2);

        let b: Option<Region> = util::get_msg(&engine, b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = Rocks(Arc::new(
            util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        ));

        engine.put(b"k1", b"v1").unwrap();
        engine.put_cf(cf, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get(b"k1").unwrap().unwrap(), b"v1");
        assert!(engine.get_cf("foo", b"k1").is_err());
        assert_eq!(&*engine.get_cf(cf, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = Rocks(Arc::new(
            util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap(),
        ));
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
