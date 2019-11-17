// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

mod db;

pub mod util;

mod snapshot;
pub use self::snapshot::*;

pub use rocksdb::rocksdb_options::UnsafeSnap;
pub use rocksdb::{
    load_latest_options, rocksdb::supported_compression, run_ldb_tool,
    set_external_sst_file_global_seq_no, BlockBasedOptions, CColumnFamilyDescriptor, CFHandle,
    Cache, ColumnFamilyOptions, CompactOptions, CompactionJobInfo, CompactionOptions,
    CompactionPriority, DBBackgroundErrorReason, DBBottommostLevelCompaction, DBCompactionStyle,
    DBCompressionType, DBEntryType, DBIterator, DBOptions, DBRateLimiterMode, DBRecoveryMode,
    DBStatisticsHistogramType, DBStatisticsTickerType, DBTitanDBBlobRunMode, DBVector, Env,
    EnvOptions, EventListener, ExternalSstFileInfo, FlushJobInfo, HistogramData,
    IngestExternalFileOptions, IngestionInfo, Kv, LRUCacheOptions, MapProperty, MemoryAllocator,
    PerfContext, Range, RateLimiter, ReadOptions, SeekKey, SequentialFile, SliceTransform,
    TablePropertiesCollection, TablePropertiesCollector, TablePropertiesCollectorFactory,
    TitanBlobIndex, TitanDBOptions, UserCollectedProperties, Writable, WriteBatch, WriteOptions,
    WriteStallCondition, WriteStallInfo, DB,
};

#[cfg(test)]
mod tests {
    use super::Snapshot;
    use crate::rocks::{util, Writable};
    use crate::{Iterable, Mutable, Peekable};
    use kvproto::metapb::Region;
    use std::sync::Arc;
    use tempfile::Builder;

    #[test]
    fn test_base() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine =
            Arc::new(util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap());

        let mut r = Region::default();
        r.set_id(10);

        let key = b"key";
        let handle = util::get_cf_handle(&engine, cf).unwrap();
        engine.put_msg(key, &r).unwrap();
        engine.put_msg_cf(handle, key, &r).unwrap();

        let r1: Region = engine.get_msg(key).unwrap().unwrap();
        assert_eq!(r, r1);
        let r1_cf: Region = engine.get_msg_cf(cf, key).unwrap().unwrap();
        assert_eq!(r, r1_cf);

        let b: Option<Region> = engine.get_msg(b"missing_key").unwrap();
        assert!(b.is_none());
    }

    #[test]
    fn test_peekable() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine = util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap();

        engine.put(b"k1", b"v1").unwrap();
        let handle = engine.cf_handle("cf").unwrap();
        engine.put_cf(handle, b"k1", b"v2").unwrap();

        assert_eq!(&*engine.get_value(b"k1").unwrap().unwrap(), b"v1");
        assert!(engine.get_value_cf("foo", b"k1").is_err());
        assert_eq!(&*engine.get_value_cf(cf, b"k1").unwrap().unwrap(), b"v2");
    }

    #[test]
    fn test_scan() {
        let path = Builder::new().prefix("var").tempdir().unwrap();
        let cf = "cf";
        let engine =
            Arc::new(util::new_engine(path.path().to_str().unwrap(), None, &[cf], None).unwrap());
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
