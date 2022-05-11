// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    CFNamesExt, DeleteStrategy, ImportExt, IterOptions, Iterable, Iterator, MiscExt, Mutable,
    Range, Result, SstWriter, SstWriterBuilder, WriteBatch, WriteBatchExt, ALL_CFS,
};
use rocksdb::Range as RocksRange;
use tikv_util::{box_try, keybuilder::KeyBuilder};

use crate::{
    engine::RocksEngine, rocks_metrics_defs::*, sst::RocksSstWriterBuilder, util, RocksSstWriter,
};

pub const MAX_DELETE_COUNT_BY_KEY: usize = 2048;

impl RocksEngine {
    fn is_titan(&self) -> bool {
        self.as_inner().is_titan()
    }

    // We store all data which would be deleted in memory at first because the data of region will never be larger than
    // max-region-size.
    fn delete_all_in_range_cf_by_ingest(
        &self,
        cf: &str,
        sst_path: String,
        ranges: &[Range<'_>],
    ) -> Result<()> {
        let mut ranges = ranges.to_owned();
        ranges.sort_by(|a, b| a.start_key.cmp(b.start_key));
        let max_end_key = ranges
            .iter()
            .fold(ranges[0].end_key, |x, y| std::cmp::max(x, y.end_key));
        let start = KeyBuilder::from_slice(ranges[0].start_key, 0, 0);
        let end = KeyBuilder::from_slice(max_end_key, 0, 0);
        let mut opts = IterOptions::new(Some(start), Some(end), false);
        if self.is_titan() {
            // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
            // to avoid referring to missing blob files.
            opts.set_key_only(true);
        }

        let mut writer_wrapper: Option<RocksSstWriter> = None;
        let mut data: Vec<Vec<u8>> = vec![];
        let mut last_end_key: Option<Vec<u8>> = None;
        for r in ranges {
            // There may be a range overlap with next range
            if last_end_key
                .as_ref()
                .map_or(false, |key| key.as_slice() > r.start_key)
            {
                self.delete_all_in_range_cf_by_key(cf, &r)?;
                continue;
            }
            last_end_key = Some(r.end_key.to_owned());

            let mut it = self.iterator_cf_opt(cf, opts.clone())?;
            let mut it_valid = it.seek(r.start_key.into())?;
            while it_valid {
                if it.key() >= r.end_key {
                    break;
                }
                if let Some(writer) = writer_wrapper.as_mut() {
                    writer.delete(it.key())?;
                } else {
                    data.push(it.key().to_vec());
                }
                if data.len() > MAX_DELETE_COUNT_BY_KEY {
                    let builder = RocksSstWriterBuilder::new().set_db(self).set_cf(cf);
                    let mut writer = builder.build(sst_path.as_str())?;
                    for key in data.iter() {
                        writer.delete(key)?;
                    }
                    data.clear();
                    writer_wrapper = Some(writer);
                }
                it_valid = it.next()?;
            }
        }

        if let Some(writer) = writer_wrapper {
            writer.finish()?;
            self.ingest_external_file_cf(cf, &[sst_path.as_str()])?;
        } else {
            let mut wb = self.write_batch();
            for key in data.iter() {
                wb.delete_cf(cf, key)?;
                if wb.count() >= Self::WRITE_BATCH_MAX_KEYS {
                    wb.write()?;
                    wb.clear();
                }
            }
            if wb.count() > 0 {
                wb.write()?;
            }
        }
        Ok(())
    }

    fn delete_all_in_range_cf_by_key(&self, cf: &str, range: &Range<'_>) -> Result<()> {
        let start = KeyBuilder::from_slice(range.start_key, 0, 0);
        let end = KeyBuilder::from_slice(range.end_key, 0, 0);
        let mut opts = IterOptions::new(Some(start), Some(end), false);
        if self.is_titan() {
            // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
            // to avoid referring to missing blob files.
            opts.set_key_only(true);
        }
        let mut it = self.iterator_cf_opt(cf, opts)?;
        let mut it_valid = it.seek(range.start_key.into())?;
        let mut wb = self.write_batch();
        while it_valid {
            wb.delete_cf(cf, it.key())?;
            if wb.count() >= Self::WRITE_BATCH_MAX_KEYS {
                wb.write()?;
                wb.clear();
            }
            it_valid = it.next()?;
        }
        if wb.count() > 0 {
            wb.write()?;
        }
        self.sync_wal()?;
        Ok(())
    }
}

impl MiscExt for RocksEngine {
    fn flush(&self, sync: bool) -> Result<()> {
        Ok(self.as_inner().flush(sync)?)
    }

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self.as_inner().flush_cf(handle, sync)?)
    }

    fn delete_ranges_cf(
        &self,
        cf: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        match strategy {
            DeleteStrategy::DeleteFiles => {
                let handle = util::get_cf_handle(self.as_inner(), cf)?;
                for r in ranges {
                    if r.start_key >= r.end_key {
                        continue;
                    }
                    self.as_inner().delete_files_in_range_cf(
                        handle,
                        r.start_key,
                        r.end_key,
                        false,
                    )?;
                }
            }
            DeleteStrategy::DeleteBlobs => {
                let handle = util::get_cf_handle(self.as_inner(), cf)?;
                if self.is_titan() {
                    for r in ranges {
                        if r.start_key >= r.end_key {
                            continue;
                        }
                        self.as_inner().delete_blob_files_in_range_cf(
                            handle,
                            r.start_key,
                            r.end_key,
                            false,
                        )?;
                    }
                }
            }
            DeleteStrategy::DeleteByRange => {
                let mut wb = self.write_batch();
                for r in ranges.iter() {
                    wb.delete_range_cf(cf, r.start_key, r.end_key)?;
                }
                wb.write()?;
            }
            DeleteStrategy::DeleteByKey => {
                for r in ranges {
                    self.delete_all_in_range_cf_by_key(cf, r)?;
                }
            }
            DeleteStrategy::DeleteByWriter { sst_path } => {
                self.delete_all_in_range_cf_by_ingest(cf, sst_path, ranges)?;
            }
        }
        Ok(())
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range<'_>) -> Result<(u64, u64)> {
        let range = util::range_to_rocks_range(range);
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self
            .as_inner()
            .get_approximate_memtable_stats_cf(handle, &range))
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        if let Some(n) = util::get_cf_num_files_at_level(self.as_inner(), handle, 0) {
            let options = self.as_inner().get_options_cf(handle);
            let slowdown_trigger = options.get_level_zero_slowdown_writes_trigger();
            // Leave enough buffer to tolerate heavy write workload,
            // which may flush some memtables in a short time.
            if n > u64::from(slowdown_trigger) / 2 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        let mut used_size: u64 = 0;
        for cf in ALL_CFS {
            let handle = util::get_cf_handle(self.as_inner(), cf)?;
            used_size += util::get_engine_cf_used_size(self.as_inner(), handle);
        }
        Ok(used_size)
    }

    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let db = self.as_inner();
        let mut delete_ranges = Vec::new();
        for &(ref start, ref end) in ranges {
            if start == end {
                continue;
            }
            assert!(start < end);
            delete_ranges.push(RocksRange::new(start, end));
        }
        if delete_ranges.is_empty() {
            return Ok(());
        }

        for cf in db.cf_names() {
            let handle = util::get_cf_handle(db, cf)?;
            db.delete_files_in_ranges_cf(handle, &delete_ranges, /* include_end */ false)?;
        }

        Ok(())
    }

    fn path(&self) -> &str {
        self.as_inner().path()
    }

    fn sync_wal(&self) -> Result<()> {
        Ok(self.as_inner().sync_wal()?)
    }

    fn exists(path: &str) -> bool {
        crate::raw_util::db_exist(path)
    }

    fn dump_stats(&self) -> Result<String> {
        const ROCKSDB_DB_STATS_KEY: &str = "rocksdb.dbstats";
        const ROCKSDB_CF_STATS_KEY: &str = "rocksdb.cfstats";

        let mut s = Vec::with_capacity(1024);
        // common rocksdb stats.
        for name in self.cf_names() {
            let handler = util::get_cf_handle(self.as_inner(), name)?;
            if let Some(v) = self
                .as_inner()
                .get_property_value_cf(handler, ROCKSDB_CF_STATS_KEY)
            {
                s.extend_from_slice(v.as_bytes());
            }
        }

        if let Some(v) = self.as_inner().get_property_value(ROCKSDB_DB_STATS_KEY) {
            s.extend_from_slice(v.as_bytes());
        }

        // more stats if enable_statistics is true.
        if let Some(v) = self.as_inner().get_statistics() {
            s.extend_from_slice(v.as_bytes());
        }

        Ok(box_try!(String::from_utf8(s)))
    }

    fn get_latest_sequence_number(&self) -> u64 {
        self.as_inner().get_latest_sequence_number()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        match self
            .as_inner()
            .get_property_int(ROCKSDB_OLDEST_SNAPSHOT_SEQUENCE)
        {
            // Some(0) indicates that no snapshot is in use
            Some(0) => None,
            s => s,
        }
    }

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self
            .as_inner()
            .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE))
    }

    fn get_range_entries_and_versions(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(u64, u64)>> {
        Ok(crate::properties::get_range_entries_and_versions(
            self, cf, start, end,
        ))
    }

    fn is_stalled_or_stopped(&self) -> bool {
        const ROCKSDB_IS_WRITE_STALLED: &str = "rocksdb.is-write-stalled";
        const ROCKSDB_IS_WRITE_STOPPED: &str = "rocksdb.is-write-stopped";
        self.as_inner()
            .get_property_int(ROCKSDB_IS_WRITE_STALLED)
            .unwrap_or_default()
            != 0
            || self
                .as_inner()
                .get_property_int(ROCKSDB_IS_WRITE_STOPPED)
                .unwrap_or_default()
                != 0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use engine_traits::{
        DeleteStrategy, Iterable, Iterator, Mutable, SeekKey, SyncMutable, WriteBatchExt, ALL_CFS,
    };
    use tempfile::Builder;

    use super::*;
    use crate::{
        engine::RocksEngine,
        raw::{ColumnFamilyOptions, DBOptions, DB},
        raw_util::{new_engine_opt, CFOptions},
    };

    fn check_data(db: &RocksEngine, cfs: &[&str], expected: &[(&[u8], &[u8])]) {
        for cf in cfs {
            let mut iter = db.iterator_cf(cf).unwrap();
            iter.seek(SeekKey::Start).unwrap();
            for &(k, v) in expected {
                assert_eq!(k, iter.key());
                assert_eq!(v, iter.value());
                iter.next().unwrap();
            }
            assert!(!iter.valid().unwrap());
        }
    }

    fn test_delete_all_in_range(
        strategy: DeleteStrategy,
        origin_keys: &[Vec<u8>],
        ranges: &[Range<'_>],
    ) {
        let path = Builder::new()
            .prefix("engine_delete_all_in_range")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, ColumnFamilyOptions::new()))
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();
        let db = Arc::new(db);
        let db = RocksEngine::from_db(db);

        let mut wb = db.write_batch();
        let ts: u8 = 12;
        let keys: Vec<_> = origin_keys
            .iter()
            .map(|k| {
                let mut k2 = k.clone();
                k2.append(&mut vec![ts; 8]);
                k2
            })
            .collect();

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for (_, key) in keys.iter().enumerate() {
            kvs.push((key.as_slice(), b"value"));
        }
        for &(k, v) in kvs.as_slice() {
            for cf in ALL_CFS {
                wb.put_cf(cf, k, v).unwrap();
            }
        }
        wb.write().unwrap();
        check_data(&db, ALL_CFS, kvs.as_slice());

        // Delete all in ranges.
        db.delete_all_in_range(strategy, ranges).unwrap();

        let mut kvs_left: Vec<_> = kvs;
        for r in ranges {
            kvs_left = kvs_left
                .into_iter()
                .filter(|k| k.0 < r.start_key || k.0 >= r.end_key)
                .collect();
        }
        check_data(&db, ALL_CFS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_all_in_range_use_delete_range() {
        let data = vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ];
        // Single range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k4")],
        );
        // Two ranges without overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k0", b"k1"), Range::new(b"k3", b"k4")],
        );
        // Two ranges with overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k3"), Range::new(b"k2", b"k4")],
        );
        // One range contains the other range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k4"), Range::new(b"k2", b"k3")],
        );
    }

    #[test]
    fn test_delete_all_in_range_by_key() {
        let data = vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ];
        // Single range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k4")],
        );
        // Two ranges without overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k0", b"k1"), Range::new(b"k3", b"k4")],
        );
        // Two ranges with overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k3"), Range::new(b"k2", b"k4")],
        );
        // One range contains the other range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k4"), Range::new(b"k2", b"k3")],
        );
    }

    #[test]
    fn test_delete_all_in_range_by_writer() {
        let path = Builder::new()
            .prefix("test_delete_all_in_range_by_writer")
            .tempdir()
            .unwrap();
        let path_str = path.path();
        let sst_path = path_str.join("tmp_file").to_str().unwrap().to_owned();
        let mut data = vec![];
        for i in 1000..5000 {
            data.push(i.to_string().as_bytes().to_vec());
        }
        test_delete_all_in_range(
            DeleteStrategy::DeleteByWriter { sst_path },
            &data,
            &[
                Range::new(&data[2], &data[499]),
                Range::new(&data[502], &data[999]),
                Range::new(&data[1002], &data[1999]),
                Range::new(&data[1499], &data[2499]),
                Range::new(&data[2502], &data[3999]),
                Range::new(&data[3002], &data[3499]),
            ],
        );
    }

    #[test]
    fn test_delete_all_files_in_range() {
        let path = Builder::new()
            .prefix("engine_delete_all_files_in_range")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let mut cf_opts = ColumnFamilyOptions::new();
                cf_opts.set_level_zero_file_num_compaction_trigger(1);
                CFOptions::new(cf, cf_opts)
            })
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();
        let db = Arc::new(db);
        let db = RocksEngine::from_db(db);

        let keys = vec![b"k1", b"k2", b"k3", b"k4"];

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for key in keys {
            kvs.push((key, b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for cf in ALL_CFS {
            for &(k, v) in kvs.as_slice() {
                db.put_cf(cf, k, v).unwrap();
                db.flush_cf(cf, true).unwrap();
            }
        }
        check_data(&db, ALL_CFS, kvs.as_slice());

        db.delete_all_in_range(DeleteStrategy::DeleteFiles, &[Range::new(b"k2", b"k4")])
            .unwrap();
        db.delete_all_in_range(DeleteStrategy::DeleteBlobs, &[Range::new(b"k2", b"k4")])
            .unwrap();
        check_data(&db, ALL_CFS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_range_prefix_bloom_case() {
        let path = Builder::new()
            .prefix("engine_delete_range_prefix_bloom")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let mut opts = DBOptions::new();
        opts.create_if_missing(true);

        let mut cf_opts = ColumnFamilyOptions::new();
        // Prefix extractor(trim the timestamp at tail) for write cf.
        cf_opts
            .set_prefix_extractor(
                "FixedSuffixSliceTransform",
                crate::util::FixedSuffixSliceTransform::new(8),
            )
            .unwrap_or_else(|err| panic!("{:?}", err));
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1_f64);
        let cf = "default";
        let db = DB::open_cf(opts, path_str, vec![(cf, cf_opts)]).unwrap();
        let db = Arc::new(db);
        let db = RocksEngine::from_db(db);
        let mut wb = db.write_batch();
        let kvs: Vec<(&[u8], &[u8])> = vec![
            (b"kabcdefg1", b"v1"),
            (b"kabcdefg2", b"v2"),
            (b"kabcdefg3", b"v3"),
            (b"kabcdefg4", b"v4"),
        ];
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(b"kabcdefg1", b"v1"), (b"kabcdefg4", b"v4")];

        for &(k, v) in kvs.as_slice() {
            wb.put_cf(cf, k, v).unwrap();
        }
        wb.write().unwrap();
        check_data(&db, &[cf], kvs.as_slice());

        // Delete all in ["k2", "k4").
        db.delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &[Range::new(b"kabcdefg2", b"kabcdefg4")],
        )
        .unwrap();
        check_data(&db, &[cf], kvs_left.as_slice());
    }
}
