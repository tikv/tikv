// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::u64;

use crate::rocks;
use crate::rocks::{Range, TablePropertiesCollection, Writable, WriteBatch, DB};
use crate::CF_LOCK;

use super::{Error, Result};
use super::{IterOption, Iterable};
use tikv_util::keybuilder::KeyBuilder;

/// Check if key in range [`start_key`, `end_key`).
pub fn check_key_in_range(
    key: &[u8],
    region_id: u64,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<()> {
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::NotInRange(
            key.to_vec(),
            region_id,
            start_key.to_vec(),
            end_key.to_vec(),
        ))
    }
}

// In our tests, we found that if the batch size is too large, running delete_all_in_range will
// reduce OLTP QPS by 30% ~ 60%. We found that 32K is a proper choice.
pub const MAX_DELETE_BATCH_SIZE: usize = 32 * 1024;

pub fn delete_all_in_range(
    db: &DB,
    start_key: &[u8],
    end_key: &[u8],
    use_delete_range: bool,
) -> Result<()> {
    if start_key >= end_key {
        return Ok(());
    }

    for cf in db.cf_names() {
        delete_all_in_range_cf(db, cf, start_key, end_key, use_delete_range)?;
    }

    Ok(())
}

pub fn delete_all_in_range_cf(
    db: &DB,
    cf: &str,
    start_key: &[u8],
    end_key: &[u8],
    use_delete_range: bool,
) -> Result<()> {
    let handle = rocks::util::get_cf_handle(db, cf)?;
    let wb = WriteBatch::default();
    if use_delete_range && cf != CF_LOCK {
        wb.delete_range_cf(handle, start_key, end_key)?;
    } else {
        let start = KeyBuilder::from_slice(start_key, 0, 0);
        let end = KeyBuilder::from_slice(end_key, 0, 0);
        let mut iter_opt = IterOption::new(Some(start), Some(end), false);
        if db.is_titan() {
            // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
            // to avoid referring to missing blob files.
            iter_opt.titan_key_only(true);
        }
        let mut it = db.new_iterator_cf(cf, iter_opt)?;
        it.seek(start_key.into());
        while it.valid() {
            wb.delete_cf(handle, it.key())?;
            if wb.data_size() >= MAX_DELETE_BATCH_SIZE {
                // Can't use write_without_wal here.
                // Otherwise it may cause dirty data when applying snapshot.
                db.write(&wb)?;
                wb.clear();
            }

            if !it.next() {
                break;
            }
        }
        it.status()?;
    }

    if wb.count() > 0 {
        db.write(&wb)?;
    }

    Ok(())
}

pub fn delete_all_files_in_range(db: &DB, start_key: &[u8], end_key: &[u8]) -> Result<()> {
    if start_key >= end_key {
        return Ok(());
    }

    for cf in db.cf_names() {
        let handle = rocks::util::get_cf_handle(db, cf)?;
        db.delete_files_in_range_cf(handle, start_key, end_key, false)?;
    }

    Ok(())
}

pub fn get_range_properties_cf(
    db: &DB,
    cfname: &str,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<TablePropertiesCollection> {
    let cf = rocks::util::get_cf_handle(db, cfname)?;
    let range = Range::new(start_key, end_key);
    db.get_properties_of_tables_in_range(cf, &[range])
        .map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use crate::rocks;
    use crate::rocks::util::{get_cf_handle, new_engine_opt, CFOptions};
    use crate::rocks::{ColumnFamilyOptions, DBOptions, SeekKey, Writable};
    use crate::ALL_CFS;
    use crate::DB;

    use super::*;

    fn check_data(db: &DB, cfs: &[&str], expected: &[(&[u8], &[u8])]) {
        for cf in cfs {
            let handle = get_cf_handle(db, cf).unwrap();
            let mut iter = db.iter_cf(handle);
            iter.seek(SeekKey::Start);
            for &(k, v) in expected {
                assert_eq!(k, iter.key());
                assert_eq!(v, iter.value());
                iter.next();
            }
            assert!(!iter.valid());
        }
    }

    fn test_delete_all_in_range(use_delete_range: bool) {
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

        let wb = WriteBatch::default();
        let ts: u8 = 12;
        let keys: Vec<_> = vec![
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ]
        .into_iter()
        .map(|mut k| {
            k.append(&mut vec![ts; 8]);
            k
        })
        .collect();

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for (_, key) in keys.iter().enumerate() {
            kvs.push((key.as_slice(), b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for &(k, v) in kvs.as_slice() {
            for cf in ALL_CFS {
                let handle = get_cf_handle(&db, cf).unwrap();
                wb.put_cf(handle, k, v).unwrap();
            }
        }
        db.write(&wb).unwrap();
        check_data(&db, ALL_CFS, kvs.as_slice());

        // Delete all in ["k2", "k4").
        let start = b"k2";
        let end = b"k4";
        delete_all_in_range(&db, start, end, use_delete_range).unwrap();
        check_data(&db, ALL_CFS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_all_in_range_use_delete_range() {
        test_delete_all_in_range(true);
    }

    #[test]
    fn test_delete_all_in_range_not_use_delete_range() {
        test_delete_all_in_range(false);
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

        let keys = vec![b"k1", b"k2", b"k3", b"k4"];

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for key in keys {
            kvs.push((key, b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for cf in ALL_CFS {
            let handle = get_cf_handle(&db, cf).unwrap();
            for &(k, v) in kvs.as_slice() {
                db.put_cf(handle, k, v).unwrap();
                db.flush_cf(handle, true).unwrap();
            }
        }
        check_data(&db, ALL_CFS, kvs.as_slice());

        delete_all_files_in_range(&db, b"k2", b"k4").unwrap();
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
                Box::new(rocks::util::FixedSuffixSliceTransform::new(8)),
            )
            .unwrap_or_else(|err| panic!("{:?}", err));
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1 as f64);
        let cf = "default";
        let db = DB::open_cf(opts, path_str, vec![(cf, cf_opts)]).unwrap();
        let wb = WriteBatch::default();
        let kvs: Vec<(&[u8], &[u8])> = vec![
            (b"kabcdefg1", b"v1"),
            (b"kabcdefg2", b"v2"),
            (b"kabcdefg3", b"v3"),
            (b"kabcdefg4", b"v4"),
        ];
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(b"kabcdefg1", b"v1"), (b"kabcdefg4", b"v4")];

        for &(k, v) in kvs.as_slice() {
            let handle = get_cf_handle(&db, cf).unwrap();
            wb.put_cf(handle, k, v).unwrap();
        }
        db.write(&wb).unwrap();
        check_data(&db, &[cf], kvs.as_slice());

        // Delete all in ["k2", "k4").
        delete_all_in_range(&db, b"kabcdefg2", b"kabcdefg4", true).unwrap();
        check_data(&db, &[cf], kvs_left.as_slice());
    }
}
