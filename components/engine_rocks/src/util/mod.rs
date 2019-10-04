// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Error, IterOptions, Result};
use engine_traits::{CF_LOCK, MAX_DELETE_BATCH_SIZE};
use rocksdb::{CFHandle, DBIterator, Writable, WriteBatch as RawWriteBatch, DB};
use tikv_util::keybuilder::KeyBuilder;

use crate::options::RocksReadOptions;

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle> {
    let handle = db
        .cf_handle(cf)
        .ok_or_else(|| Error::Engine(format!("cf {} not found", cf)))?;
    Ok(handle)
}

pub fn delete_all_in_range_cf(
    db: &DB,
    cf: &str,
    start_key: &[u8],
    end_key: &[u8],
    use_delete_range: bool,
) -> Result<()> {
    let handle = get_cf_handle(db, cf)?;
    let wb = RawWriteBatch::default();
    if use_delete_range && cf != CF_LOCK {
        wb.delete_range_cf(handle, start_key, end_key)?;
    } else {
        let start = KeyBuilder::from_slice(start_key, 0, 0);
        let end = KeyBuilder::from_slice(end_key, 0, 0);
        let mut iter_opt = IterOptions::new(Some(start), Some(end), false);
        if db.is_titan() {
            // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
            // to avoid referring to missing blob files.
            iter_opt.set_key_only(true);
        }
        let handle = get_cf_handle(db, cf)?;
        let opts: RocksReadOptions = iter_opt.into();
        let mut it = DBIterator::new_cf(db, handle, opts.into_raw());
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
