// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::{self, File};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use engine_traits::{
    Error, IterOptions, Iterable, KvEngine, Mutable, Peekable, ReadOptions, Result, WriteOptions,
    IngestExternalFileOptions,
};
use rocksdb::{
    set_external_sst_file_global_seq_no, DBIterator, Writable, DB,
};
use tikv_util::file::calc_crc32;

use crate::options::{RocksReadOptions, RocksWriteOptions};
use crate::util::{delete_all_in_range_cf, get_cf_handle};
use crate::{Iterator, Snapshot};

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Rocks(Arc<DB>);

impl Rocks {
    pub fn from_db(db: Arc<DB>) -> Self {
        Rocks(db)
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

// TODO: Remove these cast methods after the engine traits are completed.
impl AsRef<DB> for Rocks {
    fn as_ref(&self) -> &DB {
        self.0.deref()
    }
}

impl AsRef<Arc<DB>> for Rocks {
    fn as_ref(&self) -> &Arc<DB> {
        &self.0
    }
}

impl AsMut<DB> for Rocks {
    fn as_mut(&mut self) -> &mut DB {
        Arc::get_mut(&mut self.0).unwrap()
    }
}

impl AsRef<Rocks> for Arc<DB> {
    fn as_ref(&self) -> &Rocks {
        unsafe { &*(self as *const Arc<DB> as *const Rocks) }
    }
}

impl AsMut<Rocks> for Arc<DB> {
    fn as_mut(&mut self) -> &mut Rocks {
        unsafe { &mut *(self as *mut Arc<DB> as *mut Rocks) }
    }
}

impl Into<Arc<DB>> for Rocks {
    fn into(self) -> Arc<DB> {
        self.0
    }
}

impl KvEngine for Rocks {
    type Snap = Snapshot;
    type Batch = crate::WriteBatch;

    fn write_opt(&self, opts: &WriteOptions, wb: &Self::Batch) -> Result<()> {
        if wb.get_db().path() != self.0.path() {
            return Err(Error::Engine("mismatched db path".to_owned()));
        }
        let opt: RocksWriteOptions = opts.into();
        self.0
            .write_opt(wb.as_ref(), &opt.into_raw())
            .map_err(Error::Engine)
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::Batch {
        Self::Batch::with_capacity(Arc::clone(&self.0), cap)
    }

    fn write_batch(&self) -> Self::Batch {
        Self::Batch::new(Arc::clone(&self.0))
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.0.clone())
    }

    fn sync(&self) -> Result<()> {
        self.0.sync_wal().map_err(Error::Engine)
    }

    fn cf_names(&self) -> Vec<&str> {
        self.0.cf_names()
    }

    fn delete_all_in_range_cf(&self, cf: &str, start_key: &[u8], end_key: &[u8], use_delete_range: bool) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }
        let handle = get_cf_handle(&self.0, cf)?;
        self.0
            .delete_files_in_range_cf(handle, start_key, end_key, false)?;
        delete_all_in_range_cf(
            &self.0, cf, start_key, end_key, use_delete_range,
        )
    }

    fn delete_files_in_range_cf(&self, cf: &str, start_key: &[u8], end_key: &[u8], include_end: bool) -> Result<()> {
        let handle = get_cf_handle(&self.0, cf)?;
        self.0.delete_files_in_range_cf(handle, start_key, end_key, include_end)
            .map_err(From::from)
    }

    fn ingest_external_file_cf(&self, cf: &str, opts: &IngestExternalFileOptions, files: &[&str]) -> Result<()> {
        let mut rocks_opts = rocksdb::IngestExternalFileOptions::new();
        rocks_opts.move_files(opts.move_files);
        let handle = get_cf_handle(&self.0, cf)?;
        self.0
            .ingest_external_file_cf(&handle, &rocks_opts, files)?;
        Ok(())
    }

    fn validate_file_for_ingestion<P: AsRef<Path>>(
        &self,
        cf: &str,
        path: P,
        expected_size: u64,
        expected_checksum: u32,
    ) -> Result<()> {
        let path = path.as_ref().to_str().unwrap();
        let f = File::open(path)?;

        let meta = f.metadata()?;
        if meta.len() != expected_size {
            return Err(Error::Engine(format!(
                "invalid size {} for {}, expected {}",
                meta.len(),
                path,
                expected_size
            )));
        }

        let checksum = calc_crc32(path)?;
        if checksum == expected_checksum {
            return Ok(());
        }

        // RocksDB may have modified the global seqno.
        let cf_handle = get_cf_handle(&self.0, cf)?;
        set_external_sst_file_global_seq_no(&self.0, cf_handle, path, 0)?;
        f.sync_all()
            .map_err(|e| format!("sync {}: {:?}", path, e))?;

        let checksum = calc_crc32(path)?;
        if checksum != expected_checksum {
            return Err(Error::Engine(format!(
                "invalid checksum {} for {}, expected {}",
                checksum, path, expected_checksum
            )));
        }

        Ok(())
    }
}

impl Iterable for Rocks {
    type Iter = Iterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iter> {
        let opt: RocksReadOptions = opts.into();
        Ok(Iterator::from_raw(DBIterator::new(
            self.0.clone(),
            opt.into_raw(),
        )))
    }

    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iter> {
        let handle = get_cf_handle(&self.0, cf)?;
        let opt: RocksReadOptions = opts.into();
        Ok(Iterator::from_raw(DBIterator::new_cf(
            self.0.clone(),
            handle,
            opt.into_raw(),
        )))
    }
}

impl Peekable for Rocks {
    fn get_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let opt: RocksReadOptions = opts.into();
        let v = self.0.get_opt(key, &opt.into_raw())?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf_opt(&self, opts: &ReadOptions, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let opt: RocksReadOptions = opts.into();
        let handle = get_cf_handle(&self.0, cf)?;
        let v = self.0.get_cf_opt(handle, key, &opt.into_raw())?;
        Ok(v.map(|v| v.to_vec()))
    }
}

impl Mutable for Rocks {
    fn put_opt(&self, _: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()> {
        self.0.put(key, value).map_err(Error::Engine)
    }

    fn put_cf_opt(&self, _: &WriteOptions, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.0, cf)?;
        self.0.put_cf(handle, key, value).map_err(Error::Engine)
    }

    fn delete_opt(&self, _: &WriteOptions, key: &[u8]) -> Result<()> {
        self.0.delete(key).map_err(Error::Engine)
    }

    fn delete_cf_opt(&self, _: &WriteOptions, cf: &str, key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(&self.0, cf)?;
        self.0.delete_cf(handle, key).map_err(Error::Engine)
    }
}
