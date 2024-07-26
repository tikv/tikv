// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use engine_traits::{
    self, CfNamesExt, IterOptions, Iterable, Peekable, ReadOptions, Result, Snapshot,
    SnapshotMiscExt,
};
use rocksdb::{rocksdb_options::UnsafeSnap, DBIterator, DB};

use crate::{
    db_vector::RocksDbVector, options::RocksReadOptions, r2e, util::get_cf_handle,
    RocksEngineIterator,
};

#[derive(Clone)]
pub struct RocksSnapshot {
    inner: Arc<RocksUnsafeSnapshot>,
}

unsafe impl Send for RocksSnapshot {}
unsafe impl Sync for RocksSnapshot {}

impl RocksSnapshot {
    pub fn new(db: Arc<DB>) -> Self {
        unsafe {
            RocksSnapshot {
                inner: Arc::new(RocksUnsafeSnapshot {
                    snap: db.unsafe_snap(),
                    db,
                }),
            }
        }
    }
}

impl Snapshot for RocksSnapshot {}

impl Debug for RocksSnapshot {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(fmt, "Engine Snapshot Impl")
    }
}

struct RocksUnsafeSnapshot {
    db: Arc<DB>,
    snap: UnsafeSnap,
}

impl Drop for RocksUnsafeSnapshot {
    fn drop(&mut self) {
        unsafe {
            self.db.release_snap(&self.snap);
        }
    }
}

impl Iterable for RocksSnapshot {
    type Iterator = RocksEngineIterator;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.inner.snap);
        }
        let handle = get_cf_handle(self.inner.db.as_ref(), cf)?;
        Ok(RocksEngineIterator::from_raw(DBIterator::new_cf(
            self.inner.db.clone(),
            handle,
            opt,
        )))
    }
}

impl Peekable for RocksSnapshot {
    type DbVector = RocksDbVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<RocksDbVector>> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.inner.snap);
        }
        let v = self.inner.db.get_opt(key, &opt).map_err(r2e)?;
        Ok(v.map(RocksDbVector::from_raw))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<RocksDbVector>> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.inner.snap);
        }
        let handle = get_cf_handle(self.inner.db.as_ref(), cf)?;
        let v = self.inner.db.get_cf_opt(handle, key, &opt).map_err(r2e)?;
        Ok(v.map(RocksDbVector::from_raw))
    }
}

impl CfNamesExt for RocksSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        self.inner.db.cf_names()
    }
}

impl SnapshotMiscExt for RocksSnapshot {
    fn sequence_number(&self) -> u64 {
        unsafe { self.inner.snap.get_sequence_number() }
    }
}
