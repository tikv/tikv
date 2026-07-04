// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use engine_traits::{
    self, CfNamesExt, DataBlockKeyAnchor, IterOptions, Iterable, Peekable, ReadOptions, Result,
    Snapshot, SnapshotMiscExt,
};
use rocksdb::{DB, DBIterator, rocksdb_options::UnsafeSnap};

use crate::{
    RocksEngineIterator, db_vector::RocksDbVector, options::RocksReadOptions, r2e,
    util::get_cf_handle,
};

pub struct RocksSnapshot {
    db: Arc<DB>,
    snap: UnsafeSnap,
}

unsafe impl Send for RocksSnapshot {}
unsafe impl Sync for RocksSnapshot {}

impl RocksSnapshot {
    pub fn new(db: Arc<DB>) -> Self {
        unsafe {
            RocksSnapshot {
                snap: db.unsafe_snap(),
                db,
            }
        }
    }
}

impl Snapshot for RocksSnapshot {
    fn approximate_key_anchors_cf(
        &self,
        cf: &str,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> Result<Vec<DataBlockKeyAnchor>> {
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        let anchors = self
            .db
            .approximate_key_anchors_cf(handle, lower_bound, upper_bound)
            .map_err(r2e)?;
        Ok(anchors
            .into_iter()
            .map(|anchor| DataBlockKeyAnchor {
                user_key: anchor.user_key,
                range_size: anchor.range_size as u64,
            })
            .collect())
    }
}

impl Debug for RocksSnapshot {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(fmt, "Engine Snapshot Impl")
    }
}

impl Drop for RocksSnapshot {
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
            opt.set_snapshot(&self.snap);
        }
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        Ok(RocksEngineIterator::from_raw(DBIterator::new_cf(
            self.db.clone(),
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
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_opt(key, &opt).map_err(r2e)?;
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
            opt.set_snapshot(&self.snap);
        }
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        let v = self.db.get_cf_opt(handle, key, &opt).map_err(r2e)?;
        Ok(v.map(RocksDbVector::from_raw))
    }
}

impl CfNamesExt for RocksSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
    }
}

impl SnapshotMiscExt for RocksSnapshot {
    fn sequence_number(&self) -> u64 {
        unsafe { self.snap.get_sequence_number() }
    }
}
