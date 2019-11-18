// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use engine::rocks::Snapshot as RawSnapshot;
use engine::rocks::SyncSnapshot as RawSyncSnapshot;
use engine_traits::{self, IterOptions, Iterable, Peekable, ReadOptions, Result, Snapshot, SyncSnapshot};
use rocksdb::rocksdb_options::UnsafeSnap;
use rocksdb::{DBIterator, DB};

use crate::db_vector::RocksDBVector;
use crate::options::RocksReadOptions;
use crate::util::get_cf_handle;
use crate::RocksEngineIterator;

#[repr(C)] // Guarantee same representation as in engine/rocks
pub struct RocksSnapshot {
    // TODO: use &DB.
    db: Arc<DB>,
    snap: UnsafeSnap,
}

static_assertions::assert_eq_size!(RocksSnapshot, RawSnapshot);
static_assertions::assert_eq_align!(RocksSnapshot, RawSnapshot);

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

    pub fn from_ref(raw: &RawSnapshot) -> &RocksSnapshot {
        unsafe { &*(raw as *const _ as *const _) }
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }
}

impl Snapshot for RocksSnapshot {
    type SyncSnapshot = RocksSyncSnapshot;

    fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
    }

    fn into_sync(self) -> RocksSyncSnapshot {
        RocksSyncSnapshot(Arc::new(self))
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

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(RocksEngineIterator::from_raw(DBIterator::new(
            self.db.clone(),
            opt,
        )))
    }

    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
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
    type DBVector = RocksDBVector;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<RocksDBVector>> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_opt(key, &opt)?;
        Ok(v.map(RocksDBVector::from_raw))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<RocksDBVector>> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        let v = self.db.get_cf_opt(handle, key, &opt)?;
        Ok(v.map(RocksDBVector::from_raw))
    }
}

#[derive(Clone, Debug)]
#[repr(transparent)] // Guarantee same representation as in engine/rocks
pub struct RocksSyncSnapshot(Arc<RocksSnapshot>);

impl Deref for RocksSyncSnapshot {
    type Target = RocksSnapshot;

    fn deref(&self) -> &RocksSnapshot {
        &self.0
    }
}

impl RocksSyncSnapshot {
    pub fn new(db: Arc<DB>) -> RocksSyncSnapshot {
        RocksSyncSnapshot(Arc::new(RocksSnapshot::new(db)))
    }

    pub fn from_ref(raw: &RawSyncSnapshot) -> &RocksSyncSnapshot {
        unsafe { &*(raw as *const _ as *const _) }
    }
}

impl SyncSnapshot<RocksSnapshot> for RocksSyncSnapshot { }
