// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use engine_traits::{self, IterOptions, Iterable, Peekable, ReadOptions, Result};
use rocksdb::rocksdb_options::UnsafeSnap;
use rocksdb::{DBIterator, DB};

use crate::options::RocksReadOptions;
use crate::util::get_cf_handle;
use crate::Iterator;

pub struct Snapshot {
    // TODO: use &DB.
    db: Arc<DB>,
    snap: UnsafeSnap,
}

unsafe impl Send for Snapshot {}
unsafe impl Sync for Snapshot {}

impl Snapshot {
    pub fn new(db: Arc<DB>) -> Self {
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

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }
}

impl engine_traits::Snapshot for Snapshot {
    fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
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

impl Iterable for Snapshot {
    type Iter = Iterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iter> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(Iterator::from_raw(DBIterator::new(self.db.clone(), opt)))
    }

    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iter> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        Ok(Iterator::from_raw(DBIterator::new_cf(
            self.db.clone(),
            handle,
            opt,
        )))
    }
}

impl Peekable for Snapshot {
    fn get_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_opt(key, &opt)?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf_opt(&self, opts: &ReadOptions, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let opt: RocksReadOptions = opts.into();
        let mut opt = opt.into_raw();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        let v = self.db.get_cf_opt(handle, key, &opt)?;
        Ok(v.map(|v| v.to_vec()))
    }
}

#[derive(Clone, Debug)]
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
}
