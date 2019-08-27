// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use super::{util, CFHandle, Iterator, RawReadOptions, UnsafeSnap, DB};
use crate::options::*;
use crate::rocks::DBIterator;
use crate::{Error, Iterable, Peekable, Result, Snapshot};

pub struct RocksSnapshot {
    // TODO: use reference.
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

    pub fn into_sync(self) -> SyncRocksSnapshot {
        SyncRocksSnapshot(Arc::new(self))
    }

    pub fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
    }

    pub fn cf_handle(&self, cf: &str) -> Result<&CFHandle> {
        super::util::get_cf_handle(&self.db, cf).map_err(Error::from)
    }

    pub fn get_db(&self) -> Arc<DB> {
        Arc::clone(&self.db)
    }
}

impl Snapshot for RocksSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
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
    type Iter = Iterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iter> {
        let mut opt: RawReadOptions = opts.into();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(DBIterator::new(self.db.clone(), opt))
    }

    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iter> {
        let mut opt: RawReadOptions = opts.into();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = util::get_cf_handle(self.db.as_ref(), cf)?;
        Ok(DBIterator::new_cf(self.db.clone(), handle, opt))
    }
}

impl Peekable for RocksSnapshot {
    fn get_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut opt: RawReadOptions = opts.into();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_opt(key, &opt)?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf_opt(&self, opts: &ReadOptions, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut opt: RawReadOptions = opts.into();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = util::get_cf_handle(self.db.as_ref(), cf)?;
        let v = self.db.get_cf_opt(handle, key, &opt)?;
        Ok(v.map(|v| v.to_vec()))
    }
}

#[derive(Clone, Debug)]
pub struct SyncRocksSnapshot(Arc<RocksSnapshot>);

impl Deref for SyncRocksSnapshot {
    type Target = RocksSnapshot;

    fn deref(&self) -> &RocksSnapshot {
        &self.0
    }
}

impl SyncRocksSnapshot {
    pub fn new(db: Arc<DB>) -> SyncRocksSnapshot {
        SyncRocksSnapshot(Arc::new(RocksSnapshot::new(db)))
    }
}
