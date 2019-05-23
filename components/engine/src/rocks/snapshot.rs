// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};
use std::ops::Deref;
use std::option::Option;
use std::sync::Arc;

use super::{CFHandle, DBVector, ReadOptions, UnsafeSnap, DB};
use crate::{DBIterator, Error, IterOption, Iterable, Peekable, Result};

pub struct Snapshot {
    db: Arc<DB>,
    snap: UnsafeSnap,
}

/// Because snap will be valid whenever db is valid, so it's safe to send
/// it around.
unsafe impl Send for Snapshot {}
unsafe impl Sync for Snapshot {}

impl Snapshot {
    pub fn new(db: Arc<DB>) -> Snapshot {
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

    pub fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
    }

    pub fn cf_handle(&self, cf: &str) -> Result<&CFHandle> {
        super::util::get_cf_handle(&self.db, cf).map_err(Error::from)
    }

    pub fn get_db(&self) -> Arc<DB> {
        Arc::clone(&self.db)
    }

    pub fn db_iterator(&self, iter_opt: IterOption) -> DBIterator<Arc<DB>> {
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        DBIterator::new(Arc::clone(&self.db), opt)
    }

    pub fn db_iterator_cf(&self, cf: &str, iter_opt: IterOption) -> Result<DBIterator<Arc<DB>>> {
        let handle = super::util::get_cf_handle(&self.db, cf)?;
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(DBIterator::new_cf(Arc::clone(&self.db), handle, opt))
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

#[derive(Debug, Clone)]
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

    pub fn clone(&self) -> SyncSnapshot {
        SyncSnapshot(Arc::clone(&self.0))
    }
}

impl Iterable for Snapshot {
    fn new_iterator(&self, iter_opt: IterOption) -> DBIterator<&DB> {
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        DBIterator::new(&self.db, opt)
    }

    fn new_iterator_cf(&self, cf: &str, iter_opt: IterOption) -> Result<DBIterator<&DB>> {
        let handle = super::util::get_cf_handle(&self.db, cf)?;
        let mut opt = iter_opt.build_read_opts();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(DBIterator::new_cf(&self.db, handle, opt))
    }
}

impl Peekable for Snapshot {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBVector>> {
        let mut opt = ReadOptions::new();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_opt(key, &opt)?;
        Ok(v)
    }

    fn get_value_cf(&self, cf: &str, key: &[u8]) -> Result<Option<DBVector>> {
        let handle = super::util::get_cf_handle(&self.db, cf)?;
        let mut opt = ReadOptions::new();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_cf_opt(handle, key, &opt)?;
        Ok(v)
    }
}
