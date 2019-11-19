// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use super::{CFHandle, UnsafeSnap, DB};
use crate::iterable::IterOptionsExt;
use crate::{DBIterator, Error, IterOption, Iterable, Result};

#[repr(C)] // Guarantee same representation as in engine_rocks
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
