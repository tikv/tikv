// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use super::{util, RocksIterator, RocksReadOptions, UnsafeSnap, DB};
use crate::options::*;
use crate::rocks::DBIterator;
use crate::{Iterable, Peekable, Result, Snapshot};

pub struct RocksSnapshot {
    // TODO: use reference.
    db: Arc<DB>,
    snap: UnsafeSnap,
}

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

impl Snapshot for RocksSnapshot {}

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
    type Iter = RocksIterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iter> {
        let mut opt: RocksReadOptions = opts.into();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(DBIterator::new(self.db.clone(), opt))
    }

    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iter> {
        let mut opt: RocksReadOptions = opts.into();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = util::get_cf_handle(self.db.as_ref(), cf)?;
        Ok(DBIterator::new_cf(self.db.clone(), handle, opt))
    }
}

impl Peekable for RocksSnapshot {
    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut opt: RocksReadOptions = opts.into();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = self.db.get_opt(key, &opt)?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let mut opt: RocksReadOptions = opts.into();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let handle = util::get_cf_handle(self.db.as_ref(), cf)?;
        let v = self.db.get_cf_opt(handle, key, &opt)?;
        Ok(v.map(|v| v.to_vec()))
    }
}
