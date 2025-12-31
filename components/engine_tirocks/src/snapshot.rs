// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use engine_traits::Result;
use tirocks::{db::RawCfHandle, option::ReadOptions, Db, Iterator, Snapshot};

use crate::{db_vector::RocksPinSlice, engine_iterator, r2e, util, RocksSnapIterator};

pub struct RocksSnapshot(Arc<Snapshot<'static, Arc<Db>>>);

impl RocksSnapshot {
    #[inline]
    pub(crate) fn new(db: Arc<Db>) -> Self {
        Self(Arc::new(Snapshot::new(db)))
    }

    #[inline]
    fn get(
        &self,
        opts: &engine_traits::ReadOptions,
        handle: &RawCfHandle,
        key: &[u8],
    ) -> Result<Option<RocksPinSlice>> {
        let mut opt = ReadOptions::default();
        opt.set_fill_cache(opts.fill_cache());
        // TODO: reuse slice.
        let mut slice = RocksPinSlice::default();
        match self.0.get_pinned(&mut opt, handle, key, &mut slice.0) {
            Ok(true) => Ok(Some(slice)),
            Ok(false) => Ok(None),
            Err(s) => Err(r2e(s)),
        }
    }
}

impl engine_traits::Snapshot for RocksSnapshot {}

impl Debug for RocksSnapshot {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> fmt::Result {
        write!(fmt, "tirocks Snapshot Impl")
    }
}

impl engine_traits::Iterable for RocksSnapshot {
    type Iterator = RocksSnapIterator;

    fn iterator_opt(&self, cf: &str, opts: engine_traits::IterOptions) -> Result<Self::Iterator> {
        let opt = engine_iterator::to_tirocks_opt(opts);
        let handle = util::cf_handle(self.0.db(), cf)?;
        Ok(RocksSnapIterator::from_raw(Iterator::new(
            self.0.clone(),
            opt,
            handle,
        )))
    }
}

impl engine_traits::Peekable for RocksSnapshot {
    type DbVector = RocksPinSlice;

    #[inline]
    fn get_value_opt(
        &self,
        opts: &engine_traits::ReadOptions,
        key: &[u8],
    ) -> Result<Option<Self::DbVector>> {
        self.get(opts, self.0.db().default_cf(), key)
    }

    #[inline]
    fn get_value_cf_opt(
        &self,
        opts: &engine_traits::ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<Self::DbVector>> {
        let handle = util::cf_handle(self.0.db(), cf)?;
        self.get(opts, handle, key)
    }
}
