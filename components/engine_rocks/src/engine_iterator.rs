// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{self, IterMetricsCollector, MetricsExt, Result};
use rocksdb::{DBIterator, PerfContext, DB};

use crate::r2e;

// FIXME: Would prefer using &DB instead of Arc<DB>.  As elsewhere in
// this crate, it would require generic associated types.
pub struct RocksEngineIterator(DBIterator<Arc<DB>>);

impl RocksEngineIterator {
    pub fn from_raw(iter: DBIterator<Arc<DB>>) -> RocksEngineIterator {
        RocksEngineIterator(iter)
    }
}

pub struct RocksIterMetricsCollector;

impl IterMetricsCollector for RocksIterMetricsCollector {
    fn internal_delete_skipped_count(&self) -> u64 {
        PerfContext::get().internal_delete_skipped_count()
    }

    fn internal_key_skipped_count(&self) -> u64 {
        PerfContext::get().internal_key_skipped_count()
    }
}

impl MetricsExt for RocksEngineIterator {
    type Collector = RocksIterMetricsCollector;
    fn metrics_collector(&self) -> Self::Collector {
        RocksIterMetricsCollector {}
    }
}

impl engine_traits::Iterator for RocksEngineIterator {
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::Key(key)).map_err(r2e)
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.0
            .seek_for_prev(rocksdb::SeekKey::Key(key))
            .map_err(r2e)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::Start).map_err(r2e)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::End).map_err(r2e)
    }

    fn prev(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e("Iterator invalid"));
        }
        self.0.prev().map_err(r2e)
    }

    fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e("Iterator invalid"));
        }
        self.0.next().map_err(r2e)
    }

    fn key(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.0.value()
    }

    fn valid(&self) -> Result<bool> {
        self.0.valid().map_err(r2e)
    }
}
