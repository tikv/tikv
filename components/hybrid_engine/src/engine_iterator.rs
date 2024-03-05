// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Iterable, Iterator, KvEngine, RangeCacheEngine, Result};
use tikv_util::{info, Either};

pub struct HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    pub(crate) disk_iter: <EK::Snapshot as Iterable>::Iterator,
    pub(crate) iter:
        Either<<EK::Snapshot as Iterable>::Iterator, <EC::Snapshot as Iterable>::Iterator>,
}

impl<EK, EC> HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    pub fn disk_engine_iterator(
        disk_iter: <EK::Snapshot as Iterable>::Iterator,
        iter: <EK::Snapshot as Iterable>::Iterator,
    ) -> Self {
        Self {
            disk_iter,
            iter: Either::Left(iter),
        }
    }

    pub fn region_cache_engine_iterator(
        disk_iter: <EK::Snapshot as Iterable>::Iterator,
        iter: <EC::Snapshot as Iterable>::Iterator,
    ) -> Self {
        Self {
            disk_iter,
            iter: Either::Right(iter),
        }
    }
}

impl<EK, EC> Iterator for HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.disk_iter.seek(key);
        match self.iter {
            Either::Left(ref mut iter) => iter.seek(key),
            Either::Right(ref mut iter) => {
                let r = iter.seek(key);
                if iter.valid().unwrap_or(false) && self.disk_iter.valid().unwrap_or(false) {
                    if self.disk_iter.key() != iter.key() {
                        let seq = iter.sequence_number();
                        let cf = iter.cf();
                        info!(
                            "key not equal, seek";
                            "seq" => seq,
                            "cf" => cf,
                            "disk" => log_wrappers::Value::key(self.disk_iter.key()),
                            "memory" => log_wrappers::Value::key(iter.key()),
                            "mem_value" => log_wrappers::Value::key(iter.value()),
                        );
                    }
                }
                r
            }
        }
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.disk_iter.seek_for_prev(key);
        match self.iter {
            Either::Left(ref mut iter) => iter.seek_for_prev(key),
            Either::Right(ref mut iter) => iter.seek_for_prev(key),
        }
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        self.disk_iter.seek_to_first();
        match self.iter {
            Either::Left(ref mut iter) => iter.seek_to_first(),
            Either::Right(ref mut iter) => iter.seek_to_first(),
        }
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        self.disk_iter.seek_to_last();
        match self.iter {
            Either::Left(ref mut iter) => iter.seek_to_last(),
            Either::Right(ref mut iter) => iter.seek_to_last(),
        }
    }

    fn prev(&mut self) -> Result<bool> {
        self.disk_iter.prev();
        match self.iter {
            Either::Left(ref mut iter) => iter.prev(),
            Either::Right(ref mut iter) => iter.prev(),
        }
    }

    fn next(&mut self) -> Result<bool> {
        self.disk_iter.next();
        match self.iter {
            Either::Left(ref mut iter) => iter.next(),
            Either::Right(ref mut iter) => {
                let r = iter.next();
                if iter.valid().unwrap_or(false) && self.disk_iter.valid().unwrap_or(false) {
                    if self.disk_iter.key() != iter.key() {
                        info!(
                            "key not equal, next";
                            "disk" => log_wrappers::Value::key(self.disk_iter.key()),
                            "memory" => log_wrappers::Value::key(iter.key()),
                        );
                    }
                }
                r
            }
        }
    }

    fn key(&self) -> &[u8] {
        match self.iter {
            Either::Left(ref iter) => iter.key(),
            Either::Right(ref iter) => iter.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.iter {
            Either::Left(ref iter) => iter.value(),
            Either::Right(ref iter) => iter.value(),
        }
    }

    fn valid(&self) -> Result<bool> {
        match self.iter {
            Either::Left(ref iter) => iter.valid(),
            Either::Right(ref iter) => iter.valid(),
        }
    }
}
