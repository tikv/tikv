// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    result,
    sync::atomic::{AtomicBool, Ordering},
};

use engine_traits::{
    IterMetricsCollector, IterOptions, Iterable, Iterator, KvEngine, MetricsExt, RangeCacheEngine,
    Result,
};
use tikv_util::{error, warn, Either};
use txn_types::Key;

pub static AUDIT_MODE: AtomicBool = AtomicBool::new(false);

pub struct HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    iter: Either<<EK::Snapshot as Iterable>::Iterator, <EC::Snapshot as Iterable>::Iterator>,
    disk_iter: <EK::Snapshot as Iterable>::Iterator,
    seqno: u64,
    iter_opts: IterOptions,
}

impl<EK, EC> HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    pub fn disk_engine_iterator(
        iter: <EK::Snapshot as Iterable>::Iterator,
        disk_iter: <EK::Snapshot as Iterable>::Iterator,
        seqno: u64,
        iter_opts: IterOptions,
    ) -> Self {
        Self {
            iter: Either::Left(iter),
            disk_iter,
            seqno,
            iter_opts,
        }
    }

    pub fn region_cache_engine_iterator(
        iter: <EC::Snapshot as Iterable>::Iterator,
        disk_iter: <EK::Snapshot as Iterable>::Iterator,
        seqno: u64,
        iter_opts: IterOptions,
    ) -> Self {
        Self {
            iter: Either::Right(iter),
            disk_iter,
            seqno,
            iter_opts,
        }
    }

    fn next_to_match(&mut self, next_first: bool) {
        match self.iter {
            Either::Left(ref mut iter) => unreachable!(),
            Either::Right(ref mut iter) => {
                let key = iter.key();
                let val = iter.value();
                if next_first {
                    if !self.disk_iter.next().unwrap() {
                        let (lower, upper) = self.iter_opts.clone().build_bounds();
                        let prefix_same_as_start = self.iter_opts.prefix_same_as_start();
                        error!(
                            "prev inconsistent, disk iterator prev failed";
                            "cache_key" => log_wrappers::Value(key),
                            "cache_val" => log_wrappers::Value(val),
                            "lower" => log_wrappers::Value(&lower.unwrap_or_default()),
                            "upper" => log_wrappers::Value(&upper.unwrap_or_default()),
                            "prefix_same_as_start" => prefix_same_as_start,
                            "seqno" => self.seqno,
                        );
                        unreachable!()
                    }
                }
                loop {
                    let disk_key = self.disk_iter.key();
                    let disk_val = self.disk_iter.value();
                    if disk_key == key {
                        break;
                    }
                    if disk_key > key {
                        let (lower, upper) = self.iter_opts.clone().build_bounds();
                        let prefix_same_as_start = self.iter_opts.prefix_same_as_start();
                        error!(
                            "next inconsistent";
                            "cache_key" => log_wrappers::Value(key),
                            "cache_val" => log_wrappers::Value(val),
                            "disk_key" => log_wrappers::Value(disk_key),
                            "disk_val" => log_wrappers::Value(disk_val),
                            "lower" => log_wrappers::Value(&lower.unwrap_or_default()),
                            "upper" => log_wrappers::Value(&upper.unwrap_or_default()),
                            "prefix_same_as_start" => prefix_same_as_start,
                            "seqno" => self.seqno,
                        );
                        unreachable!()
                    }
                    assert!(self.disk_iter.next().unwrap());
                }
            }
        }
    }

    fn prev_to_match(&mut self, prev_first: bool) {
        match self.iter {
            Either::Left(ref mut iter) => unreachable!(),
            Either::Right(ref mut iter) => {
                let key = iter.key();
                let val = iter.value();
                if prev_first {
                    if !self.disk_iter.prev().unwrap() {
                        let (lower, upper) = self.iter_opts.clone().build_bounds();
                        let prefix_same_as_start = self.iter_opts.prefix_same_as_start();
                        error!(
                            "prev inconsistent, disk iterator prev failed";
                            "cache_key" => log_wrappers::Value(key),
                            "cache_val" => log_wrappers::Value(val),
                            "lower" => log_wrappers::Value(&lower.unwrap_or_default()),
                            "upper" => log_wrappers::Value(&upper.unwrap_or_default()),
                            "prefix_same_as_start" => prefix_same_as_start,
                            "seqno" => self.seqno,
                        );
                        unreachable!()
                    }
                }
                loop {
                    let disk_key = self.disk_iter.key();
                    let disk_val = self.disk_iter.value();
                    if disk_key == key {
                        break;
                    }
                    if disk_key < key {
                        let (lower, upper) = self.iter_opts.clone().build_bounds();
                        let prefix_same_as_start = self.iter_opts.prefix_same_as_start();
                        error!(
                            "prev inconsistent";
                            "cache_key" => log_wrappers::Value(key),
                            "cache_val" => log_wrappers::Value(val),
                            "disk_key" => log_wrappers::Value(disk_key),
                            "disk_val" => log_wrappers::Value(disk_val),
                            "lower" => log_wrappers::Value(&lower.unwrap_or_default()),
                            "upper" => log_wrappers::Value(&upper.unwrap_or_default()),
                            "prefix_same_as_start" => prefix_same_as_start,
                            "seqno" => self.seqno,
                        );
                        unreachable!()
                    }
                    assert!(self.disk_iter.prev().unwrap());
                }
            }
        }
    }
}

fn split_ts(key: &[u8]) -> result::Result<(&[u8], u64), String> {
    match Key::split_on_ts_for(key) {
        Ok((key, ts)) => Ok((key, ts.into_inner())),
        Err(_) => Err(format!(
            "invalid write cf key: {}",
            log_wrappers::Value(key)
        )),
    }
}

impl<EK, EC> Iterator for HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.seek(key),
            Either::Right(ref mut iter) => {
                if AUDIT_MODE.load(Ordering::Relaxed) {
                    let valid = iter.seek(key)?;
                    let valid2 = self
                        .disk_iter
                        .seek(key)
                        .map_err(|e| {
                            error!(
                                "in-memory engine seek succeed, but disk engine failed";
                                "key" => log_wrappers::Value(key),
                                "seqno" => self.seqno,
                            );
                            e
                        })
                        .unwrap();
                    if valid && !valid2 {
                        let prefix_same_as_start = self.iter_opts.prefix_same_as_start();
                        let (lower, upper) = self.iter_opts.clone().build_bounds();
                        error!(
                            "seek result not equal";
                            "key" => log_wrappers::Value(key),
                            "cache_key" => log_wrappers::Value(iter.key()),
                            "lower" => log_wrappers::Value(&lower.unwrap_or_default()),
                            "upper" => log_wrappers::Value(&upper.unwrap_or_default()),
                            "prefix_same_as_start" => prefix_same_as_start,
                            "seqno" => self.seqno,
                        );
                        unreachable!();
                    }
                    if valid {
                        self.next_to_match(false);
                    }
                    Ok(valid)
                } else {
                    iter.seek(key)
                }
            }
        }
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.seek_for_prev(key),
            Either::Right(ref mut iter) => {
                if AUDIT_MODE.load(Ordering::Relaxed) {
                    let valid = iter.seek_for_prev(key)?;
                    let valid2 = self
                        .disk_iter
                        .seek_for_prev(key)
                        .map_err(|e| {
                            error!(
                                "in-memory engine seek_for_prev succeed, but disk engine failed";
                                "key" => log_wrappers::Value(key),
                                "seqno" => self.seqno,
                            );
                            e
                        })
                        .unwrap();
                    if valid && !valid2 {
                        let prefix_same_as_start = self.iter_opts.prefix_same_as_start();
                        let (lower, upper) = self.iter_opts.clone().build_bounds();
                        error!(
                            "seek_for_prev result not equal";
                            "key" => log_wrappers::Value(key),
                            "res" => valid,
                            "res2" => valid2,
                            "cache_key" => log_wrappers::Value(iter.key()),
                            "lower" => log_wrappers::Value(&lower.unwrap_or_default()),
                            "upper" => log_wrappers::Value(&upper.unwrap_or_default()),
                            "prefix_same_as_start" => prefix_same_as_start,
                            "seqno" => self.seqno,
                        );
                        unreachable!();
                    }

                    if valid {
                        self.prev_to_match(false);
                    }
                    Ok(valid)
                } else {
                    iter.seek_for_prev(key)
                }
            }
        }
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.seek_to_first(),
            Either::Right(ref mut iter) => {
                if AUDIT_MODE.load(Ordering::Relaxed) {
                    let valid = iter.seek_to_first()?;
                    let valid2 = self
                        .disk_iter
                        .seek_to_first()
                        .map_err(|e| {
                            error!(
                                "in-memory engine seek_to_first succeed, but disk engine failed";
                                "seqno" => self.seqno,
                            );
                            e
                        })
                        .unwrap();
                    if valid && !valid2 {
                        let prefix_same_as_start = self.iter_opts.prefix_same_as_start();
                        let (lower, upper) = self.iter_opts.clone().build_bounds();
                        error!(
                            "seek_to_first result not equal";
                            "res" => valid,
                            "res2" => valid2,
                            "lower" => log_wrappers::Value(&lower.unwrap_or_default()),
                            "upper" => log_wrappers::Value(&upper.unwrap_or_default()),
                            "prefix_same_as_start" => prefix_same_as_start,
                            "seqno" => self.seqno,
                        );
                        unreachable!();
                    }

                    if valid {
                        self.next_to_match(false);
                    }
                    Ok(valid)
                } else {
                    iter.seek_to_first()
                }
            }
        }
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.seek_to_last(),
            Either::Right(ref mut iter) => {
                if AUDIT_MODE.load(Ordering::Relaxed) {
                    let valid = iter.seek_to_last()?;
                    let valid2 = self
                        .disk_iter
                        .seek_to_last()
                        .map_err(|e| {
                            error!(
                                "in-memory engine seek_to_last succeed, but disk engine failed";
                                "seqno" => self.seqno,
                            );
                            e
                        })
                        .unwrap();
                    if valid != valid2 {
                        let prefix_same_as_start = self.iter_opts.prefix_same_as_start();
                        let (lower, upper) = self.iter_opts.clone().build_bounds();
                        error!(
                            "seek_to_last result not equal";
                            "res" => valid,
                            "res2" => valid2,
                            "lower" => log_wrappers::Value(&lower.unwrap_or_default()),
                            "upper" => log_wrappers::Value(&upper.unwrap_or_default()),
                            "prefix_same_as_start" => prefix_same_as_start,
                            "seqno" => self.seqno,
                        );
                        unreachable!();
                    }

                    if valid {
                        self.prev_to_match(false);
                    }
                    Ok(valid)
                } else {
                    iter.seek_to_last()
                }
            }
        }
    }

    fn prev(&mut self) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.prev(),
            Either::Right(ref mut iter) => {
                if AUDIT_MODE.load(Ordering::Relaxed) {
                    let valid = iter.prev()?;
                    if valid {
                        self.prev_to_match(true);
                    }
                    Ok(valid)
                } else {
                    iter.prev()
                }
            }
        }
    }

    fn next(&mut self) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.next(),
            Either::Right(ref mut iter) => {
                if AUDIT_MODE.load(Ordering::Relaxed) {
                    let valid = iter.next()?;
                    if valid {
                        self.next_to_match(true);
                    }
                    Ok(valid)
                } else {
                    iter.next()
                }
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

pub struct HybridEngineIterMetricsCollector<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    collector: Either<
        <<EK::Snapshot as Iterable>::Iterator as MetricsExt>::Collector,
        <<EC::Snapshot as Iterable>::Iterator as MetricsExt>::Collector,
    >,
}

impl<EK, EC> IterMetricsCollector for HybridEngineIterMetricsCollector<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn internal_delete_skipped_count(&self) -> u64 {
        match &self.collector {
            Either::Left(c) => c.internal_delete_skipped_count(),
            Either::Right(c) => c.internal_delete_skipped_count(),
        }
    }

    fn internal_key_skipped_count(&self) -> u64 {
        match &self.collector {
            Either::Left(c) => c.internal_key_skipped_count(),
            Either::Right(c) => c.internal_key_skipped_count(),
        }
    }
}

impl<EK, EC> MetricsExt for HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type Collector = HybridEngineIterMetricsCollector<EK, EC>;

    fn metrics_collector(&self) -> Self::Collector {
        match self.iter {
            Either::Left(ref iter) => HybridEngineIterMetricsCollector {
                collector: Either::Left(iter.metrics_collector()),
            },
            Either::Right(ref iter) => HybridEngineIterMetricsCollector {
                collector: Either::Right(iter.metrics_collector()),
            },
        }
    }
}
