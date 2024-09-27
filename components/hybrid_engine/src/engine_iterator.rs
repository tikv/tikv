// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    IterMetricsCollector, Iterable, Iterator, KvEngine, MetricsExt, RegionCacheEngine, Result,
};
use tikv_util::Either;

pub struct HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    iter: Either<<EK::Snapshot as Iterable>::Iterator, <EC::Snapshot as Iterable>::Iterator>,
}

impl<EK, EC> HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    pub fn disk_engine_iterator(iter: <EK::Snapshot as Iterable>::Iterator) -> Self {
        Self {
            iter: Either::Left(iter),
        }
    }

    pub fn region_cache_engine_iterator(iter: <EC::Snapshot as Iterable>::Iterator) -> Self {
        Self {
            iter: Either::Right(iter),
        }
    }
}

impl<EK, EC> Iterator for HybridEngineIterator<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.seek(key),
            Either::Right(ref mut iter) => iter.seek(key),
        }
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.seek_for_prev(key),
            Either::Right(ref mut iter) => iter.seek_for_prev(key),
        }
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.seek_to_first(),
            Either::Right(ref mut iter) => iter.seek_to_first(),
        }
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.seek_to_last(),
            Either::Right(ref mut iter) => iter.seek_to_last(),
        }
    }

    fn prev(&mut self) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.prev(),
            Either::Right(ref mut iter) => iter.prev(),
        }
    }

    fn next(&mut self) -> Result<bool> {
        match self.iter {
            Either::Left(ref mut iter) => iter.next(),
            Either::Right(ref mut iter) => iter.next(),
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
    EC: RegionCacheEngine,
{
    collector: Either<
        <<EK::Snapshot as Iterable>::Iterator as MetricsExt>::Collector,
        <<EC::Snapshot as Iterable>::Iterator as MetricsExt>::Collector,
    >,
}

impl<EK, EC> IterMetricsCollector for HybridEngineIterMetricsCollector<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
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
    EC: RegionCacheEngine,
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
