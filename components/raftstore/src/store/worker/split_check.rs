// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fmt::{self, Display, Formatter},
    mem,
};

use engine_traits::{CfName, IterOptions, Iterable, Iterator, KvEngine, CF_WRITE, LARGE_CFS};
use file_system::{IOType, WithIOType};
use itertools::Itertools;
use kvproto::{
    metapb::{Region, RegionEpoch},
    pdpb::CheckPolicy,
};
use online_config::{ConfigChange, OnlineConfig};
use tikv_util::{box_err, debug, error, info, keybuilder::KeyBuilder, warn, worker::Runnable};

use super::metrics::*;
#[cfg(any(test, feature = "testexport"))]
use crate::coprocessor::Config;
use crate::{
    coprocessor::{
        split_observer::{is_valid_split_key, strip_timestamp_if_exists},
        CoprocessorHost, SplitCheckerHost,
    },
    store::{Callback, CasualMessage, CasualRouter},
    Result,
};

#[derive(PartialEq, Eq)]
pub struct KeyEntry {
    key: Vec<u8>,
    pos: usize,
    value_size: usize,
    cf: CfName,
}

impl KeyEntry {
    pub fn new(key: Vec<u8>, pos: usize, value_size: usize, cf: CfName) -> KeyEntry {
        KeyEntry {
            key,
            pos,
            value_size,
            cf,
        }
    }

    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn is_commit_version(&self) -> bool {
        self.cf == CF_WRITE
    }

    pub fn entry_size(&self) -> usize {
        self.value_size + self.key.len()
    }
}

impl PartialOrd for KeyEntry {
    fn partial_cmp(&self, rhs: &KeyEntry) -> Option<Ordering> {
        // BinaryHeap is max heap, so we have to reverse order to get a min heap.
        Some(self.key.cmp(&rhs.key).reverse())
    }
}

impl Ord for KeyEntry {
    fn cmp(&self, rhs: &KeyEntry) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

struct MergedIterator<I> {
    iters: Vec<(CfName, I)>,
    heap: BinaryHeap<KeyEntry>,
}

impl<I> MergedIterator<I>
where
    I: Iterator,
{
    fn new<E: KvEngine>(
        db: &E,
        cfs: &[CfName],
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
    ) -> Result<MergedIterator<E::Iterator>> {
        let mut iters = Vec::with_capacity(cfs.len());
        let mut heap = BinaryHeap::with_capacity(cfs.len());
        for (pos, cf) in cfs.iter().enumerate() {
            let iter_opt = IterOptions::new(
                Some(KeyBuilder::from_slice(start_key, 0, 0)),
                Some(KeyBuilder::from_slice(end_key, 0, 0)),
                fill_cache,
            );
            let mut iter = db.iterator_cf_opt(cf, iter_opt)?;
            let found: Result<bool> = iter.seek(start_key.into()).map_err(|e| box_err!(e));
            if found? {
                heap.push(KeyEntry::new(
                    iter.key().to_vec(),
                    pos,
                    iter.value().len(),
                    *cf,
                ));
            }
            iters.push((*cf, iter));
        }
        Ok(MergedIterator { iters, heap })
    }

    fn next(&mut self) -> Option<KeyEntry> {
        let pos = match self.heap.peek() {
            None => return None,
            Some(e) => e.pos,
        };
        let (cf, iter) = &mut self.iters[pos];
        if iter.next().unwrap() {
            // TODO: avoid copy key.
            let mut e = KeyEntry::new(iter.key().to_vec(), pos, iter.value().len(), cf);
            let mut front = self.heap.peek_mut().unwrap();
            mem::swap(&mut e, &mut front);
            Some(e)
        } else {
            self.heap.pop()
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct BucketRange(pub Vec<u8>, pub Vec<u8>);

#[derive(Default, Clone, Debug)]
pub struct Bucket {
    // new proposed split keys under the bucket for split
    // if it does not need split, it's empty
    pub keys: Vec<Vec<u8>>,
    // total size of the bucket
    pub size: u64,
}

pub enum Task {
    SplitCheckTask {
        region: Region,
        auto_split: bool,
        policy: CheckPolicy,
        bucket_ranges: Option<Vec<BucketRange>>,
    },
    ApproximateBuckets(Region),
    ChangeConfig(ConfigChange),
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&Config) + Send>),
}

impl Task {
    pub fn split_check(
        region: Region,
        auto_split: bool,
        policy: CheckPolicy,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) -> Task {
        Task::SplitCheckTask {
            region,
            auto_split,
            policy,
            bucket_ranges,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::SplitCheckTask {
                region, auto_split, ..
            } => write!(
                f,
                "[split check worker] Split Check Task for {}, auto_split: {:?}",
                region.get_id(),
                auto_split
            ),
            Task::ChangeConfig(_) => write!(f, "[split check worker] Change Config Task"),
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(_) => write!(f, "[split check worker] Validate config"),
            Task::ApproximateBuckets(_) => write!(f, "[split check worker] Approximate buckets"),
        }
    }
}

pub struct Runner<E, S>
where
    E: KvEngine,
{
    engine: E,
    router: S,
    coprocessor: CoprocessorHost<E>,
}

impl<E, S> Runner<E, S>
where
    E: KvEngine,
    S: CasualRouter<E>,
{
    pub fn new(engine: E, router: S, coprocessor: CoprocessorHost<E>) -> Runner<E, S> {
        Runner {
            engine,
            router,
            coprocessor,
        }
    }

    fn approximate_check_bucket(
        &self,
        region: &Region,
        host: &mut SplitCheckerHost<'_, E>,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) -> Result<()> {
        let ranges = bucket_ranges.clone().unwrap_or_else(|| {
            vec![BucketRange(
                region.get_start_key().to_vec(),
                region.get_end_key().to_vec(),
            )]
        });
        let mut buckets = vec![];
        for range in &ranges {
            let mut bucket = region.clone();
            bucket.set_start_key(range.0.clone());
            bucket.set_end_key(range.1.clone());
            let bucket_entry = host.approximate_bucket_keys(&bucket, &self.engine)?;
            debug!(
                "bucket_entry size {} keys count {}",
                bucket_entry.size,
                bucket_entry.keys.len()
            );
            buckets.push(bucket_entry);
        }

        self.on_buckets_created(&mut buckets, region, &ranges);
        self.refresh_region_buckets(buckets, region, bucket_ranges);
        Ok(())
    }

    fn on_buckets_created(
        &self,
        buckets: &mut [Bucket],
        region: &Region,
        bucket_ranges: &Vec<BucketRange>,
    ) {
        for (mut bucket, bucket_range) in &mut buckets.iter_mut().zip(bucket_ranges) {
            let mut bucket_region = region.clone();
            bucket_region.set_start_key(bucket_range.0.clone());
            bucket_region.set_end_key(bucket_range.1.clone());
            let adjusted_keys = std::mem::take(&mut bucket.keys)
                .into_iter()
                .enumerate()
                .filter_map(|(i, key)| {
                    let key = strip_timestamp_if_exists(key);
                    if is_valid_split_key(&key, i, &bucket_region) {
                        assert!(
                            is_valid_split_key(&key, i, region),
                            "region_id={}, key={}, region start_key={}, end_key={}, bucket_range start_key={}, end_key={}",
                            region.get_id(),
                            log_wrappers::Value::key(&key),
                            log_wrappers::Value::key(region.get_start_key()),
                            log_wrappers::Value::key(region.get_end_key()),
                            log_wrappers::Value::key(&bucket_range.0),
                            log_wrappers::Value::key(&bucket_range.1),
                        );
                        Some(key)
                    } else {
                        None
                    }
                })
                .coalesce(|prev, curr| {
                    // Make sure that the split keys are sorted and unique.
                    if prev < curr {
                        Err((prev, curr))
                    } else {
                        warn!(
                            "skip invalid split key: key should not be larger than the previous.";
                            "region_id" => region.id,
                            "key" => log_wrappers::Value::key(&curr),
                            "previous" => log_wrappers::Value::key(&prev),
                        );
                        Ok(prev)
                    }
                })
                .collect::<Vec<_>>();
            bucket.keys = adjusted_keys;
        }
    }

    fn refresh_region_buckets(
        &self,
        buckets: Vec<Bucket>,
        region: &Region,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) {
        let _ = self.router.send(
            region.get_id(),
            CasualMessage::RefreshRegionBuckets {
                region_epoch: region.get_region_epoch().clone(),
                buckets,
                bucket_ranges,
                cb: Callback::None,
            },
        );
    }

    /// Checks a Region with split and bucket checkers to produce split keys and buckets keys and generates split admin command.
    fn check_split_and_bucket(
        &mut self,
        region: &Region,
        auto_split: bool,
        policy: CheckPolicy,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) {
        let region_id = region.get_id();
        let start_key = keys::enc_start_key(region);
        let end_key = keys::enc_end_key(region);
        debug!(
            "executing task";
            "region_id" => region_id,
            "start_key" => log_wrappers::Value::key(&start_key),
            "end_key" => log_wrappers::Value::key(&end_key),
            "policy" => ?policy,
        );
        CHECK_SPILT_COUNTER.all.inc();
        let mut host =
            self.coprocessor
                .new_split_checker_host(region, &self.engine, auto_split, policy);

        if host.skip() {
            debug!("skip split check"; "region_id" => region.get_id());
            return;
        }

        let split_keys = match host.policy() {
            CheckPolicy::Scan => {
                match self.scan_split_keys(&mut host, region, &start_key, &end_key, bucket_ranges) {
                    Ok(keys) => keys,
                    Err(e) => {
                        error!(%e; "failed to scan split key"; "region_id" => region_id,);
                        return;
                    }
                }
            }
            CheckPolicy::Approximate => match host.approximate_split_keys(region, &self.engine) {
                Ok(keys) => {
                    if host.enable_region_bucket() {
                        if let Err(e) =
                            self.approximate_check_bucket(region, &mut host, bucket_ranges)
                        {
                            error!(%e;
                                "approximate_check_bucket failed";
                                "region_id" => region_id,
                            );
                        }
                    }
                    keys.into_iter()
                        .map(|k| keys::origin_key(&k).to_vec())
                        .collect()
                }
                Err(e) => {
                    error!(%e;
                        "failed to get approximate split key, try scan way";
                        "region_id" => region_id,
                    );
                    match self.scan_split_keys(
                        &mut host,
                        region,
                        &start_key,
                        &end_key,
                        bucket_ranges,
                    ) {
                        Ok(keys) => keys,
                        Err(e) => {
                            error!(%e; "failed to scan split key"; "region_id" => region_id,);
                            return;
                        }
                    }
                }
            },
            CheckPolicy::Usekey => vec![], // Handled by pd worker directly.
        };

        if !split_keys.is_empty() {
            let region_epoch = region.get_region_epoch().clone();
            let msg = new_split_region(region_epoch, split_keys, "split checker");
            let res = self.router.send(region_id, msg);
            if let Err(e) = res {
                warn!("failed to send check result"; "region_id" => region_id, "err" => %e);
            }

            CHECK_SPILT_COUNTER.success.inc();
        } else {
            debug!(
                "no need to send, split key not found";
                "region_id" => region_id,
            );

            CHECK_SPILT_COUNTER.ignore.inc();
        }
    }

    /// Gets the split keys by scanning the range.
    /// bucket_ranges: specify the ranges to generate buckets.
    ///                If none, gengerate buckets for the whole region.
    ///                If it's Some(vec![]), skip generating buckets.
    fn scan_split_keys(
        &self,
        host: &mut SplitCheckerHost<'_, E>,
        region: &Region,
        start_key: &[u8],
        end_key: &[u8],
        bucket_ranges: Option<Vec<BucketRange>>,
    ) -> Result<Vec<Vec<u8>>> {
        let timer = CHECK_SPILT_HISTOGRAM.start_coarse_timer();
        let mut buckets = Vec::new();
        let mut bucket = Bucket::default();
        let empty_bucket = vec![];
        let (mut skip_check_bucket, bucket_range_list) =
            if let Some(ref bucket_range_list) = bucket_ranges {
                (
                    bucket_range_list.is_empty() || !host.enable_region_bucket(),
                    bucket_range_list,
                )
            } else {
                (!host.enable_region_bucket(), &empty_bucket)
            };

        MergedIterator::<<E as Iterable>::Iterator>::new(
            &self.engine,
            LARGE_CFS,
            start_key,
            end_key,
            false,
        )
        .map(|mut iter| {
            let mut size = 0;
            let mut keys = 0;
            let mut bucket_size: u64 = 0;
            let mut bucket_range_idx = 0;
            let mut skip_on_kv = false;
            while let Some(e) = iter.next() {
                if skip_on_kv && skip_check_bucket {
                    return;
                }
                if !skip_on_kv && host.on_kv(region, &e) {
                    skip_on_kv = true;
                }
                size += e.entry_size() as u64;
                keys += 1;
                if !skip_check_bucket {
                    let origin_key = keys::origin_key(e.key());
                    // generate buckets for the whole region,
                    // skip checking bucket range
                    if bucket_range_list.is_empty() {
                        bucket_size += e.entry_size() as u64;
                        if bucket_size >= host.region_bucket_size() {
                            bucket.keys.push(origin_key.to_vec());
                            bucket.size += bucket_size;
                            bucket_size = 0;
                        }
                    } else {
                        // find proper bucket range that covers e.key
                        while bucket_range_idx < bucket_range_list.len()
                            && origin_key >= bucket_range_list[bucket_range_idx].1.as_slice()
                            && !bucket_range_list[bucket_range_idx].1.is_empty()
                        {
                            bucket_range_idx += 1;
                            bucket_size = 0;
                            if buckets.len() < bucket_range_idx {
                                buckets.push(bucket);
                                bucket = Bucket::default();
                            }
                        }
                        if bucket_range_idx == bucket_range_list.len() {
                            skip_check_bucket = true;
                        } else if origin_key >= bucket_range_list[bucket_range_idx].0.as_slice() {
                            // e.key() is between bucket_range_list[bucket_range_idx].0, bucket_range_list[bucket_range_idx].1
                            bucket_size += e.entry_size() as u64;
                            if bucket_size >= host.region_bucket_size() {
                                bucket.keys.push(origin_key.to_vec());
                                bucket.size += bucket_size;
                                bucket_size = 0;
                            }
                        }
                    }
                }
            }

            if buckets.len() < bucket_range_list.len()
                || bucket_range_list.is_empty() && !skip_check_bucket
            {
                buckets.push(bucket);
                // in case some range's data in bucket_range_list is deleted
                if buckets.len() < bucket_range_list.len() {
                    let mut deleted_buckets =
                        vec![Bucket::default(); bucket_range_list.len() - buckets.len()];
                    buckets.append(&mut deleted_buckets);
                }
                if !bucket_range_list.is_empty() {
                    assert_eq!(buckets.len(), bucket_range_list.len());
                }
            }

            // if we scan the whole range, we can update approximate size and keys with accurate value.
            info!(
                "update approximate size and keys with accurate value";
                "region_id" => region.get_id(),
                "size" => size,
                "keys" => keys,
                "bucket_count" => buckets.len(),
                "bucket_size" => bucket_size,
            );
            let _ = self.router.send(
                region.get_id(),
                CasualMessage::RegionApproximateSize { size },
            );
            let _ = self.router.send(
                region.get_id(),
                CasualMessage::RegionApproximateKeys { keys },
            );
        })?;

        if host.enable_region_bucket() {
            let ranges = bucket_ranges.clone().unwrap_or_else(|| {
                vec![BucketRange(
                    region.get_start_key().to_vec(),
                    region.get_end_key().to_vec(),
                )]
            });
            self.on_buckets_created(&mut buckets, region, &ranges);
            self.refresh_region_buckets(buckets, region, bucket_ranges);
        }
        timer.observe_duration();

        Ok(host.split_keys())
    }

    fn change_cfg(&mut self, change: ConfigChange) {
        info!(
            "split check config updated";
            "change" => ?change
        );
        self.coprocessor.cfg.update(change);
    }
}

impl<E, S> Runnable for Runner<E, S>
where
    E: KvEngine,
    S: CasualRouter<E>,
{
    type Task = Task;
    fn run(&mut self, task: Task) {
        let _io_type_guard = WithIOType::new(IOType::LoadBalance);
        match task {
            Task::SplitCheckTask {
                region,
                auto_split,
                policy,
                bucket_ranges,
            } => self.check_split_and_bucket(&region, auto_split, policy, bucket_ranges),
            Task::ChangeConfig(c) => self.change_cfg(c),
            Task::ApproximateBuckets(region) => {
                if self.coprocessor.cfg.enable_region_bucket {
                    let mut host = self.coprocessor.new_split_checker_host(
                        &region,
                        &self.engine,
                        false,
                        CheckPolicy::Approximate,
                    );
                    if let Err(e) = self.approximate_check_bucket(&region, &mut host, None) {
                        error!(%e;
                            "approximate_check_bucket failed";
                            "region_id" => region.get_id(),
                        );
                    }
                }
            }
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(f) => f(&self.coprocessor.cfg),
        }
    }
}

fn new_split_region<E>(
    region_epoch: RegionEpoch,
    split_keys: Vec<Vec<u8>>,
    source: &'static str,
) -> CasualMessage<E>
where
    E: KvEngine,
{
    CasualMessage::SplitRegion {
        region_epoch,
        split_keys,
        callback: Callback::None,
        source: source.into(),
    }
}
