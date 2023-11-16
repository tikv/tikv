// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fmt::{self, Display, Formatter},
    mem,
    sync::Arc,
};

use engine_traits::{
    CfName, IterOptions, Iterable, Iterator, KvEngine, TabletRegistry, CF_WRITE, LARGE_CFS,
};
use file_system::{IoType, WithIoType};
use itertools::Itertools;
use kvproto::{
    metapb::{Region, RegionEpoch},
    pdpb::CheckPolicy,
};
use online_config::{ConfigChange, OnlineConfig};
use pd_client::{BucketMeta, BucketStat};
use tikv_util::{
    box_err, debug, error, info, keybuilder::KeyBuilder, warn, worker::Runnable, Either,
};
use txn_types::Key;

use super::metrics::*;
use crate::{
    coprocessor::{
        dispatcher::StoreHandle,
        split_observer::{is_valid_split_key, strip_timestamp_if_exists},
        Config, CoprocessorHost, SplitCheckerHost,
    },
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
            let mut iter = db.iterator_opt(cf, iter_opt)?;
            let found: Result<bool> = iter.seek(start_key).map_err(|e| box_err!(e));
            if found? {
                heap.push(KeyEntry::new(
                    iter.key().to_vec(),
                    pos,
                    iter.value().len(),
                    cf,
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

#[derive(Default, Clone, Debug, PartialEq)]
pub struct BucketRange(pub Vec<u8>, pub Vec<u8>);

#[derive(Default, Clone, Debug, PartialEq)]
pub struct Bucket {
    // new proposed split keys under the bucket for split
    // if it does not need split, it's empty
    pub keys: Vec<Vec<u8>>,
    // total size of the bucket
    pub size: u64,
}

#[derive(Debug, Clone, Default)]
pub struct BucketStatsInfo {
    // the stats is increment flow.
    bucket_stat: Option<BucketStat>,
    // the report bucket stat records the increment stats after last report pd.
    // it will be reset after report pd.
    report_bucket_stat: Option<BucketStat>,
    // avoid the version roll back, it record the last bucket version if bucket stat isn't none.
    last_bucket_version: u64,
}

impl BucketStatsInfo {
    /// returns all bucket ranges those's write_bytes exceed the given
    /// diff_size_threshold.
    pub fn gen_bucket_range_for_update(
        &self,
        region_bucket_max_size: u64,
    ) -> Option<Vec<BucketRange>> {
        let region_buckets = self.bucket_stat.as_ref()?;
        let stats = &region_buckets.stats;
        let keys = &region_buckets.meta.keys;
        let sizes = &region_buckets.meta.sizes;

        let mut suspect_bucket_ranges = vec![];
        assert_eq!(keys.len(), stats.write_bytes.len() + 1);
        for i in 0..stats.write_bytes.len() {
            let estimated_bucket_size = stats.write_bytes[i] + sizes[i];
            if estimated_bucket_size >= region_bucket_max_size {
                suspect_bucket_ranges.push(BucketRange(keys[i].clone(), keys[i + 1].clone()));
            }
        }
        Some(suspect_bucket_ranges)
    }

    #[inline]
    pub fn version(&self) -> u64 {
        self.bucket_stat
            .as_ref()
            .map_or(self.last_bucket_version, |b| b.meta.version)
    }

    #[inline]
    pub fn add_bucket_flow(&mut self, delta: &Option<BucketStat>) {
        if let (Some(buckets), Some(report_buckets), Some(delta)) = (
            self.bucket_stat.as_mut(),
            self.report_bucket_stat.as_mut(),
            delta,
        ) {
            buckets.merge(delta);
            report_buckets.merge(delta);
        }
    }

    #[inline]
    pub fn set_bucket_stat(&mut self, buckets: Option<BucketStat>) {
        self.bucket_stat = buckets.clone();
        if let Some(new_buckets) = buckets {
            self.last_bucket_version = new_buckets.meta.version;
            let mut new_report_buckets = BucketStat::from_meta(new_buckets.meta);
            if let Some(old) = &mut self.report_bucket_stat {
                new_report_buckets.merge(old);
                *old = new_report_buckets;
            } else {
                self.report_bucket_stat = Some(new_report_buckets);
            }
        } else {
            self.report_bucket_stat = None;
        }
    }

    #[inline]
    pub fn report_bucket_stat(&mut self) -> BucketStat {
        let current = self.report_bucket_stat.as_mut().unwrap();
        let delta = current.clone();
        current.clear_stats();
        delta
    }

    #[inline]
    pub fn bucket_stat(&self) -> &Option<BucketStat> {
        &self.bucket_stat
    }

    #[inline]
    pub fn bucket_stat_mut(&mut self) -> Option<&mut BucketStat> {
        self.bucket_stat.as_mut()
    }

    pub fn on_refresh_region_buckets(
        &mut self,
        cfg: &Config,
        next_bucket_version: u64,
        buckets: Vec<Bucket>,
        region_epoch: RegionEpoch,
        region: &Region,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) -> bool {
        let change_bucket_version: bool;
        // The region buckets reset after this region happened split or merge.
        // The message should be dropped if it's epoch is lower than the regions.
        // The bucket ranges is none when the region buckets is also none.
        // So this condition indicates that the region buckets needs to refresh not
        // renew.
        if let Some(bucket_ranges) = bucket_ranges&&self.bucket_stat.is_some(){
            assert_eq!(buckets.len(), bucket_ranges.len());
            change_bucket_version=self.update_buckets(cfg, next_bucket_version, buckets, region_epoch,  &bucket_ranges);
        }else{
            change_bucket_version = true;
            // when the region buckets is none, the exclusive buckets includes all the
            // bucket keys.
           self.init_buckets(cfg, next_bucket_version, buckets, region_epoch, region);
        }
        change_bucket_version
    }

    fn update_buckets(
        &mut self,
        cfg: &Config,
        next_bucket_version: u64,
        buckets: Vec<Bucket>,
        region_epoch: RegionEpoch,
        bucket_ranges: &Vec<BucketRange>,
    ) -> bool {
        let origin_region_buckets = self.bucket_stat.as_ref().unwrap();
        let mut change_bucket_version = false;
        let mut meta_idx = 0;
        let mut region_buckets = origin_region_buckets.clone();
        let mut meta = (*region_buckets.meta).clone();
        meta.region_epoch = region_epoch;

        // bucket stats will clean if the bucket size is updated.
        for (bucket, bucket_range) in buckets.into_iter().zip(bucket_ranges) {
            // the bucket ranges maybe need to split or merge not all the meta keys, so it
            // needs to find the first keys.
            while meta_idx < meta.keys.len() && meta.keys[meta_idx] != bucket_range.0 {
                meta_idx += 1;
            }
            // meta_idx can't be not the last entry (which is end key)
            if meta_idx >= meta.keys.len() - 1 {
                break;
            }
            // the bucket size is small and does not have split keys,
            // then it should be merged with its left neighbor
            let region_bucket_merge_size =
                cfg.region_bucket_merge_size_ratio * (cfg.region_bucket_size.0 as f64);
            if bucket.keys.is_empty() && bucket.size <= (region_bucket_merge_size as u64) {
                meta.sizes[meta_idx] = bucket.size;
                region_buckets.clean_stats(meta_idx);
                // the region has more than one bucket
                // and the left neighbor + current bucket size is not very big
                if meta.keys.len() > 2
                    && meta_idx != 0
                    && meta.sizes[meta_idx - 1] + bucket.size < cfg.region_bucket_size.0 * 2
                {
                    // bucket is too small
                    region_buckets.left_merge(meta_idx);
                    meta.left_merge(meta_idx);
                    change_bucket_version = true;
                    continue;
                }
            } else {
                // update size
                meta.sizes[meta_idx] = bucket.size / (bucket.keys.len() + 1) as u64;
                region_buckets.clean_stats(meta_idx);
                // insert new bucket keys (split the original bucket)
                for bucket_key in bucket.keys {
                    meta_idx += 1;
                    region_buckets.split(meta_idx);
                    meta.split(meta_idx, bucket_key);
                    change_bucket_version = true;
                }
            }
            meta_idx += 1;
        }
        if change_bucket_version {
            meta.version = next_bucket_version;
        }
        region_buckets.meta = Arc::new(meta);
        self.set_bucket_stat(Some(region_buckets));
        change_bucket_version
    }

    fn init_buckets(
        &mut self,
        cfg: &Config,
        next_bucket_version: u64,
        mut buckets: Vec<Bucket>,
        region_epoch: RegionEpoch,
        region: &Region,
    ) {
        // when the region buckets is none, the exclusive buckets includes all the
        // bucket keys.
        assert_eq!(buckets.len(), 1);
        let bucket_keys = buckets.pop().unwrap().keys;
        let bucket_count = bucket_keys.len() + 1;
        let mut meta = BucketMeta {
            region_id: region.get_id(),
            region_epoch,
            version: next_bucket_version,
            keys: bucket_keys,
            sizes: vec![cfg.region_bucket_size.0; bucket_count],
        };
        // padding the boundary keys and initialize the flow.
        meta.keys.insert(0, region.get_start_key().to_vec());
        meta.keys.push(region.get_end_key().to_vec());
        let bucket_stats = BucketStat::from_meta(Arc::new(meta));
        self.set_bucket_stat(Some(bucket_stats));
    }
}

pub enum Task {
    SplitCheckTask {
        region: Region,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
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
            start_key: None,
            end_key: None,
            auto_split,
            policy,
            bucket_ranges,
        }
    }

    pub fn split_check_key_range(
        region: Region,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        auto_split: bool,
        policy: CheckPolicy,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) -> Task {
        Task::SplitCheckTask {
            region,
            start_key,
            end_key,
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
                region,
                start_key,
                end_key,
                auto_split,
                ..
            } => write!(
                f,
                "[split check worker] Split Check Task for {}, start_key: {:?}, end_key: {:?}, auto_split: {:?}",
                region.get_id(),
                start_key,
                end_key,
                auto_split
            ),
            Task::ChangeConfig(_) => write!(f, "[split check worker] Change Config Task"),
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(_) => write!(f, "[split check worker] Validate config"),
            Task::ApproximateBuckets(_) => write!(f, "[split check worker] Approximate buckets"),
        }
    }
}

pub struct Runner<EK: KvEngine, S> {
    // We can't just use `TabletRegistry` here, otherwise v1 may create many
    // invalid records and cause other problems.
    engine: Either<EK, TabletRegistry<EK>>,
    router: S,
    coprocessor: CoprocessorHost<EK>,
}

impl<EK: KvEngine, S: StoreHandle> Runner<EK, S> {
    pub fn new(engine: EK, router: S, coprocessor: CoprocessorHost<EK>) -> Runner<EK, S> {
        Runner {
            engine: Either::Left(engine),
            router,
            coprocessor,
        }
    }

    pub fn with_registry(
        registry: TabletRegistry<EK>,
        router: S,
        coprocessor: CoprocessorHost<EK>,
    ) -> Runner<EK, S> {
        Runner {
            engine: Either::Right(registry),
            router,
            coprocessor,
        }
    }

    fn approximate_check_bucket(
        &self,
        tablet: &EK,
        region: &Region,
        host: &mut SplitCheckerHost<'_, EK>,
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
            let bucket_entry = host.approximate_bucket_keys(&bucket, tablet)?;
            debug!(
                "bucket_entry size {} keys count {}, region_id {}",
                bucket_entry.size,
                bucket_entry.keys.len(),
                region.get_id(),
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
        self.router.refresh_region_buckets(
            region.get_id(),
            region.get_region_epoch().clone(),
            buckets,
            bucket_ranges,
        );
    }

    /// Checks a Region with split and bucket checkers to produce split keys and
    /// buckets keys and generates split admin command.
    fn check_split_and_bucket(
        &mut self,
        region: &Region,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        auto_split: bool,
        policy: CheckPolicy,
        bucket_ranges: Option<Vec<BucketRange>>,
    ) {
        let mut cached;
        let tablet = match &self.engine {
            Either::Left(e) => e,
            Either::Right(r) => match r.get(region.get_id()) {
                Some(c) => {
                    cached = Some(c);
                    match cached.as_mut().unwrap().latest() {
                        Some(t) => t,
                        None => return,
                    }
                }
                None => return,
            },
        };
        let region_id = region.get_id();
        let is_key_range = start_key.is_some() && end_key.is_some();
        let start_key = if is_key_range {
            // This key is usually from a request, which should be encoded first.
            keys::data_key(Key::from_raw(&start_key.unwrap()).as_encoded().as_slice())
        } else {
            keys::enc_start_key(region)
        };
        let end_key = if is_key_range {
            keys::data_end_key(Key::from_raw(&end_key.unwrap()).as_encoded().as_slice())
        } else {
            keys::enc_end_key(region)
        };
        debug!(
            "executing task";
            "region_id" => region_id,
            "is_key_range" => is_key_range,
            "start_key" => log_wrappers::Value::key(&start_key),
            "end_key" => log_wrappers::Value::key(&end_key),
            "policy" => ?policy,
        );
        CHECK_SPILT_COUNTER.all.inc();
        let mut host = self
            .coprocessor
            .new_split_checker_host(region, tablet, auto_split, policy);

        if host.skip() {
            debug!("skip split check";
                "region_id" => region.get_id(),
                "is_key_range" => is_key_range,
                "start_key" => log_wrappers::Value::key(&start_key),
                "end_key" => log_wrappers::Value::key(&end_key),
            );
            return;
        }

        let split_keys = match host.policy() {
            CheckPolicy::Scan => {
                match self.scan_split_keys(
                    &mut host,
                    tablet,
                    region,
                    is_key_range,
                    &start_key,
                    &end_key,
                    bucket_ranges,
                ) {
                    Ok(keys) => keys,
                    Err(e) => {
                        error!(%e; "failed to scan split key";
                            "region_id" => region_id,
                            "is_key_range" => is_key_range,
                            "start_key" => log_wrappers::Value::key(&start_key),
                            "end_key" => log_wrappers::Value::key(&end_key),
                        );
                        return;
                    }
                }
            }
            CheckPolicy::Approximate => match host.approximate_split_keys(region, tablet) {
                Ok(keys) => {
                    if host.enable_region_bucket() {
                        if let Err(e) =
                            self.approximate_check_bucket(tablet, region, &mut host, bucket_ranges)
                        {
                            error!(%e;
                                "approximate_check_bucket failed";
                                "region_id" => region_id,
                                "is_key_range" => is_key_range,
                                "start_key" => log_wrappers::Value::key(&start_key),
                                "end_key" => log_wrappers::Value::key(&end_key),
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
                        "is_key_range" => is_key_range,
                        "start_key" => log_wrappers::Value::key(&start_key),
                        "end_key" => log_wrappers::Value::key(&end_key),
                    );
                    match self.scan_split_keys(
                        &mut host,
                        tablet,
                        region,
                        is_key_range,
                        &start_key,
                        &end_key,
                        bucket_ranges,
                    ) {
                        Ok(keys) => keys,
                        Err(e) => {
                            error!(%e; "failed to scan split key";
                                "region_id" => region_id,
                                "is_key_range" => is_key_range,
                                "start_key" => log_wrappers::Value::key(&start_key),
                                "end_key" => log_wrappers::Value::key(&end_key),
                            );
                            return;
                        }
                    }
                }
            },
            CheckPolicy::Usekey => vec![], // Handled by pd worker directly.
        };

        if !split_keys.is_empty() {
            // Notify peer that if the region is truly splitable.
            // If it's truly splitable, then skip_split_check should be false;
            self.router.update_approximate_size(
                region.get_id(),
                None,
                Some(!split_keys.is_empty()),
            );
            self.router.update_approximate_keys(
                region.get_id(),
                None,
                Some(!split_keys.is_empty()),
            );

            let region_epoch = region.get_region_epoch().clone();
            self.router
                .ask_split(region_id, region_epoch, split_keys, "split checker".into());
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
    ///                If none, generate buckets for the whole region.
    ///                If it's Some(vec![]), skip generating buckets.
    fn scan_split_keys(
        &self,
        host: &mut SplitCheckerHost<'_, EK>,
        tablet: &EK,
        region: &Region,
        is_key_range: bool,
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
        let mut split_keys = vec![];

        MergedIterator::<<EK as Iterable>::Iterator>::new(
            tablet, LARGE_CFS, start_key, end_key, false,
        )
        .map(|mut iter| {
            let mut size = 0;
            let mut keys = 0;
            let mut bucket_size: u64 = 0;
            let mut bucket_range_idx = 0;
            let mut skip_on_kv = false;
            while let Some(e) = iter.next() {
                if skip_on_kv && skip_check_bucket {
                    split_keys = host.split_keys();
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
                            // e.key() is between bucket_range_list[bucket_range_idx].0,
                            // bucket_range_list[bucket_range_idx].1
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

            split_keys = host.split_keys();

            // if we scan the whole range, we can update approximate size and keys with
            // accurate value.
            if is_key_range {
                return;
            }
            info!(
                "update approximate size and keys with accurate value";
                "region_id" => region.get_id(),
                "size" => size,
                "keys" => keys,
                "bucket_count" => buckets.len(),
                "bucket_size" => bucket_size,
            );

            self.router.update_approximate_size(
                region.get_id(),
                Some(size),
                Some(!split_keys.is_empty()),
            );
            self.router.update_approximate_keys(
                region.get_id(),
                Some(keys),
                Some(!split_keys.is_empty()),
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

        Ok(split_keys)
    }

    fn change_cfg(&mut self, change: ConfigChange) {
        if let Err(e) = self.coprocessor.cfg.update(change.clone()) {
            error!("update split check config failed"; "err" => ?e);
            return;
        };
        info!(
            "split check config updated";
            "change" => ?change
        );
    }
}

impl<EK, S> Runnable for Runner<EK, S>
where
    EK: KvEngine,
    S: StoreHandle,
{
    type Task = Task;
    fn run(&mut self, task: Task) {
        let _io_type_guard = WithIoType::new(IoType::LoadBalance);
        match task {
            Task::SplitCheckTask {
                region,
                start_key,
                end_key,
                auto_split,
                policy,
                bucket_ranges,
            } => self.check_split_and_bucket(
                &region,
                start_key,
                end_key,
                auto_split,
                policy,
                bucket_ranges,
            ),
            Task::ChangeConfig(c) => self.change_cfg(c),
            Task::ApproximateBuckets(region) => {
                if self.coprocessor.cfg.enable_region_bucket() {
                    let mut cached;
                    let tablet = match &self.engine {
                        Either::Left(e) => e,
                        Either::Right(r) => match r.get(region.get_id()) {
                            Some(c) => {
                                cached = Some(c);
                                match cached.as_mut().unwrap().latest() {
                                    Some(t) => t,
                                    None => return,
                                }
                            }
                            None => return,
                        },
                    };
                    let mut host = self.coprocessor.new_split_checker_host(
                        &region,
                        tablet,
                        false,
                        CheckPolicy::Approximate,
                    );
                    if let Err(e) = self.approximate_check_bucket(tablet, &region, &mut host, None)
                    {
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

#[cfg(test)]
mod tests {
    use super::*;

    // create BucketStatsInfo include three keys: ["","100","200",""].
    fn mock_bucket_stats_info() -> BucketStatsInfo {
        let mut bucket_stats_info = BucketStatsInfo::default();
        let cfg = Config::default();
        let next_bucket_version = 1;
        let bucket_ranges = None;
        let mut region_epoch = RegionEpoch::default();
        region_epoch.set_conf_ver(1);
        region_epoch.set_version(1);
        let mut region = Region::default();
        region.set_id(1);

        let mut buckets = vec![];
        let mut bucket = Bucket::default();
        bucket.keys.push(vec![100]);
        bucket.keys.push(vec![200]);
        buckets.insert(0, bucket);

        let _ = bucket_stats_info.on_refresh_region_buckets(
            &cfg,
            next_bucket_version,
            buckets,
            region_epoch,
            &region,
            bucket_ranges,
        );
        bucket_stats_info
    }

    #[test]
    pub fn test_version() {
        let mut bucket_stats_info = mock_bucket_stats_info();
        assert_eq!(1, bucket_stats_info.version());
        bucket_stats_info.set_bucket_stat(None);
        assert_eq!(1, bucket_stats_info.version());

        let mut meta = BucketMeta::default();
        meta.version = 2;
        meta.keys.push(vec![]);
        meta.keys.push(vec![]);
        let bucket_stat = BucketStat::from_meta(Arc::new(meta));
        bucket_stats_info.set_bucket_stat(Some(bucket_stat));
        assert_eq!(2, bucket_stats_info.version());
    }

    #[test]
    pub fn test_insert_new_buckets() {
        let bucket_stats_info = mock_bucket_stats_info();

        let cfg = Config::default();
        let bucket_stat = bucket_stats_info.bucket_stat.unwrap();
        assert_eq!(
            vec![vec![], vec![100], vec![200], vec![]],
            bucket_stat.meta.keys
        );
        for i in 0..bucket_stat.stats.write_bytes.len() {
            assert_eq!(cfg.region_bucket_size.0, bucket_stat.meta.sizes[i]);
            assert_eq!(0, bucket_stat.stats.write_bytes[i]);
        }
    }

    #[test]
    pub fn test_report_buckets() {
        let mut bucket_stats_info = mock_bucket_stats_info();
        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        let mut delta_bucket_stats = bucket_stats.clone();
        delta_bucket_stats.write_key(&[1], 1);
        delta_bucket_stats.write_key(&[201], 1);
        bucket_stats_info.add_bucket_flow(&Some(delta_bucket_stats.clone()));
        let bucket_stats = bucket_stats_info.report_bucket_stat();
        assert_eq!(vec![2, 0, 2], bucket_stats.stats.write_bytes);

        let report_bucket_stats = bucket_stats_info.report_bucket_stat();
        assert_eq!(vec![0, 0, 0], report_bucket_stats.stats.write_bytes);
        bucket_stats_info.add_bucket_flow(&Some(delta_bucket_stats));
        assert_eq!(vec![2, 0, 2], bucket_stats.stats.write_bytes);
    }

    #[test]
    pub fn test_spilt_and_merge_buckets() {
        let mut bucket_stats_info = mock_bucket_stats_info();
        let next_bucket_version = 2;
        let mut region = Region::default();
        region.set_id(1);
        let cfg = Config::default();
        let bucket_size = cfg.region_bucket_size.0;
        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        let region_epoch = bucket_stats.meta.region_epoch.clone();

        // step1: update buckets flow
        let mut delta_bucket_stats = bucket_stats.clone();
        delta_bucket_stats.write_key(&[1], 1);
        delta_bucket_stats.write_key(&[201], 1);
        bucket_stats_info.add_bucket_flow(&Some(delta_bucket_stats));
        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        assert_eq!(vec![2, 0, 2], bucket_stats.stats.write_bytes);

        // step2: tick not affect anything
        let bucket_ranges = Some(vec![]);
        let buckets = vec![];
        let mut change_bucket_version = bucket_stats_info.on_refresh_region_buckets(
            &cfg,
            next_bucket_version,
            buckets,
            region_epoch.clone(),
            &region,
            bucket_ranges,
        );
        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        assert!(!change_bucket_version);
        assert_eq!(vec![2, 0, 2], bucket_stats.stats.write_bytes);

        // step3: split key 50
        let mut bucket_ranges = Some(vec![BucketRange(vec![], vec![100])]);
        let mut bucket = Bucket::default();
        bucket.keys = vec![vec![50]];
        bucket.size = bucket_size;
        let mut buckets = vec![bucket];
        change_bucket_version = bucket_stats_info.on_refresh_region_buckets(
            &cfg,
            next_bucket_version,
            buckets.clone(),
            region_epoch.clone(),
            &region,
            bucket_ranges.clone(),
        );
        assert!(change_bucket_version);
        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        assert_eq!(
            vec![vec![], vec![50], vec![100], vec![200], vec![]],
            bucket_stats.meta.keys
        );
        assert_eq!(
            vec![bucket_size / 2, bucket_size / 2, bucket_size, bucket_size],
            bucket_stats.meta.sizes
        );
        assert_eq!(vec![0, 0, 0, 2], bucket_stats.stats.write_bytes);

        // step4: merge [50-100] to [0-50],
        bucket_ranges = Some(vec![BucketRange(vec![50], vec![100])]);
        let mut bucket = Bucket::default();
        bucket.keys = vec![];
        bucket.size = 0;
        buckets = vec![bucket];
        change_bucket_version = bucket_stats_info.on_refresh_region_buckets(
            &cfg,
            next_bucket_version,
            buckets,
            region_epoch,
            &region,
            bucket_ranges,
        );
        assert!(change_bucket_version);

        let bucket_stats = bucket_stats_info.bucket_stat().as_ref().unwrap();
        assert_eq!(
            vec![vec![], vec![100], vec![200], vec![]],
            bucket_stats.meta.keys
        );
        assert_eq!(
            vec![bucket_size / 2, bucket_size, bucket_size],
            bucket_stats.meta.sizes
        );
        assert_eq!(vec![0, 0, 2], bucket_stats.stats.write_bytes);

        // report buckets doesn't be affected by the split and merge.
        let report_bucket_stats = bucket_stats_info.report_bucket_stat();
        assert_eq!(vec![4, 0, 2], report_bucket_stats.stats.write_bytes);
    }
}
