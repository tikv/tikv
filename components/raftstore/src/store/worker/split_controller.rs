// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{Ordering, min},
    collections::{BinaryHeap, HashSet},
    slice::{Iter, IterMut},
    sync::{Arc, mpsc::Receiver},
    time::{Duration, SystemTime},
};

use collections::HashMap;
use kvproto::{
    kvrpcpb::KeyRange,
    metapb::{self, Peer},
    pdpb::QueryKind,
};
use pd_client::{BucketMeta, BucketStat, RegionWriteCfCopDetail};
use rand::Rng;
use resource_metering::RawRecords;
use tikv_util::{
    config::Tracker,
    debug, info,
    metrics::ThreadInfoStatistics,
    store::{QueryStats, is_read_query},
    time::Instant,
    warn,
};

use crate::store::{
    metrics::*,
    util::build_key_range,
    worker::{FlowStatistics, SplitConfig, SplitConfigManager, split_config::get_sample_num},
};

const DEFAULT_MAX_SAMPLE_LOOP_COUNT: usize = 10000;
pub const TOP_N: usize = 10;

// It will return prefix sum of the given iter,
// `read` is a function to process the item from the iter.
#[inline(always)]
fn prefix_sum<F, T>(iter: Iter<'_, T>, read: F) -> Vec<usize>
where
    F: Fn(&T) -> usize,
{
    let mut sum = 0;
    iter.map(|item| {
        sum += read(item);
        sum
    })
    .collect()
}

#[inline(always)]
fn prefix_sum_mut<F, T>(iter: IterMut<'_, T>, read: F) -> Vec<usize>
where
    F: Fn(&mut T) -> usize,
{
    let mut sum = 0;
    iter.map(|item| {
        sum += read(item);
        sum
    })
    .collect()
}

// This function uses the distributed/parallel reservoir sampling algorithm.
// It will sample min(sample_num, all_key_ranges_num) key ranges from multiple
// `key_ranges_provider` with the same possibility.
fn sample<F, T>(
    sample_num: usize,
    mut key_ranges_providers: Vec<T>,
    key_ranges_getter: F,
) -> Vec<KeyRange>
where
    F: Fn(&mut T) -> &mut Vec<KeyRange>,
{
    let mut sampled_key_ranges = vec![];
    // Retain the non-empty key ranges.
    // `key_ranges_provider` may return an empty key ranges vector, which will cause
    // the later sampling to fall into a dead loop. So we need to filter it out
    // here.
    key_ranges_providers
        .retain_mut(|key_ranges_provider| !key_ranges_getter(key_ranges_provider).is_empty());
    if key_ranges_providers.is_empty() {
        return sampled_key_ranges;
    }
    let prefix_sum = prefix_sum_mut(key_ranges_providers.iter_mut(), |key_ranges_provider| {
        key_ranges_getter(key_ranges_provider).len()
    });
    // The last sum is the number of all the key ranges.
    let all_key_ranges_num = *prefix_sum.last().unwrap();
    if all_key_ranges_num == 0 {
        return sampled_key_ranges;
    }
    // If the number of key ranges is less than the sample number,
    // we will return them directly without sampling.
    if all_key_ranges_num <= sample_num {
        key_ranges_providers
            .iter_mut()
            .for_each(|key_ranges_provider| {
                sampled_key_ranges.append(key_ranges_getter(key_ranges_provider));
            });
        return sampled_key_ranges;
    }
    // To prevent the sampling from falling into a dead loop.
    let mut sample_loop_count = min(
        sample_num.saturating_mul(100),
        DEFAULT_MAX_SAMPLE_LOOP_COUNT,
    );
    let mut rng = rand::thread_rng();
    // If the number of key ranges is greater than the sample number,
    // we will randomly sample the key ranges.
    while sampled_key_ranges.len() < sample_num && sample_loop_count > 0 {
        sample_loop_count -= 1;
        // Generate a random number in [1, all_key_ranges_num].
        // Starting from 1 is to achieve equal probability.
        // For example, for a `prefix_sum` like [1, 2, 3, 4],
        // if we generate a random number in [0, 4], the probability of choosing the
        // first index is 0.4 rather than 0.25 due to that 0 and 1 will both
        // make `binary_search` get the same result.
        let i = prefix_sum
            .binary_search(&rng.gen_range(1..=all_key_ranges_num))
            .unwrap_or_else(|i| i);
        let key_ranges = key_ranges_getter(&mut key_ranges_providers[i]);
        if !key_ranges.is_empty() {
            let j = rng.gen_range(0..key_ranges.len());
            sampled_key_ranges.push(key_ranges.remove(j)); // Sampling without replacement
        }
    }
    if sample_loop_count == 0 {
        warn!("the number of sampled key ranges could be less than the sample_num, the sampling may fall into a dead loop before";
            "sampled_key_ranges_length" => sampled_key_ranges.len(),
            "sample_num" => sample_num,
        );
    }
    sampled_key_ranges
}

pub struct Sample {
    pub key: Vec<u8>,
    // left means the number of key ranges located on the sample's left.
    pub left: i32,
    // contained means the number of key ranges the sample locates inside.
    pub contained: i32,
    // right means the number of key ranges located on the sample's right.
    pub right: i32,
}

impl Sample {
    fn new(key: &[u8]) -> Sample {
        Sample {
            key: key.to_owned(),
            left: 0,
            contained: 0,
            right: 0,
        }
    }
}

struct Samples(Vec<Sample>);

impl From<Vec<KeyRange>> for Samples {
    fn from(key_ranges: Vec<KeyRange>) -> Self {
        Samples(
            key_ranges
                .iter()
                .fold(HashSet::new(), |mut hash_set, key_range| {
                    hash_set.insert(&key_range.start_key);
                    hash_set.insert(&key_range.end_key);
                    hash_set
                })
                .into_iter()
                .map(|key| Sample::new(key))
                .collect(),
        )
    }
}

impl Samples {
    // evaluate the samples according to the given key range, it will update the
    // sample's left, right and contained counter.
    fn evaluate(&mut self, key_range: &KeyRange) {
        for sample in self.0.iter_mut() {
            let order_start = if key_range.start_key.is_empty() {
                Ordering::Greater
            } else {
                sample.key.cmp(&key_range.start_key)
            };

            let order_end = if key_range.end_key.is_empty() {
                Ordering::Less
            } else {
                sample.key.cmp(&key_range.end_key)
            };

            if order_start == Ordering::Greater && order_end == Ordering::Less {
                sample.contained += 1;
            } else if order_start != Ordering::Greater {
                sample.right += 1;
            } else {
                sample.left += 1;
            }
        }
    }

    // split the keys with the given split config and sampled data.
    fn split_key(&self, split_balance_score: f64, split_contained_score: f64) -> Vec<u8> {
        let mut best_index: i32 = -1;
        let mut best_score = 2.0;
        for (index, sample) in self.0.iter().enumerate() {
            if sample.key.is_empty() {
                continue;
            }
            let evaluated_key_num_lr = sample.left + sample.right;
            if evaluated_key_num_lr == 0 {
                LOAD_BASE_SPLIT_EVENT.no_enough_lr_key.inc();
                continue;
            }
            let evaluated_key_num = (sample.contained + evaluated_key_num_lr) as f64;

            // The balance score is the difference in the number of requested keys between
            // the left and right of a sample key. The smaller the balance
            // score, the more balanced the load will be after this splitting.
            let balance_score =
                (sample.left as f64 - sample.right as f64).abs() / evaluated_key_num_lr as f64;
            LOAD_BASE_SPLIT_SAMPLE_VEC
                .with_label_values(&["balance_score"])
                .observe(balance_score);
            if balance_score >= split_balance_score {
                LOAD_BASE_SPLIT_EVENT.no_balance_key.inc();
                continue;
            }

            // The contained score is the ratio of a sample key that are contained in the
            // requested key. The larger the contained score, the more RPCs the
            // cluster will receive after this splitting.
            let contained_score = sample.contained as f64 / evaluated_key_num;
            LOAD_BASE_SPLIT_SAMPLE_VEC
                .with_label_values(&["contained_score"])
                .observe(contained_score);
            if contained_score >= split_contained_score {
                LOAD_BASE_SPLIT_EVENT.no_uncross_key.inc();
                continue;
            }

            // We try to find a split key that has the smallest balance score and the
            // smallest contained score to make the splitting keep the load
            // balanced while not increasing too many RPCs.
            let final_score = balance_score + contained_score;
            if final_score < best_score {
                best_index = index as i32;
                best_score = final_score;
            }
        }
        if best_index >= 0 {
            return self.0[best_index as usize].key.clone();
        }
        vec![]
    }
}

// Recorder is used to record the potential split-able key ranges,
// sample and split them according to the split config appropriately.
pub struct Recorder {
    pub detect_times: usize,
    pub peer: Peer,
    pub key_ranges: Vec<Vec<KeyRange>>,
    pub create_time: SystemTime,
    pub cpu_usage: f64,
    pub hottest_key_range: Option<KeyRange>,
}

impl Recorder {
    fn new(detect_times: u64) -> Recorder {
        Recorder {
            detect_times: detect_times as usize,
            peer: Peer::default(),
            key_ranges: vec![],
            create_time: SystemTime::now(),
            cpu_usage: 0.0,
            hottest_key_range: None,
        }
    }

    fn record(&mut self, key_ranges: Vec<KeyRange>) {
        self.key_ranges.push(key_ranges);
    }

    fn update_peer(&mut self, peer: &Peer) {
        if self.peer != *peer {
            self.peer = peer.clone();
        }
    }

    fn update_cpu_usage(&mut self, cpu_usage: f64) {
        self.cpu_usage = cpu_usage;
    }

    fn update_hottest_key_range(&mut self, key_range: KeyRange) {
        self.hottest_key_range = Some(key_range);
    }

    fn is_ready(&self) -> bool {
        self.key_ranges.len() >= self.detect_times
    }

    // collect the split keys from the recorded key_ranges.
    // This will start a second-level sampling on the previous sampled key ranges,
    // evaluate the samples according to the given key range, and compute the split
    // keys finally.
    fn collect(&self, config: &SplitConfig) -> Vec<u8> {
        let sampled_key_ranges = sample(config.sample_num, self.key_ranges.clone(), |x| x);
        let mut samples = Samples::from(sampled_key_ranges);
        let recorded_key_ranges: Vec<&KeyRange> = self.key_ranges.iter().flatten().collect();
        // Because we need to observe the number of `no_enough_key` of all the actual
        // keys, so we do this check after the samples are calculated.
        if (recorded_key_ranges.len() as u64) < config.sample_threshold {
            LOAD_BASE_SPLIT_EVENT
                .no_enough_sampled_key
                .inc_by(samples.0.len() as u64);
            return vec![];
        }
        recorded_key_ranges.into_iter().for_each(|key_range| {
            samples.evaluate(key_range);
        });
        samples.split_key(config.split_balance_score, config.split_contained_score)
    }
}

// RegionInfo will maintain key_ranges with sample_num length by reservoir
// sampling. And it will save qps num and peer.
#[derive(Debug, Clone)]
pub struct RegionInfo {
    pub sample_num: usize,
    pub query_stats: QueryStats,
    pub cop_detail: RegionWriteCfCopDetail,
    pub peer: Peer,
    pub key_ranges: Vec<KeyRange>,
    pub flow: FlowStatistics,
}

impl RegionInfo {
    fn new(sample_num: usize) -> RegionInfo {
        RegionInfo {
            sample_num,
            query_stats: QueryStats::default(),
            cop_detail: RegionWriteCfCopDetail::default(),
            key_ranges: Vec::with_capacity(sample_num),
            peer: Peer::default(),
            flow: FlowStatistics::default(),
        }
    }

    fn get_read_qps(&self) -> usize {
        self.query_stats.get_read_query_num() as usize
    }

    fn get_key_ranges_mut(&mut self) -> &mut Vec<KeyRange> {
        &mut self.key_ranges
    }

    fn add_key_ranges(&mut self, key_ranges: Vec<KeyRange>) {
        for (i, key_range) in key_ranges.into_iter().enumerate() {
            let n = self.get_read_qps() + i;
            if n == 0 || self.key_ranges.len() < self.sample_num {
                self.key_ranges.push(key_range);
            } else {
                let j = rand::thread_rng().gen_range(0..n);
                if j < self.sample_num {
                    self.key_ranges[j] = key_range;
                }
            }
        }
    }

    fn add_query_num(&mut self, kind: QueryKind, query_num: u64) {
        self.query_stats.add_query_num(kind, query_num);
    }

    fn update_peer(&mut self, peer: &Peer) {
        if self.peer != *peer {
            self.peer = peer.clone();
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReadStats {
    // RegionID -> RegionInfo
    // There're three methods could insert a `RegionInfo` into the map:
    //   1. add_query_num
    //   2. add_query_num_batch
    //   3. add_flow
    // Among these three methods, `add_flow` will not update `key_ranges` of `RegionInfo`,
    // and due to this, an `RegionInfo` without `key_ranges` may occur. The caller should be aware
    // of this.
    pub region_infos: HashMap<u64, RegionInfo>,
    pub sample_num: usize,
    pub region_buckets: HashMap<u64, BucketStat>,
}

impl ReadStats {
    pub fn with_sample_num(sample_num: usize) -> Self {
        ReadStats {
            region_infos: HashMap::default(),
            region_buckets: HashMap::default(),
            sample_num,
        }
    }

    pub fn add_query_num(
        &mut self,
        region_id: u64,
        peer: &Peer,
        key_range: KeyRange,
        kind: QueryKind,
    ) {
        self.add_query_num_batch(region_id, peer, vec![key_range], kind);
    }

    pub fn add_query_num_batch(
        &mut self,
        region_id: u64,
        peer: &Peer,
        key_ranges: Vec<KeyRange>,
        kind: QueryKind,
    ) {
        let sample_num = self.sample_num;
        let query_num = key_ranges.len() as u64;
        let region_info = self
            .region_infos
            .entry(region_id)
            .or_insert_with(|| RegionInfo::new(sample_num));
        region_info.update_peer(peer);
        if is_read_query(kind) {
            region_info.add_key_ranges(key_ranges);
        }
        region_info.add_query_num(kind, query_num);
    }

    pub fn add_flow(
        &mut self,
        region_id: u64,
        buckets: Option<&Arc<BucketMeta>>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        write: &FlowStatistics,
        data: &FlowStatistics,
        write_cf_cop_detail: &RegionWriteCfCopDetail,
    ) {
        let num = self.sample_num;
        let region_info = self
            .region_infos
            .entry(region_id)
            .or_insert_with(|| RegionInfo::new(num));
        region_info.flow.add(write);
        region_info.flow.add(data);
        region_info.cop_detail.add(write_cf_cop_detail);
        // the bucket of the follower only have the version info and not needs to be
        // recorded the hot bucket.
        if let Some(buckets) = buckets
            && !buckets.sizes.is_empty()
        {
            let bucket_stat = self
                .region_buckets
                .entry(region_id)
                .and_modify(|current| {
                    if current.meta < *buckets {
                        let mut new = BucketStat::from_meta(buckets.clone());
                        std::mem::swap(current, &mut new);
                        current.merge(&new);
                    }
                })
                .or_insert_with(|| BucketStat::from_meta(buckets.clone()));
            let mut delta = metapb::BucketStats::default();
            delta.set_read_bytes(vec![(write.read_bytes + data.read_bytes) as u64]);
            delta.set_read_keys(vec![(write.read_keys + data.read_keys) as u64]);
            bucket_stat.add_flows(
                &[start.unwrap_or_default(), end.unwrap_or_default()],
                &delta,
            );
        }
    }

    pub fn is_empty(&self) -> bool {
        self.region_infos.is_empty()
    }
}

impl Default for ReadStats {
    fn default() -> ReadStats {
        ReadStats {
            sample_num: get_sample_num(),
            region_infos: HashMap::default(),
            region_buckets: HashMap::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct WriteStats {
    pub region_infos: HashMap<u64, QueryStats>,
}

impl WriteStats {
    pub fn add_query_num(&mut self, region_id: u64, kind: QueryKind) {
        let query_stats = self.region_infos.entry(region_id).or_default();
        query_stats.add_query_num(kind, 1);
    }

    pub fn is_empty(&self) -> bool {
        self.region_infos.is_empty()
    }
}

pub struct SplitInfo {
    pub region_id: u64,
    pub peer: Peer,
    pub split_key: Option<Vec<u8>>,
    pub start_key: Option<Vec<u8>>,
    pub end_key: Option<Vec<u8>>,
}

impl SplitInfo {
    // Create a SplitInfo with the given region_id, peer and split_key.
    // This is used to split the region with this specified split key later.
    fn with_split_key(region_id: u64, peer: Peer, split_key: Vec<u8>) -> Self {
        SplitInfo {
            region_id,
            peer,
            split_key: Some(split_key),
            start_key: None,
            end_key: None,
        }
    }

    // Create a SplitInfo with the given region_id, peer, start_key and end_key.
    // This is used to split the region on half within the specified start and end
    // keys later.
    fn with_start_end_key(
        region_id: u64,
        peer: Peer,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Self {
        SplitInfo {
            region_id,
            peer,
            split_key: None,
            start_key: Some(start_key),
            end_key: Some(end_key),
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum SplitConfigChange {
    Noop,
    UpdateRegionCpuCollector(bool),
}

pub struct AutoSplitController {
    // RegionID -> Recorder
    pub recorders: HashMap<u64, Recorder>,
    pub cfg: SplitConfig,
    cfg_tracker: Tracker<SplitConfig>,
    // Thread-related info
    max_grpc_thread_count: usize,
    max_unified_read_pool_thread_count: usize,
    unified_read_pool_scale_receiver: Option<Receiver<usize>>,
    grpc_thread_usage_vec: Vec<f64>,
}

impl AutoSplitController {
    pub fn new(
        config_manager: SplitConfigManager,
        max_grpc_thread_count: usize,
        max_unified_read_pool_thread_count: usize,
        unified_read_pool_scale_receiver: Option<Receiver<usize>>,
    ) -> AutoSplitController {
        AutoSplitController {
            recorders: HashMap::default(),
            cfg: config_manager.value().clone(),
            cfg_tracker: config_manager.0.clone().tracker("split_hub".to_owned()),
            max_grpc_thread_count,
            max_unified_read_pool_thread_count,
            unified_read_pool_scale_receiver,
            grpc_thread_usage_vec: vec![],
        }
    }

    pub fn default() -> AutoSplitController {
        AutoSplitController::new(SplitConfigManager::default(), 0, 0, None)
    }

    fn update_grpc_thread_usage(&mut self, grpc_thread_usage: f64) {
        self.grpc_thread_usage_vec.push(grpc_thread_usage);
        let length = self.grpc_thread_usage_vec.len();
        let detect_times = self.cfg.detect_times as usize;
        // Only keep the last `self.cfg.detect_times` elements.
        if length > detect_times {
            self.grpc_thread_usage_vec.drain(..length - detect_times);
        }
    }

    fn get_avg_grpc_thread_usage(&self) -> f64 {
        let length = self.grpc_thread_usage_vec.len();
        if length == 0 {
            return 0.0;
        }
        let sum = self.grpc_thread_usage_vec.iter().sum::<f64>();
        sum / length as f64
    }

    fn should_check_region_cpu(&self) -> bool {
        self.cfg.region_cpu_overload_threshold_ratio() > 0.0
    }

    fn is_grpc_poll_busy(&self, avg_grpc_thread_usage: f64) -> bool {
        fail::fail_point!("mock_grpc_poll_is_not_busy", |_| { false });
        if self.max_grpc_thread_count == 0 {
            return false;
        }
        if self.cfg.grpc_thread_cpu_overload_threshold_ratio <= 0.0 {
            return true;
        }
        avg_grpc_thread_usage
            >= self.max_grpc_thread_count as f64 * self.cfg.grpc_thread_cpu_overload_threshold_ratio
    }

    fn is_unified_read_pool_busy(&self, unified_read_pool_thread_usage: f64) -> bool {
        fail::fail_point!("mock_unified_read_pool_is_busy", |_| { true });
        if self.max_unified_read_pool_thread_count == 0 {
            return false;
        }
        let unified_read_pool_cpu_overload_threshold = self.max_unified_read_pool_thread_count
            as f64
            * self
                .cfg
                .unified_read_pool_thread_cpu_overload_threshold_ratio;
        unified_read_pool_thread_usage > 0.0
            && unified_read_pool_thread_usage >= unified_read_pool_cpu_overload_threshold
    }

    fn is_region_busy(&self, unified_read_pool_thread_usage: f64, region_cpu_usage: f64) -> bool {
        fail::fail_point!("mock_region_is_busy", |_| { true });
        if unified_read_pool_thread_usage <= 0.0 || !self.should_check_region_cpu() {
            return false;
        }
        region_cpu_usage / unified_read_pool_thread_usage
            >= self.cfg.region_cpu_overload_threshold_ratio()
    }

    // collect the read stats from read_stats_vec and dispatch them to a Region
    // HashMap.
    fn collect_read_stats(
        ctx: &mut AutoSplitControllerContext,
        read_stats_receiver: &Receiver<ReadStats>,
    ) -> HashMap<u64, Vec<RegionInfo>> {
        let read_stats_vec = ctx.batch_recv_read_stats(read_stats_receiver);
        // RegionID -> Vec<RegionInfo>, collect the RegionInfo from different threads.
        let mut region_infos_map = HashMap::default();
        let capacity = read_stats_vec.len();
        for read_stats in read_stats_vec.drain(..) {
            for (region_id, region_info) in read_stats.region_infos {
                let region_infos = region_infos_map
                    .entry(region_id)
                    .or_insert_with(|| Vec::with_capacity(capacity));
                region_infos.push(region_info);
            }
        }
        region_infos_map
    }

    // collect the CPU stats from cpu_stats_vec and dispatch them to a Region
    // HashMap.
    fn collect_cpu_stats<'c>(
        &mut self,
        ctx: &'c mut AutoSplitControllerContext,
        cpu_stats_receiver: &Receiver<Arc<RawRecords>>,
    ) -> &'c HashMap<u64, (f64, Option<KeyRange>)> {
        // RegionID -> (CPU usage, Hottest Key Range), calculate the CPU usage and its
        // hottest key range.
        if !self.should_check_region_cpu() {
            return ctx.empty_region_cpu_map();
        }

        let (
            cpu_stats_vec,
            CpuStatsCache {
                region_cpu_map,
                hottest_key_range_cpu_time_map,
            },
        ) = ctx.batch_recv_cpu_stats(cpu_stats_receiver);
        // Calculate the Region CPU usage.
        let mut collect_interval_ms = 0;
        let mut region_key_range_cpu_time_map = HashMap::default();
        cpu_stats_vec.iter().for_each(|cpu_stats| {
            cpu_stats.records.iter().for_each(|(tag, record)| {
                // Calculate the Region ID -> CPU Time.
                region_cpu_map
                    .entry(tag.region_id)
                    .and_modify(|(cpu_time, _)| *cpu_time += record.cpu_time as f64)
                    .or_insert_with(|| (record.cpu_time as f64, None));
                // Calculate the (Region ID, Key Range) -> CPU Time.
                tag.key_ranges.iter().for_each(|key_range| {
                    region_key_range_cpu_time_map
                        .entry((tag.region_id, key_range))
                        .and_modify(|cpu_time| *cpu_time += record.cpu_time)
                        .or_insert_with(|| record.cpu_time);
                })
            });
            collect_interval_ms += cpu_stats.duration.as_millis();
        });
        // Calculate the Region CPU usage.
        region_cpu_map.iter_mut().for_each(|(_, (cpu_time, _))| {
            if collect_interval_ms == 0 {
                *cpu_time = 0.0;
            } else {
                *cpu_time /= collect_interval_ms as f64;
            }
        });
        // Choose the hottest key range for each Region.
        region_key_range_cpu_time_map
            .iter()
            .for_each(|((region_id, key_range), cpu_time)| {
                let hottest_key_range_cpu_time = hottest_key_range_cpu_time_map
                    .entry(*region_id)
                    .or_insert_with(|| 0);
                if cpu_time > hottest_key_range_cpu_time {
                    region_cpu_map
                        .entry(*region_id)
                        .and_modify(|(_, old_key_range)| {
                            *old_key_range =
                                Some(build_key_range(&key_range.0, &key_range.1, false));
                        });
                    *hottest_key_range_cpu_time = *cpu_time;
                }
            });
        region_cpu_map
    }

    fn collect_thread_usage(thread_stats: &ThreadInfoStatistics, name: &str) -> f64 {
        thread_stats
            .get_cpu_usages()
            .iter()
            .filter(|(thread_name, _)| thread_name.contains(name))
            .fold(0, |cpu_usage_sum, (_, cpu_usage)| {
                // `cpu_usage` is in [0, 100].
                cpu_usage_sum + cpu_usage
            }) as f64
            / 100.0
    }

    // flush the read stats info into the recorder and check if the region needs to
    // be split according to all the stats info the recorder has collected before.
    pub fn flush(
        &mut self,
        ctx: &mut AutoSplitControllerContext,
        read_stats_receiver: &Receiver<ReadStats>,
        cpu_stats_receiver: &Receiver<Arc<RawRecords>>,
        thread_stats: &mut ThreadInfoStatistics,
    ) -> (Vec<usize>, Vec<SplitInfo>) {
        let mut top_cpu_usage = vec![];
        let mut top_qps = BinaryHeap::with_capacity(TOP_N);
        let region_infos_map = Self::collect_read_stats(ctx, read_stats_receiver);
        let region_cpu_map = self.collect_cpu_stats(ctx, cpu_stats_receiver);
        // Prepare some diagnostic info.
        thread_stats.record();
        let (grpc_thread_usage, unified_read_pool_thread_usage) = (
            Self::collect_thread_usage(thread_stats, "grpc-server"),
            Self::collect_thread_usage(thread_stats, "unified-read-po"),
        );
        // Update first before calculating the latest average gRPC poll CPU usage.
        self.update_grpc_thread_usage(grpc_thread_usage);
        let avg_grpc_thread_usage = self.get_avg_grpc_thread_usage();
        let (is_grpc_poll_busy, is_unified_read_pool_busy) = (
            self.is_grpc_poll_busy(avg_grpc_thread_usage),
            self.is_unified_read_pool_busy(unified_read_pool_thread_usage),
        );
        debug!("flush to load base split";
            "max_grpc_thread_count" => self.max_grpc_thread_count,
            "grpc_thread_usage" => grpc_thread_usage,
            "avg_grpc_thread_usage" => avg_grpc_thread_usage,
            "max_unified_read_pool_thread_count" => self.max_unified_read_pool_thread_count,
            "unified_read_pool_thread_usage" => unified_read_pool_thread_usage,
            "is_grpc_poll_busy" => is_grpc_poll_busy,
            "is_unified_read_pool_busy" => is_unified_read_pool_busy,
        );

        // Start to record the read stats info.
        let mut split_infos = vec![];
        for (region_id, region_infos) in region_infos_map {
            let qps_prefix_sum = prefix_sum(region_infos.iter(), RegionInfo::get_read_qps);
            // region_infos is not empty, so it's safe to unwrap here.
            let qps = *qps_prefix_sum.last().unwrap();
            let byte = region_infos
                .iter()
                .fold(0, |flow, region_info| flow + region_info.flow.read_bytes);
            let (cpu_usage, hottest_key_range) = region_cpu_map
                .get(&region_id)
                .map(|(cpu_usage, key_range)| (*cpu_usage, key_range.clone()))
                .unwrap_or((0.0, None));
            let is_region_busy = self.is_region_busy(unified_read_pool_thread_usage, cpu_usage);
            debug!("load base split params";
                "region_id" => region_id,
                "qps" => qps,
                "qps_threshold" => self.cfg.qps_threshold(),
                "byte" => byte,
                "byte_threshold" => self.cfg.byte_threshold(),
                "cpu_usage" => cpu_usage,
                "is_region_busy" => is_region_busy,
            );

            QUERY_REGION_VEC
                .with_label_values(&["read"])
                .observe(qps as f64);

            // 1. If the QPS or the byte does not meet the threshold, skip.
            // 2. If the Unified Read Pool or the region is not hot enough, skip.
            if qps < self.cfg.qps_threshold()
                && byte < self.cfg.byte_threshold()
                && (!is_unified_read_pool_busy || !is_region_busy)
            {
                self.recorders.remove_entry(&region_id);
                continue;
            }

            LOAD_BASE_SPLIT_EVENT.load_fit.inc();

            let detect_times = self.cfg.detect_times;
            let recorder = self
                .recorders
                .entry(region_id)
                .or_insert_with(|| Recorder::new(detect_times));
            recorder.update_peer(&region_infos[0].peer);
            recorder.update_cpu_usage(cpu_usage);
            if let Some(hottest_key_range) = hottest_key_range {
                recorder.update_hottest_key_range(hottest_key_range);
            }

            let key_ranges = sample(
                self.cfg.sample_num,
                region_infos,
                RegionInfo::get_key_ranges_mut,
            );
            if key_ranges.is_empty() {
                LOAD_BASE_SPLIT_EVENT.empty_statistical_key.inc();
                continue;
            }
            recorder.record(key_ranges);
            if recorder.is_ready() {
                let key = recorder.collect(&self.cfg);
                if !key.is_empty() {
                    info!("load base split region";
                        "region_id" => region_id,
                        "qps" => qps,
                        "byte" => byte,
                        "cpu_usage" => cpu_usage,
                        "split_key" => log_wrappers::Value::key(&key),
                        "start_key" => log_wrappers::Value::key(&recorder.hottest_key_range.as_ref().unwrap_or_default().start_key),
                        "end_key" => log_wrappers::Value::key(&recorder.hottest_key_range.as_ref().unwrap_or_default().end_key),
                    );
                    split_infos.push(SplitInfo::with_split_key(
                        region_id,
                        recorder.peer.clone(),
                        key,
                    ));
                    LOAD_BASE_SPLIT_EVENT.ready_to_split.inc();
                    self.recorders.remove(&region_id);
                } else if is_unified_read_pool_busy && is_region_busy {
                    LOAD_BASE_SPLIT_EVENT.cpu_load_fit.inc();
                    top_cpu_usage.push(region_id);
                }
            } else {
                LOAD_BASE_SPLIT_EVENT.not_ready_to_split.inc();
            }

            top_qps.push(qps);
        }

        // Check if the top CPU usage region could be split.
        // TODO: avoid unnecessary split by introducing the feedback mechanism from PD.
        if !top_cpu_usage.is_empty() {
            // Only split the top CPU region when the gRPC poll is not busy.
            if !is_grpc_poll_busy {
                // Calculate by using the latest CPU usage.
                top_cpu_usage.sort_unstable_by(|a, b| {
                    let cpu_usage_a = self.recorders.get(a).unwrap().cpu_usage;
                    let cpu_usage_b = self.recorders.get(b).unwrap().cpu_usage;
                    cpu_usage_b.partial_cmp(&cpu_usage_a).unwrap()
                });
                let region_id = top_cpu_usage[0];
                let recorder = self.recorders.get_mut(&region_id).unwrap();
                if recorder.hottest_key_range.is_some() {
                    split_infos.push(SplitInfo::with_start_end_key(
                        region_id,
                        recorder.peer.clone(),
                        recorder
                            .hottest_key_range
                            .as_ref()
                            .unwrap()
                            .start_key
                            .clone(),
                        recorder.hottest_key_range.as_ref().unwrap().end_key.clone(),
                    ));
                    LOAD_BASE_SPLIT_EVENT.ready_to_split_cpu_top.inc();
                    info!("load base split region";
                        "region_id" => region_id,
                        "start_key" => log_wrappers::Value::key(&recorder.hottest_key_range.as_ref().unwrap().start_key),
                        "end_key" => log_wrappers::Value::key(&recorder.hottest_key_range.as_ref().unwrap().end_key),
                        "cpu_usage" => recorder.cpu_usage,
                    );
                } else {
                    LOAD_BASE_SPLIT_EVENT.empty_hottest_key_range.inc();
                }
            } else {
                LOAD_BASE_SPLIT_EVENT.unable_to_split_cpu_top.inc();
            }
            // Clean up the rest top CPU usage recorders.
            for region_id in top_cpu_usage {
                self.recorders.remove(&region_id);
            }
        }

        (top_qps.into_vec(), split_infos)
    }

    pub fn clear(&mut self) {
        let interval = Duration::from_secs(self.cfg.detect_times * 2);
        self.recorders
            .retain(|_, recorder| match recorder.create_time.elapsed() {
                Ok(life_time) => life_time < interval,
                Err(_) => true,
            });
    }

    pub fn refresh_and_check_cfg(&mut self) -> SplitConfigChange {
        let mut cfg_change = SplitConfigChange::Noop;
        if let Some(incoming) = self.cfg_tracker.any_new() {
            if self.cfg.region_cpu_overload_threshold_ratio() <= 0.0
                && incoming.region_cpu_overload_threshold_ratio() > 0.0
            {
                cfg_change = SplitConfigChange::UpdateRegionCpuCollector(true);
            }
            if self.cfg.region_cpu_overload_threshold_ratio() > 0.0
                && incoming.region_cpu_overload_threshold_ratio() <= 0.0
            {
                cfg_change = SplitConfigChange::UpdateRegionCpuCollector(false);
            }
            self.cfg = incoming.clone();
        }
        // Adjust with the size change of the Unified Read Pool.
        if let Some(rx) = &self.unified_read_pool_scale_receiver {
            if let Ok(max_thread_count) = rx.try_recv() {
                self.max_unified_read_pool_thread_count = max_thread_count;
            }
        }
        cfg_change
    }
}

#[derive(Default)]
pub struct CpuStatsCache {
    region_cpu_map: HashMap<u64, (f64, Option<KeyRange>)>,
    hottest_key_range_cpu_time_map: HashMap<u64, u32>,
}

pub struct AutoSplitControllerContext {
    read_stats_vec: Vec<ReadStats>,
    cpu_stats_vec: Vec<Arc<RawRecords>>,
    cpu_stats_cache: CpuStatsCache,
    batch_recv_len: usize,

    last_gc_time: Instant,
    gc_duration: Duration,
}

impl AutoSplitControllerContext {
    pub fn new(batch_recv_len: usize) -> Self {
        AutoSplitControllerContext {
            read_stats_vec: Vec::default(),
            cpu_stats_vec: Vec::default(),
            cpu_stats_cache: CpuStatsCache::default(),
            batch_recv_len,
            last_gc_time: Instant::now_coarse(),
            // 30 seconds is a balance between efficient memory usage and
            // maintaining performance under load.
            gc_duration: Duration::from_secs(30),
        }
    }

    pub fn batch_recv_read_stats(
        &mut self,
        read_stats_receiver: &Receiver<ReadStats>,
    ) -> &mut Vec<ReadStats> {
        self.read_stats_vec.clear();

        while let Ok(read_stats) = read_stats_receiver.try_recv() {
            self.read_stats_vec.push(read_stats);
            if self.read_stats_vec.len() == self.batch_recv_len {
                break;
            }
        }
        &mut self.read_stats_vec
    }

    pub fn batch_recv_cpu_stats(
        &mut self,
        cpu_stats_receiver: &Receiver<Arc<RawRecords>>,
    ) -> (&mut Vec<Arc<RawRecords>>, &mut CpuStatsCache) {
        self.cpu_stats_vec.clear();
        self.cpu_stats_cache.region_cpu_map.clear();
        self.cpu_stats_cache.hottest_key_range_cpu_time_map.clear();

        while let Ok(cpu_stats) = cpu_stats_receiver.try_recv() {
            self.cpu_stats_vec.push(cpu_stats);
            if self.cpu_stats_vec.len() == self.batch_recv_len {
                break;
            }
        }
        (&mut self.cpu_stats_vec, &mut self.cpu_stats_cache)
    }

    pub fn empty_region_cpu_map(&mut self) -> &HashMap<u64, (f64, Option<KeyRange>)> {
        self.cpu_stats_cache.region_cpu_map.clear();
        &self.cpu_stats_cache.region_cpu_map
    }

    pub fn maybe_gc(&mut self) {
        let now = Instant::now_coarse();
        if now.saturating_duration_since(self.last_gc_time) > self.gc_duration {
            self.read_stats_vec = Vec::default();
            self.cpu_stats_vec = Vec::default();
            self.cpu_stats_cache = CpuStatsCache::default();

            self.last_gc_time = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc::{self, TryRecvError};

    use online_config::{ConfigChange, ConfigManager, ConfigValue};
    use resource_metering::{RawRecord, TagInfos};
    use tikv_util::config::{ReadableSize, VersionTrack};
    use txn_types::Key;

    use super::*;
    use crate::store::worker::split_config::{
        BIG_REGION_CPU_OVERLOAD_THRESHOLD_RATIO, DEFAULT_SAMPLE_NUM,
    };

    enum Position {
        Left,
        Right,
        Contained,
    }

    impl Sample {
        fn num(&self, pos: Position) -> i32 {
            match pos {
                Position::Left => self.left,
                Position::Right => self.right,
                Position::Contained => self.contained,
            }
        }
    }

    struct SampleCase {
        key: Vec<u8>,
    }

    impl SampleCase {
        fn sample_key(&self, start_key: &[u8], end_key: &[u8], pos: Position) {
            let mut samples = Samples(vec![Sample::new(&self.key)]);
            let key_range = build_key_range(start_key, end_key, false);
            samples.evaluate(&key_range);
            assert_eq!(
                samples.0[0].num(pos),
                1,
                "start_key is {:?}, end_key is {:?}",
                String::from_utf8(Vec::from(start_key)).unwrap(),
                String::from_utf8(Vec::from(end_key)).unwrap()
            );
        }
    }

    #[test]
    fn test_prefix_sum() {
        let v = [1, 2, 3, 4, 5, 6, 7, 8, 9];
        let expect = [1, 3, 6, 10, 15, 21, 28, 36, 45];
        let pre = prefix_sum(v.iter(), |x| *x);
        for i in 0..v.len() {
            assert_eq!(expect[i], pre[i]);
        }
    }

    #[test]
    fn test_sample() {
        let sc = SampleCase { key: vec![b'c'] };

        // limit scan
        sc.sample_key(b"a", b"b", Position::Left);
        sc.sample_key(b"a", b"c", Position::Left);
        sc.sample_key(b"a", b"d", Position::Contained);
        sc.sample_key(b"c", b"d", Position::Right);
        sc.sample_key(b"d", b"e", Position::Right);

        // point get
        sc.sample_key(b"a", b"a", Position::Left);
        sc.sample_key(b"c", b"c", Position::Right); // when happened 100 times (a,a) and 100 times (c,c), we will split from c.
        sc.sample_key(b"d", b"d", Position::Right);

        // unlimited scan
        sc.sample_key(b"", b"", Position::Contained);
        sc.sample_key(b"a", b"", Position::Contained);
        sc.sample_key(b"c", b"", Position::Right);
        sc.sample_key(b"d", b"", Position::Right);
        sc.sample_key(b"", b"a", Position::Left);
        sc.sample_key(b"", b"c", Position::Left);
        sc.sample_key(b"", b"d", Position::Contained);
    }

    fn gen_read_stats(region_id: u64, key_ranges: Vec<KeyRange>) -> ReadStats {
        let mut qps_stats = ReadStats::default();
        for key_range in &key_ranges {
            qps_stats.add_query_num(
                region_id,
                &Peer::default(),
                key_range.clone(),
                QueryKind::Get,
            );
        }
        qps_stats
    }

    #[test]
    fn test_recorder() {
        let mut config = SplitConfig::default();
        config.detect_times = 10;
        config.sample_threshold = 20;

        let mut recorder = Recorder::new(config.detect_times);
        for _ in 0..config.detect_times {
            assert!(!recorder.is_ready());
            recorder.record(vec![
                build_key_range(b"a", b"b", false),
                build_key_range(b"b", b"c", false),
            ]);
        }
        assert!(recorder.is_ready());
        let key = recorder.collect(&config);
        assert_eq!(key, b"b");
    }

    #[test]
    fn test_hub() {
        // raw key mode
        let raw_key_ranges = vec![
            build_key_range(b"a", b"b", false),
            build_key_range(b"b", b"c", false),
        ];
        check_split_key(
            b"raw key",
            vec![gen_read_stats(1, raw_key_ranges.clone())],
            vec![b"b"],
        );

        // encoded key mode
        let key_a = Key::from_raw(b"0080").append_ts(2.into());
        let key_b = Key::from_raw(b"0160").append_ts(2.into());
        let key_c = Key::from_raw(b"0240").append_ts(2.into());
        let encoded_key_ranges = vec![
            build_key_range(key_a.as_encoded(), key_b.as_encoded(), false),
            build_key_range(key_b.as_encoded(), key_c.as_encoded(), false),
        ];
        check_split_key(
            b"encoded key",
            vec![gen_read_stats(1, encoded_key_ranges.clone())],
            vec![key_b.as_encoded()],
        );

        // mix mode
        check_split_key(
            b"mix key",
            vec![
                gen_read_stats(1, raw_key_ranges),
                gen_read_stats(2, encoded_key_ranges),
            ],
            vec![b"b", key_b.as_encoded()],
        );

        // test distribution with contained key
        for _ in 0..100 {
            let key_ranges = vec![
                build_key_range(b"a", b"k", false),
                build_key_range(b"b", b"j", false),
                build_key_range(b"c", b"i", false),
                build_key_range(b"d", b"h", false),
                build_key_range(b"e", b"g", false),
                build_key_range(b"f", b"f", false),
            ];
            check_split_key(
                b"isosceles triangle",
                vec![gen_read_stats(1, key_ranges)],
                vec![],
            );

            let key_ranges = vec![
                build_key_range(b"a", b"f", false),
                build_key_range(b"b", b"g", false),
                build_key_range(b"c", b"h", false),
                build_key_range(b"d", b"i", false),
                build_key_range(b"e", b"j", false),
                build_key_range(b"f", b"k", false),
            ];
            check_split_key(
                b"parallelogram",
                vec![gen_read_stats(1, key_ranges)],
                vec![],
            );

            let key_ranges = vec![
                build_key_range(b"a", b"l", false),
                build_key_range(b"a", b"m", false),
            ];
            check_split_key(
                b"right-angle trapezoid",
                vec![gen_read_stats(1, key_ranges)],
                vec![],
            );

            let key_ranges = vec![
                build_key_range(b"a", b"l", false),
                build_key_range(b"b", b"l", false),
            ];
            check_split_key(
                b"right-angle trapezoid",
                vec![gen_read_stats(1, key_ranges)],
                vec![],
            );
        }

        // test high CPU usage
        fail::cfg("mock_grpc_poll_is_not_busy", "return(0)").unwrap();
        fail::cfg("mock_unified_read_pool_is_busy", "return(0)").unwrap();
        fail::cfg("mock_region_is_busy", "return(0)").unwrap();
        for _ in 0..100 {
            let key_ranges = vec![
                build_key_range(b"a", b"l", false),
                build_key_range(b"a", b"m", false),
            ];
            check_split_key_range(
                b"right-angle trapezoid with high CPU usage",
                vec![gen_read_stats(1, key_ranges.clone())],
                vec![gen_cpu_stats(1, key_ranges.clone(), vec![100, 200])],
                b"a",
                b"m",
            );
            check_split_key_range(
                b"right-angle trapezoid with high CPU usage",
                vec![gen_read_stats(1, key_ranges.clone())],
                vec![gen_cpu_stats(1, key_ranges, vec![200, 100])],
                b"a",
                b"l",
            );

            let key_ranges = vec![
                build_key_range(b"a", b"l", false),
                build_key_range(b"b", b"l", false),
            ];
            check_split_key_range(
                b"right-angle trapezoid with high CPU usage",
                vec![gen_read_stats(1, key_ranges.clone())],
                vec![gen_cpu_stats(1, key_ranges.clone(), vec![100, 200])],
                b"b",
                b"l",
            );
            check_split_key_range(
                b"right-angle trapezoid with high CPU usage",
                vec![gen_read_stats(1, key_ranges.clone())],
                vec![gen_cpu_stats(1, key_ranges, vec![200, 100])],
                b"a",
                b"l",
            );
        }
        fail::remove("mock_grpc_poll_is_not_busy");
        fail::remove("mock_unified_read_pool_is_busy");
        fail::remove("mock_region_is_busy");
    }

    fn new_auto_split_controller_ctx(
        read_stats: Vec<ReadStats>,
        cpu_stats: Vec<Arc<RawRecords>>,
    ) -> (
        AutoSplitControllerContext,
        Receiver<ReadStats>,
        Receiver<Arc<RawRecords>>,
    ) {
        let len = std::cmp::max(read_stats.len(), cpu_stats.len());
        let (read_stats_sender, read_stats_receiver) = mpsc::sync_channel(len);
        let (cpu_stats_sender, cpu_stats_receiver) = mpsc::sync_channel(len);
        for s in cpu_stats {
            cpu_stats_sender.try_send(s).unwrap();
        }
        for s in read_stats {
            read_stats_sender.try_send(s).unwrap();
        }
        (
            AutoSplitControllerContext::new(len),
            read_stats_receiver,
            cpu_stats_receiver,
        )
    }

    fn check_split_key(mode: &[u8], qps_stats: Vec<ReadStats>, split_keys: Vec<&[u8]>) {
        let mode = String::from_utf8(Vec::from(mode)).unwrap();
        let mut hub = AutoSplitController::default();
        hub.cfg.qps_threshold = Some(1);
        hub.cfg.sample_threshold = 0;

        for i in 0..10 {
            let (mut ctx, read_stats_receiver, cpu_stats_receiver) =
                new_auto_split_controller_ctx(qps_stats.clone(), vec![]);
            let (_, split_infos) = hub.flush(
                &mut ctx,
                &read_stats_receiver,
                &cpu_stats_receiver,
                &mut ThreadInfoStatistics::default(),
            );
            if (i + 1) % hub.cfg.detect_times != 0 {
                continue;
            }
            // Check the split key.
            assert_eq!(split_infos.len(), split_keys.len(), "mode: {:?}", mode);
            for obtain in &split_infos {
                let mut equal = false;
                for expect in &split_keys {
                    if obtain.split_key.as_ref().unwrap().cmp(&expect.to_vec()) == Ordering::Equal {
                        equal = true;
                        break;
                    }
                }
                assert!(equal, "mode: {:?}", mode);
            }
        }
    }

    fn check_split_key_range(
        mode: &[u8],
        qps_stats: Vec<ReadStats>,
        cpu_stats: Vec<Arc<RawRecords>>,
        start_key: &[u8],
        end_key: &[u8],
    ) {
        let mode = String::from_utf8(Vec::from(mode)).unwrap();
        let mut hub = AutoSplitController::default();
        hub.cfg.qps_threshold = Some(1);
        hub.cfg.sample_threshold = 0;

        for i in 0..10 {
            let (mut ctx, read_stats_receiver, cpu_stats_receiver) =
                new_auto_split_controller_ctx(qps_stats.clone(), cpu_stats.clone());
            let (_, split_infos) = hub.flush(
                &mut ctx,
                &read_stats_receiver,
                &cpu_stats_receiver,
                &mut ThreadInfoStatistics::default(),
            );
            if (i + 1) % hub.cfg.detect_times != 0 {
                continue;
            }
            assert_eq!(split_infos.len(), 1, "mode: {:?}", mode);
            // Check the split key range.
            let split_info = &split_infos[0];
            assert!(split_info.split_key.is_none(), "mode: {:?}", mode);
            assert_eq!(
                split_info
                    .start_key
                    .as_ref()
                    .unwrap()
                    .cmp(&start_key.to_vec()),
                Ordering::Equal,
                "mode: {:?}",
                mode
            );
            assert_eq!(
                split_info.end_key.as_ref().unwrap().cmp(&end_key.to_vec()),
                Ordering::Equal,
                "mode: {:?}",
                mode
            );
        }
    }

    fn gen_cpu_stats(
        region_id: u64,
        key_ranges: Vec<KeyRange>,
        cpu_times: Vec<u32>,
    ) -> Arc<RawRecords> {
        let mut raw_records = RawRecords::default();
        raw_records.duration = Duration::from_millis(100);
        for (idx, key_range) in key_ranges.iter().enumerate() {
            let key_range_tag = Arc::new(TagInfos {
                store_id: 0,
                region_id,
                peer_id: 0,
                key_ranges: vec![(key_range.start_key.clone(), key_range.end_key.clone())],
                extra_attachment: Arc::new(vec![]),
            });
            raw_records.records.insert(
                key_range_tag.clone(),
                RawRecord {
                    cpu_time: cpu_times[idx],
                    read_keys: 0,
                    write_keys: 0,
                    network_in_bytes: 0,
                    network_out_bytes: 0,
                    logical_read_bytes: 0,
                    logical_write_bytes: 0,
                },
            );
        }
        Arc::new(raw_records)
    }

    #[test]
    fn test_sample_key_num() {
        let mut hub = AutoSplitController::default();
        hub.cfg.qps_threshold = Some(2000);
        hub.cfg.sample_num = 2000;
        hub.cfg.sample_threshold = 0;

        for _ in 0..100 {
            // qps_stats_vec contains 2000 qps and a readStats with a key range;
            let mut qps_stats_vec = vec![];

            let mut qps_stats = ReadStats::with_sample_num(hub.cfg.sample_num);
            qps_stats.add_query_num(
                1,
                &Peer::default(),
                build_key_range(b"a", b"b", false),
                QueryKind::Get,
            );
            qps_stats_vec.push(qps_stats);

            let mut qps_stats = ReadStats::with_sample_num(hub.cfg.sample_num);
            for _ in 0..2000 {
                qps_stats.add_query_num(
                    1,
                    &Peer::default(),
                    build_key_range(b"b", b"c", false),
                    QueryKind::Get,
                );
            }
            qps_stats_vec.push(qps_stats);

            let (mut ctx, read_stats_receiver, cpu_stats_receiver) =
                new_auto_split_controller_ctx(qps_stats_vec.clone(), vec![]);
            hub.flush(
                &mut ctx,
                &read_stats_receiver,
                &cpu_stats_receiver,
                &mut ThreadInfoStatistics::default(),
            );
        }

        // Test the empty key ranges.
        let mut qps_stats_vec = vec![];
        let mut qps_stats = ReadStats::with_sample_num(hub.cfg.sample_num);
        qps_stats.add_query_num(1, &Peer::default(), KeyRange::default(), QueryKind::Get);
        qps_stats_vec.push(qps_stats);
        let mut qps_stats = ReadStats::with_sample_num(hub.cfg.sample_num);
        for _ in 0..2000 {
            qps_stats.add_query_num(1, &Peer::default(), KeyRange::default(), QueryKind::Get);
        }
        qps_stats_vec.push(qps_stats);

        let (mut ctx, read_stats_receiver, cpu_stats_receiver) =
            new_auto_split_controller_ctx(qps_stats_vec, vec![]);
        hub.flush(
            &mut ctx,
            &read_stats_receiver,
            &cpu_stats_receiver,
            &mut ThreadInfoStatistics::default(),
        );
    }

    fn check_sample_length(key_ranges: Vec<Vec<KeyRange>>) {
        for sample_num in 0..=DEFAULT_SAMPLE_NUM {
            for _ in 0..100 {
                let sampled_key_ranges = sample(sample_num, key_ranges.clone(), |x| x);
                let all_key_ranges_num = *prefix_sum(key_ranges.iter(), Vec::len).last().unwrap();
                assert_eq!(
                    sampled_key_ranges.len(),
                    std::cmp::min(sample_num, all_key_ranges_num)
                );
            }
        }
    }

    #[test]
    fn test_sample_length() {
        // Test the sample_num = key range number.
        let mut key_ranges = vec![];
        for _ in 0..DEFAULT_SAMPLE_NUM {
            key_ranges.push(vec![build_key_range(b"a", b"b", false)]);
        }
        check_sample_length(key_ranges);

        // Test the sample_num < key range number.
        let mut key_ranges = vec![];
        for _ in 0..DEFAULT_SAMPLE_NUM + 1 {
            key_ranges.push(vec![build_key_range(b"a", b"b", false)]);
        }
        check_sample_length(key_ranges);

        let mut key_ranges = vec![];
        let num = 100;
        for _ in 0..num {
            let mut ranges = vec![];
            for _ in 0..num {
                ranges.push(build_key_range(b"a", b"b", false));
            }
            key_ranges.push(ranges);
        }
        check_sample_length(key_ranges);

        // Test the sample_num > key range number.
        check_sample_length(vec![vec![build_key_range(b"a", b"b", false)]]);

        let mut key_ranges = vec![];
        for _ in 0..DEFAULT_SAMPLE_NUM - 1 {
            key_ranges.push(vec![build_key_range(b"a", b"b", false)]);
        }
        check_sample_length(key_ranges);

        // Test the empty key range gap.
        // See https://github.com/tikv/tikv/issues/12185 for more details.
        let test_cases = vec![
            // Case 1: small gap.
            vec![
                vec![],
                vec![build_key_range(b"a", b"b", false)],
                vec![build_key_range(b"a", b"b", false)],
                vec![build_key_range(b"a", b"b", false)],
            ],
            vec![
                vec![build_key_range(b"a", b"b", false)],
                vec![],
                vec![build_key_range(b"a", b"b", false)],
                vec![build_key_range(b"a", b"b", false)],
            ],
            vec![
                vec![build_key_range(b"a", b"b", false)],
                vec![build_key_range(b"a", b"b", false)],
                vec![],
                vec![build_key_range(b"a", b"b", false)],
            ],
            vec![
                vec![build_key_range(b"a", b"b", false)],
                vec![build_key_range(b"a", b"b", false)],
                vec![build_key_range(b"a", b"b", false)],
                vec![],
            ],
            // Case 2: big gap.
            vec![
                vec![],
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                    build_key_range(b"i", b"j", false),
                ],
                vec![build_key_range(b"a", b"b", false)],
                vec![build_key_range(b"c", b"d", false)],
            ],
            vec![
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                    build_key_range(b"i", b"j", false),
                ],
                vec![],
                vec![build_key_range(b"a", b"b", false)],
                vec![build_key_range(b"c", b"d", false)],
            ],
            vec![
                vec![build_key_range(b"a", b"b", false)],
                vec![],
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                    build_key_range(b"i", b"j", false),
                ],
                vec![build_key_range(b"c", b"d", false)],
            ],
            vec![
                vec![build_key_range(b"a", b"b", false)],
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                    build_key_range(b"i", b"j", false),
                ],
                vec![],
                vec![build_key_range(b"c", b"d", false)],
            ],
            vec![
                vec![build_key_range(b"c", b"d", false)],
                vec![build_key_range(b"a", b"b", false)],
                vec![],
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                    build_key_range(b"i", b"j", false),
                ],
            ],
            vec![
                vec![build_key_range(b"c", b"d", false)],
                vec![build_key_range(b"a", b"b", false)],
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                    build_key_range(b"i", b"j", false),
                ],
                vec![],
            ],
            // Case 3: multiple gaps.
            vec![
                vec![],
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                ],
                vec![],
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"i", b"j", false),
                ],
                vec![],
                vec![
                    build_key_range(b"g", b"h", false),
                    build_key_range(b"i", b"j", false),
                ],
                vec![],
            ],
            vec![
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                ],
                vec![],
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                    build_key_range(b"i", b"j", false),
                ],
                vec![],
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                ],
            ],
            // Case 4: all empty.
            vec![vec![], vec![], vec![], vec![]],
            // Case 5: multiple one-length gaps.
            vec![
                vec![
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                    build_key_range(b"e", b"f", false),
                    build_key_range(b"g", b"h", false),
                    build_key_range(b"e", b"f", false),
                ],
                vec![build_key_range(b"e", b"f", false)],
                vec![],
                vec![build_key_range(b"e", b"f", false)],
                vec![],
            ],
        ];
        for test_case in test_cases.iter() {
            check_sample_length(test_case.clone());
        }
    }

    #[test]
    fn test_sample_length_randomly() {
        let mut rng = rand::thread_rng();
        let mut test_case = vec![vec![]; DEFAULT_SAMPLE_NUM / 2];
        for _ in 0..100 {
            for key_ranges in test_case.iter_mut() {
                key_ranges.clear();
                // Make the empty range more likely to appear.
                if rng.gen_range(0..=1) == 0 {
                    continue;
                }
                for _ in 0..rng.gen_range(1..=5) as usize {
                    key_ranges.push(build_key_range(b"a", b"b", false));
                }
            }
            check_sample_length(test_case.clone());
        }
    }

    #[test]
    fn test_samples_from_key_ranges() {
        let key_ranges = vec![];
        assert_eq!(Samples::from(key_ranges).0.len(), 0);

        let key_ranges = vec![build_key_range(b"a", b"b", false)];
        assert_eq!(Samples::from(key_ranges).0.len(), 2);

        let key_ranges = vec![
            build_key_range(b"a", b"a", false),
            build_key_range(b"b", b"c", false),
        ];
        assert_eq!(Samples::from(key_ranges).0.len(), 3);
    }

    fn build_key_ranges(start_key: &[u8], end_key: &[u8], num: usize) -> Vec<KeyRange> {
        let mut key_ranges = vec![];
        for _ in 0..num {
            key_ranges.push(build_key_range(start_key, end_key, false));
        }
        key_ranges
    }

    #[test]
    fn test_add_query() {
        let region_id = 1;
        let mut r = ReadStats::default();
        let key_ranges = build_key_ranges(b"a", b"a", r.sample_num);
        r.add_query_num_batch(region_id, &Peer::default(), key_ranges, QueryKind::Get);
        let key_ranges = build_key_ranges(b"b", b"b", r.sample_num * 1000);
        r.add_query_num_batch(region_id, &Peer::default(), key_ranges, QueryKind::Get);
        let samples = &r.region_infos.get(&region_id).unwrap().key_ranges;
        let num = samples
            .iter()
            .filter(|key_range| key_range.start_key == b"b")
            .count();
        assert!(num >= r.sample_num - 1);
    }

    const REGION_NUM: u64 = 1000;
    const KEY_RANGE_NUM: u64 = 1000;

    fn default_qps_stats() -> ReadStats {
        let mut qps_stats = ReadStats::default();
        for i in 0..REGION_NUM {
            for _j in 0..KEY_RANGE_NUM {
                qps_stats.add_query_num(
                    i,
                    &Peer::default(),
                    build_key_range(b"a", b"b", false),
                    QueryKind::Get,
                )
            }
        }
        qps_stats
    }

    #[test]
    fn test_refresh_and_check_cfg() {
        let mut split_config = SplitConfig::default();
        split_config.optimize_for(ReadableSize::mb(5000));
        let mut split_cfg_manager =
            SplitConfigManager::new(Arc::new(VersionTrack::new(split_config)));
        let mut auto_split_controller =
            AutoSplitController::new(split_cfg_manager.clone(), 0, 0, None);
        assert_eq!(
            auto_split_controller.refresh_and_check_cfg(),
            SplitConfigChange::Noop,
        );
        assert_eq!(
            auto_split_controller
                .cfg
                .region_cpu_overload_threshold_ratio(),
            BIG_REGION_CPU_OVERLOAD_THRESHOLD_RATIO
        );
        // Set to zero.
        dispatch_split_cfg_change(
            &mut split_cfg_manager,
            "region_cpu_overload_threshold_ratio",
            ConfigValue::F64(0.0),
        );
        assert_eq!(
            auto_split_controller.refresh_and_check_cfg(),
            SplitConfigChange::UpdateRegionCpuCollector(false),
        );
        assert_eq!(
            auto_split_controller
                .cfg
                .region_cpu_overload_threshold_ratio(),
            0.0
        );
        assert_eq!(
            auto_split_controller.refresh_and_check_cfg(),
            SplitConfigChange::Noop,
        );
        // Set to non-zero.
        dispatch_split_cfg_change(
            &mut split_cfg_manager,
            "region_cpu_overload_threshold_ratio",
            ConfigValue::F64(0.1),
        );
        assert_eq!(
            auto_split_controller.refresh_and_check_cfg(),
            SplitConfigChange::UpdateRegionCpuCollector(true),
        );
        assert_eq!(
            auto_split_controller
                .cfg
                .region_cpu_overload_threshold_ratio(),
            0.1
        );
        assert_eq!(
            auto_split_controller.refresh_and_check_cfg(),
            SplitConfigChange::Noop,
        );
    }

    fn dispatch_split_cfg_change(
        split_cfg_manager: &mut SplitConfigManager,
        cfg_name: &str,
        cfg_value: ConfigValue,
    ) {
        let mut config_change = ConfigChange::new();
        config_change.insert(String::from(cfg_name), cfg_value);
        split_cfg_manager.dispatch(config_change).unwrap();
    }

    #[test]
    fn test_collect_cpu_stats() {
        let mut auto_split_controller = AutoSplitController::default();

        let (mut ctx, _, cpu_stats_receiver) = new_auto_split_controller_ctx(vec![], vec![]);
        let region_cpu_map = auto_split_controller.collect_cpu_stats(&mut ctx, &cpu_stats_receiver);
        assert!(region_cpu_map.is_empty());

        let ab_key_range_tag = Arc::new(TagInfos {
            store_id: 0,
            region_id: 1,
            peer_id: 0,
            key_ranges: vec![(b"a".to_vec(), b"b".to_vec())],
            extra_attachment: Arc::new(vec![]),
        });
        let cd_key_range_tag = Arc::new(TagInfos {
            store_id: 0,
            region_id: 1,
            peer_id: 0,
            key_ranges: vec![(b"c".to_vec(), b"d".to_vec())],
            extra_attachment: Arc::new(vec![]),
        });
        let multiple_key_ranges_tag = Arc::new(TagInfos {
            store_id: 0,
            region_id: 1,
            peer_id: 0,
            key_ranges: vec![
                (b"a".to_vec(), b"b".to_vec()),
                (b"c".to_vec(), b"d".to_vec()),
            ],
            extra_attachment: Arc::new(vec![]),
        });
        let empty_key_range_tag = Arc::new(TagInfos {
            store_id: 0,
            region_id: 1,
            peer_id: 0,
            key_ranges: vec![],
            extra_attachment: Arc::new(vec![]),
        });

        let test_cases = vec![
            (300, 150, 50, 50, Some(build_key_range(b"a", b"b", false))),
            (150, 300, 50, 50, Some(build_key_range(b"c", b"d", false))),
            (150, 50, 300, 50, Some(build_key_range(b"a", b"b", false))),
            (50, 150, 300, 50, Some(build_key_range(b"c", b"d", false))),
            (150, 50, 50, 300, Some(build_key_range(b"a", b"b", false))),
            (100, 0, 0, 0, Some(build_key_range(b"a", b"b", false))),
            (50, 0, 0, 50, Some(build_key_range(b"a", b"b", false))),
            (50, 0, 0, 100, Some(build_key_range(b"a", b"b", false))),
            (50, 0, 50, 0, Some(build_key_range(b"a", b"b", false))),
            (0, 50, 50, 0, Some(build_key_range(b"c", b"d", false))),
            (0, 0, 0, 100, None),
            (0, 0, 0, 0, None),
        ];
        for (i, test_case) in test_cases.iter().enumerate() {
            let mut raw_records = RawRecords::default();
            raw_records.duration = Duration::from_millis(100);
            // ["a", "b"] with (test_case.0)ms CPU time.
            raw_records.records.insert(
                ab_key_range_tag.clone(),
                RawRecord {
                    cpu_time: test_case.0,
                    read_keys: 0,
                    write_keys: 0,
                    network_in_bytes: 0,
                    network_out_bytes: 0,
                    logical_read_bytes: 0,
                    logical_write_bytes: 0,
                },
            );
            // ["c", "d"] with (test_case.1)ms CPU time.
            raw_records.records.insert(
                cd_key_range_tag.clone(),
                RawRecord {
                    cpu_time: test_case.1,
                    read_keys: 0,
                    write_keys: 0,
                    network_in_bytes: 0,
                    network_out_bytes: 0,
                    logical_read_bytes: 0,
                    logical_write_bytes: 0,
                },
            );
            // Multiple key ranges with (test_case.2)ms CPU time.
            raw_records.records.insert(
                multiple_key_ranges_tag.clone(),
                RawRecord {
                    cpu_time: test_case.2,
                    read_keys: 0,
                    write_keys: 0,
                    network_in_bytes: 0,
                    network_out_bytes: 0,
                    logical_read_bytes: 0,
                    logical_write_bytes: 0,
                },
            );
            // Empty key range with (test_case.3)ms CPU time.
            raw_records.records.insert(
                empty_key_range_tag.clone(),
                RawRecord {
                    cpu_time: test_case.3,
                    read_keys: 0,
                    write_keys: 0,
                    network_in_bytes: 0,
                    network_out_bytes: 0,
                    logical_read_bytes: 0,
                    logical_write_bytes: 0,
                },
            );

            let (mut ctx, _, cpu_stats_receiver) =
                new_auto_split_controller_ctx(vec![], vec![Arc::new(raw_records)]);
            let region_cpu_map =
                auto_split_controller.collect_cpu_stats(&mut ctx, &cpu_stats_receiver);
            assert_eq!(
                region_cpu_map.len(),
                1,
                "test_collect_cpu_stats case: {}",
                i
            );
            assert_eq!(
                region_cpu_map.get(&1).unwrap().0,
                (test_case.0 + test_case.1 + test_case.2 + test_case.3) as f64 / 100.0,
                "test_collect_cpu_stats case: {}",
                i
            );
            assert_eq!(
                region_cpu_map.get(&1).unwrap().1,
                test_case.4,
                "test_collect_cpu_stats case: {}",
                i
            );
        }
    }

    #[test]
    fn test_avg_grpc_thread_cpu_usage_calculation() {
        let mut auto_split_controller = AutoSplitController::default();
        let detect_times = auto_split_controller.cfg.detect_times as f64;
        for grpc_thread_usage in 1..=5 {
            auto_split_controller.update_grpc_thread_usage(grpc_thread_usage as f64);
        }
        assert_eq!(
            auto_split_controller.get_avg_grpc_thread_usage(),
            [1.0, 2.0, 3.0, 4.0, 5.0].iter().sum::<f64>() / 5.0,
        );
        for grpc_thread_usage in 6..=10 {
            auto_split_controller.update_grpc_thread_usage(grpc_thread_usage as f64);
        }
        assert_eq!(
            auto_split_controller.get_avg_grpc_thread_usage(),
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
                .iter()
                .sum::<f64>()
                / detect_times,
        );
        for grpc_thread_usage in 11..=15 {
            auto_split_controller.update_grpc_thread_usage(grpc_thread_usage as f64);
        }
        assert_eq!(
            auto_split_controller.get_avg_grpc_thread_usage(),
            [6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
                .iter()
                .sum::<f64>()
                / detect_times,
        );
        for grpc_thread_usage in 1..=10 {
            auto_split_controller.update_grpc_thread_usage(grpc_thread_usage as f64);
        }
        assert_eq!(
            auto_split_controller.get_avg_grpc_thread_usage(),
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
                .iter()
                .sum::<f64>()
                / detect_times,
        );
        // Change the `detect_times` to a smaller value.
        auto_split_controller.cfg.detect_times = 5;
        let detect_times = auto_split_controller.cfg.detect_times as f64;
        auto_split_controller.update_grpc_thread_usage(11.0);
        assert_eq!(
            auto_split_controller.get_avg_grpc_thread_usage(),
            [7.0, 8.0, 9.0, 10.0, 11.0].iter().sum::<f64>() / detect_times,
        );
        // Change the `detect_times` to a bigger value.
        auto_split_controller.cfg.detect_times = 6;
        let detect_times = auto_split_controller.cfg.detect_times as f64;
        auto_split_controller.update_grpc_thread_usage(12.0);
        assert_eq!(
            auto_split_controller.get_avg_grpc_thread_usage(),
            [7.0, 8.0, 9.0, 10.0, 11.0, 12.0].iter().sum::<f64>() / detect_times,
        );
        auto_split_controller.update_grpc_thread_usage(13.0);
        assert_eq!(
            auto_split_controller.get_avg_grpc_thread_usage(),
            [8.0, 9.0, 10.0, 11.0, 12.0, 13.0].iter().sum::<f64>() / detect_times,
        );
    }

    #[bench]
    fn samples_evaluate(b: &mut test::Bencher) {
        let mut samples = Samples(vec![Sample::new(b"c")]);
        let key_range = build_key_range(b"a", b"b", false);
        b.iter(|| {
            samples.evaluate(&key_range);
        });
    }

    #[bench]
    fn hub_flush(b: &mut test::Bencher) {
        let mut other_qps_stats = vec![];
        for _i in 0..10 {
            other_qps_stats.push(default_qps_stats());
        }
        let (read_stats_sender, read_stats_receiver) = mpsc::sync_channel(other_qps_stats.len());
        let (_, cpu_stats_receiver) = mpsc::sync_channel(other_qps_stats.len());
        let mut ctx = AutoSplitControllerContext::new(other_qps_stats.len());
        let mut threads = ThreadInfoStatistics::default();

        b.iter(|| {
            let mut hub = AutoSplitController::default();
            for s in other_qps_stats.clone() {
                read_stats_sender.send(s).unwrap();
            }
            hub.flush(
                &mut ctx,
                &read_stats_receiver,
                &cpu_stats_receiver,
                &mut threads,
            );
        });
    }

    #[bench]
    fn qps_scan(b: &mut test::Bencher) {
        let mut qps_stats = default_qps_stats();
        let start_key = Key::from_raw(b"a");
        let end_key = Some(Key::from_raw(b"b"));

        b.iter(|| {
            if let Ok(start_key) = start_key.to_owned().into_raw() {
                let mut key = vec![];
                if let Some(end_key) = &end_key {
                    if let Ok(end_key) = end_key.to_owned().into_raw() {
                        key = end_key;
                    }
                }
                qps_stats.add_query_num(
                    1,
                    &Peer::default(),
                    build_key_range(&start_key, &key, false),
                    QueryKind::Scan,
                );
            }
        });
    }

    #[bench]
    fn qps_add(b: &mut test::Bencher) {
        let mut qps_stats = default_qps_stats();
        b.iter(|| {
            qps_stats.add_query_num(
                1,
                &Peer::default(),
                build_key_range(b"a", b"b", false),
                QueryKind::Get,
            );
        });
    }

    #[test]
    fn test_auto_split_controller_ctx_batch_recv() {
        let batch_limit = 3;
        let mut ctx = AutoSplitControllerContext::new(batch_limit);
        for len in [0, 2, 3, 5, 6] {
            let (read_stats_sender, read_stats_receiver) = mpsc::sync_channel(len);
            let (cpu_stats_sender, cpu_stats_receiver) = mpsc::sync_channel(len);

            let read_stats = ReadStats::default();
            let cpu_stats = Arc::new(RawRecords::default());
            for _ in 0..len {
                read_stats_sender.try_send(read_stats.clone()).unwrap();
                cpu_stats_sender.try_send(cpu_stats.clone()).unwrap();
            }
            // If channel is full, should return error.
            assert!(read_stats_sender.try_send(read_stats.clone()).is_err());
            assert!(cpu_stats_sender.try_send(cpu_stats.clone()).is_err());
            loop {
                let batch = ctx.batch_recv_read_stats(&read_stats_receiver);
                if batch.is_empty() {
                    break;
                }
                assert!(
                    batch.len() == batch_limit || batch.len() == len % batch_limit,
                    "{:?}",
                    (len, batch.len())
                );
            }
            assert_eq!(
                read_stats_receiver.try_recv().unwrap_err(),
                TryRecvError::Empty
            );

            loop {
                let (batch, cache) = ctx.batch_recv_cpu_stats(&cpu_stats_receiver);
                if batch.is_empty() {
                    break;
                }
                assert!(
                    batch.len() == batch_limit || batch.len() == len % batch_limit,
                    "{:?}",
                    (len, batch.len())
                );
                assert!(cache.region_cpu_map.is_empty());
                assert!(cache.hottest_key_range_cpu_time_map.is_empty());
                // The cache should be empty after the batch_recv_cpu_stats.
                cache.region_cpu_map.insert(1, (0.0, None));
                cache.hottest_key_range_cpu_time_map.insert(1, 1);
            }
            assert_eq!(
                read_stats_receiver.try_recv().unwrap_err(),
                TryRecvError::Empty
            );
        }
    }

    #[test]
    fn test_auto_split_controller_empty_region_cpu_map() {
        let mut ctx = AutoSplitControllerContext::new(1);
        ctx.cpu_stats_cache.region_cpu_map.insert(1, (0.0, None));
        assert!(ctx.empty_region_cpu_map().is_empty());
    }

    #[test]
    fn test_auto_split_controller_empty_gc() {
        let mut ctx = AutoSplitControllerContext::new(1);
        ctx.cpu_stats_cache.region_cpu_map.insert(1, (0.0, None));
        ctx.cpu_stats_cache
            .hottest_key_range_cpu_time_map
            .insert(1, 1);
        ctx.cpu_stats_vec.push(Arc::new(RawRecords::default()));
        ctx.read_stats_vec.push(ReadStats::default());

        ctx.last_gc_time = Instant::now_coarse();
        ctx.maybe_gc();

        assert!(!ctx.cpu_stats_cache.region_cpu_map.is_empty());
        assert!(
            !ctx.cpu_stats_cache
                .hottest_key_range_cpu_time_map
                .is_empty()
        );
        assert!(!ctx.cpu_stats_vec.is_empty());
        assert!(!ctx.read_stats_vec.is_empty());

        ctx.last_gc_time = Instant::now_coarse() - 2 * ctx.gc_duration;
        ctx.maybe_gc();

        assert!(ctx.cpu_stats_cache.region_cpu_map.is_empty());
        assert!(
            ctx.cpu_stats_cache
                .hottest_key_range_cpu_time_map
                .is_empty()
        );
        assert!(ctx.cpu_stats_vec.is_empty());
        assert!(ctx.read_stats_vec.is_empty());
    }
}
