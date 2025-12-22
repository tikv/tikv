// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    sync::mpsc,
    thread::{self, Builder as ThreadBuilder, JoinHandle},
    time::{Duration, Instant},
    vec::Vec,
};

use engine_traits::{
    CF_DEFAULT, CF_WRITE, KvEngine, ManualCompactionOptions, Range, TableProperties,
    TablePropertiesCollection, UserCollectedProperties,
};
use keys::{enc_end_key, enc_start_key};
use kvproto::metapb::Region;
use prometheus::*;
use prometheus_static_metric::*;
use raftstore::coprocessor::RegionInfoProvider;
use tikv_util::{box_err, debug, error, info, sys::thread::StdThreadBuildWrapper, warn};
use txn_types::TimeStamp;

use super::{
    Error, Result,
    config::{GcConfig, GcWorkerConfigManager},
    gc_worker::GcSafePointProvider,
};

make_static_metric! {
    pub label_enum AutoCompactionDurationType {
        initial_evaluation,
        re_evaluation,
        compact,
    }

    pub struct AutoCompactionDurationHistogramVec: Histogram {
        "type" => AutoCompactionDurationType,
    }


}

lazy_static::lazy_static! {
    pub static ref AUTO_COMPACTION_DURATION_HISTOGRAM_VEC: AutoCompactionDurationHistogramVec = register_static_histogram_vec!(
        AutoCompactionDurationHistogramVec,
        "tikv_auto_compaction_duration_seconds",
        "Time spent on auto compaction operations",
        &["type"],
        exponential_buckets(0.0001, 2.0, 26).unwrap()
    ).unwrap();

    pub static ref AUTO_COMPACTION_REGIONS_MEET_THRESHOLD_GAUGE: IntGauge = register_int_gauge!(
        "tikv_auto_compaction_regions_meet_threshold",
        "Number of regions that meet the compaction threshold"
    ).unwrap();

    pub static ref AUTO_COMPACTION_PENDING_CANDIDATES_GAUGE: IntGauge = register_int_gauge!(
        "tikv_auto_compaction_pending_candidates",
        "Number of pending compaction candidates"
    ).unwrap();

}

/// A candidate for compaction with its priority score
#[derive(Debug, Clone)]
pub struct CompactionCandidate {
    pub score: f64,
    pub num_tombstones: u64,  // RocksDB tombstones
    pub num_discardable: u64, // Estimated discardable TiKV MVCC versions
    pub num_total_entries: u64,
    pub num_rows: u64, // TiKV rows
    pub region: Region,
}

impl PartialEq for CompactionCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for CompactionCandidate {}

impl PartialOrd for CompactionCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl Ord for CompactionCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Handle for managing compaction runner
pub struct CompactionRunnerHandle {
    join_handle: JoinHandle<()>,
    stop_signal_sender: mpsc::Sender<()>,
}

impl CompactionRunnerHandle {
    pub fn stop(self) -> Result<()> {
        let res: Result<()> = self.stop_signal_sender.send(()).map_err(|e| {
            box_err!(
                "failed to send stop signal to compaction runner thread: {:?}",
                e
            )
        });
        res?;
        self.join_handle
            .join()
            .map_err(|e| box_err!("failed to join compaction runner thread: {:?}", e))
    }
}

/// Runs automatic compaction on TiKV
/// Runs in a dedicated thread and continuously evaluates compaction candidates
pub struct CompactionRunner<S: GcSafePointProvider, R: RegionInfoProvider, E: KvEngine> {
    safe_point_provider: S,
    region_info_provider: R,
    engine: E,
    stop_signal_receiver: Option<mpsc::Receiver<()>>,
    is_stopped: bool,
    cfg_tracker: GcWorkerConfigManager,
}

impl<S: GcSafePointProvider, R: RegionInfoProvider + 'static, E: KvEngine>
    CompactionRunner<S, R, E>
{
    pub fn new(
        safe_point_provider: S,
        region_info_provider: R,
        engine: E,
        cfg_tracker: GcWorkerConfigManager,
    ) -> Self {
        Self {
            safe_point_provider,
            region_info_provider,
            engine,
            stop_signal_receiver: None,
            is_stopped: false,
            cfg_tracker,
        }
    }

    fn curr_safe_point(&self) -> TimeStamp {
        self.safe_point_provider
            .get_safe_point()
            .unwrap_or(TimeStamp::zero())
    }

    /// Starts the compaction runner in a separate thread
    pub fn start(mut self) -> Result<CompactionRunnerHandle> {
        fail_point!("gc_worker_auto_compaction_thread_start");
        let (tx, rx) = mpsc::channel();
        self.stop_signal_receiver = Some(rx);

        let props = tikv_util::thread_group::current_properties();
        let res: Result<_> = ThreadBuilder::new()
            .name(tikv_util::thd_name!("compaction-runner"))
            .spawn_wrapper(move || {
                tikv_util::thread_group::set_properties(props);
                self.run();
            })
            .map_err(|e| box_err!("failed to start compaction runner: {:?}", e));

        res.map(|join_handle| CompactionRunnerHandle {
            join_handle,
            stop_signal_sender: tx,
        })
    }

    /// Main loop for the compaction runner
    fn run(&mut self) {
        info!("compaction-runner started");
        fail_point!("gc_worker_auto_compaction_start");
        loop {
            if self.check_stopped() {
                break;
            }

            // Get consistent config snapshot for this run
            let config = self.cfg_tracker.value().clone();
            let check_interval = config.auto_compaction.check_interval.0;

            // Get current safe point
            let gc_safe_point = self.curr_safe_point().into_inner();

            // Skip compaction if safe point is zero (GC not initialized)
            if gc_safe_point == 0 {
                info!("skipping compaction: GC safe point is zero");
                if self.sleep_or_stop(check_interval) {
                    break;
                }
                continue;
            }

            // Collect and rank compaction candidates
            let candidates = match self.collect_compaction_candidates(gc_safe_point, &config) {
                Ok(candidates) => {
                    // Add failpoints to check specific candidates (using compact_top_n pattern)
                    fail_point!(
                        "gc_worker_auto_compaction_candidate_k05_k10",
                        candidates
                            .iter()
                            .any(|c| c.num_total_entries == 10 && c.num_discardable == 5),
                        |_| {}
                    );
                    fail_point!(
                        "gc_worker_auto_compaction_candidate_k10_k15",
                        candidates
                            .iter()
                            .any(|c| c.num_total_entries == 15 && c.num_discardable == 2),
                        |_| {}
                    );
                    fail_point!(
                        "gc_worker_auto_compaction_candidate_k15_k20",
                        candidates
                            .iter()
                            .any(|c| c.num_total_entries == 20 && c.num_discardable == 7),
                        |_| {}
                    );
                    fail_point!(
                        "gc_worker_auto_compaction_candidate_k20_k35",
                        candidates
                            .iter()
                            .any(|c| c.num_total_entries == 30 && c.num_discardable == 10),
                        |_| {}
                    );
                    candidates
                }
                Err(e) => {
                    error!("failed to collect compaction candidates: {:?}", e);
                    if self.sleep_or_stop(check_interval) {
                        break;
                    }
                    continue;
                }
            };

            if candidates.is_empty() {
                info!("no compaction candidates found, sleeping");
                if self.sleep_or_stop(check_interval) {
                    break;
                }
                continue;
            }

            // Compact the candidates
            let elapsed = match self.compact_candidates(candidates, &config) {
                Some(elapsed) => elapsed,
                None => {
                    break;
                }
            };

            // Sleep for remaining time in check interval, or start next round immediately
            if elapsed < check_interval {
                let remaining_sleep = check_interval - elapsed;
                if self.sleep_or_stop(remaining_sleep) {
                    break;
                }
            }
        }
        debug!("compaction-runner stopped");
    }

    /// Collects all compaction candidates from all regions
    fn collect_compaction_candidates(
        &self,
        gc_safe_point: u64,
        config: &GcConfig,
    ) -> Result<Vec<CompactionCandidate>> {
        // Calculate heap capacity based on check duration (assuming 1 sec per
        // compaction)
        let check_duration_secs = config.auto_compaction.check_interval.as_secs() as usize;
        let heap_capacity = check_duration_secs.max(10); // At least 10 candidates

        debug!(
            "collecting compaction candidates with heap capacity: {}",
            heap_capacity
        );

        // Use a min-heap to keep top candidates (using Reverse for min-heap behavior)
        // We use a heap to retain only the top candidates, which helps limit memory
        // usage in case we need to track compaction scores for all regions.
        // This module assumes that each compaction takes at least 1 second, ensuring
        // full utilization of the time window between check intervals.
        let mut candidates_heap: BinaryHeap<Reverse<CompactionCandidate>> =
            BinaryHeap::with_capacity(heap_capacity);
        let mut current_key = b"".to_vec();
        let mut regions_meeting_threshold = 0;

        while let Some(region) = self.get_next_region_context(&current_key)? {
            // Evaluate this region as a compaction candidate
            let evaluation_start = Instant::now();
            match self.evaluate_range_candidate(&region, gc_safe_point, config) {
                Ok(Some(candidate)) => {
                    regions_meeting_threshold += 1;
                    if candidates_heap.len() < heap_capacity {
                        // Heap not full, add candidate
                        candidates_heap.push(Reverse(candidate));
                    } else if let Some(top) = candidates_heap.peek() {
                        // Heap is full, check if new candidate has higher score than the lowest
                        if candidate.score > top.0.score {
                            candidates_heap.pop(); // Remove lowest score
                            candidates_heap.push(Reverse(candidate)); // Add new candidate
                        }
                    }
                }
                Ok(None) => {} // No compaction needed
                Err(e) => {
                    warn!(
                        "failed to evaluate region {} as compaction candidate: {:?}",
                        region.get_id(),
                        e
                    );
                }
            }

            // Record initial evaluation duration
            AUTO_COMPACTION_DURATION_HISTOGRAM_VEC
                .initial_evaluation
                .observe(evaluation_start.elapsed().as_secs_f64());

            if region.get_end_key().is_empty() {
                // Reached the end of regions, stop seeking
                break;
            } else {
                current_key = region.get_end_key().to_vec();
            }
        }

        // Convert heap to sorted vector (highest score first)
        let mut candidates: Vec<CompactionCandidate> = candidates_heap
            .into_sorted_vec()
            .into_iter()
            .map(|reverse| reverse.0)
            .collect();
        candidates.reverse(); // Min-heap gives us lowest first, we want highest first

        // Log details for top 10 candidates
        for (rank, candidate) in candidates.iter().take(10).enumerate() {
            info!("top compaction candidate";
                "rank" => rank + 1,
                "region_id" => candidate.region.get_id(),
                "score" => candidate.score,
                "total_entries" => candidate.num_total_entries,
                "tikv_estimated_discardable" => candidate.num_discardable,
                "rocksdb_tombstones" => candidate.num_tombstones,
                "tikv_rows" => candidate.num_rows,
            );
        }

        // Update gauge metrics
        AUTO_COMPACTION_REGIONS_MEET_THRESHOLD_GAUGE.set(regions_meeting_threshold as i64);
        AUTO_COMPACTION_PENDING_CANDIDATES_GAUGE.set(candidates.len() as i64);

        info!("collected {} compaction candidates", candidates.len());
        fail_point!("gc_worker_auto_compaction_candidates_collected");
        Ok(candidates)
    }

    /// Compact candidates and return elapsed time
    fn compact_candidates(
        &mut self,
        candidates: Vec<CompactionCandidate>,
        config: &GcConfig,
    ) -> Option<Duration> {
        let start_time = Instant::now();
        let mut processed_count = 0;
        let total_candidates = candidates.len();
        let check_interval = config.auto_compaction.check_interval.0;
        fail_point!("gc_worker_auto_compaction_start_compacting");

        for (index, candidate) in candidates.into_iter().enumerate() {
            if self.check_stopped() {
                return None; // Stopped
            }

            // Check if we've exceeded the check interval, return to start next round
            let elapsed = start_time.elapsed();
            if elapsed >= check_interval {
                debug!("check interval exceeded, returning to start next round");
                return Some(elapsed);
            }

            // Update pending candidates gauge (remaining candidates)
            AUTO_COMPACTION_PENDING_CANDIDATES_GAUGE.set((total_candidates - index) as i64);

            // Get current safe point for this candidate (might have advanced)
            let current_gc_safe_point = self.curr_safe_point().into_inner();

            // Recheck candidate before compacting in case it's been resolved
            let re_evaluation_start = Instant::now();
            let current_candidate = match self.evaluate_range_candidate(
                &candidate.region,
                current_gc_safe_point,
                config,
            ) {
                Ok(Some(updated_candidate)) => updated_candidate,
                Ok(None) => {
                    info!(
                        "candidate region {} no longer needs compaction, skipping",
                        candidate.region.get_id()
                    );
                    continue;
                }
                Err(e) => {
                    warn!(
                        "failed to recheck candidate region {}: {:?}, proceeding with original",
                        candidate.region.get_id(),
                        e
                    );
                    candidate
                }
            };

            // Record re-evaluation duration
            AUTO_COMPACTION_DURATION_HISTOGRAM_VEC
                .re_evaluation
                .observe(re_evaluation_start.elapsed().as_secs_f64());

            // Compact this candidate
            let compact_start = Instant::now();
            if let Err(e) = self.compact_candidate(&current_candidate, config) {
                error!("failed to compact candidate: {:?}", e);
                continue;
            }
            let compact_duration = compact_start.elapsed();
            AUTO_COMPACTION_DURATION_HISTOGRAM_VEC
                .compact
                .observe(compact_duration.as_secs_f64());

            processed_count += 1;
            info!("compacted candidate"; 
                  "region_id" => current_candidate.region.get_id(),
                  "score" => current_candidate.score,
                  "processed_count" => processed_count,
                  "duration_ms" => compact_duration.as_millis());
        }

        Some(start_time.elapsed())
    }

    /// Compacts a single candidate
    fn compact_candidate(&self, candidate: &CompactionCandidate, config: &GcConfig) -> Result<()> {
        let bottommost_level_force = config.auto_compaction.bottommost_level_force;
        let start_key = enc_start_key(&candidate.region);
        let end_key = enc_end_key(&candidate.region);

        // Compact write CF first (most important for GC)
        self.compact_range_cf(
            CF_WRITE,
            Some(&start_key),
            Some(&end_key),
            bottommost_level_force,
        )?;

        // Then compact default CF
        self.compact_range_cf(
            CF_DEFAULT,
            Some(&start_key),
            Some(&end_key),
            bottommost_level_force,
        )?;

        Ok(())
    }

    /// Compacts a range in a specific column family
    fn compact_range_cf(
        &self,
        cf_name: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        bottommost_level_force: bool,
    ) -> Result<()> {
        let compact_options = ManualCompactionOptions::new(false, 1, bottommost_level_force);
        self.engine
            .compact_range_cf(cf_name, start_key, end_key, compact_options)
            .map_err(|e: engine_traits::Error| -> Error {
                box_err!("compact range failed: {:?}", e)
            })?;
        Ok(())
    }

    /// Gets the next region for compaction evaluation
    fn get_next_region_context(&self, key: &[u8]) -> Result<Option<Region>> {
        let (tx, rx) = mpsc::channel();

        let res = self.region_info_provider.seek_region(
            key,
            Box::new(move |iter| {
                if let Some(info) = iter.next() {
                    // Assume any region returned by seek_region has a peer on this store
                    let _ = tx.send(Some(info.region.clone()));
                    return;
                }
                let _ = tx.send(None);
            }),
        );

        if let Err(e) = res {
            return Err(box_err!("failed to get next region information: {:?}", e));
        }

        match rx.recv() {
            Ok(Some(region)) => Ok(Some(region)),
            Ok(None) => Ok(None),
            Err(e) => Err(box_err!("failed to receive region information: {:?}", e)),
        }
    }

    /// Evaluates a key range as a compaction candidate using MVCC-aware scoring
    fn evaluate_range_candidate(
        &self,
        region: &Region,
        gc_safe_point: u64,
        config: &GcConfig,
    ) -> Result<Option<CompactionCandidate>> {
        let start_key = enc_start_key(region);
        let end_key = enc_end_key(region);

        let mut num_tombstones = 0;
        let mut num_discardable = 0;
        let mut num_total_entries = 0;
        let mut num_rows = 0;

        let collection = self
            .engine
            .table_properties_collection(CF_WRITE, &[Range::new(&start_key, &end_key)])
            .map_err(|e: engine_traits::Error| -> Error {
                box_err!("failed to get table properties: {:?}", e)
            })?;

        collection.iter_table_properties(|table_prop| {
            let num_entries = table_prop.get_num_entries();
            num_total_entries += num_entries;

            if let Some(mvcc_properties) = table_prop
                .get_user_collected_properties()
                .get_mvcc_properties()
            {
                // Collect MVCC stats
                num_rows += mvcc_properties.num_rows;

                // RocksDB tombstones are guaranteed to be discardable
                num_tombstones += num_entries - mvcc_properties.num_versions;
                if config.enable_compaction_filter {
                    // Estimate discardable TiKV MVCC delete versions
                    num_discardable += self.get_estimated_discardable_entries(
                        mvcc_properties.num_deletes,
                        mvcc_properties.oldest_delete_ts,
                        mvcc_properties.newest_delete_ts,
                        gc_safe_point,
                    );
                    // Estimate discardable stale MVCC versions
                    num_discardable += self.get_estimated_discardable_entries(
                        mvcc_properties.num_versions - mvcc_properties.num_rows,
                        mvcc_properties.oldest_stale_version_ts,
                        mvcc_properties.newest_stale_version_ts,
                        gc_safe_point,
                    );
                }
            }
            true
        });

        let score =
            self.get_compact_score(num_tombstones, num_discardable, num_total_entries, config);

        if score > 0.0 {
            fail_point!("gc_worker_auto_compaction_candidate_found");
            Ok(Some(CompactionCandidate {
                score,
                num_tombstones,
                num_discardable,
                num_total_entries,
                num_rows,
                region: region.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Estimates the number of discardable MVCC entries based on GC safe point
    fn get_estimated_discardable_entries(
        &self,
        num_entries: u64,
        oldest_ts: TimeStamp,
        newest_ts: TimeStamp,
        gc_safe_point: u64,
    ) -> u64 {
        if num_entries == 0 || oldest_ts >= newest_ts {
            return 0;
        }
        let oldest_ts = oldest_ts.into_inner();
        let newest_ts = newest_ts.into_inner();

        if gc_safe_point >= newest_ts {
            return num_entries;
        }
        if gc_safe_point < oldest_ts {
            return 0;
        }

        let total_range = newest_ts - oldest_ts;
        let discardable_range = gc_safe_point - oldest_ts;
        let portion = (discardable_range as f64) / (total_range as f64);
        (num_entries as f64 * portion).round() as u64
    }

    /// Calculates compaction score based on tombstones and discardable entries
    fn get_compact_score(
        &self,
        num_tombstones: u64,
        num_discardable: u64,
        num_total_entries: u64,
        config: &GcConfig,
    ) -> f64 {
        if num_total_entries == 0 || num_total_entries < num_discardable {
            return 0.0;
        }

        if !config.enable_compaction_filter {
            // Only consider deletes (tombstones)
            let ratio = num_tombstones as f64 / num_total_entries as f64;
            if num_tombstones < config.auto_compaction.tombstones_num_threshold
                && ratio < config.auto_compaction.tombstones_percent_threshold as f64 / 100.0
            {
                return 0.0;
            }
            return num_tombstones as f64 * ratio;
        }

        // When compaction filter is enabled, ignore tombstone threshold,
        // just add deletes to redundant keys for scoring.
        let ratio = (num_tombstones + num_discardable) as f64 / num_total_entries as f64;
        if num_discardable < config.auto_compaction.redundant_rows_threshold
            && ratio < config.auto_compaction.redundant_rows_percent_threshold as f64 / 100.0
        {
            return 0.0;
        }
        num_discardable as f64 * ratio
    }

    fn sleep_or_stop(&mut self, timeout: Duration) -> bool {
        if self.is_stopped {
            return true; // Already stopped
        }
        match self.stop_signal_receiver.as_ref() {
            Some(rx) => match rx.recv_timeout(timeout) {
                Ok(_) => {
                    self.is_stopped = true;
                    true // Stop requested
                }
                Err(mpsc::RecvTimeoutError::Timeout) => false, // Continue
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    error!("stop_signal_receiver unexpectedly disconnected");
                    self.is_stopped = true;
                    true // Stop
                }
            },
            None => {
                thread::sleep(timeout);
                false // Continue
            }
        }
    }

    fn check_stopped(&mut self) -> bool {
        if self.is_stopped {
            return true; // Already stopped
        }
        match self.stop_signal_receiver.as_ref() {
            Some(rx) => match rx.try_recv() {
                Ok(_) => {
                    self.is_stopped = true;
                    true // Stop requested
                }
                Err(mpsc::TryRecvError::Empty) => false, // Continue
                Err(mpsc::TryRecvError::Disconnected) => {
                    error!(
                        "stop_signal_receiver unexpectedly disconnected, compaction_runner will stop"
                    );
                    true // Stop
                }
            },
            None => false, // Continue
        }
    }
}
