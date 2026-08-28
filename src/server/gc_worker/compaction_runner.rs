// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(any(test, feature = "failpoints"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    sync::{Arc, Condvar, Mutex, mpsc},
    thread::{Builder as ThreadBuilder, JoinHandle},
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
use raftstore::coprocessor::{RegionInfoProvider, split_observer::NoValidSplitKeyNotifier};
use tikv_util::{
    box_err, debug, error, info, sys::thread::StdThreadBuildWrapper,
    thread_name_prefix::COMPACTION_RUNNER_THREAD, warn,
};
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

// Global variable for testing: stores the region_id of the first candidate
// selected for compaction Used by failpoint tests to verify MVCC-aware
// prioritization
#[cfg(any(test, feature = "failpoints"))]
pub static FIRST_COMPACTION_CANDIDATE_REGION: AtomicU64 = AtomicU64::new(0);

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

    pub static ref AUTO_COMPACTION_NUM_TOMBSTONES_HISTOGRAM: Histogram = register_histogram!(
        "tikv_auto_compaction_num_tombstones",
        "Histogram of number of tombstones in compaction candidates",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    ).unwrap();

    pub static ref AUTO_COMPACTION_NUM_DISCARDABLE_HISTOGRAM: Histogram = register_histogram!(
        "tikv_auto_compaction_num_discardable",
        "Histogram of number of discardable MVCC versions in compaction candidates",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    ).unwrap();

    pub static ref AUTO_COMPACTION_MVCC_VERSIONS_SCANNED_HISTOGRAM: Histogram = register_histogram!(
        "tikv_auto_compaction_mvcc_versions_scanned",
        "Histogram of average MVCC versions scanned per request for compaction candidates",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    ).unwrap();

    pub static ref AUTO_COMPACTION_SCORE_HISTOGRAM: Histogram = register_histogram!(
        "tikv_auto_compaction_score",
        "Histogram of compaction scores for candidates",
        exponential_buckets(0.1, 2.0, 20).unwrap()
    ).unwrap();

    pub static ref AUTO_COMPACTION_RECLAIMABLE_BYTES_HISTOGRAM: Histogram = register_histogram!(
        "tikv_auto_compaction_reclaimable_bytes",
        "Histogram of estimated reclaimable bytes in compaction candidates",
        exponential_buckets(1024.0 * 1024.0, 2.0, 20).unwrap()
    ).unwrap();

    pub static ref AUTO_COMPACTION_SPLIT_FAILURE_HINTS_TOTAL: IntCounter = register_int_counter!(
        "tikv_auto_compaction_split_failure_hints_total",
        "Number of no-valid-split-key hints received by auto compaction"
    ).unwrap();

    pub static ref AUTO_COMPACTION_SPLIT_FAILURE_HINTS_COALESCED_TOTAL: IntCounter = register_int_counter!(
        "tikv_auto_compaction_split_failure_hints_coalesced_total",
        "Number of no-valid-split-key hints coalesced with an existing hint"
    ).unwrap();

    pub static ref AUTO_COMPACTION_SPLIT_FAILURE_HINTS_DROPPED_TOTAL: IntCounter = register_int_counter!(
        "tikv_auto_compaction_split_failure_hints_dropped_total",
        "Number of no-valid-split-key hints dropped by bounded auto-compaction admission"
    ).unwrap();

    pub static ref AUTO_COMPACTION_SPLIT_FAILURE_TRIGGERED_ROUNDS_TOTAL: IntCounter = register_int_counter!(
        "tikv_auto_compaction_split_failure_triggered_rounds_total",
        "Number of auto-compaction scan rounds woken by no-valid-split-key hints"
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
    pub estimated_reclaimable_bytes: u64,
    pub mvcc_versions_scanned: u64, /* Average MVCC versions scanned per request from online
                                     * traffic (indicates read overhead) */
    pub region: Region,
}

/// Converts estimated reclaimable bytes to MiB-sized score units. Keeping the
/// score near the scale of the existing row-based score allows the optional
/// MVCC read-activity score to remain meaningful when the two are combined.
const RECLAIMABLE_BYTES_PER_SCORE: f64 = 1024.0 * 1024.0;

/// Minimum interval between split-failure-triggered full-store scans. Hints are
/// coalesced, but this cooldown still bounds the metadata work caused by a
/// burst of `NO_VALID_SPLIT_KEY` results.
const MIN_GAP_BETWEEN_SPLIT_FAILURE_ROUNDS: Duration = Duration::from_secs(30);

#[derive(Default)]
struct CompactionControlState {
    stopped: bool,
    split_failure_pending: bool,
    gc_safe_point: u64,
    last_triggered_safe_point: Option<u64>,
}

#[derive(Default)]
pub(super) struct CompactionControl {
    state: Mutex<CompactionControlState>,
    /// Wakes the runner when a coalesced split-failure hint or stop request is
    /// available. The associated state mutex is always checked in a loop to
    /// handle spurious wake-ups.
    wake_up: Condvar,
}

impl CompactionControl {
    /// Initializes metrics before this control is exposed to the raftstore
    /// request path. Counter increments after initialization are lock-free.
    pub(super) fn initialize_metrics() {
        lazy_static::initialize(&AUTO_COMPACTION_SPLIT_FAILURE_HINTS_TOTAL);
        lazy_static::initialize(&AUTO_COMPACTION_SPLIT_FAILURE_HINTS_COALESCED_TOTAL);
        lazy_static::initialize(&AUTO_COMPACTION_SPLIT_FAILURE_HINTS_DROPPED_TOTAL);
        lazy_static::initialize(&AUTO_COMPACTION_SPLIT_FAILURE_TRIGGERED_ROUNDS_TOTAL);
    }

    /// Waits until the periodic interval expires, a split-failure hint arrives,
    /// or the runner is stopped. Returns `true` only for stop.
    fn wait(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap();
        loop {
            if state.stopped {
                return true;
            }
            if state.split_failure_pending {
                return false;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            state = self.wake_up.wait_timeout(state, deadline - now).unwrap().0;
        }
    }

    /// Waits for a cooldown while ignoring additional coalesced hints. Stop
    /// still wakes the runner immediately.
    fn wait_for_stop(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap();
        loop {
            if state.stopped {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            state = self.wake_up.wait_timeout(state, deadline - now).unwrap().0;
        }
    }

    fn consume_split_failure_hint(&self, gc_safe_point: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if std::mem::take(&mut state.split_failure_pending) {
            state.last_triggered_safe_point = Some(gc_safe_point);
            true
        } else {
            false
        }
    }

    fn update_gc_safe_point(&self, gc_safe_point: u64) {
        self.state.lock().unwrap().gc_safe_point = gc_safe_point;
    }

    fn stop(&self) {
        let mut state = self.state.lock().unwrap();
        state.stopped = true;
        self.wake_up.notify_one();
    }

    fn is_stopped(&self) -> bool {
        self.state.lock().unwrap().stopped
    }
}

impl NoValidSplitKeyNotifier for CompactionControl {
    fn notify(&self, _region_id: u64) {
        AUTO_COMPACTION_SPLIT_FAILURE_HINTS_TOTAL.inc();

        // Never wait on the raftstore request path. A dropped hint is safe
        // because periodic scanning remains the source of truth.
        let Ok(mut state) = self.state.try_lock() else {
            AUTO_COMPACTION_SPLIT_FAILURE_HINTS_DROPPED_TOTAL.inc();
            return;
        };
        if state.stopped {
            AUTO_COMPACTION_SPLIT_FAILURE_HINTS_DROPPED_TOTAL.inc();
            return;
        }

        // One triggered round scans the entire store, so all failures observed
        // at the same GC safe point are covered by that round. A periodic scan
        // still runs if the cached safe point lags behind PD.
        if state.gc_safe_point > 0 && state.last_triggered_safe_point == Some(state.gc_safe_point) {
            AUTO_COMPACTION_SPLIT_FAILURE_HINTS_COALESCED_TOTAL.inc();
            return;
        }
        if std::mem::replace(&mut state.split_failure_pending, true) {
            AUTO_COMPACTION_SPLIT_FAILURE_HINTS_COALESCED_TOTAL.inc();
            return;
        }
        self.wake_up.notify_one();
    }
}

fn meets_redundant_bytes_threshold(estimated_bytes: u64, threshold_bytes: u64) -> bool {
    threshold_bytes > 0 && estimated_bytes >= threshold_bytes
}

fn proportional_bytes(total_bytes: u64, part_entries: u64, total_entries: u64) -> u64 {
    if total_bytes == 0 || part_entries == 0 || total_entries == 0 {
        return 0;
    }
    let bytes = (total_bytes as u128).saturating_mul(part_entries.min(total_entries) as u128)
        / total_entries as u128;
    bytes.min(u64::MAX as u128) as u64
}

/// Estimates discardable MVCC entries from their timestamp range and GC safe
/// point.
fn estimate_discardable_entries(
    num_entries: u64,
    oldest_ts: TimeStamp,
    newest_ts: TimeStamp,
    gc_safe_point: u64,
) -> u64 {
    if num_entries == 0 || oldest_ts > newest_ts {
        return 0;
    }
    let oldest_ts = oldest_ts.into_inner();
    let newest_ts = newest_ts.into_inner();

    // A zero-width timestamp range is valid when multiple entries share the
    // same timestamp. All of them are discardable once that timestamp reaches
    // the GC safe point.
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

fn estimate_reclaimable_bytes(
    write_cf_bytes: u64,
    default_cf_bytes: u64,
    num_tombstones: u64,
    num_discardable: u64,
    num_total_entries: u64,
    num_discardable_value_versions: u64,
    num_total_puts: u64,
    compaction_filter_enabled: bool,
) -> u64 {
    let write_discardable = if compaction_filter_enabled {
        num_tombstones.saturating_add(num_discardable)
    } else {
        num_tombstones
    };
    let write_bytes = proportional_bytes(write_cf_bytes, write_discardable, num_total_entries);
    let default_bytes = if compaction_filter_enabled {
        // RocksDB tombstones and MVCC Delete records do not have corresponding
        // default-CF values. Only use stale MVCC versions for this estimate.
        proportional_bytes(
            default_cf_bytes,
            num_discardable_value_versions,
            num_total_puts,
        )
    } else {
        0
    };
    write_bytes.saturating_add(default_bytes)
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
    control: Arc<CompactionControl>,
}

impl CompactionRunnerHandle {
    pub fn stop(self) -> Result<()> {
        self.control.stop();
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
    control: Arc<CompactionControl>,
    cfg_tracker: GcWorkerConfigManager,
}

/// Calculates compaction score based on tombstones, estimated reclaimable
/// bytes, and MVCC read intensity.
fn calculate_compaction_score(
    num_tombstones: u64,
    num_discardable: u64,
    num_total_entries: u64,
    estimated_reclaimable_bytes: u64,
    mvcc_versions_scanned: u64,
    config: &GcConfig,
) -> f64 {
    if num_total_entries == 0 || num_total_entries < num_discardable {
        return 0.0;
    }

    let meets_bytes_threshold = meets_redundant_bytes_threshold(
        estimated_reclaimable_bytes,
        config.auto_compaction.redundant_bytes_threshold.0,
    );
    let base_score = if !config.enable_compaction_filter {
        // Only consider deletes (tombstones).
        let ratio = num_tombstones as f64 / num_total_entries as f64;
        if num_tombstones < config.auto_compaction.tombstones_num_threshold
            && ratio < config.auto_compaction.tombstones_percent_threshold as f64 / 100.0
            && !meets_bytes_threshold
        {
            0.0
        } else if estimated_reclaimable_bytes > 0 {
            estimated_reclaimable_bytes as f64 / RECLAIMABLE_BYTES_PER_SCORE
        } else {
            // Keep candidates visible when old SSTs do not have range properties.
            num_tombstones as f64 * ratio
        }
    } else {
        // When compaction filter is enabled, ignore tombstone threshold,
        // just add deletes to redundant keys for admission.
        let ratio = (num_tombstones + num_discardable) as f64 / num_total_entries as f64;
        if num_discardable < config.auto_compaction.redundant_rows_threshold
            && ratio < config.auto_compaction.redundant_rows_percent_threshold as f64 / 100.0
            && !meets_bytes_threshold
        {
            0.0
        } else if estimated_reclaimable_bytes > 0 {
            // Rank admitted candidates by physical impact instead of version count.
            // Use MiB-sized units so the additive MVCC-read score remains useful.
            estimated_reclaimable_bytes as f64 / RECLAIMABLE_BYTES_PER_SCORE
        } else {
            // Keep candidates visible when old SSTs do not have range properties.
            num_discardable as f64 * ratio
        }
    };

    // If MVCC-read-aware compaction is disabled, return base score.
    if !config.auto_compaction.mvcc_read_aware_enabled
        || (num_discardable == 0 && num_tombstones == 0)
    {
        return base_score;
    }

    // Calculate MVCC read score based on throughput. Regions with high
    // mvcc_versions_scanned benefit from compaction even if their base score is
    // low, because compaction improves read performance.
    let mvcc_read_weight = config.auto_compaction.mvcc_read_weight;
    let mvcc_score = (mvcc_versions_scanned as f64) * mvcc_read_weight;

    // Use an additive formula so regions with high MVCC overhead get compacted
    // even when base_score is zero.
    base_score + mvcc_score
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
        Self::new_with_control(
            safe_point_provider,
            region_info_provider,
            engine,
            cfg_tracker,
            Arc::new(CompactionControl::default()),
        )
    }

    pub(super) fn new_with_control(
        safe_point_provider: S,
        region_info_provider: R,
        engine: E,
        cfg_tracker: GcWorkerConfigManager,
        control: Arc<CompactionControl>,
    ) -> Self {
        Self {
            safe_point_provider,
            region_info_provider,
            engine,
            control,
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
        let control = self.control.clone();

        let props = tikv_util::thread_group::current_properties();
        let res: Result<_> = ThreadBuilder::new()
            .name(tikv_util::thd_name!(COMPACTION_RUNNER_THREAD))
            .spawn_wrapper(move || {
                tikv_util::thread_group::set_properties(props);
                self.run();
            })
            .map_err(|e| box_err!("failed to start compaction runner: {:?}", e));

        res.map(|join_handle| CompactionRunnerHandle {
            join_handle,
            control,
        })
    }

    /// Main loop for the compaction runner
    fn run(&mut self) {
        info!("compaction-runner started");
        fail_point!("gc_worker_auto_compaction_start");
        let mut last_split_failure_round = None;
        loop {
            if self.check_stopped() {
                break;
            }

            // Get consistent config snapshot for this run
            let config = self.cfg_tracker.value().clone();
            let check_interval = config.auto_compaction.check_interval.0;

            // Get current safe point and publish it for best-effort hint
            // deduplication. The runner remains the source of truth.
            let gc_safe_point = self.curr_safe_point().into_inner();
            self.control.update_gc_safe_point(gc_safe_point);

            // Keep a pending split-failure hint until GC has a usable safe
            // point. Compaction cannot reclaim MVCC versions before then, and
            // consuming it here would turn a useful hint into a no-op.
            if gc_safe_point == 0 {
                info!("skipping compaction: GC safe point is zero");
                if self.control.wait_for_stop(check_interval) {
                    break;
                }
                continue;
            }

            if self.control.consume_split_failure_hint(gc_safe_point) {
                if let Some(last_round) = last_split_failure_round {
                    let elapsed = Instant::now().saturating_duration_since(last_round);
                    if elapsed < MIN_GAP_BETWEEN_SPLIT_FAILURE_ROUNDS {
                        // Never let the anti-storm cooldown delay a normal
                        // periodic round beyond its configured interval.
                        let cooldown =
                            (MIN_GAP_BETWEEN_SPLIT_FAILURE_ROUNDS - elapsed).min(check_interval);
                        if self.control.wait_for_stop(cooldown) {
                            break;
                        }
                    }
                }
                // Hints accumulated during the cooldown or the scan are
                // covered by this full-store round. A later GC safe point can
                // request another bounded round.
                self.control.consume_split_failure_hint(gc_safe_point);
                last_split_failure_round = Some(Instant::now());
                AUTO_COMPACTION_SPLIT_FAILURE_TRIGGERED_ROUNDS_TOTAL.inc();
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
            // Reset MVCC read tracker if time window has elapsed
            use crate::storage::mvcc::mvcc_read_tracker::MVCC_READ_TRACKER;
            if let Some(tracker) = MVCC_READ_TRACKER.get() {
                tracker.reset_if_needed();
            }

            // Sleep for remaining time in check interval, or start next round
            // immediately. When MVCC-read-aware scoring is enabled, enforce a
            // minimum gap so the tracker can accumulate meaningful stats after
            // the reset.
            const MIN_GAP_BETWEEN_ROUNDS: Duration = Duration::from_secs(20);
            let remaining_sleep = if elapsed < check_interval {
                check_interval - elapsed
            } else {
                Duration::ZERO
            };
            let sleep_duration = if config.auto_compaction.mvcc_read_aware_enabled {
                remaining_sleep.max(MIN_GAP_BETWEEN_ROUNDS)
            } else {
                remaining_sleep
            };
            if sleep_duration > Duration::ZERO && self.sleep_or_stop(sleep_duration) {
                break;
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
        let candidates: Vec<CompactionCandidate> = candidates_heap
            .into_sorted_vec()
            .into_iter()
            .map(|reverse| reverse.0)
            .collect();

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
                "estimated_reclaimable_bytes" => candidate.estimated_reclaimable_bytes,
                "mvcc_versions_scanned" => candidate.mvcc_versions_scanned
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

        for (index, candidate) in candidates.into_iter().enumerate() {
            if self.check_stopped() {
                return None; // Stopped
            }

            // Failpoint for testing: capture first candidate selected for compaction
            // Log the region_id so test can verify which region was prioritized
            #[cfg(any(test, feature = "failpoints"))]
            if index == 0 {
                let region_id = candidate.region.get_id();
                info!("first compaction candidate selected"; "region_id" => region_id, "score" => candidate.score);

                // Store region_id for test verification
                FIRST_COMPACTION_CANDIDATE_REGION.store(region_id, Ordering::Relaxed);
                fail_point!("gc_worker_auto_compaction_first_candidate");
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
                  "estimated_reclaimable_bytes" => current_candidate.estimated_reclaimable_bytes,
                  "processed_count" => processed_count,
                  "duration_ms" => compact_duration.as_millis());
        }

        Some(start_time.elapsed())
    }

    /// Compacts a single candidate
    fn compact_candidate(&self, candidate: &CompactionCandidate, config: &GcConfig) -> Result<()> {
        // Large stale values can already reside at the bottommost level. Force
        // that level for candidates admitted by the byte threshold; otherwise
        // the compaction can finish without reclaiming the bytes that made the
        // region oversized (see #16493).
        let bottommost_level_force = config.auto_compaction.bottommost_level_force
            || meets_redundant_bytes_threshold(
                candidate.estimated_reclaimable_bytes,
                config.auto_compaction.redundant_bytes_threshold.0,
            );
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
        let mut num_total_puts = 0;
        let mut num_rows = 0;
        let mut num_discardable_value_versions = 0;
        let mut write_cf_bytes: u64 = 0;

        let collection = self
            .engine
            .table_properties_collection(CF_WRITE, &[Range::new(&start_key, &end_key)])
            .map_err(|e: engine_traits::Error| -> Error {
                box_err!("failed to get table properties: {:?}", e)
            })?;

        collection.iter_table_properties(|table_prop| {
            let num_entries = table_prop.get_num_entries();
            num_total_entries += num_entries;
            let user_properties = table_prop.get_user_collected_properties();

            if let Some((size, _)) = user_properties.approximate_size_and_keys(&start_key, &end_key)
            {
                write_cf_bytes = write_cf_bytes.saturating_add(size as u64);
            }

            if let Some(mvcc_properties) = user_properties.get_mvcc_properties() {
                // Collect MVCC stats
                num_rows += mvcc_properties.num_rows;
                num_total_puts += mvcc_properties.num_puts;

                // RocksDB tombstones are guaranteed to be discardable
                num_tombstones += num_entries.saturating_sub(mvcc_properties.num_versions);
                if config.enable_compaction_filter {
                    // Estimate discardable TiKV MVCC delete versions
                    num_discardable += estimate_discardable_entries(
                        mvcc_properties.num_deletes,
                        mvcc_properties.oldest_delete_ts,
                        mvcc_properties.newest_delete_ts,
                        gc_safe_point,
                    );
                    // Estimate all discardable stale MVCC versions for write CF.
                    let discardable_versions = estimate_discardable_entries(
                        mvcc_properties
                            .num_versions
                            .saturating_sub(mvcc_properties.num_rows),
                        mvcc_properties.oldest_stale_version_ts,
                        mvcc_properties.newest_stale_version_ts,
                        gc_safe_point,
                    );
                    num_discardable += discardable_versions;

                    // Only Put versions can own default-CF values. num_puts -
                    // num_rows is a conservative lower bound because the live
                    // version of each row may itself be a Delete.
                    num_discardable_value_versions += estimate_discardable_entries(
                        mvcc_properties
                            .num_puts
                            .saturating_sub(mvcc_properties.num_rows),
                        mvcc_properties.oldest_stale_version_ts,
                        mvcc_properties.newest_stale_version_ts,
                        gc_safe_point,
                    );
                }
            }
            true
        });

        // Large values are stored in default CF, while the MVCC properties used above
        // come from write CF. Include default CF bytes so large-value regions are not
        // starved by small-value regions with many more write-CF entries.
        let mut default_cf_bytes: u64 = 0;
        if config.enable_compaction_filter && num_discardable_value_versions > 0 {
            match self
                .engine
                .table_properties_collection(CF_DEFAULT, &[Range::new(&start_key, &end_key)])
            {
                Ok(collection) => collection.iter_table_properties(|table_prop| {
                    if let Some((size, _)) = table_prop
                        .get_user_collected_properties()
                        .approximate_size_and_keys(&start_key, &end_key)
                    {
                        default_cf_bytes = default_cf_bytes.saturating_add(size as u64);
                    }
                    true
                }),
                Err(e) => warn!(
                    "failed to get default CF table properties, scoring with write CF only";
                    "region_id" => region.get_id(),
                    "err" => ?e,
                ),
            }
        }

        // Get average mvcc_versions_scanned per request from actual online read traffic
        let mvcc_versions_scanned = if config.auto_compaction.mvcc_read_aware_enabled {
            use crate::storage::mvcc::mvcc_read_tracker::MVCC_READ_TRACKER;
            MVCC_READ_TRACKER
                .get()
                .map(|tracker| tracker.get_mvcc_versions_scanned(region.get_id()))
                .unwrap_or(0)
        } else {
            0
        };

        let estimated_reclaimable_bytes = estimate_reclaimable_bytes(
            write_cf_bytes,
            default_cf_bytes,
            num_tombstones,
            num_discardable,
            num_total_entries,
            num_discardable_value_versions,
            num_total_puts,
            config.enable_compaction_filter,
        );
        let score = calculate_compaction_score(
            num_tombstones,
            num_discardable,
            num_total_entries,
            estimated_reclaimable_bytes,
            mvcc_versions_scanned,
            config,
        );

        if score > 0.0 {
            fail_point!("gc_worker_auto_compaction_candidate_found");

            // Record metrics for this compaction candidate
            AUTO_COMPACTION_NUM_TOMBSTONES_HISTOGRAM.observe(num_tombstones as f64);
            AUTO_COMPACTION_NUM_DISCARDABLE_HISTOGRAM.observe(num_discardable as f64);
            AUTO_COMPACTION_RECLAIMABLE_BYTES_HISTOGRAM.observe(estimated_reclaimable_bytes as f64);
            AUTO_COMPACTION_MVCC_VERSIONS_SCANNED_HISTOGRAM.observe(mvcc_versions_scanned as f64);
            AUTO_COMPACTION_SCORE_HISTOGRAM.observe(score);

            Ok(Some(CompactionCandidate {
                score,
                num_tombstones,
                num_discardable,
                num_total_entries,
                num_rows,
                estimated_reclaimable_bytes,
                mvcc_versions_scanned,
                region: region.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    fn sleep_or_stop(&mut self, timeout: Duration) -> bool {
        self.control.wait(timeout)
    }

    fn check_stopped(&mut self) -> bool {
        self.control.is_stopped()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_failure_hints_are_coalesced() {
        let control = CompactionControl::default();
        control.update_gc_safe_point(10);

        control.notify(1);
        control.notify(1);
        control.notify(2);
        assert!(control.consume_split_failure_hint(10));
        assert!(!control.consume_split_failure_hint(10));

        // One full-store round covers all Regions at the same safe point.
        control.notify(3);
        assert!(!control.consume_split_failure_hint(10));

        // A later safe point may make additional versions reclaimable.
        control.update_gc_safe_point(11);
        control.notify(3);
        assert!(control.consume_split_failure_hint(11));
    }

    #[test]
    fn test_split_failure_wait_wakes_on_hint_and_stop() {
        let control = Arc::new(CompactionControl::default());
        let waiter = control.clone();
        let thread = std::thread::spawn(move || waiter.wait(Duration::from_secs(10)));
        std::thread::sleep(Duration::from_millis(10));
        control.notify(1);
        assert!(!thread.join().unwrap());

        assert!(control.consume_split_failure_hint(0));
        let waiter = control.clone();
        let thread = std::thread::spawn(move || waiter.wait_for_stop(Duration::from_secs(10)));
        std::thread::sleep(Duration::from_millis(10));
        control.stop();
        assert!(thread.join().unwrap());
    }

    #[test]
    fn test_redundant_bytes_threshold() {
        assert!(!meets_redundant_bytes_threshold(1, 0));
        assert!(!meets_redundant_bytes_threshold(127, 128));
        assert!(meets_redundant_bytes_threshold(128, 128));
    }

    #[test]
    fn test_large_region_has_higher_compaction_score() {
        let config = GcConfig::default();

        // The large region has fewer redundant versions, but those versions
        // carry much larger values. Its physical reclaim opportunity should
        // therefore give it a higher score than the small-value region.
        let large_region_score = calculate_compaction_score(
            0,                  // tombstones
            1,                  // discardable versions
            10,                 // total entries
            1024 * 1024 * 1024, // estimated reclaimable bytes: 1 GiB
            0,                  // MVCC versions scanned per request
            &config,
        );
        let small_region_score = calculate_compaction_score(
            0,                 // tombstones
            30,                // discardable versions
            100,               // total entries
            100 * 1024 * 1024, // estimated reclaimable bytes: 100 MiB
            0,                 // MVCC versions scanned per request
            &config,
        );

        assert!(large_region_score > small_region_score);
        assert_eq!(large_region_score, 1024.0);
        assert_eq!(small_region_score, 100.0);
    }

    #[test]
    fn test_equal_timestamp_versions_are_discardable() {
        let ts = TimeStamp::new(10);

        assert_eq!(estimate_discardable_entries(2, ts, ts, 9), 0);
        let discardable = estimate_discardable_entries(2, ts, ts, 10);
        assert_eq!(discardable, 2);

        // Equal-timestamp stale Put versions must enable proportional default-CF
        // sizing instead of making the large-value estimate disappear.
        assert_eq!(
            estimate_reclaimable_bytes(0, 1024, 0, discardable, 4, discardable, 4, true),
            512
        );
    }

    #[test]
    fn test_estimate_reclaimable_bytes_includes_large_values() {
        // Ten of one hundred stale versions are below the safe point. The
        // write-CF part is small, but default CF contains 1 GiB of values.
        let bytes = estimate_reclaimable_bytes(
            10 * 1024 * 1024,
            1024 * 1024 * 1024,
            0,
            10,
            100,
            10,
            100,
            true,
        );
        assert_eq!(bytes, 1034 * 1024 * 1024 / 10);
    }

    #[test]
    fn test_estimate_reclaimable_bytes_excludes_deletes_from_default_cf() {
        // MVCC Delete records have no default-CF values. Counting all
        // discardable records against default-CF bytes would overestimate the
        // reclaimable bytes of delete-heavy index regions.
        let bytes = estimate_reclaimable_bytes(1000, 10_000, 10, 40, 100, 0, 90, true);
        assert_eq!(bytes, 500);
    }

    #[test]
    fn test_estimate_reclaimable_bytes_saturates() {
        assert_eq!(proportional_bytes(u64::MAX, u64::MAX, u64::MAX), u64::MAX);
        assert_eq!(
            estimate_reclaimable_bytes(
                u64::MAX,
                u64::MAX,
                u64::MAX,
                u64::MAX,
                u64::MAX,
                u64::MAX,
                u64::MAX,
                true,
            ),
            u64::MAX
        );
    }
}
