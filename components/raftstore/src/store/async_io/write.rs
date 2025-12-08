// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! The implementation of asynchronous write for raftstore.
//!
//! `WriteTask` is the unit of write which is created by a certain
//! peer. Afterwards the `WriteTask` should be sent to `Worker`.
//! The `Worker` is responsible for persisting `WriteTask` to kv db or
//! raft db and then invoking callback or sending msgs if any.

use std::{
    collections::VecDeque,
    fmt, mem,
    sync::Arc,
    thread::{self, JoinHandle},
};

use collections::HashMap;
use crossbeam::channel::TryRecvError;
use engine_traits::{
    KvEngine, PerfContext, PerfContextKind, RaftEngine, RaftLogBatch, WriteBatch, WriteOptions,
};
use error_code::ErrorCodeExt;
use fail::fail_point;
use file_system::{set_io_type, IoType};
use health_controller::types::LatencyInspector;
use kvproto::{
    metapb::RegionEpoch,
    raft_serverpb::{RaftLocalState, RaftMessage},
};
use parking_lot::Mutex;
use protobuf::Message;
use raft::eraftpb::Entry;
use resource_control::{
    channel::{bounded, Receiver},
    ResourceConsumeType, ResourceController, ResourceMetered,
};
use tikv_util::{
    box_err,
    config::{ReadableSize, Tracker, VersionTrack},
    debug, info, slow_log,
    sys::thread::StdThreadBuildWrapper,
    thd_name,
    time::{duration_to_sec, setup_for_spin_interval, spin_at_least, Duration, Instant},
    warn,
};
use tracker::TrackerTokenArray;

use super::write_router::{SharedSenders, WriteSenders};
use crate::{
    store::{
        config::Config,
        fsm::RaftRouter,
        local_metrics::{RaftSendMessageMetrics, StoreWriteMetrics, TimeTracker},
        metrics::*,
        transport::Transport,
        util, PeerMsg,
    },
    Result,
};

const KV_WB_SHRINK_SIZE: usize = 1024 * 1024;
const KV_WB_DEFAULT_SIZE: usize = 16 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 10 * 1024 * 1024;
const RAFT_WB_DEFAULT_SIZE: usize = 256 * 1024;
const RAFT_WB_SPLIT_SIZE: usize = ReadableSize::gb(1).0 as usize;
/// The default size of the raft write batch recorder.
const RAFT_WB_DEFAULT_RECORDER_SIZE: usize = 30;

const QPS_THRESHOLD: u64 = 1000;
const RAFT_WB_WAIT_DURATION_UPPER_BOUND_NS: u64 = 1000_000; // 1ms
const RAFT_WB_WAIT_DURATION_LOWER_BOUND_NS: u64 = 10_000; // 10us
// Expected wait_duration for normal concurrency, 200us
const RAFT_WB_WAIT_DURATION_NORMAL_EXPECTED_NS: u64 = 200_000; 

// Adaptive batching QPS-related constants for calculate_adaptive_wait_duration()
const ADAPTIVE_QPS_BASELINE_HISTORY_MIN_LEN: usize = 10;
const ADAPTIVE_DEFAULT_QPS_BASELINE: u64 = 1000;
const ADAPTIVE_QPS_PRESSURE_LOWER_BOUND: u64 = 10000;
// Dynamic threshold for high concurrency - initial and minimum value
const ADAPTIVE_MIN_HIGH_CONCURRENCY_QPS: u64 = 45000;
// EWMA smoothing factor for high_concurrency_qps_threshold update
const ADAPTIVE_HIGH_CONCURRENCY_EWMA_ALPHA: f64 = 0.2; // 0.8*old + 0.2*new
// Very high concurrency multiplier (1.2x of high_concurrency_qps_threshold)
const ADAPTIVE_VERY_HIGH_CONCURRENCY_MULTIPLIER: f64 = 1.2;
const BATCH_ACHIEVEMENT_RATIO_LOW_THRESHOLD: f64 = 0.3;
// Consecutive low batch_ratio count threshold for futile waiting detection (< 0.3)
const CONSECUTIVE_LOW_BATCH_RATIO_THRESHOLD: u32 = 2; // Require 2 consecutive times < 0.3

/// Notify the event to the specified region.
pub trait PersistedNotifier: Clone + Send + 'static {
    fn notify(&self, region_id: u64, peer_id: u64, ready_number: u64);
}

impl<EK, ER> PersistedNotifier for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn notify(&self, region_id: u64, peer_id: u64, ready_number: u64) {
        if let Err(e) = self.force_send(
            region_id,
            PeerMsg::Persisted {
                peer_id,
                ready_number,
            },
        ) {
            warn!(
                "failed to send noop to trigger persisted ready";
                "region_id" => region_id,
                "peer_id" => peer_id,
                "ready_number" => ready_number,
                "error" => ?e,
            );
        }
    }
}

/// Extra writes besides raft engine.
///
/// For now, applying snapshot needs to persist some extra states. For v1,
/// these states are written to KvEngine. For v2, they are written to
/// RaftEngine. Although in v2 these states are also written to raft engine,
/// but we have to use `ExtraState` as they should be written as the last
/// updates.
// TODO: perhaps we should always pass states instead of a write batch even
// for v1.
pub enum ExtraWrite<W, L> {
    None,
    V1(W),
    V2(L),
}

impl<W: WriteBatch, L: RaftLogBatch> ExtraWrite<W, L> {
    #[inline]
    pub fn is_empty(&self) -> bool {
        match self {
            ExtraWrite::None => true,
            ExtraWrite::V1(w) => w.is_empty(),
            ExtraWrite::V2(l) => l.is_empty(),
        }
    }

    #[inline]
    fn data_size(&self) -> usize {
        match self {
            ExtraWrite::None => 0,
            ExtraWrite::V1(w) => w.data_size(),
            ExtraWrite::V2(l) => l.persist_size(),
        }
    }

    #[inline]
    pub fn ensure_v1(&mut self, write_batch: impl FnOnce() -> W) -> &mut W {
        if let ExtraWrite::None = self {
            *self = ExtraWrite::V1(write_batch());
        } else if let ExtraWrite::V2(_) = self {
            unreachable!("v1 and v2 are mixed used");
        }
        match self {
            ExtraWrite::V1(w) => w,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn v1_mut(&mut self) -> Option<&mut W> {
        if let ExtraWrite::V1(w) = self {
            Some(w)
        } else {
            None
        }
    }

    #[inline]
    pub fn ensure_v2(&mut self, log_batch: impl FnOnce() -> L) -> &mut L {
        if let ExtraWrite::None = self {
            *self = ExtraWrite::V2(log_batch());
        } else if let ExtraWrite::V1(_) = self {
            unreachable!("v1 and v2 are mixed used");
        }
        match self {
            ExtraWrite::V2(l) => l,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn merge_v2(&mut self, log_batch: L) {
        if let ExtraWrite::None = self {
            *self = ExtraWrite::V2(log_batch);
        } else if let ExtraWrite::V1(_) = self {
            unreachable!("v1 and v2 are mixed used");
        } else if let ExtraWrite::V2(l) = self {
            l.merge(log_batch).unwrap();
        }
    }

    #[inline]
    pub fn v2_mut(&mut self) -> Option<&mut L> {
        if let ExtraWrite::V2(l) = self {
            Some(l)
        } else {
            None
        }
    }
}

/// WriteTask contains write tasks which need to be persisted to kv db and raft
/// db.
pub struct WriteTask<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    region_id: u64,
    peer_id: u64,
    ready_number: u64,
    pub send_time: Instant,
    pub raft_wb: Option<ER::LogBatch>,
    // called after writing to kvdb and raftdb.
    pub persisted_cbs: Vec<Box<dyn FnOnce() + Send>>,
    overwrite_to: Option<u64>,
    entries: Vec<Entry>,
    pub raft_state: Option<RaftLocalState>,
    pub extra_write: ExtraWrite<EK::WriteBatch, ER::LogBatch>,
    pub messages: Vec<RaftMessage>,
    pub trackers: Vec<TimeTracker>,
    pub has_snapshot: bool,
    pub flushed_epoch: Option<RegionEpoch>,
}

impl<EK, ER> WriteTask<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(region_id: u64, peer_id: u64, ready_number: u64) -> Self {
        Self {
            region_id,
            peer_id,
            ready_number,
            send_time: Instant::now(),
            raft_wb: None,
            overwrite_to: None,
            entries: vec![],
            raft_state: None,
            extra_write: ExtraWrite::None,
            messages: vec![],
            trackers: vec![],
            persisted_cbs: Vec::new(),
            has_snapshot: false,
            flushed_epoch: None,
        }
    }

    pub fn has_data(&self) -> bool {
        !(self.raft_state.is_none()
            && self.entries.is_empty()
            && self.extra_write.is_empty()
            && self.raft_wb.as_ref().map_or(true, |wb| wb.is_empty()))
    }

    /// Append continous entries.
    ///
    /// All existing entries with same index will be overwritten. If
    /// `overwrite_to` is set to a larger value, then entries in
    /// `[entries.last().get_index(), overwrite_to)` will be deleted. If
    /// entries is empty, nothing will be deleted.
    pub fn set_append(&mut self, overwrite_to: Option<u64>, entries: Vec<Entry>) {
        self.entries = entries;
        self.overwrite_to = overwrite_to;
    }

    #[inline]
    pub fn ready_number(&self) -> u64 {
        self.ready_number
    }

    /// Sanity check for robustness.
    pub fn valid(&self) -> Result<()> {
        if self.region_id == 0 || self.peer_id == 0 || self.ready_number == 0 {
            return Err(box_err!(
                "invalid id, region_id {}, peer_id {}, ready_number {}",
                self.region_id,
                self.peer_id,
                self.ready_number
            ));
        }

        Ok(())
    }
}

/// Message that can be sent to Worker
pub enum WriteMsg<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    WriteTask(WriteTask<EK, ER>),
    LatencyInspect {
        send_time: Instant,
        inspector: Vec<LatencyInspector>,
    },
    Shutdown,
    #[cfg(test)]
    Pause(std::sync::mpsc::Receiver<()>),
}

impl<EK, ER> ResourceMetered for WriteMsg<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn consume_resource(&self, resource_ctl: &Arc<ResourceController>) -> Option<String> {
        if !resource_ctl.is_customized() {
            return None;
        }
        match self {
            WriteMsg::WriteTask(t) => {
                let mut dominant_group = "".to_owned();
                let mut max_write_bytes = 0;
                for entry in &t.entries {
                    let header = util::get_entry_header(entry);
                    let group_name = header.get_resource_group_name().to_owned();
                    let write_bytes = entry.compute_size() as u64;
                    resource_ctl.consume(
                        group_name.as_bytes(),
                        ResourceConsumeType::IoBytes(write_bytes),
                    );
                    if write_bytes > max_write_bytes {
                        dominant_group = group_name;
                        max_write_bytes = write_bytes;
                    }
                }
                Some(dominant_group)
            }
            _ => None,
        }
    }
}

impl<EK, ER> fmt::Debug for WriteMsg<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WriteMsg::WriteTask(t) => write!(
                fmt,
                "WriteMsg::WriteTask(region_id {} peer_id {} ready_number {})",
                t.region_id, t.peer_id, t.ready_number
            ),
            WriteMsg::Shutdown => write!(fmt, "WriteMsg::Shutdown"),
            WriteMsg::LatencyInspect { .. } => write!(fmt, "WriteMsg::LatencyInspect"),
            #[cfg(test)]
            WriteMsg::Pause(_) => write!(fmt, "WriteMsg::Pause"),
        }
    }
}

pub enum ExtraBatchWrite<W, L> {
    None,
    V1(W),
    V2(L),
}

impl<W: WriteBatch, L: RaftLogBatch> ExtraBatchWrite<W, L> {
    #[inline]
    fn clear(&mut self) {
        match self {
            ExtraBatchWrite::None => {}
            ExtraBatchWrite::V1(w) => w.clear(),
            // No clear in in `RaftLogBatch`.
            ExtraBatchWrite::V2(_) => *self = ExtraBatchWrite::None,
        }
    }

    /// Merge the extra_write with this batch.
    ///
    /// If there is any new states inserted, return the size of the state.
    fn merge(&mut self, extra_write: &mut ExtraWrite<W, L>) {
        match mem::replace(extra_write, ExtraWrite::None) {
            ExtraWrite::None => (),
            ExtraWrite::V1(wb) => match self {
                ExtraBatchWrite::None => *self = ExtraBatchWrite::V1(wb),
                ExtraBatchWrite::V1(kv_wb) => kv_wb.merge(wb).unwrap(),
                ExtraBatchWrite::V2(_) => unreachable!("v2 and v1 are mixed used"),
            },
            ExtraWrite::V2(lb) => match self {
                ExtraBatchWrite::None => *self = ExtraBatchWrite::V2(lb),
                ExtraBatchWrite::V1(_) => unreachable!("v2 and v1 are mixed used"),
                ExtraBatchWrite::V2(raft_wb) => raft_wb.merge(lb).unwrap(),
            },
        }
    }
}

/// WriteTaskBatchRecorder is a sliding window, used to record the batch size
/// and calculate the wait duration.
/// If the batch size is smaller than the threshold, it will return a
/// recommended wait duration for the caller as a hint to wait for more writes.
/// The wait duration is calculated based on the trend of the change of the
/// batch size. The range of the trend is [0.5, 2.0]. If the batch size is
/// increasing, the trend will be larger than 1.0, and the wait duration will
/// be shorter.
/// By default, the wait duration is 20us, the relative range for wait duration
/// is [10us, 40us].
struct WriteTaskBatchRecorder {
    batch_size_hint: usize,
    capacity: usize,
    history: VecDeque<usize>,
    sum: usize,
    avg: usize,
    trend: f64,
    /// Wait duration in nanoseconds.
    wait_duration: Duration,
    /// The count of wait.
    wait_count: u64,
    /// The max count of wait.
    wait_max_count: u64,
}

impl WriteTaskBatchRecorder {
    fn new(batch_size_hint: usize, wait_duration: Duration) -> Self {
        // Initialize the spin duration in advance.
        setup_for_spin_interval();
        Self {
            batch_size_hint,
            history: VecDeque::new(),
            capacity: RAFT_WB_DEFAULT_RECORDER_SIZE,
            sum: 0,
            avg: 0,
            trend: 1.0,
            wait_duration,
            wait_count: 0,
            wait_max_count: 1,
        }
    }

    #[inline]
    pub fn get_avg(&self) -> usize {
        self.avg
    }

    #[inline]
    pub fn get_batch_size_hint(&self) -> usize {
        self.batch_size_hint
    }

    #[cfg(test)]
    fn get_trend(&self) -> f64 {
        self.trend
    }

    #[inline]
    fn update_config(&mut self, batch_size: usize, wait_duration: Duration) {
        self.batch_size_hint = batch_size;
        self.wait_duration = wait_duration;
    }

    fn record(&mut self, size: usize, adaptive_batch_enabled: bool) {
        self.history.push_back(size);
        self.sum += size;

        let mut len = self.history.len();
        if len > self.capacity {
            self.sum = self
                .sum
                .saturating_sub(self.history.pop_front().unwrap_or(0));
            len = self.capacity;
        }
        self.avg = self.sum / len;

        if len >= self.capacity && self.batch_size_hint > 0 && !adaptive_batch_enabled {
            // The trend ranges from 0.5 to 2.0.
            let trend = self.avg as f64 / self.batch_size_hint as f64;
            self.trend = trend.clamp(0.5, 2.0);
        } else {
            self.trend = 1.0;
        }
    }

    #[inline]
    fn reset_wait_count(&mut self) {
        self.wait_count = 0;
    }

    #[inline]
    fn should_wait(&self, batch_size: usize) -> bool {
        batch_size < self.batch_size_hint && self.wait_count < self.wait_max_count
    }

    fn wait_for_a_while(&mut self) {
        self.wait_count += 1;
        // Use a simple linear function to calculate the wait duration.
        spin_at_least(Duration::from_nanos(
            (self.wait_duration.as_nanos() as f64 * (1.0 / self.trend)) as u64,
        ));
    }
}

/// WriteTaskBatch is used for combining several WriteTask into one.
struct WriteTaskBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    // When a single batch becomes too large, we uses multiple batches each containing atomic
    // writes.
    pub raft_wbs: Vec<ER::LogBatch>,
    // Write states once for a region everytime writing to disk.
    // These states only corresponds to entries inside `raft_wbs.last()`. States for other write
    // batches must be inlined early.
    pub raft_states: HashMap<u64, RaftLocalState>,
    pub extra_batch_write: ExtraBatchWrite<EK::WriteBatch, ER::LogBatch>,
    pub state_size: usize,
    pub tasks: Vec<WriteTask<EK, ER>>,
    pub persisted_cbs: Vec<Box<dyn FnOnce() + Send>>,
    // region_id -> (peer_id, ready_number)
    pub readies: HashMap<u64, (u64, u64)>,
    pub(crate) raft_wb_split_size: usize,
    recorder: WriteTaskBatchRecorder,
}

impl<EK, ER> WriteTaskBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(
        raft_wb: ER::LogBatch,
        write_batch_size_hint: usize,
        write_wait_duration: Duration,
    ) -> Self {
        Self {
            raft_wbs: vec![raft_wb],
            raft_states: HashMap::default(),
            extra_batch_write: ExtraBatchWrite::None,
            state_size: 0,
            tasks: vec![],
            persisted_cbs: vec![],
            readies: HashMap::default(),
            raft_wb_split_size: RAFT_WB_SPLIT_SIZE,
            recorder: WriteTaskBatchRecorder::new(write_batch_size_hint, write_wait_duration),
        }
    }

    #[inline]
    fn flush_states_to_raft_wb(&mut self) {
        let wb = self.raft_wbs.last_mut().unwrap();
        for (region_id, state) in self.raft_states.drain() {
            wb.put_raft_state(region_id, &state).unwrap();
        }
        self.state_size = 0;
        if let ExtraBatchWrite::V2(_) = self.extra_batch_write {
            let ExtraBatchWrite::V2(lb) =
                mem::replace(&mut self.extra_batch_write, ExtraBatchWrite::None)
            else {
                unreachable!()
            };
            wb.merge(lb).unwrap();
        }
    }

    /// Add write task to this batch
    fn add_write_task(&mut self, raft_engine: &ER, mut task: WriteTask<EK, ER>, adaptive_batch_enabled: bool) {
        if let Err(e) = task.valid() {
            panic!("task is not valid: {:?}", e);
        }

        if self.raft_wb_split_size > 0
            && self.raft_wbs.last().unwrap().persist_size() >= self.raft_wb_split_size
        {
            self.flush_states_to_raft_wb();
            self.raft_wbs
                .push(raft_engine.log_batch(RAFT_WB_DEFAULT_SIZE));
        }

        let raft_wb = self.raft_wbs.last_mut().unwrap();
        if let Some(wb) = task.raft_wb.take() {
            raft_wb.merge(wb).unwrap();
        }
        raft_wb
            .append(
                task.region_id,
                task.overwrite_to,
                std::mem::take(&mut task.entries),
            )
            .unwrap();

        if let Some(raft_state) = task.raft_state.take()
            && self
                .raft_states
                .insert(task.region_id, raft_state)
                .is_none()
        {
            self.state_size += std::mem::size_of::<RaftLocalState>();
        }
        self.extra_batch_write.merge(&mut task.extra_write);

        if let Some(prev_readies) = self
            .readies
            .insert(task.region_id, (task.peer_id, task.ready_number))
        {
            // The peer id must be same if they belong to the same region because
            // the peer must be destroyed after all write tasks have been finished.
            if task.peer_id != prev_readies.0 {
                panic!(
                    "task has a different peer id, region {} peer_id {} != prev_peer_id {}",
                    task.region_id, task.peer_id, prev_readies.0
                );
            }
            // The ready number must be monotonically increasing.
            if task.ready_number <= prev_readies.1 {
                panic!(
                    "task has a smaller ready number, region {} peer_id {} ready_number {} <= prev_ready_number {}",
                    task.region_id, task.peer_id, task.ready_number, prev_readies.1
                );
            }
        }
        for v in task.persisted_cbs.drain(..) {
            self.persisted_cbs.push(v);
        }
        self.tasks.push(task);
        // Record the size of the batch.
        self.recorder.record(self.get_raft_size(), adaptive_batch_enabled);
    }

    fn clear(&mut self) {
        // raft_wb doesn't have clear interface and it should be consumed by raft db
        // before
        self.raft_states.clear();
        self.extra_batch_write.clear();
        self.state_size = 0;
        self.tasks.clear();
        self.readies.clear();
        self.recorder.reset_wait_count();
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    #[inline]
    fn get_raft_size(&self) -> usize {
        self.state_size
            + self
                .raft_wbs
                .iter()
                .map(|wb| wb.persist_size())
                .sum::<usize>()
    }

    fn before_write_to_db(&mut self, metrics: &StoreWriteMetrics) {
        self.flush_states_to_raft_wb();
        if metrics.waterfall_metrics {
            let now = std::time::Instant::now();
            for task in &mut self.tasks {
                for tracker in &mut task.trackers {
                    tracker.observe(now, &metrics.wf_before_write, |t| {
                        &mut t.metrics.wf_before_write_nanos
                    });
                    tracker.reset(now);
                }
            }
        }
    }

    fn after_write_to_kv_db(&mut self, metrics: &StoreWriteMetrics) {
        if metrics.waterfall_metrics {
            let now = std::time::Instant::now();
            for task in &self.tasks {
                for tracker in &task.trackers {
                    tracker.observe(now, &metrics.wf_kvdb_end, |t| {
                        &mut t.metrics.wf_kvdb_end_nanos
                    });
                }
            }
        }
    }

    fn after_write_all(&mut self) {
        for hook in mem::take(&mut self.persisted_cbs) {
            hook();
        }
    }

    fn after_write_to_raft_db(&mut self, metrics: &StoreWriteMetrics) {
        if metrics.waterfall_metrics {
            let now = std::time::Instant::now();
            for task in &self.tasks {
                for tracker in &task.trackers {
                    tracker.observe(now, &metrics.wf_write_end, |t| {
                        &mut t.metrics.wf_write_end_nanos
                    });
                }
            }
        }
    }

    #[inline]
    fn update_config(&mut self, batch_size: usize, wait_duration: Duration) {
        self.recorder.update_config(batch_size, wait_duration);
    }

    #[inline]
    fn should_wait(&self) -> bool {
        self.recorder.should_wait(self.get_raft_size())
    }

    fn wait_for_a_while(&mut self) {
        self.recorder.wait_for_a_while();
    }
}

/// A writer that handles asynchronous writes to the storage engine.
pub struct Worker<EK, ER, N, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: PersistedNotifier,
{
    store_id: u64,
    tag: String,
    raft_engine: ER,
    kv_engine: Option<EK>,
    receiver: Receiver<WriteMsg<EK, ER>>,
    notifier: N,
    trans: T,
    batch: WriteTaskBatch<EK, ER>,
    cfg_tracker: Tracker<Config>,
    raft_write_size_limit: usize,
    metrics: StoreWriteMetrics,
    message_metrics: RaftSendMessageMetrics,
    perf_context: ER::PerfContext,
    pending_latency_inspect: Vec<(Instant, Vec<LatencyInspector>)>,
    
    // Adaptive batching related fields
    /// Whether to enable adaptive batching
    adaptive_batch_enabled: bool,
    /// Write QPS history records
    qps_history: VecDeque<u64>,
    batch_task_count_history: VecDeque<u64>,
    /// Last adaptive update time
    last_adaptive_update: Instant,
    /// Last QPS statistics time
    last_qps_stat: Instant,
    /// Write task count in current period
    current_write_tasks: u64,
    current_batch_write_tasks: u64,
    wait_count: u64,
    wasted_wait_count: u64, // For debug purpose
    valid_wait_count: u64, // For debug purpose
    /// Maximum history record count
    max_history_size: usize,
    /// QPS baseline value history records
    qps_baseline_history: VecDeque<u64>,
    /// Counter for consecutive low batch_achievement_ratio periods (< 0.3)
    consecutive_low_batch_ratio_count: u32,
    /// Dynamic threshold for high concurrency QPS (adapts to machine specs)
    /// Updated using EWMA based on observed P90 QPS in history
    /// Initial and minimum value: 45000
    high_concurrency_qps_threshold: u64,
}

impl<EK, ER, N, T> Worker<EK, ER, N, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: PersistedNotifier,
    T: Transport,
{
    pub fn new(
        store_id: u64,
        tag: String,
        raft_engine: ER,
        kv_engine: Option<EK>,
        receiver: Receiver<WriteMsg<EK, ER>>,
        notifier: N,
        trans: T,
        cfg: &Arc<VersionTrack<Config>>,
    ) -> Self {
        let batch = WriteTaskBatch::new(
            raft_engine.log_batch(RAFT_WB_DEFAULT_SIZE),
            cfg.value().raft_write_batch_size_hint.0 as usize,
            cfg.value().raft_write_wait_duration.0,
        );
        let perf_context =
            ER::get_perf_context(cfg.value().perf_level, PerfContextKind::RaftstoreStore);
        let cfg_tracker = cfg.clone().tracker(tag.clone());
        Self {
            store_id,
            tag,
            raft_engine,
            kv_engine,
            receiver,
            notifier,
            trans,
            batch,
            cfg_tracker,
            raft_write_size_limit: cfg.value().raft_write_size_limit.0 as usize,
            metrics: StoreWriteMetrics::new(cfg.value().waterfall_metrics),
            message_metrics: RaftSendMessageMetrics::default(),
            perf_context,
            pending_latency_inspect: vec![],
            
            // Adaptive batching initialization
            adaptive_batch_enabled: cfg.value().adaptive_batch_enabled,
            qps_history: VecDeque::with_capacity(100),     // Keep the latest 100 QPS samples
            batch_task_count_history: VecDeque::with_capacity(10),     // Keep the latest 10 QPS samples
            last_adaptive_update: Instant::now(),
            last_qps_stat: Instant::now(),
            current_write_tasks: 0,
            current_batch_write_tasks: 0,
            wait_count: 0,
            wasted_wait_count: 0,
            valid_wait_count: 0,
            max_history_size: 100,
            qps_baseline_history: VecDeque::with_capacity(10), // Keep the latest 10 baseline samples
            consecutive_low_batch_ratio_count: 0,
            high_concurrency_qps_threshold: ADAPTIVE_MIN_HIGH_CONCURRENCY_QPS,
        }
    }

    fn run(&mut self) {
        let mut stopped = false;
        while !stopped {
            let handle_begin = match self.receiver.recv() {
                Ok(msg) => {
                    let now = Instant::now();
                    stopped |= self.handle_msg(msg);
                    now
                }
                Err(_) => return,
            };

            // Update QPS statistics
            self.current_write_tasks += 1;
            self.current_batch_write_tasks += 1;
            
            // Check if QPS history needs to be updated
            let now = Instant::now();
            if now.saturating_duration_since(self.last_qps_stat) >= Duration::from_secs(1) {
                self.update_qps_history();
            }
            let mut last_round_is_wait = false;
            while self.batch.get_raft_size() < self.raft_write_size_limit {
                match self.receiver.try_recv() {
                    Ok(msg) => {
                        stopped |= self.handle_msg(msg);
                        self.current_write_tasks += 1;
                        self.current_batch_write_tasks += 1;
                        if last_round_is_wait {
                            self.valid_wait_count += 1;
                            last_round_is_wait = false;
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        // If the batch size is not large enough, wait for a while to make the batch larger.
                        // This can reduce IOPS amplification when there are many trivial writes.
                        if self.batch.should_wait() {
                            self.batch.wait_for_a_while();
                            self.wait_count += 1;
                            last_round_is_wait = true;
                            continue;
                        } else {
                            if last_round_is_wait {
                                self.wasted_wait_count += 1;
                            }
                            break;
                        }
                    }
                    Err(TryRecvError::Disconnected) => {
                        stopped = true;
                        break;
                    }
                };
            }

            if self.batch.is_empty() {
                self.clear_latency_inspect();
                continue;
            }

            STORE_WRITE_HANDLE_MSG_DURATION_HISTOGRAM
                .observe(duration_to_sec(handle_begin.saturating_elapsed()));

            STORE_WRITE_TRIGGER_SIZE_HISTOGRAM.observe(self.batch.get_raft_size() as f64);

            self.write_to_db(true);

            self.clear_latency_inspect();

            STORE_WRITE_LOOP_DURATION_HISTOGRAM
                .observe(duration_to_sec(handle_begin.saturating_elapsed()));
            self.update_batch_task_count_history();
            // Periodically update adaptive parameters
            self.update_adaptive_parameters();
        }
    }

    fn update_batch_task_count_history(&mut self) {
        self.batch_task_count_history.push_back(self.current_batch_write_tasks);
        // Keep history records within limit
        if self.batch_task_count_history.len() > 10 {
            self.batch_task_count_history.pop_front();
        }
        // Reset counter
        self.current_batch_write_tasks = 0;
    }

    /// Update QPS history records
    fn update_qps_history(&mut self) {
        let now = Instant::now();
        let duration = now.saturating_duration_since(self.last_qps_stat);
        if !duration.is_zero() {
            let qps = (self.current_write_tasks as f64 / duration.as_secs_f64()) as u64;
            self.qps_history.push_back(qps);
            
            // Keep history records within limit
            if self.qps_history.len() > self.max_history_size {
                self.qps_history.pop_front();
            }
        }
        
        // Reset counter
        self.current_write_tasks = 0;
        self.last_qps_stat = now;
    }
    
    /// Update adaptive parameters
    fn update_adaptive_parameters(&mut self) {
        if !self.adaptive_batch_enabled || self.qps_history.is_empty() {
            return;
        }
        
        let now = Instant::now();
        // Update parameters every 2s to allow for more stable metrics and reduce oscillations
        if now.saturating_duration_since(self.last_adaptive_update) < Duration::from_secs(2) {
            return;
        }
        
        self.last_adaptive_update = now;
        
        // Calculate median QPS for more stable metrics
        let mut sorted_qps: Vec<u64> = self.qps_history.iter().cloned().collect();
        sorted_qps.sort();
        let avg_qps = sorted_qps[sorted_qps.len() / 2]; // Median
        
        // Update baseline history
        self.update_baseline_history(avg_qps);
        
        // Get current configuration
        let current_wait_duration = self.batch.recorder.wait_duration;
        
        // Calculate adjustment factors based on QPS
        let new_wait_duration = self.calculate_adaptive_wait_duration(avg_qps, current_wait_duration);
        
        // Update configuration (only wait_duration)
        if self.adaptive_batch_enabled {
            self.batch.update_config(
                self.batch.recorder.batch_size_hint, // Keep batch_size_hint unchanged
                new_wait_duration
            );
        }
        self.wait_count = 0;
        self.wasted_wait_count = 0;
        self.valid_wait_count = 0;
    }
    
    /// Calculate adaptive wait duration based on QPS using exponential weighted moving average
    fn calculate_adaptive_wait_duration(&mut self, avg_qps: u64, current_duration: Duration) -> Duration {
        // Dynamically calculate QPS baseline using P50 (median)
        // Also update high_concurrency_qps_threshold using P90 and EWMA
        let qps_baseline = if self.qps_baseline_history.len() >= ADAPTIVE_QPS_BASELINE_HISTORY_MIN_LEN {
            let mut sorted_qps: Vec<u64> = self.qps_baseline_history.iter().cloned().collect();
            sorted_qps.sort();

            // Use P50 (median, 50th percentile) as baseline for qps_ratio and other logic
            let p50_index = sorted_qps.len() / 2;
            let p50_qps = sorted_qps[p50_index];

            // Use P90 (90th percentile) to update high_concurrency_qps_threshold
            let p90_index = (sorted_qps.len() as f64 * 0.9) as usize;
            let p90_qps = sorted_qps[p90_index.min(sorted_qps.len() - 1)];

            // Update high_concurrency_qps_threshold using EWMA if P90 > current threshold
            if p90_qps > self.high_concurrency_qps_threshold {
                self.high_concurrency_qps_threshold = (
                    (1.0 - ADAPTIVE_HIGH_CONCURRENCY_EWMA_ALPHA) * self.high_concurrency_qps_threshold as f64
                    + ADAPTIVE_HIGH_CONCURRENCY_EWMA_ALPHA * p90_qps as f64
                ) as u64;
            }
            // Ensure it doesn't drop below minimum
            self.high_concurrency_qps_threshold = self.high_concurrency_qps_threshold.max(ADAPTIVE_MIN_HIGH_CONCURRENCY_QPS);

            p50_qps // Return P50 as baseline
        } else if !self.qps_baseline_history.is_empty() {
            // If we don't have enough samples, use the minimum value
            *self.qps_baseline_history.iter().min().unwrap()
        } else {
            // Default baseline value is 1000 QPS
            ADAPTIVE_DEFAULT_QPS_BASELINE
        };
        
        // Calculate QPS ratio (relative to baseline QPS)
        let qps_ratio = if qps_baseline > 0 && avg_qps > QPS_THRESHOLD {
            avg_qps as f64 / qps_baseline as f64
        } else {
            1.0
        };
        
        let wasted_wait_ratio = if self.wait_count > 0 {
            self.wasted_wait_count as f64 / self.wait_count as f64
                } else {
            0.0
        };

        // Calculate batch size achievement ratio
        // This indicates how well we're achieving the target batch size
        let batch_size_hint = self.batch.recorder.get_batch_size_hint();
        let batch_avg = self.batch.recorder.get_avg();
        let batch_achievement_ratio = if batch_size_hint > 0 && batch_avg > 0 {
            batch_avg as f64 / batch_size_hint as f64
        } else {
            1.0
        };
        
        // New strategy: Use batch_achievement_ratio as the primary signal for adjustment
        // The key insight: low batch_achievement_ratio means we need MORE wait_duration to improve batching,
        // not less! Only reduce wait_duration when batch_achievement_ratio is high (good batching already).
        
        let current_duration_nanos = current_duration.as_nanos() as u64;
        
        // Calculate QPS pressure factor (0.0 to 1.0)
        // This helps us understand if the system is under heavy load
        // For QPS < 10000: factor = 0.0 (very light load, can be more aggressive with reduction)
        // For QPS >= high_concurrency_qps_threshold: factor = 1.0 (heavy load, need more batching)
        let qps_pressure_factor = if avg_qps <= ADAPTIVE_QPS_PRESSURE_LOWER_BOUND {
            0.0
        } else if avg_qps >= self.high_concurrency_qps_threshold {
            avg_qps as f64 / self.high_concurrency_qps_threshold as f64
        } else {
            (avg_qps - ADAPTIVE_QPS_PRESSURE_LOWER_BOUND) as f64
                / (self.high_concurrency_qps_threshold - ADAPTIVE_QPS_PRESSURE_LOWER_BOUND) as f64
        };

        // Critical insight: Distinguish between low/medium/high concurrency scenarios
        // High concurrency should be treated specially - even with low batch_ratio,
        // we should allow higher wait_duration to force aggregation and reduce IOPS
        let is_very_high_concurrency = qps_pressure_factor >= ADAPTIVE_VERY_HIGH_CONCURRENCY_MULTIPLIER;
        let is_high_concurrency = qps_pressure_factor >= 1.0;
        
        // Detect "futile waiting" - when wait_duration is high but batching is still poor
        // Use consecutive low batch_ratio count to prevent oscillation
        // Trigger futile waiting only if batch_ratio < 0.3 for CONSECUTIVE periods
        let futile_waiting_threshold = if is_very_high_concurrency {
            // Very high concurrency: allow wait_duration up to 600us before giving up
            // Even with poor batch_ratio, the sheer volume of requests means aggregation is valuable
            600_000 // 600us
        } else if is_high_concurrency {
            // High concurrency: allow up to 400us
            400_000 // 400us
        } else if batch_achievement_ratio < 0.2 {
            // Low/medium concurrency with extremely poor batching -> detect early
            200_000 // 200us
        } else if batch_achievement_ratio < 0.3 {
            // Low/medium concurrency with very poor batching
            300_000 // 300us
        } else {
            // Standard threshold
            400_000 // 400us
        };

        // Track consecutive low batch_ratio periods (< 0.3) to prevent oscillation
        let batch_ratio_is_very_low = batch_achievement_ratio < BATCH_ACHIEVEMENT_RATIO_LOW_THRESHOLD;
        if batch_ratio_is_very_low {
            self.consecutive_low_batch_ratio_count += 1;
        } else {
            // Reset counter if batch_ratio improves
            self.consecutive_low_batch_ratio_count = 0;
        }

        // Require consecutive low batch_ratio periods (>= 2) before declaring futile waiting
        let is_futile_waiting = current_duration_nanos >= futile_waiting_threshold
            && self.consecutive_low_batch_ratio_count >= CONSECUTIVE_LOW_BATCH_RATIO_THRESHOLD;
        
        let target_duration_nanos = if is_futile_waiting {
            // Futile waiting detected: high wait_duration but still poor batching
            // This means traffic is sparse and we're wasting time waiting
            // Strategy: Aggressively reduce wait_duration to improve QPS
            info!("[adaptive] Futile waiting detected: wait_duration={}us, batch_ratio={:.2}, reducing wait time", 
                  current_duration_nanos / 1000, batch_achievement_ratio);
            
            // Reduce based on how bad the batching is
            let reduction = if batch_achievement_ratio < 0.2 {
                // Extremely poor batching despite long wait -> cut wait time significantly
                0.5
            } else if batch_achievement_ratio < 0.3 {
                0.6
            } else {
                0.7
            };
            (current_duration_nanos as f64 * reduction) as u64
            
        } else if batch_achievement_ratio >= 0.8 {
            // Excellent batching (>= 80% of target)
            // We can safely reduce wait_duration to improve latency
            let reduction = if qps_pressure_factor > 0.7 {
                0.9
            } else {
                0.85
            };
            (current_duration_nanos as f64 * reduction) as u64
            
        } else if batch_achievement_ratio >= 0.6 {
            // Good batching (60-80% of target)
            // Maintain current wait_duration or reduce slightly
            let adjustment = if qps_pressure_factor > 0.7 {
                1.0
            } else {
                0.95
            };
            (current_duration_nanos as f64 * adjustment) as u64
            
        } else if batch_achievement_ratio >= 0.4 {
            // Moderate batching (40-60% of target)
            // Only increase if wait_duration is still relatively low
            if current_duration_nanos < futile_waiting_threshold {
                // Still have room to increase
                let increase = if qps_pressure_factor > 0.7 {
                    1.15
                } else {
                    1.1
                };
                (current_duration_nanos as f64 * increase) as u64
            } else {
                // Already waiting long enough, keep current
                current_duration_nanos
            }
            
        } else {
            // Poor batching (< 40% of target)
            // BUT: in high concurrency, we should still try to increase wait_duration to force aggregation
            if qps_pressure_factor < 0.1 {
                // Very low traffic, reduce wait_duration
                (current_duration_nanos as f64 * 0.7) as u64
            } else if is_very_high_concurrency {
                // Very high concurrency with poor batching
                // Continue increasing wait_duration to force aggregation
                (1.1 * current_duration_nanos as f64) as u64
            } else if current_duration_nanos < (RAFT_WB_WAIT_DURATION_NORMAL_EXPECTED_NS / 2) {
                // Normal concurrency: wait duration is very low (< 100us), try increasing
                let increase = if qps_pressure_factor > 0.7 {
                    1.3
                } else {
                    1.2
                };
                (current_duration_nanos as f64 * increase) as u64
            } else if current_duration_nanos < RAFT_WB_WAIT_DURATION_NORMAL_EXPECTED_NS {
                // Normal concurrency: wait duration is moderate (100-200us), increase conservatively
                (1.1 * current_duration_nanos as f64) as u64
            } else {
                // Normal concurrency: wait duration >= 200us, stop increasing
                current_duration_nanos
            }
        };
        
        // Use exponential weighted moving average for smoother transitions
        // Smoothing factor (α) - higher values make quicker adjustments
        const SMOOTHING_FACTOR: f64 = 0.2;
        let smoothed_duration_nanos = ((1.0 - SMOOTHING_FACTOR) * current_duration_nanos as f64 
                                     + SMOOTHING_FACTOR * target_duration_nanos as f64) as u64;
        
        info!("[adaptive adjustment] adaptive_batch_enabled: {}, update wait_duration, wait_count: {}, wasted_wait_count: {}, \
            valid_wait_count: {}, wasted_wait_ratio: {:.2}, qps_baseline(P50): {}, avg_qps: {}, qps_ratio: {:.2}, \
            batch_achievement_ratio: {:.2}, qps_pressure_factor: {:.2}, high_concurrency_qps_threshold: {}, \
            is_high_concurrency: {}, is_very_high_concurrency: {}, \
            consecutive_low_batch_ratio_count: {}, target_duration: {}μs, wait_duration: {}μs => {}μs, \
            batch_size_hint: {}, batch_avg: {}, batch_task_count_history: {:?}",
            self.adaptive_batch_enabled, self.wait_count, self.wasted_wait_count, self.valid_wait_count,
            wasted_wait_ratio, qps_baseline, avg_qps, qps_ratio, batch_achievement_ratio, qps_pressure_factor,
            self.high_concurrency_qps_threshold, is_high_concurrency, is_very_high_concurrency,
            self.consecutive_low_batch_ratio_count, target_duration_nanos / 1_000,
            current_duration.as_micros(), smoothed_duration_nanos / 1_000,
            batch_size_hint, batch_avg, self.batch_task_count_history);
        // Ensure new wait time is within reasonable range (10us to 1ms)
        Duration::from_nanos(smoothed_duration_nanos.clamp(RAFT_WB_WAIT_DURATION_LOWER_BOUND_NS, RAFT_WB_WAIT_DURATION_UPPER_BOUND_NS))
    }
    
    /// Update baseline value history records
    fn update_baseline_history(&mut self, qps: u64) {
        self.qps_baseline_history.push_back(qps);
        
        // Keep history for baseline calculation with same size as the history
        if self.qps_baseline_history.len() > self.max_history_size {
            self.qps_baseline_history.pop_front();
        }
    }

    // Returns whether it's a shutdown msg.
    fn handle_msg(&mut self, msg: WriteMsg<EK, ER>) -> bool {
        match msg {
            WriteMsg::Shutdown => return true,
            WriteMsg::WriteTask(task) => {
                debug!(
                    "handle write task";
                    "tag" => &self.tag,
                    "region_id" => task.region_id,
                    "peer_id" => task.peer_id,
                    "ready_number" => task.ready_number,
                    "extra_write_size" => task.extra_write.data_size(),
                    "raft_wb_size" => task.raft_wb.as_ref().map_or(0, |wb| wb.persist_size()),
                    "entry_count" => task.entries.len(),
                );

                self.metrics
                    .task_wait
                    .observe(duration_to_sec(task.send_time.saturating_elapsed()));
                self.handle_write_task(task);
            }
            WriteMsg::LatencyInspect {
                send_time,
                inspector,
            } => {
                self.pending_latency_inspect.push((send_time, inspector));
            }
            #[cfg(test)]
            WriteMsg::Pause(rx) => {
                let _ = rx.recv();
            }
        }
        false
    }

    pub fn handle_write_task(&mut self, task: WriteTask<EK, ER>) {
        self.batch.add_write_task(&self.raft_engine, task, self.adaptive_batch_enabled);
    }

    pub fn write_to_db(&mut self, notify: bool) {
        if self.batch.is_empty() {
            return;
        }

        let timer = Instant::now();

        self.batch.before_write_to_db(&self.metrics);

        fail_point!("raft_before_save");

        let store_id = self.store_id;
        fail_point!("raft_before_persist_on_store_1", store_id == 1, |_| {});

        let mut write_kv_time = 0f64;
        if let ExtraBatchWrite::V1(kv_wb) = &mut self.batch.extra_batch_write {
            if !kv_wb.is_empty() {
                let raft_before_save_kv_on_store_3 = || {
                    fail_point!("raft_before_save_kv_on_store_3", store_id == 3, |_| {});
                };
                raft_before_save_kv_on_store_3();
                let now = Instant::now();
                let mut write_opts = WriteOptions::new();
                write_opts.set_sync(true);
                // TODO: Add perf context
                let tag = &self.tag;
                kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
                    panic!(
                        "store {}: {} failed to write to kv engine: {:?}",
                        store_id, tag, e
                    );
                });
                if kv_wb.data_size() > KV_WB_SHRINK_SIZE {
                    *kv_wb = self
                        .kv_engine
                        .as_ref()
                        .unwrap()
                        .write_batch_with_cap(KV_WB_DEFAULT_SIZE);
                }
                write_kv_time = duration_to_sec(now.saturating_elapsed());
                STORE_WRITE_KVDB_DURATION_HISTOGRAM.observe(write_kv_time);
            }
            self.batch.after_write_to_kv_db(&self.metrics);
        }
        fail_point!("raft_between_save");

        let mut write_raft_time = 0f64;
        if !self.batch.raft_wbs[0].is_empty() {
            fail_point!("raft_before_save_on_store_1", self.store_id == 1, |_| {});

            let now = Instant::now();
            self.perf_context.start_observe();
            for i in 0..self.batch.raft_wbs.len() {
                self.raft_engine
                    .consume_and_shrink(
                        &mut self.batch.raft_wbs[i],
                        true,
                        RAFT_WB_SHRINK_SIZE,
                        RAFT_WB_DEFAULT_SIZE,
                    )
                    .unwrap_or_else(|e| {
                        panic!(
                            "store {}: {} failed to write to raft engine: {:?}",
                            self.store_id, self.tag, e
                        );
                    });
            }
            self.batch.raft_wbs.truncate(1);
            let trackers: Vec<_> = self
                .batch
                .tasks
                .iter()
                .flat_map(|task| task.trackers.iter().flat_map(|t| t.as_tracker_token()))
                .collect();
            self.perf_context.report_metrics(&trackers);
            write_raft_time = duration_to_sec(now.saturating_elapsed());
            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(write_raft_time);
            debug!("raft log is persisted";
                "req_info" => TrackerTokenArray::new(trackers.as_slice()));
        }

        fail_point!("raft_after_save");

        self.batch.after_write_to_raft_db(&self.metrics);

        fail_point!(
            "async_write_before_cb",
            !self.batch.persisted_cbs.is_empty(),
            |_| ()
        );
        self.batch.after_write_all();

        fail_point!("raft_before_follower_send");

        let mut now = Instant::now();
        for task in &mut self.batch.tasks {
            for msg in task.messages.drain(..) {
                let msg_type = msg.get_message().get_msg_type();
                let to_peer_id = msg.get_to_peer().get_id();
                let to_store_id = msg.get_to_peer().get_store_id();

                debug!(
                    "send raft msg in write thread";
                    "tag" => &self.tag,
                    "region_id" => task.region_id,
                    "peer_id" => task.peer_id,
                    "msg_type" => %util::MsgType(&msg),
                    "msg_size" => msg.get_message().compute_size(),
                    "to" => to_peer_id,
                    "disk_usage" => ?msg.get_disk_usage(),
                );

                if let Err(e) = self.trans.send(msg) {
                    // We use metrics to observe failure on production.
                    debug!(
                        "failed to send msg to other peer in async-writer";
                        "region_id" => task.region_id,
                        "peer_id" => task.peer_id,
                        "target_peer_id" => to_peer_id,
                        "target_store_id" => to_store_id,
                        "err" => ?e,
                        "error_code" => %e.error_code(),
                    );
                    self.message_metrics.add(msg_type, false);
                    // If this msg is snapshot, it is unnecessary to send
                    // snapshot status to this peer because it has already
                    // become follower. (otherwise the snapshot msg should be
                    // sent in store thread other than here) Also, the follower
                    // don't need flow control, so don't send unreachable msg
                    // here.
                } else {
                    self.message_metrics.add(msg_type, true);
                }
            }
        }
        if self.trans.need_flush() {
            self.trans.flush();
            self.message_metrics.flush();
        }
        let now2 = Instant::now();
        let send_time = duration_to_sec(now2.saturating_duration_since(now));
        STORE_WRITE_SEND_DURATION_HISTOGRAM.observe(send_time);

        let mut callback_time = 0f64;
        if notify {
            for (region_id, (peer_id, ready_number)) in &self.batch.readies {
                self.notifier.notify(*region_id, *peer_id, *ready_number);
            }
            now = Instant::now();
            callback_time = duration_to_sec(now.saturating_duration_since(now2));
            STORE_WRITE_CALLBACK_DURATION_HISTOGRAM.observe(callback_time);
        }

        let total_cost = now.saturating_duration_since(timer);
        STORE_WRITE_TO_DB_DURATION_HISTOGRAM.observe(duration_to_sec(total_cost));

        slow_log!(
            total_cost,
            "[store {}] async write too slow, write_kv: {}s, write_raft: {}s, send: {}s, callback: {}s thread: {}",
            self.store_id,
            write_kv_time,
            write_raft_time,
            send_time,
            callback_time,
            self.tag
        );

        self.batch.clear();
        self.metrics.flush();

        // update config
        if let Some(incoming) = self.cfg_tracker.any_new() {
            self.raft_write_size_limit = incoming.raft_write_size_limit.0 as usize;
            self.adaptive_batch_enabled = incoming.adaptive_batch_enabled;
            self.metrics.waterfall_metrics = incoming.waterfall_metrics;
            // Update the batch configuration with new values if adaptive batching is disabled
            if !self.adaptive_batch_enabled {
                self.batch.update_config(
                    incoming.raft_write_batch_size_hint.0 as usize,
                    incoming.raft_write_wait_duration.0,
                );
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }

    fn clear_latency_inspect(&mut self) {
        if self.pending_latency_inspect.is_empty() {
            return;
        }
        let now = Instant::now();
        for (time, inspectors) in std::mem::take(&mut self.pending_latency_inspect) {
            for mut inspector in inspectors {
                inspector.record_store_write(now.saturating_duration_since(time));
                inspector.finish();
            }
        }
    }
}

#[derive(Clone)]
pub struct StoreWritersContext<EK, ER, T, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport + 'static,
    N: PersistedNotifier,
{
    pub store_id: u64,
    pub raft_engine: ER,
    pub kv_engine: Option<EK>,
    pub transfer: T,
    pub notifier: N,
    pub cfg: Arc<VersionTrack<Config>>,
}

#[derive(Clone)]
pub struct StoreWriters<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    resource_ctl: Option<Arc<ResourceController>>,
    /// Mailboxes for sending raft messages to async ios.
    writers: Arc<VersionTrack<SharedSenders<EK, ER>>>,
    /// Background threads for handling asynchronous messages.
    handlers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl<EK: KvEngine, ER: RaftEngine> StoreWriters<EK, ER> {
    pub fn new(resource_ctl: Option<Arc<ResourceController>>) -> Self {
        Self {
            resource_ctl,
            writers: Arc::new(VersionTrack::default()),
            handlers: Arc::new(Mutex::new(vec![])),
        }
    }
}

impl<EK, ER> StoreWriters<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn senders(&self) -> WriteSenders<EK, ER> {
        WriteSenders::new(self.writers.clone())
    }

    pub fn spawn<T: Transport + 'static, N: PersistedNotifier>(
        &mut self,
        store_id: u64,
        raft_engine: ER,
        kv_engine: Option<EK>,
        notifier: &N,
        trans: &T,
        cfg: &Arc<VersionTrack<Config>>,
    ) -> Result<()> {
        let pool_size = cfg.value().store_io_pool_size;
        if pool_size > 0 {
            self.increase_to(
                pool_size,
                StoreWritersContext {
                    store_id,
                    notifier: notifier.clone(),
                    raft_engine,
                    kv_engine,
                    transfer: trans.clone(),
                    cfg: cfg.clone(),
                },
            )?;
        }
        Ok(())
    }

    pub fn shutdown(&mut self) {
        let mut handlers = self.handlers.lock();
        let writers = self.writers.value().get();
        assert_eq!(writers.len(), handlers.len());
        for (i, handler) in handlers.drain(..).enumerate() {
            info!("stopping store writer {}", i);
            writers[i].send(WriteMsg::Shutdown, None).unwrap();
            handler.join().unwrap();
        }
    }

    #[inline]
    /// Returns the valid size of store writers.
    pub fn size(&self) -> usize {
        self.writers.value().get().len()
    }

    pub fn decrease_to(&mut self, size: usize) -> Result<()> {
        // Only update logical version of writers but not destroying the workers, so
        // that peers that are still using the writer_id (because there're
        // unpersisted tasks) can proceed to finish their tasks. After the peer
        // gets rescheduled, it will use a new writer_id within the new
        // capacity, specified by refreshed `store-io-pool-size`.
        //
        // TODO: find an elegant way to effectively free workers.
        assert_eq!(self.writers.value().get().len(), self.handlers.lock().len());
        self.writers
            .update(move |writers: &mut SharedSenders<EK, ER>| -> Result<()> {
                assert!(writers.get().len() > size);
                Ok(())
            })?;
        Ok(())
    }

    pub fn increase_to<T: Transport + 'static, N: PersistedNotifier>(
        &mut self,
        size: usize,
        writer_meta: StoreWritersContext<EK, ER, T, N>,
    ) -> Result<()> {
        let mut handlers = self.handlers.lock();
        let current_size = self.writers.value().get().len();
        assert_eq!(current_size, handlers.len());
        let resource_ctl = self.resource_ctl.clone();
        self.writers
            .update(move |writers: &mut SharedSenders<EK, ER>| -> Result<()> {
                let mut cached_senders = writers.get();
                for i in current_size..size {
                    let tag = format!("store-writer-{}", i);
                    let (tx, rx) = bounded(
                        resource_ctl.clone(),
                        writer_meta.cfg.value().store_io_notify_capacity,
                    );
                    let mut worker = Worker::new(
                        writer_meta.store_id,
                        tag.clone(),
                        writer_meta.raft_engine.clone(),
                        writer_meta.kv_engine.clone(),
                        rx,
                        writer_meta.notifier.clone(),
                        writer_meta.transfer.clone(),
                        &writer_meta.cfg,
                    );
                    info!("starting store writer {}", i);
                    let t =
                        thread::Builder::new()
                            .name(thd_name!(tag))
                            .spawn_wrapper(move || {
                                set_io_type(IoType::ForegroundWrite);
                                worker.run();
                            })?;
                    cached_senders.push(tx);
                    handlers.push(t);
                }
                writers.set(cached_senders);
                Ok(())
            })?;
        Ok(())
    }
}

/// Used for test to write task to kv db and raft db.
pub fn write_to_db_for_test<EK, ER>(
    engines: &engine_traits::Engines<EK, ER>,
    task: WriteTask<EK, ER>,
) where
    EK: KvEngine,
    ER: RaftEngine,
{
    let mut batch = WriteTaskBatch::new(
        engines.raft.log_batch(RAFT_WB_DEFAULT_SIZE),
        0,
        Duration::default(),
    );
    batch.add_write_task(&engines.raft, task, false);
    let metrics = StoreWriteMetrics::new(false);
    batch.before_write_to_db(&metrics);
    if let ExtraBatchWrite::V1(kv_wb) = &mut batch.extra_batch_write {
        if !kv_wb.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
                panic!("test failed to write to kv engine: {:?}", e);
            });
        }
    }
    if !batch.raft_wbs[0].is_empty() {
        for wb in &mut batch.raft_wbs {
            engines.raft.consume(wb, true).unwrap_or_else(|e| {
                panic!("test failed to write to raft engine: {:?}", e);
            });
        }
    }
    batch.after_write_to_raft_db(&metrics);
    batch.after_write_all();
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
