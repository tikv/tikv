// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! The implementation of asynchronous write for raftstore.
//!
//! `WriteTask` is the unit of write which is created by a certain
//! peer. Afterwards the `WriteTask` should be sent to `Worker`.
//! The `Worker` is responsible for persisting `WriteTask` to kv db or
//! raft db and then invoking callback or sending msgs if any.

use std::{
    fmt,
    sync::Arc,
    thread::{self, JoinHandle},
};

use collections::HashMap;
use crossbeam::channel::{bounded, Receiver, Sender, TryRecvError};
use engine_traits::{
    Engines, KvEngine, PerfContext, PerfContextKind, RaftEngine, RaftLogBatch, WriteBatch,
    WriteOptions,
};
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvproto::raft_serverpb::{RaftLocalState, RaftMessage};
use protobuf::Message;
use raft::eraftpb::Entry;
use tikv_util::{
    box_err,
    config::{Tracker, VersionTrack},
    debug, info, slow_log,
    sys::thread::StdThreadBuildWrapper,
    thd_name,
    time::{duration_to_sec, Instant},
    warn,
};

use crate::{
    store::{
        config::Config,
        fsm::RaftRouter,
        local_metrics::{RaftSendMessageMetrics, StoreWriteMetrics, TimeTracker},
        metrics::*,
        transport::Transport,
        util::LatencyInspector,
        PeerMsg,
    },
    Result,
};

const KV_WB_SHRINK_SIZE: usize = 1024 * 1024;
const KV_WB_DEFAULT_SIZE: usize = 16 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 10 * 1024 * 1024;
const RAFT_WB_DEFAULT_SIZE: usize = 256 * 1024;

/// Notify the event to the specified region.
pub trait Notifier: Clone + Send + 'static {
    fn notify_persisted(&self, region_id: u64, peer_id: u64, ready_number: u64);
}

impl<EK, ER> Notifier for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn notify_persisted(&self, region_id: u64, peer_id: u64, ready_number: u64) {
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

/// WriteTask contains write tasks which need to be persisted to kv db and raft db.
pub struct WriteTask<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    region_id: u64,
    peer_id: u64,
    ready_number: u64,
    pub send_time: Instant,
    pub kv_wb: Option<EK::WriteBatch>,
    pub raft_wb: Option<ER::LogBatch>,
    pub entries: Vec<Entry>,
    pub cut_logs: Option<(u64, u64)>,
    pub raft_state: Option<RaftLocalState>,
    pub messages: Vec<RaftMessage>,
    pub trackers: Vec<TimeTracker>,
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
            kv_wb: None,
            raft_wb: None,
            entries: vec![],
            cut_logs: None,
            raft_state: None,
            messages: vec![],
            trackers: vec![],
        }
    }

    pub fn has_data(&self) -> bool {
        !(self.raft_state.is_none()
            && self.entries.is_empty()
            && self.cut_logs.is_none()
            && self.kv_wb.as_ref().map_or(true, |wb| wb.is_empty())
            && self.raft_wb.as_ref().map_or(true, |wb| wb.is_empty()))
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
        if let Some(last_index) = self.entries.last().map(|e| e.get_index()) {
            if let Some((from, _)) = self.cut_logs {
                if from != last_index + 1 {
                    // Entries are put and deleted in the same writebatch.
                    return Err(box_err!(
                        "invalid cut logs, last_index {}, cut_logs {:?}",
                        last_index,
                        self.cut_logs
                    ));
                }
            }
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
        }
    }
}

/// WriteTaskBatch is used for combining several WriteTask into one.
struct WriteTaskBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub kv_wb: EK::WriteBatch,
    pub raft_wb: ER::LogBatch,
    // Write raft state once for a region everytime writing to disk
    pub raft_states: HashMap<u64, RaftLocalState>,
    pub state_size: usize,
    pub tasks: Vec<WriteTask<EK, ER>>,
    // region_id -> (peer_id, ready_number)
    pub readies: HashMap<u64, (u64, u64)>,
}

impl<EK, ER> WriteTaskBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(kv_wb: EK::WriteBatch, raft_wb: ER::LogBatch) -> Self {
        Self {
            kv_wb,
            raft_wb,
            raft_states: HashMap::default(),
            state_size: 0,
            tasks: vec![],
            readies: HashMap::default(),
        }
    }

    /// Add write task to this batch
    fn add_write_task(&mut self, mut task: WriteTask<EK, ER>) {
        if let Err(e) = task.valid() {
            panic!("task is not valid: {:?}", e);
        }
        if let Some(kv_wb) = task.kv_wb.take() {
            self.kv_wb.merge(kv_wb).unwrap();
        }
        if let Some(raft_wb) = task.raft_wb.take() {
            self.raft_wb.merge(raft_wb).unwrap();
        }

        let entries = std::mem::take(&mut task.entries);
        self.raft_wb.append(task.region_id, entries).unwrap();
        if let Some((from, to)) = task.cut_logs {
            self.raft_wb.cut_logs(task.region_id, from, to);
        }

        if let Some(raft_state) = task.raft_state.take() {
            if self
                .raft_states
                .insert(task.region_id, raft_state)
                .is_none()
            {
                self.state_size += std::mem::size_of::<RaftLocalState>();
            }
        }

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

        self.tasks.push(task);
    }

    fn clear(&mut self) {
        // raft_wb doesn't have clear interface and it should be consumed by raft db before
        self.kv_wb.clear();
        self.raft_states.clear();
        self.state_size = 0;
        self.tasks.clear();
        self.readies.clear();
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    #[inline]
    fn get_raft_size(&self) -> usize {
        self.state_size + self.raft_wb.persist_size()
    }

    fn before_write_to_db(&mut self, metrics: &StoreWriteMetrics) {
        // Put raft state to raft writebatch
        for (region_id, state) in self.raft_states.drain() {
            self.raft_wb.put_raft_state(region_id, &state).unwrap();
        }
        self.state_size = 0;
        if metrics.waterfall_metrics {
            let now = std::time::Instant::now();
            for task in &self.tasks {
                for tracker in &task.trackers {
                    tracker.observe(now, &metrics.wf_before_write, |t| {
                        &mut t.metrics.wf_before_write_nanos
                    });
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
}

pub struct Worker<EK, ER, N, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: Notifier,
{
    store_id: u64,
    tag: String,
    engines: Engines<EK, ER>,
    receiver: Receiver<WriteMsg<EK, ER>>,
    notifier: N,
    trans: T,
    batch: WriteTaskBatch<EK, ER>,
    cfg_tracker: Tracker<Config>,
    raft_write_size_limit: usize,
    metrics: StoreWriteMetrics,
    message_metrics: RaftSendMessageMetrics,
    perf_context: EK::PerfContext,
    pending_latency_inspect: Vec<(Instant, Vec<LatencyInspector>)>,
}

impl<EK, ER, N, T> Worker<EK, ER, N, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: Notifier,
    T: Transport,
{
    pub fn new(
        store_id: u64,
        tag: String,
        engines: Engines<EK, ER>,
        receiver: Receiver<WriteMsg<EK, ER>>,
        notifier: N,
        trans: T,
        cfg: &Arc<VersionTrack<Config>>,
    ) -> Self {
        let batch = WriteTaskBatch::new(
            engines.kv.write_batch_with_cap(KV_WB_DEFAULT_SIZE),
            engines.raft.log_batch(RAFT_WB_DEFAULT_SIZE),
        );
        let perf_context = engines
            .kv
            .get_perf_context(cfg.value().perf_level, PerfContextKind::RaftstoreStore);
        let cfg_tracker = cfg.clone().tracker(tag.clone());
        Self {
            store_id,
            tag,
            engines,
            receiver,
            notifier,
            trans,
            batch,
            cfg_tracker,
            raft_write_size_limit: cfg.value().raft_write_size_limit.0 as usize,
            metrics: StoreWriteMetrics::new(cfg.value().waterfall_metrics),
            message_metrics: Default::default(),
            perf_context,
            pending_latency_inspect: vec![],
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

            while self.batch.get_raft_size() < self.raft_write_size_limit {
                match self.receiver.try_recv() {
                    Ok(msg) => {
                        stopped |= self.handle_msg(msg);
                    }
                    Err(TryRecvError::Empty) => break,
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
                    "kv_wb_size" => task.kv_wb.as_ref().map_or(0, |wb| wb.data_size()),
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
        }
        false
    }

    pub fn handle_write_task(&mut self, task: WriteTask<EK, ER>) {
        self.batch.add_write_task(task);
    }

    pub fn write_to_db(&mut self, notify: bool) {
        if self.batch.is_empty() {
            return;
        }

        let timer = Instant::now();

        self.batch.before_write_to_db(&self.metrics);

        fail_point!("raft_before_save");

        let mut write_kv_time = 0f64;
        if !self.batch.kv_wb.is_empty() {
            let raft_before_save_kv_on_store_3 = || {
                fail_point!("raft_before_save_kv_on_store_3", self.store_id == 3, |_| {});
            };
            raft_before_save_kv_on_store_3();
            let now = Instant::now();
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            // TODO: Add perf context
            self.batch.kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
                panic!(
                    "store {}: {} failed to write to kv engine: {:?}",
                    self.store_id, self.tag, e
                );
            });
            if self.batch.kv_wb.data_size() > KV_WB_SHRINK_SIZE {
                self.batch.kv_wb = self.engines.kv.write_batch_with_cap(KV_WB_DEFAULT_SIZE);
            }
            write_kv_time = duration_to_sec(now.saturating_elapsed());
            STORE_WRITE_KVDB_DURATION_HISTOGRAM.observe(write_kv_time);
        }

        self.batch.after_write_to_kv_db(&self.metrics);

        fail_point!("raft_between_save");

        let mut write_raft_time = 0f64;
        if !self.batch.raft_wb.is_empty() {
            fail_point!("raft_before_save_on_store_1", self.store_id == 1, |_| {});

            let now = Instant::now();
            self.perf_context.start_observe();
            self.engines
                .raft
                .consume_and_shrink(
                    &mut self.batch.raft_wb,
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
            let trackers: Vec<_> = self
                .batch
                .tasks
                .iter()
                .flat_map(|task| task.trackers.iter().flat_map(|t| t.as_tracker_token()))
                .collect();
            // TODO: Add a different perf context for raft engine.
            self.perf_context.report_metrics(&trackers);
            write_raft_time = duration_to_sec(now.saturating_elapsed());
            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(write_raft_time);
        }

        fail_point!("raft_after_save");

        self.batch.after_write_to_raft_db(&self.metrics);

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
                    "msg_type" => ?msg_type,
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
                    // If this msg is snapshot, it is unnecessary to send snapshot
                    // status to this peer because it has already become follower.
                    // (otherwise the snapshot msg should be sent in store thread other than here)
                    // Also, the follower don't need flow control, so don't send
                    // unreachable msg here.
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
                self.notifier
                    .notify_persisted(*region_id, *peer_id, *ready_number);
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
            self.metrics.waterfall_metrics = incoming.waterfall_metrics;
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

pub struct StoreWriters<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    writers: Vec<Sender<WriteMsg<EK, ER>>>,
    handlers: Vec<JoinHandle<()>>,
}

impl<EK, ER> StoreWriters<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new() -> Self {
        Self {
            writers: vec![],
            handlers: vec![],
        }
    }

    pub fn senders(&self) -> &Vec<Sender<WriteMsg<EK, ER>>> {
        &self.writers
    }

    pub fn spawn<T: Transport + 'static, N: Notifier>(
        &mut self,
        store_id: u64,
        engines: &Engines<EK, ER>,
        notifier: &N,
        trans: &T,
        cfg: &Arc<VersionTrack<Config>>,
    ) -> Result<()> {
        let pool_size = cfg.value().store_io_pool_size;
        for i in 0..pool_size {
            let tag = format!("store-writer-{}", i);
            let (tx, rx) = bounded(cfg.value().store_io_notify_capacity);
            let mut worker = Worker::new(
                store_id,
                tag.clone(),
                engines.clone(),
                rx,
                notifier.clone(),
                trans.clone(),
                cfg,
            );
            info!("starting store writer {}", i);
            let t = thread::Builder::new()
                .name(thd_name!(tag))
                .spawn_wrapper(move || {
                    worker.run();
                })?;
            self.writers.push(tx);
            self.handlers.push(t);
        }
        Ok(())
    }

    pub fn shutdown(&mut self) {
        assert_eq!(self.writers.len(), self.handlers.len());
        for (i, handler) in self.handlers.drain(..).enumerate() {
            info!("stopping store writer {}", i);
            self.writers[i].send(WriteMsg::Shutdown).unwrap();
            handler.join().unwrap();
        }
    }
}

/// Used for test to write task to kv db and raft db.
#[cfg(test)]
pub fn write_to_db_for_test<EK, ER>(engines: &Engines<EK, ER>, task: WriteTask<EK, ER>)
where
    EK: KvEngine,
    ER: RaftEngine,
{
    let mut batch = WriteTaskBatch::new(
        engines.kv.write_batch(),
        engines.raft.log_batch(RAFT_WB_DEFAULT_SIZE),
    );
    batch.add_write_task(task);
    batch.before_write_to_db(&StoreWriteMetrics::new(false));
    if !batch.kv_wb.is_empty() {
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        batch.kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
            panic!("test failed to write to kv engine: {:?}", e);
        });
    }
    if !batch.raft_wb.is_empty() {
        engines
            .raft
            .consume(&mut batch.raft_wb, true)
            .unwrap_or_else(|e| {
                panic!("test failed to write to raft engine: {:?}", e);
            });
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
