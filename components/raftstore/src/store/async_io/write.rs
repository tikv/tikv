// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(dead_code)]
// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::store::config::Config;
use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::{RaftSendMessageMetrics, StoreWriteMetrics};
use crate::store::metrics::*;
use crate::store::transport::Transport;
use crate::store::{PeerMsg, SignificantMsg};
use crate::Result;

use collections::{HashMap, HashSet};
use crossbeam::channel::{bounded, unbounded, Receiver, Sender, TryRecvError};
use crossbeam::utils::CachePadded;
use engine_traits::{
    Engines, KvEngine, PerfContext, PerfContextKind, RaftEngine, RaftLogBatch, WriteBatch,
    WriteOptions,
};
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvproto::raft_serverpb::{RaftLocalState, RaftMessage};
use raft::eraftpb::Entry;
use tikv_util::time::{duration_to_sec, Instant};
use tikv_util::{box_err, debug, thd_name, warn};

const KV_WB_SHRINK_SIZE: usize = 1024 * 1024;
const KV_WB_DEFAULT_SIZE: usize = 16 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 10 * 1024 * 1024;
const RAFT_WB_DEFAULT_SIZE: usize = 256 * 1024;

/// Notify the event to the specified region.
pub trait Notifier: Clone + Send + 'static {
    fn notify_persisted(&self, region_id: u64, peer_id: u64, ready_number: u64, now: Instant);
    fn notify_unreachable(&self, region_id: u64, to_peer_id: u64);
}

impl<EK, ER> Notifier for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn notify_persisted(
        &self,
        region_id: u64,
        peer_id: u64,
        ready_number: u64,
        send_time: Instant,
    ) {
        if let Err(e) = self.force_send(
            region_id,
            PeerMsg::Persisted {
                peer_id,
                ready_number,
                send_time,
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

    fn notify_unreachable(&self, region_id: u64, to_peer_id: u64) {
        let msg = SignificantMsg::Unreachable {
            region_id,
            to_peer_id,
        };
        if let Err(e) = self.force_send(region_id, PeerMsg::SignificantMsg(msg)) {
            warn!(
                "failed to send unreachable";
                "region_id" => region_id,
                "unreachable_peer_id" => to_peer_id,
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
    pub request_times: Vec<Instant>,
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
            request_times: vec![],
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
        let last_index = self.entries.last().map_or(0, |e| e.get_index());
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
        if last_index != 0
            && self
                .raft_state
                .as_ref()
                .map_or(true, |r| r.get_last_index() != last_index)
        {
            return Err(box_err!(
                "invalid raft state, last_index {}, raft_state {:?}",
                last_index,
                self.raft_state
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
    Shutdown,
    Persisted,
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
            WriteMsg::Persisted => write!(fmt, "WriteMsg::Persisted"),
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
            tasks: vec![],
            state_size: 0,
        }
    }

    /// Add write task to this batch
    fn add_write_task(&mut self, mut task: WriteTask<EK, ER>) {
        if let Err(e) = task.valid() {
            panic!("task is not valid: {:?}", e);
        }
        if let Some(kv_wb) = task.kv_wb.take() {
            self.kv_wb.merge(kv_wb);
        }
        if let Some(raft_wb) = task.raft_wb.take() {
            self.raft_wb.merge(raft_wb);
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
        self.tasks.push(task);
    }

    fn clear(&mut self) {
        // raft_wb doesn't have clear interface and it should be consumed by raft db before
        self.kv_wb.clear();
        self.raft_states.clear();
        self.tasks.clear();
        self.state_size = 0;
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
        let raft_states = std::mem::take(&mut self.raft_states);
        for (region_id, state) in raft_states {
            self.raft_wb.put_raft_state(region_id, &state).unwrap();
        }
        self.state_size = 0;
        if metrics.waterfall_metrics {
            let now = Instant::now();
            for task in &self.tasks {
                for t in &task.request_times {
                    metrics
                        .before_write
                        .observe(duration_to_sec(now.saturating_duration_since(*t)));
                }
            }
        }
    }

    fn after_write_to_kv_db(&mut self, metrics: &StoreWriteMetrics) {
        if metrics.waterfall_metrics {
            let now = Instant::now();
            for task in &self.tasks {
                for t in &task.request_times {
                    metrics
                        .kvdb_end
                        .observe(duration_to_sec(now.saturating_duration_since(*t)));
                }
            }
        }
    }

    fn after_write_to_db(&mut self, metrics: &StoreWriteMetrics) {
        if metrics.waterfall_metrics {
            let now = Instant::now();
            for task in &self.tasks {
                for t in &task.request_times {
                    metrics
                        .write_end
                        .observe(duration_to_sec(now.saturating_duration_since(*t)))
                }
            }
        }
    }
}

struct Worker<EK, ER, T, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport,
    N: Notifier,
{
    store_id: u64,
    tag: String,
    worker_id: usize,
    engines: Engines<EK, ER>,
    receiver: Receiver<WriteMsg<EK, ER>>,
    notifier: N,
    trans: T,
    batch: WriteTaskBatch<EK, ER>,
    pending_tasks: VecDeque<(u64, Vec<WriteTask<EK, ER>>)>,
    // Last id of the pending task
    last_unpersisted_id: u64,
    // Last id of the known persisted task
    last_persisted_id: u64,
    // Used for getting the latest persisted id from sync worker
    global_persisted_id: Arc<CachePadded<AtomicU64>>,
    sync_sender: Sender<SyncMsg>,
    raft_write_size_limit: usize,
    metrics: StoreWriteMetrics,
    message_metrics: RaftSendMessageMetrics,
    perf_context: EK::PerfContext,
}

impl<EK, ER, T, N> Worker<EK, ER, T, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport,
    N: Notifier,
{
    fn new(
        store_id: u64,
        tag: String,
        worker_id: usize,
        engines: Engines<EK, ER>,
        receiver: Receiver<WriteMsg<EK, ER>>,
        notifier: N,
        trans: T,
        global_persisted_id: Arc<CachePadded<AtomicU64>>,
        sync_sender: Sender<SyncMsg>,
        config: &Config,
    ) -> Self {
        let batch = WriteTaskBatch::new(
            engines.kv.write_batch_with_cap(KV_WB_DEFAULT_SIZE),
            engines.raft.log_batch(RAFT_WB_DEFAULT_SIZE),
        );
        let perf_context = engines
            .kv
            .get_perf_context(config.perf_level, PerfContextKind::RaftstoreStore);
        Self {
            store_id,
            tag,
            worker_id,
            engines,
            receiver,
            notifier,
            trans,
            batch,
            pending_tasks: VecDeque::new(),
            last_unpersisted_id: 0,
            last_persisted_id: 0,
            global_persisted_id,
            sync_sender,
            raft_write_size_limit: config.raft_write_size_limit.0 as usize,
            metrics: StoreWriteMetrics::new(config.store_waterfall_metrics),
            message_metrics: Default::default(),
            perf_context,
        }
    }

    fn run(&mut self) {
        let mut notify_ready_buffer = HashMap::default();
        loop {
            let loop_begin = Instant::now();
            let mut handle_begin = loop_begin;

            let mut after_persist = Duration::from_micros(0);
            let mut first_time = true;
            while self.batch.get_raft_size() < self.raft_write_size_limit {
                let msg = if first_time {
                    first_time = false;
                    match self.receiver.recv() {
                        Ok(msg) => {
                            self.metrics
                                .task_gen
                                .observe(duration_to_sec(loop_begin.saturating_elapsed()));
                            handle_begin = Instant::now();
                            msg
                        }
                        Err(_) => return,
                    }
                } else {
                    match self.receiver.try_recv() {
                        Ok(msg) => msg,
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => return,
                    }
                };
                if self.last_persisted_id < self.global_persisted_id.load(Ordering::Acquire) {
                    self.last_persisted_id = self.global_persisted_id.load(Ordering::Acquire);
                    let now = Instant::now();
                    self.after_persist(&mut notify_ready_buffer);
                    after_persist += now.saturating_elapsed();
                }
                match msg {
                    WriteMsg::Shutdown => return,
                    WriteMsg::WriteTask(task) => {
                        self.metrics
                            .task_wait
                            .observe(duration_to_sec(task.send_time.saturating_elapsed()));
                        self.batch.add_write_task(task);
                    }
                    WriteMsg::Persisted => (),
                }
            }

            let now = Instant::now();
            STORE_WRITE_HANDLE_MSG_DURATION_HISTOGRAM.observe(duration_to_sec(
                now.saturating_duration_since(handle_begin)
                    .saturating_sub(after_persist),
            ));

            if self.trans.need_flush() {
                self.trans.flush();
                self.message_metrics.flush();
                STORE_WRITE_FLUSH_SEND_DURATION_HISTOGRAM
                    .observe(duration_to_sec(now.saturating_elapsed()));
            }

            if self.batch.is_empty() {
                continue;
            }

            STORE_WRITE_TRIGGER_SIZE_HISTOGRAM.observe(self.batch.get_raft_size() as f64);

            self.write_to_db();

            self.metrics.flush();

            STORE_WRITE_LOOP_DURATION_HISTOGRAM
                .observe(duration_to_sec(handle_begin.saturating_elapsed()));
        }
    }

    fn write_to_db(&mut self) {
        self.batch.before_write_to_db(&self.metrics);

        fail_point!("raft_before_save");

        if !self.batch.kv_wb.is_empty() {
            let now = Instant::now();
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.batch.kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
                panic!(
                    "store {}: {} failed to write to kv engine: {:?}",
                    self.store_id, self.tag, e
                );
            });
            if self.batch.kv_wb.data_size() > KV_WB_SHRINK_SIZE {
                self.batch.kv_wb = self.engines.kv.write_batch_with_cap(KV_WB_DEFAULT_SIZE);
            }
            STORE_WRITE_KVDB_DURATION_HISTOGRAM
                .observe(duration_to_sec(now.saturating_elapsed()) as f64);
        }

        self.batch.after_write_to_kv_db(&self.metrics);

        fail_point!("raft_between_save");

        if !self.batch.raft_wb.is_empty() {
            fail_point!("raft_before_save_on_store_1", self.store_id == 1, |_| {});

            let now = Instant::now();
            self.perf_context.start_observe();
            self.engines
                .raft
                .consume_and_shrink(
                    &mut self.batch.raft_wb,
                    false,
                    RAFT_WB_SHRINK_SIZE,
                    RAFT_WB_DEFAULT_SIZE,
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "store {}: {} failed to write to raft engine: {:?}",
                        self.store_id, self.tag, e
                    );
                });
            self.perf_context.report_metrics();
            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM
                .observe(duration_to_sec(now.saturating_elapsed()) as f64);
        }

        self.batch.after_write_to_db(&self.metrics);

        self.last_unpersisted_id += 1;
        self.pending_tasks.push_back((
            self.last_unpersisted_id,
            std::mem::take(&mut self.batch.tasks),
        ));

        self.sync_sender
            .send(SyncMsg::Sync {
                worker_id: self.worker_id,
                unpersisted_id: self.last_unpersisted_id,
            })
            .unwrap();

        self.batch.clear();

        fail_point!("raft_after_save");
    }

    fn after_persist(&mut self, notify_ready_buffer: &mut HashMap<u64, (u64, u64)>) {
        let mut unreachable_peers = HashSet::default();
        let now = Instant::now();
        fail_point!("raft_before_follower_send");
        while !self.pending_tasks.is_empty() {
            if self.pending_tasks.front().unwrap().0 > self.last_persisted_id {
                break;
            }
            let (_, tasks) = self.pending_tasks.pop_front().unwrap();
            for mut task in tasks {
                notify_ready_buffer.insert(task.region_id, (task.peer_id, task.ready_number));
                for msg in task.messages.drain(..) {
                    let msg_type = msg.get_message().get_msg_type();
                    let to_peer_id = msg.get_to_peer().get_id();
                    let to_store_id = msg.get_to_peer().get_store_id();
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
                        // Send unreachable to this peer.
                        // If this msg is snapshot, it is unnecessary to send snapshot
                        // status to this peer because it has already become follower.
                        // (otherwise the snapshot msg should be sent in store thread other than here)
                        unreachable_peers.insert((task.region_id, to_peer_id));
                    } else {
                        self.message_metrics.add(msg_type, true);
                    }
                }
            }
        }
        for (region_id, to_peer_id) in unreachable_peers {
            self.notifier.notify_unreachable(region_id, to_peer_id);
        }
        let now2 = Instant::now();
        STORE_WRITE_SEND_DURATION_HISTOGRAM
            .observe(duration_to_sec(now2.saturating_duration_since(now)));

        for (region_id, (peer_id, ready_number)) in notify_ready_buffer.iter() {
            self.notifier
                .notify_persisted(*region_id, *peer_id, *ready_number, now2);
        }

        STORE_WRITE_CALLBACK_DURATION_HISTOGRAM.observe(duration_to_sec(now2.saturating_elapsed()));

        notify_ready_buffer.clear();
    }
}

trait EngineSync {
    fn sync(&self);
}

struct RaftEngineSync<ER: RaftEngine>(ER);

impl<ER: RaftEngine> EngineSync for RaftEngineSync<ER> {
    fn sync(&self) {
        self.0.sync().unwrap_or_else(|e| {
            panic!("sync worker failed to sync raft db: {:?}", e);
        });
    }
}

enum SyncMsg {
    Shutdown,
    Sync {
        worker_id: usize,
        unpersisted_id: u64,
    },
}

/// Sync worker is used for calling raft db's sync when receiving sync msg.
// TODO: The synchronization method between sync worker and write workers can be
// more efficient.
struct SyncWorker<EK, ER, S>
where
    EK: KvEngine,
    ER: RaftEngine,
    S: EngineSync,
{
    sync: S,
    receiver: Receiver<SyncMsg>,
    write_senders: Vec<Sender<WriteMsg<EK, ER>>>,
    global_persisted_ids: Vec<Arc<CachePadded<AtomicU64>>>,
}

impl<EK, ER, S> SyncWorker<EK, ER, S>
where
    EK: KvEngine,
    ER: RaftEngine,
    S: EngineSync,
{
    fn new(
        sync: S,
        receiver: Receiver<SyncMsg>,
        write_senders: Vec<Sender<WriteMsg<EK, ER>>>,
        global_persisted_ids: Vec<Arc<CachePadded<AtomicU64>>>,
    ) -> Self {
        assert_eq!(write_senders.len(), global_persisted_ids.len());
        Self {
            sync,
            receiver,
            write_senders,
            global_persisted_ids,
        }
    }

    fn run(&mut self) {
        let mut persist_vec = vec![];
        for _ in 0..self.write_senders.len() {
            persist_vec.push(None);
        }
        loop {
            let mut first_time = true;
            loop {
                let msg = if first_time {
                    first_time = false;
                    match self.receiver.recv() {
                        Ok(msg) => msg,
                        Err(_) => return,
                    }
                } else {
                    match self.receiver.try_recv() {
                        Ok(msg) => msg,
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            return;
                        }
                    }
                };
                match msg {
                    SyncMsg::Shutdown => return,
                    SyncMsg::Sync {
                        worker_id,
                        unpersisted_id,
                    } => {
                        persist_vec[worker_id] = Some(unpersisted_id);
                    }
                }
            }

            let now = Instant::now();
            self.sync.sync();
            STORE_WRITE_SYNC_RAFT_DB_DURATION_HISTOGRAM
                .observe(duration_to_sec(now.saturating_elapsed()));

            for i in 0..persist_vec.len() {
                if let Some(persisted_id) = persist_vec[i] {
                    self.global_persisted_ids[i].store(persisted_id, Ordering::Release);
                    // Sync worker is destroyed after write worker
                    let _ = self.write_senders[i].send(WriteMsg::Persisted);
                    persist_vec[i] = None;
                }
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
    sync_sender: Option<Sender<SyncMsg>>,
    handlers: Vec<JoinHandle<()>>,
    sync_handler: Option<JoinHandle<()>>,
}

impl<EK, ER> StoreWriters<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new() -> Self {
        Self {
            writers: vec![],
            sync_sender: None,
            handlers: vec![],
            sync_handler: None,
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
        config: &Config,
    ) -> Result<()> {
        let (sync_sender, sync_receiver) = unbounded();
        let mut global_persisted_ids = vec![];
        for i in 0..config.store_io_pool_size {
            let tag = format!("store-writer-{}", i);
            let (tx, rx) = bounded(config.store_io_notify_capacity);
            let global_persisted_id = Arc::new(CachePadded::new(AtomicU64::new(0)));
            let mut worker = Worker::new(
                store_id,
                tag.clone(),
                i,
                engines.clone(),
                rx,
                notifier.clone(),
                trans.clone(),
                global_persisted_id.clone(),
                sync_sender.clone(),
                config,
            );
            let t = thread::Builder::new().name(thd_name!(tag)).spawn(move || {
                worker.run();
            })?;
            global_persisted_ids.push(global_persisted_id);
            self.writers.push(tx);
            self.handlers.push(t);
        }
        let mut syncer = SyncWorker::new(
            RaftEngineSync(engines.raft.clone()),
            sync_receiver,
            self.writers.clone(),
            global_persisted_ids,
        );
        self.sync_handler = Some(
            thread::Builder::new()
                .name(thd_name!("store-syncer"))
                .spawn(move || {
                    syncer.run();
                })?,
        );
        self.sync_sender = Some(sync_sender);
        Ok(())
    }

    pub fn shutdown(&mut self) {
        assert_eq!(self.writers.len(), self.handlers.len());
        for (i, handler) in self.handlers.drain(..).enumerate() {
            self.writers[i].send(WriteMsg::Shutdown).unwrap();
            handler.join().unwrap();
        }
        self.sync_sender
            .as_ref()
            .unwrap()
            .send(SyncMsg::Shutdown)
            .unwrap();
        self.sync_handler.take().unwrap().join().unwrap();
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
