// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::store::config::Config;
use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::{AsyncWriteMetrics, RaftSendMessageMetrics};
use crate::store::metrics::*;
use crate::store::transport::Transport;
use crate::store::{PeerMsg, SignificantMsg};
use crate::Result;

use collections::{HashMap, HashSet};
use crossbeam::utils::CachePadded;
use engine_traits::{
    KvEngine, PerfContext, PerfContextKind, RaftEngine, RaftLogBatch, WriteBatch, WriteOptions,
};
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvproto::raft_serverpb::{RaftLocalState, RaftMessage};
use raft::eraftpb::Entry;
use tikv_util::time::duration_to_sec;
use tikv_util::{box_err, debug, thd_name, warn};

const KV_WB_SHRINK_SIZE: usize = 1024 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 10 * 1024 * 1024;

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

/// AsyncWriteTask contains write tasks which need to be persisted to kv db and raft db.
#[derive(Debug)]
pub struct AsyncWriteTask<EK, ER>
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
    pub proposal_times: Vec<Instant>,
}

impl<EK, ER> AsyncWriteTask<EK, ER>
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
            proposal_times: vec![],
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

/// Message that can be sent to AsyncWriteWorker
pub enum AsyncWriteMsg<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    WriteTask(AsyncWriteTask<EK, ER>),
    Shutdown,
    Persisted,
}

/// AsyncWriteBatch is used for combining several AsyncWriteTask into one.
struct AsyncWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub kv_wb: EK::WriteBatch,
    pub raft_wb: ER::LogBatch,
    // Write raft state once for a region everytime writing to disk
    pub raft_states: HashMap<u64, RaftLocalState>,
    pub state_size: usize,
    pub tasks: Vec<AsyncWriteTask<EK, ER>>,
    metrics: AsyncWriteMetrics,
    waterfall_metrics: bool,
}

impl<EK, ER> AsyncWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(kv_wb: EK::WriteBatch, raft_wb: ER::LogBatch, waterfall_metrics: bool) -> Self {
        Self {
            kv_wb,
            raft_wb,
            raft_states: HashMap::default(),
            tasks: vec![],
            state_size: 0,
            metrics: Default::default(),
            waterfall_metrics,
        }
    }

    /// Add write task to this batch
    fn add_write_task(&mut self, mut task: AsyncWriteTask<EK, ER>) {
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
        // raft_wb doesn't have clear interface but it should be consumed by raft db before
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

    fn before_write_to_db(&mut self) {
        // Put raft state to raft writebatch
        let raft_states = std::mem::take(&mut self.raft_states);
        for (region_id, state) in raft_states {
            self.raft_wb.put_raft_state(region_id, &state).unwrap();
        }
        self.state_size = 0;
        if self.waterfall_metrics {
            let now = Instant::now();
            for task in &self.tasks {
                for ts in &task.proposal_times {
                    self.metrics.to_write.observe(duration_to_sec(now - *ts));
                }
            }
        }
    }

    fn after_write_to_kv_db(&mut self) {
        if self.waterfall_metrics {
            let now = Instant::now();
            for task in &self.tasks {
                for ts in &task.proposal_times {
                    self.metrics.kvdb_end.observe(duration_to_sec(now - *ts));
                }
            }
        }
    }

    fn after_write_to_db(&mut self) {
        if self.waterfall_metrics {
            let now = Instant::now();
            for task in &self.tasks {
                for ts in &task.proposal_times {
                    self.metrics.write_end.observe(duration_to_sec(now - *ts))
                }
            }
        }
    }

    #[inline]
    fn flush_metrics(&mut self) {
        if self.waterfall_metrics {
            self.metrics.flush();
        }
    }
}

#[allow(dead_code)]
struct AsyncWriteWorker<EK, ER, T, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport,
    N: Notifier,
{
    store_id: u64,
    tag: String,
    worker_id: usize,
    kv_engine: EK,
    raft_engine: ER,
    receiver: Receiver<AsyncWriteMsg<EK, ER>>,
    notifier: N,
    trans: T,
    wb: AsyncWriteBatch<EK, ER>,
    pending_tasks: VecDeque<(u64, Vec<AsyncWriteTask<EK, ER>>)>,
    last_unpersisted_id: u64,
    last_persisted_id: u64,
    global_persisted_id: Arc<CachePadded<AtomicU64>>,
    sync_sender: Sender<(usize, u64)>,
    raft_write_size_limit: usize,
    message_metrics: RaftSendMessageMetrics,
    perf_context: EK::PerfContext,
}

impl<EK, ER, T, N> AsyncWriteWorker<EK, ER, T, N>
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
        kv_engine: EK,
        raft_engine: ER,
        receiver: Receiver<AsyncWriteMsg<EK, ER>>,
        notifier: N,
        trans: T,
        global_persisted_id: Arc<CachePadded<AtomicU64>>,
        sync_sender: Sender<(usize, u64)>,
        config: &Config,
    ) -> Self {
        let wb = AsyncWriteBatch::new(
            kv_engine.write_batch(),
            raft_engine.log_batch(16 * 1024),
            config.store_waterfall_metrics,
        );
        let perf_context =
            kv_engine.get_perf_context(config.perf_level, PerfContextKind::RaftstoreStore);
        Self {
            store_id,
            tag,
            worker_id,
            kv_engine,
            raft_engine,
            receiver,
            notifier,
            trans,
            wb,
            pending_tasks: VecDeque::new(),
            last_unpersisted_id: 0,
            last_persisted_id: 0,
            global_persisted_id,
            sync_sender,
            raft_write_size_limit: config.raft_write_size_limit.0 as usize,
            message_metrics: Default::default(),
            perf_context,
        }
    }

    fn run(&mut self) {
        loop {
            let loop_begin = Instant::now();
            let mut handle_begin = loop_begin;

            let mut after_persist = Duration::from_micros(0);
            let mut first_time = true;
            while self.wb.get_raft_size() < self.raft_write_size_limit {
                let msg = if first_time {
                    first_time = false;
                    match self.receiver.recv() {
                        Ok(msg) => {
                            STORE_WRITE_TASK_GEN_DURATION_HISTOGRAM
                                .observe(duration_to_sec(loop_begin.elapsed()));
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
                    self.after_persist();
                    after_persist += now.elapsed();
                }
                match msg {
                    AsyncWriteMsg::Shutdown => return,
                    AsyncWriteMsg::WriteTask(task) => {
                        STORE_WRITE_TASK_WAIT_DURATION_HISTOGRAM
                            .observe(duration_to_sec(task.send_time.elapsed()));
                        self.wb.add_write_task(task);
                    }
                    AsyncWriteMsg::Persisted => (),
                }
            }

            let now = Instant::now();
            STORE_WRITE_HANDLE_MSG_DURATION_HISTOGRAM
                .observe(duration_to_sec(now - handle_begin - after_persist));

            if self.trans.need_flush() {
                self.trans.flush();
                self.message_metrics.flush();
                STORE_WRITE_FLUSH_SEND_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()));
            }

            if self.wb.is_empty() {
                continue;
            }

            STORE_WRITE_TRIGGER_SIZE_HISTOGRAM.observe(self.wb.get_raft_size() as f64);

            self.write_to_db();

            STORE_WRITE_LOOP_DURATION_HISTOGRAM
                .observe(duration_to_sec(handle_begin.elapsed()) as f64);
        }
    }

    fn write_to_db(&mut self) {
        let now = Instant::now();
        self.wb.before_write_to_db();

        fail_point!("raft_before_save");

        if !self.wb.kv_wb.is_empty() {
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.wb.kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
                panic!("{} failed to write to kv engine: {:?}", self.tag, e);
            });
            if self.wb.kv_wb.data_size() > KV_WB_SHRINK_SIZE {
                self.wb.kv_wb = self.kv_engine.write_batch_with_cap(4 * 1024);
            }

            STORE_WRITE_KVDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }

        self.wb.after_write_to_kv_db();

        fail_point!("raft_between_save");

        if !self.wb.raft_wb.is_empty() {
            fail_point!("raft_before_save_on_store_1", self.store_id == 1, |_| {});

            self.perf_context.start_observe();
            let now = Instant::now();
            self.raft_engine
                .consume_and_shrink(&mut self.wb.raft_wb, false, RAFT_WB_SHRINK_SIZE, 16 * 1024)
                .unwrap_or_else(|e| {
                    panic!("{} failed to write to raft engine: {:?}", self.tag, e);
                });
            self.perf_context.report_metrics();

            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }

        self.wb.after_write_to_db();

        self.wb.flush_metrics();

        self.last_unpersisted_id += 1;
        self.pending_tasks
            .push_back((self.last_unpersisted_id, std::mem::take(&mut self.wb.tasks)));

        self.sync_sender
            .send((self.worker_id, self.last_unpersisted_id))
            .unwrap();

        self.wb.clear();

        fail_point!("raft_after_save");
    }

    fn after_persist(&mut self) {
        let mut readies = HashMap::default();
        let mut unreachable_peers = HashSet::default();
        let now = Instant::now();
        fail_point!("raft_before_follower_send");
        while !self.pending_tasks.is_empty() {
            if self.pending_tasks.front().unwrap().0 > self.last_persisted_id {
                break;
            }
            let (_, tasks) = self.pending_tasks.pop_front().unwrap();
            for mut task in tasks {
                readies.insert(task.region_id, (task.peer_id, task.ready_number));
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
        STORE_WRITE_SEND_DURATION_HISTOGRAM.observe(duration_to_sec(now2 - now));

        for (region_id, (peer_id, ready_number)) in &readies {
            self.notifier
                .notify_persisted(*region_id, *peer_id, *ready_number, now2);
        }

        STORE_WRITE_CALLBACK_DURATION_HISTOGRAM.observe(duration_to_sec(now2.elapsed()));
    }
}

struct SyncWorker<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    raft_engine: ER,
    receiver: Receiver<(usize, u64)>,
    io_senders: Vec<Sender<AsyncWriteMsg<EK, ER>>>,
    global_persisted_ids: Vec<Arc<CachePadded<AtomicU64>>>,
}

impl<EK, ER> SyncWorker<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(
        raft_engine: ER,
        receiver: Receiver<(usize, u64)>,
        io_senders: Vec<Sender<AsyncWriteMsg<EK, ER>>>,
        global_persisted_ids: Vec<Arc<CachePadded<AtomicU64>>>,
    ) -> Self {
        assert_eq!(io_senders.len(), global_persisted_ids.len());
        Self {
            raft_engine,
            receiver,
            io_senders,
            global_persisted_ids,
        }
    }

    fn run(&mut self) {
        let mut persist_vec = vec![];
        for _ in 0..self.io_senders.len() {
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
                // HACK, TODO: fix it
                if msg.1 == 0 {
                    return;
                }
                persist_vec[msg.0] = Some(msg.1);
            }
            let now = Instant::now();
            self.raft_engine.sync().unwrap_or_else(|e| {
                panic!("sync worker failed to sync raft db: {:?}", e);
            });
            STORE_WRITE_SYNC_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()));

            for i in 0..persist_vec.len() {
                if let Some(persisted_id) = persist_vec[i] {
                    self.global_persisted_ids[i].store(persisted_id, Ordering::Release);
                    // TODO
                    let _ = self.io_senders[i].send(AsyncWriteMsg::Persisted);
                    persist_vec[i] = None;
                }
            }
        }
    }
}

pub struct AsyncWriters<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    writers: Vec<Sender<AsyncWriteMsg<EK, ER>>>,
    sync_sender: Option<Sender<(usize, u64)>>,
    handlers: Vec<JoinHandle<()>>,
    sync_handler: Option<JoinHandle<()>>,
}

impl<EK, ER> AsyncWriters<EK, ER>
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

    pub fn senders(&self) -> &Vec<Sender<AsyncWriteMsg<EK, ER>>> {
        &self.writers
    }

    pub fn spawn<T: Transport + 'static, N: Notifier>(
        &mut self,
        store_id: u64,
        kv_engine: &EK,
        raft_engine: &ER,
        notifier: &N,
        trans: &T,
        config: &Config,
    ) -> Result<()> {
        let (sync_sender, sync_receiver) = channel();
        let mut global_persisted_ids = vec![];
        for i in 0..config.store_io_pool_size {
            let tag = format!("store-writer-{}", i);
            let (tx, rx) = channel();
            let global_persisted_id = Arc::new(CachePadded::new(AtomicU64::new(0)));
            let mut worker = AsyncWriteWorker::new(
                store_id,
                tag.clone(),
                i,
                kv_engine.clone(),
                raft_engine.clone(),
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
            raft_engine.clone(),
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
            self.writers[i].send(AsyncWriteMsg::Shutdown).unwrap();
            handler.join().unwrap();
        }
        self.sync_sender.as_ref().unwrap().send((0, 0)).unwrap();
        self.sync_handler.take().unwrap().join().unwrap();
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
