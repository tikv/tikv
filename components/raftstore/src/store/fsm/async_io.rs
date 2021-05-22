// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::store::config::Config;
use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::RaftSendMessageMetrics;
use crate::store::metrics::*;
use crate::store::transport::Transport;
use crate::store::{PeerMsg, SignificantMsg};
use crate::Result;

use collections::{HashMap, HashSet};
use engine_traits::{
    KvEngine, PerfContext, PerfContextKind, RaftEngine, RaftLogBatch, WriteBatch, WriteOptions,
};
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvproto::raft_serverpb::{RaftLocalState, RaftMessage};
use raft::eraftpb::{Entry, MessageType};
use tikv_util::time::{duration_to_micros, duration_to_sec, Instant as UtilInstant};
use tikv_util::{debug, thd_name, warn};

const KV_WB_SHRINK_SIZE: usize = 1024 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 10 * 1024 * 1024;

pub const RAFT_LOCAL_STATE_SIZE: usize = std::mem::size_of::<RaftLocalState>();

#[derive(Default)]
pub struct UnsyncedReady {
    pub peer_id: u64,
    pub number: u64,
}

impl UnsyncedReady {
    pub fn new(peer_id: u64, number: u64) -> Self {
        Self { peer_id, number }
    }

    fn flush<EK, ER>(&self, region_id: u64, router: &RaftRouter<EK, ER>)
    where
        EK: KvEngine,
        ER: RaftEngine,
    {
        if let Err(e) = router.force_send(
            region_id,
            PeerMsg::Persisted((self.peer_id, self.number, Instant::now())),
        ) {
            warn!(
                "failed to send noop to trigger persisted ready";
                "region_id" => region_id,
                "peer_id" => self.peer_id,
                "ready_number" => self.number,
                "error" => ?e,
            );
        }
    }
}

pub struct AsyncWriteTask<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    region_id: u64,
    peer_id: u64,
    pub kv_wb: Option<EK::WriteBatch>,
    pub raft_wb: Option<ER::LogBatch>,
    pub entries: Vec<Entry>,
    pub cut_logs: Option<(u64, u64)>,
    pub unsynced_ready: Option<UnsyncedReady>,
    pub raft_state: Option<RaftLocalState>,
    pub proposal_times: Vec<Instant>,
    pub messages: Vec<RaftMessage>,
}

impl<EK, ER> AsyncWriteTask<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(region_id: u64, peer_id: u64) -> Self {
        Self {
            region_id,
            peer_id,
            kv_wb: None,
            raft_wb: None,
            entries: vec![],
            cut_logs: None,
            unsynced_ready: None,
            raft_state: None,
            proposal_times: vec![],
            messages: vec![],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
            && self.entries.is_empty()
            && self.raft_state.is_none()
            && self.cut_logs.is_none()
            && self.unsynced_ready.is_none()
            && self.proposal_times.is_empty()
            && self.kv_wb.as_ref().map_or_else(|| true, |wb| wb.is_empty())
            && self
                .raft_wb
                .as_ref()
                .map_or_else(|| true, |wb| wb.is_empty())
    }
}

pub enum AsyncWriteMsg<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    WriteTask(AsyncWriteTask<EK, ER>),
    Shutdown,
}

pub struct AsyncWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub kv_wb: EK::WriteBatch,
    pub raft_wb: ER::LogBatch,
    pub unsynced_readies: HashMap<u64, UnsyncedReady>,
    pub raft_states: HashMap<u64, RaftLocalState>,
    pub state_size: usize,
    pub tasks: Vec<AsyncWriteTask<EK, ER>>,
    _phantom: PhantomData<EK>,
}

impl<EK, ER> AsyncWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(kv_wb: EK::WriteBatch, raft_wb: ER::LogBatch) -> Self {
        Self {
            kv_wb,
            raft_wb,
            unsynced_readies: HashMap::default(),
            raft_states: HashMap::default(),
            tasks: vec![],
            state_size: 0,
            _phantom: PhantomData,
        }
    }

    fn add_write_task(&mut self, mut task: AsyncWriteTask<EK, ER>) {
        if let Some(mut kv_wb) = task.kv_wb.take() {
            self.kv_wb.merge(&mut kv_wb);
        }
        if let Some(mut raft_wb) = task.raft_wb.take() {
            self.raft_wb.merge(&mut raft_wb);
        }
        let entries = std::mem::take(&mut task.entries);
        self.raft_wb.append(task.region_id, entries).unwrap();
        if let Some((from, to)) = task.cut_logs {
            self.raft_wb.cut_logs(task.region_id, from, to);
        }
        if let Some(ready) = task.unsynced_ready.take() {
            self.unsynced_readies.insert(task.region_id, ready);
        }
        if let Some(raft_state) = task.raft_state.take() {
            if self
                .raft_states
                .insert(task.region_id, raft_state)
                .is_none()
            {
                self.state_size += RAFT_LOCAL_STATE_SIZE;
            }
        }
        self.tasks.push(task);
    }

    fn clear(&mut self) {
        // raft_wb doesn't have clear interface but it should be consumed by raft db before
        self.kv_wb.clear();
        self.unsynced_readies.clear();
        self.raft_states.clear();
        self.tasks.clear();
        self.state_size = 0;
    }

    fn get_raft_size(&self) -> usize {
        self.state_size + self.raft_wb.persist_size()
    }

    fn before_write_to_db(&mut self) {
        for task in &self.tasks {
            for ts in &task.proposal_times {
                STORE_TO_WRITE_DURATION_HISTOGRAM.observe(duration_to_sec(ts.elapsed()));
            }
        }
        let raft_states = std::mem::take(&mut self.raft_states);
        for (region_id, state) in raft_states {
            self.raft_wb.put_raft_state(region_id, &state).unwrap();
        }
        self.state_size = 0;
        for task in &self.tasks {
            for ts in &task.proposal_times {
                STORE_FILL_WB_DURATION_HISTOGRAM.observe(duration_to_sec(ts.elapsed()));
            }
        }
    }

    fn after_write_to_kv_db(&self) {
        for task in &self.tasks {
            for ts in &task.proposal_times {
                STORE_WRITE_KVDB_END_DURATION_HISTOGRAM.observe(duration_to_sec(ts.elapsed()));
            }
        }
    }

    fn after_write_to_db(&self) {
        for task in &self.tasks {
            for ts in &task.proposal_times {
                STORE_WRITE_END_DURATION_HISTOGRAM.observe(duration_to_sec(ts.elapsed()));
            }
        }
    }
}

struct AsyncWriteWorker<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport,
{
    store_id: u64,
    tag: String,
    kv_engine: EK,
    raft_engine: ER,
    router: RaftRouter<EK, ER>,
    receiver: Receiver<AsyncWriteMsg<EK, ER>>,
    wb: AsyncWriteBatch<EK, ER>,
    trigger_write_size: usize,
    trans: T,
    message_metrics: RaftSendMessageMetrics,
    perf_context: EK::PerfContext,
}

impl<EK, ER, T> AsyncWriteWorker<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport,
{
    fn new(
        store_id: u64,
        tag: String,
        kv_engine: EK,
        raft_engine: ER,
        router: RaftRouter<EK, ER>,
        receiver: Receiver<AsyncWriteMsg<EK, ER>>,
        trans: T,
        config: &Config,
    ) -> Self {
        let wb = AsyncWriteBatch::new(kv_engine.write_batch(), raft_engine.log_batch(16 * 1024));
        let perf_context =
            kv_engine.get_perf_context(config.perf_level, PerfContextKind::RaftstoreStore);
        Self {
            store_id,
            tag,
            kv_engine,
            raft_engine,
            router,
            receiver,
            wb,
            trigger_write_size: config.trigger_write_size.0 as usize,
            trans,
            message_metrics: Default::default(),
            perf_context,
        }
    }

    fn run(&mut self) {
        loop {
            let loop_begin = UtilInstant::now();
            let mut handle_begin = loop_begin;

            let mut first_time = true;
            while self.wb.get_raft_size() < self.trigger_write_size {
                let msg = if first_time {
                    match self.receiver.recv() {
                        Ok(msg) => {
                            first_time = false;
                            STORE_WRITE_TASK_GEN_DURATION_HISTOGRAM
                                .observe(duration_to_sec(loop_begin.elapsed()));
                            handle_begin = UtilInstant::now();
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
                match msg {
                    AsyncWriteMsg::Shutdown => return,
                    AsyncWriteMsg::WriteTask(task) => {
                        self.wb.add_write_task(task);
                    }
                }
            }

            STORE_WRITE_HANDLE_MSG_DURATION_HISTOGRAM
                .observe(duration_to_sec(handle_begin.elapsed()));

            STORE_WRITE_TIME_TRIGGER_SIZE_HISTOGRAM.observe(self.wb.get_raft_size() as f64);

            self.sync_write();

            STORE_WRITE_LOOP_DURATION_HISTOGRAM
                .observe(duration_to_sec(loop_begin.elapsed()) as f64);
        }
    }

    fn sync_write(&mut self) {
        self.wb.before_write_to_db();

        fail_point!("raft_before_save");
        if !self.wb.kv_wb.is_empty() {
            let now = UtilInstant::now();
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
            let now = UtilInstant::now();
            self.raft_engine
                .consume_and_shrink(&mut self.wb.raft_wb, true, RAFT_WB_SHRINK_SIZE, 16 * 1024)
                .unwrap_or_else(|e| {
                    panic!("{} failed to write to raft engine: {:?}", self.tag, e);
                });
            self.perf_context.report_metrics();

            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }

        self.wb.after_write_to_db();

        fail_point!("raft_before_follower_send");
        let send_begin = UtilInstant::now();
        let mut unreachable_peers = HashSet::default();
        for task in &mut self.wb.tasks {
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
        self.trans.flush();
        self.message_metrics.flush();
        for (region_id, to_peer_id) in unreachable_peers {
            let msg = SignificantMsg::Unreachable {
                region_id,
                to_peer_id,
            };
            if let Err(e) = self
                .router
                .force_send(region_id, PeerMsg::SignificantMsg(msg))
            {
                warn!(
                    "failed to send unreachable";
                    "region_id" => region_id,
                    "unreachable_peer_id" => to_peer_id,
                    "error" => ?e,
                );
            }
        }
        STORE_WRITE_SEND_DURATION_HISTOGRAM.observe(duration_to_sec(send_begin.elapsed()));

        let callback_begin = UtilInstant::now();
        for (region_id, r) in &self.wb.unsynced_readies {
            r.flush(*region_id, &self.router);
        }
        STORE_WRITE_CALLBACK_DURATION_HISTOGRAM.observe(duration_to_sec(callback_begin.elapsed()));

        self.wb.clear();

        fail_point!("raft_after_save");
    }
}

pub struct AsyncWriters<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    writers: Vec<Sender<AsyncWriteMsg<EK, ER>>>,
    handlers: Vec<JoinHandle<()>>,
}

impl<EK, ER> AsyncWriters<EK, ER>
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

    pub fn senders(&self) -> &Vec<Sender<AsyncWriteMsg<EK, ER>>> {
        &self.writers
    }

    pub fn spawn<T: Transport + 'static>(
        &mut self,
        store_id: u64,
        kv_engine: &EK,
        raft_engine: &ER,
        router: &RaftRouter<EK, ER>,
        trans: &T,
        config: &Config,
    ) -> Result<()> {
        for i in 0..config.store_io_pool_size {
            let tag = format!("store-writer-{}", i);
            let (tx, rx) = channel();
            let mut worker = AsyncWriteWorker::new(
                store_id,
                tag.clone(),
                kv_engine.clone(),
                raft_engine.clone(),
                router.clone(),
                rx,
                trans.clone(),
                config,
            );
            let t = thread::Builder::new().name(thd_name!(tag)).spawn(move || {
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
            self.writers[i].send(AsyncWriteMsg::Shutdown).unwrap();
            handler.join().unwrap();
        }
    }
}
