// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::store::config::Config;
use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::{AsyncWriteMetrics, RaftSendMessageMetrics};
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
use raft::eraftpb::Entry;
use tikv_util::time::duration_to_sec;
use tikv_util::{debug, thd_name, warn};

const KV_WB_SHRINK_SIZE: usize = 1024 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 10 * 1024 * 1024;

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct UnpersistedReady {
    pub peer_id: u64,
    pub number: u64,
}

impl UnpersistedReady {
    pub fn new(peer_id: u64, number: u64) -> Self {
        Self { peer_id, number }
    }
}

pub trait Notifier: Clone + Send + 'static {
    fn notify_persisted(&self, region_id: u64, ready: UnpersistedReady, now: Instant);
    fn notify_unreachable(&self, region_id: u64, to_peer_id: u64);
}

#[derive(Clone)]
pub struct RegionNotifier<EK, ER> 
where
    EK: KvEngine,
    ER: RaftEngine,
{
    router: RaftRouter<EK, ER>,
}

impl<EK, ER> RegionNotifier<EK, ER> 
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(router: RaftRouter<EK, ER>) -> Self {
        Self {
            router,
        }
    }
}

impl<EK, ER> Notifier for RegionNotifier<EK, ER> 
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn notify_persisted(&self, region_id: u64, ready: UnpersistedReady, now: Instant) {
        if let Err(e) = self.router.force_send(
            region_id,
            PeerMsg::Persisted((ready.peer_id, ready.number, now)),
        ) {
            warn!(
                "failed to send noop to trigger persisted ready";
                "region_id" => region_id,
                "peer_id" => ready.peer_id,
                "ready_number" => ready.number,
                "error" => ?e,
            );
        }
    }

    fn notify_unreachable(&self, region_id: u64, to_peer_id: u64) {
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
}

pub struct AsyncWriteTask<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    region_id: u64,
    peer_id: u64,
    pub send_time: Instant,
    pub kv_wb: Option<EK::WriteBatch>,
    pub raft_wb: Option<ER::LogBatch>,
    pub entries: Vec<Entry>,
    pub cut_logs: Option<(u64, u64)>,
    pub ready: Option<UnpersistedReady>,
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
            send_time: Instant::now(),
            kv_wb: None,
            raft_wb: None,
            entries: vec![],
            cut_logs: None,
            ready: None,
            raft_state: None,
            proposal_times: vec![],
            messages: vec![],
        }
    }

    pub fn has_data(&self) -> bool {
        !(self.raft_state.is_none()
            && self.entries.is_empty()
            && self.cut_logs.is_none()
            && self.kv_wb.as_ref().map_or(true, |wb| wb.is_empty())
            && self.raft_wb.as_ref().map_or(true, |wb| wb.is_empty()))
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
    pub readies: HashMap<u64, UnpersistedReady>,
    pub raft_states: HashMap<u64, RaftLocalState>,
    pub state_size: usize,
    pub tasks: Vec<AsyncWriteTask<EK, ER>>,
    metrics: AsyncWriteMetrics,
    waterfall_metrics: bool,
    _phantom: PhantomData<EK>,
}

impl<EK, ER> AsyncWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(kv_wb: EK::WriteBatch, raft_wb: ER::LogBatch, waterfall_metrics: bool) -> Self {
        Self {
            kv_wb,
            raft_wb,
            readies: HashMap::default(),
            raft_states: HashMap::default(),
            tasks: vec![],
            state_size: 0,
            metrics: Default::default(),
            waterfall_metrics,
            _phantom: PhantomData,
        }
    }

    pub fn add_write_task(&mut self, mut task: AsyncWriteTask<EK, ER>) {
        if let Some(kv_wb) = task.kv_wb.take() {
            self.kv_wb.merge(&kv_wb);
        }
        if let Some(raft_wb) = task.raft_wb.take() {
            self.raft_wb.merge(raft_wb);
        }
        let entries = std::mem::take(&mut task.entries);
        self.raft_wb.append(task.region_id, entries).unwrap();
        if let Some((from, to)) = task.cut_logs {
            self.raft_wb.cut_logs(task.region_id, from, to);
        }
        if let Some(ready) = task.ready.take() {
            self.readies.insert(task.region_id, ready);
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

    pub fn clear(&mut self) {
        // raft_wb doesn't have clear interface but it should be consumed by raft db before
        self.kv_wb.clear();
        self.readies.clear();
        self.raft_states.clear();
        self.tasks.clear();
        self.state_size = 0;
    }

    #[inline]
    pub fn get_raft_size(&self) -> usize {
        self.state_size + self.raft_wb.persist_size()
    }

    pub fn before_write_to_db(&mut self, now: Instant) {
        let raft_states = std::mem::take(&mut self.raft_states);
        for (region_id, state) in raft_states {
            self.raft_wb.put_raft_state(region_id, &state).unwrap();
        }
        self.state_size = 0;
        if self.waterfall_metrics {
            for task in &self.tasks {
                for ts in &task.proposal_times {
                    self.metrics.to_write.observe(duration_to_sec(now - *ts));
                }
            }
        }
    }

    fn after_write_to_kv_db(&mut self, now: Instant) {
        if self.waterfall_metrics {
            for task in &self.tasks {
                for ts in &task.proposal_times {
                    self.metrics.kvdb_end.observe(duration_to_sec(now - *ts));
                }
            }
        }
    }

    fn after_write_to_db(&mut self, now: Instant) {
        if self.waterfall_metrics {
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
    kv_engine: EK,
    raft_engine: ER,
    receiver: Receiver<AsyncWriteMsg<EK, ER>>,
    notifier: N,
    trans: T,
    wb: AsyncWriteBatch<EK, ER>,
    trigger_write_size: usize,
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
        kv_engine: EK,
        raft_engine: ER,
        receiver: Receiver<AsyncWriteMsg<EK, ER>>,
        notifier: N,
        trans: T,
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
            kv_engine,
            raft_engine,
            receiver,
            notifier,
            trans,
            wb,
            trigger_write_size: config.trigger_write_size.0 as usize,
            message_metrics: Default::default(),
            perf_context,
        }
    }

    fn run(&mut self) {
        loop {
            let loop_begin = Instant::now();
            let mut handle_begin = loop_begin;

            let mut first_time = true;
            while self.wb.get_raft_size() < self.trigger_write_size {
                let msg = if first_time {
                    match self.receiver.recv() {
                        Ok(msg) => {
                            first_time = false;
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
                match msg {
                    AsyncWriteMsg::Shutdown => return,
                    AsyncWriteMsg::WriteTask(task) => {
                        STORE_WRITE_TASK_WAIT_DURATION_HISTOGRAM
                            .observe(duration_to_sec(task.send_time.elapsed()));
                        self.wb.add_write_task(task);
                    }
                }
            }

            STORE_WRITE_HANDLE_MSG_DURATION_HISTOGRAM
                .observe(duration_to_sec(handle_begin.elapsed()));

            STORE_WRITE_TRIGGER_SIZE_HISTOGRAM.observe(self.wb.get_raft_size() as f64);

            self.sync_write();

            STORE_WRITE_LOOP_DURATION_HISTOGRAM
                .observe(duration_to_sec(loop_begin.elapsed()) as f64);
        }
    }

    fn sync_write(&mut self) {
        let mut now = Instant::now();
        self.wb.before_write_to_db(now);

        fail_point!("raft_before_save");

        if !self.wb.kv_wb.is_empty() {
            let now = Instant::now();
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

        now = Instant::now();
        self.wb.after_write_to_kv_db(now);

        fail_point!("raft_between_save");

        if !self.wb.raft_wb.is_empty() {
            fail_point!("raft_before_save_on_store_1", self.store_id == 1, |_| {});

            self.perf_context.start_observe();
            let now = Instant::now();
            self.raft_engine
                .consume_and_shrink(&mut self.wb.raft_wb, true, RAFT_WB_SHRINK_SIZE, 16 * 1024)
                .unwrap_or_else(|e| {
                    panic!("{} failed to write to raft engine: {:?}", self.tag, e);
                });
            self.perf_context.report_metrics();

            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }

        now = Instant::now();
        self.wb.after_write_to_db(now);

        fail_point!("raft_before_follower_send");

        now = Instant::now();
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
            self.notifier.notify_unreachable(region_id, to_peer_id);
        }
        STORE_WRITE_SEND_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()));
        now = Instant::now();
        // The order between send msg and send ready callback is important for snapshot process
        // TODO: add more comments
        for (region_id, r) in &self.wb.readies {
            self.notifier.notify_persisted(*region_id, *r, now);
        }
        STORE_WRITE_CALLBACK_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()));

        self.wb.flush_metrics();
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

    pub fn spawn<T: Transport + 'static, N: Notifier>(
        &mut self,
        store_id: u64,
        kv_engine: &EK,
        raft_engine: &ER,
        notifier: &N,
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
                rx,
                notifier.clone(),
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::Result;
    use crate::store::{Config, Transport};
    use engine_test::kv::{new_engine, KvTestEngine};
    use engine_traits::{ALL_CFS, Mutable, Peekable, WriteBatchExt};
    use kvproto::raft_serverpb::RaftMessage;
    use tempfile::{Builder, TempDir};

    use super::*;

    pub fn create_tmp_engine(path: &str) -> (TempDir, KvTestEngine) {
        let path = Builder::new().prefix(path).tempdir().unwrap();
        let engine = new_engine(
            path.path().join("db").to_str().unwrap(),
            None,
            ALL_CFS,
            None,
        )
        .unwrap();
        (path, engine)
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
        e.set_index(index);
        e.set_term(term);
        e
    }

    #[derive(Clone)]
    struct TestNotifier {
        tx: Sender<(u64, UnpersistedReady)>,
    }

    impl Notifier for TestNotifier {
        fn notify_persisted(&self, region_id: u64, ready: UnpersistedReady, _now: Instant) {
            self.tx.send((region_id, ready)).unwrap()
        }
        fn notify_unreachable(&self, _region_id: u64, _to_peer_id: u64) {
            panic!("unreachable");
        }
    }
    #[derive(Clone)]
    struct TestTransport {
        tx: Sender<RaftMessage>,
    }

    impl Transport for TestTransport {
        fn send(&mut self, msg: RaftMessage) -> Result<()> {
            self.tx.send(msg).unwrap();
            Ok(())
        }
        fn need_flush(&self) -> bool {
            false
        }
        fn flush(&mut self) {
        }
    }

    struct TestAsyncWriteWorker {
        worker: AsyncWriteWorker<KvTestEngine, KvTestEngine, TestTransport, TestNotifier>,
        kv_engine: KvTestEngine,
        raft_engine: KvTestEngine,
        task_tx: Sender<AsyncWriteMsg<KvTestEngine, KvTestEngine>>,
        notify_rx: Receiver<(u64, UnpersistedReady)>,
        msg_rx: Receiver<RaftMessage>,
    }

    impl TestAsyncWriteWorker {
        fn new(cfg: &Config, path_kv: &str, path_raft: &str) -> Self {
            let (_dir, kv_engine) = create_tmp_engine(path_kv);
            let (_dir, raft_engine) = create_tmp_engine(path_raft);
            let (task_tx, task_rx) = channel();
            let (notify_tx, notify_rx) = channel();
            let notifier = TestNotifier {
                tx: notify_tx,
            };
            let (msg_tx, msg_rx) = channel();
            let trans = TestTransport {
                tx: msg_tx,
            };
            Self {
                worker: AsyncWriteWorker::new(1, "writer".to_string(), kv_engine.clone(), raft_engine.clone(), task_rx, notifier, trans, cfg),
                kv_engine,
                raft_engine,
                task_tx,
                notify_rx,
                msg_rx,
            }
        }
    }

    #[test]
    fn test_batch_write() {
        let mut t = TestAsyncWriteWorker::new(&Config::default(), "async-io-batch-kv", "async-io-batch-raft");

        let mut task_1 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(1, 1);
        task_1.kv_wb = Some(t.kv_engine.write_batch());
        task_1.kv_wb.as_mut().unwrap().put(b"kv_k1", b"kv_v1").unwrap();
        task_1.raft_wb = Some(t.raft_engine.write_batch());
        task_1.raft_wb.as_mut().unwrap().put(b"raft_k1", b"raft_v1").unwrap();
        task_1.entries.push(new_entry(5, 5));
        task_1.entries.push(new_entry(6, 5));
        task_1.ready = Some(UnpersistedReady::new(1, 10));

        let mut task_2 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(2, 2);
        task_2.kv_wb = Some(t.kv_engine.write_batch());
        task_2.kv_wb.as_mut().unwrap().put(b"kv_k2", b"kv_v2").unwrap();
        task_2.raft_wb = Some(t.raft_engine.write_batch());
        task_2.raft_wb.as_mut().unwrap().put(b"raft_k2", b"raft_v2").unwrap();
        task_2.entries.push(new_entry(5, 5));
        task_2.entries.push(new_entry(6, 5));
        task_2.ready = Some(UnpersistedReady::new(2, 15));

        t.worker.wb.add_write_task(task_1);
        t.worker.wb.add_write_task(task_2);

        t.worker.sync_write();

        let snapshot = t.kv_engine.snapshot();
        assert_eq!(snapshot.get_value(b"kv_k1").unwrap().unwrap(), b"kv_v1");
        assert_eq!(snapshot.get_value(b"kv_k2").unwrap().unwrap(), b"kv_v2");

        let snapshot = t.raft_engine.snapshot();
        assert_eq!(snapshot.get_value(b"raft_k1").unwrap().unwrap(), b"raft_v1");
        assert_eq!(snapshot.get_value(b"raft_k2").unwrap().unwrap(), b"raft_v2");

        let notify = t.notify_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(notify, (1, UnpersistedReady::new(1, 10)));
        let notify = t.notify_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(notify, (2, UnpersistedReady::new(2, 15)));
    }
}