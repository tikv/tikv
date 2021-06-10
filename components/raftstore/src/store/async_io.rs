// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

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
use tikv_util::{box_err, debug, thd_name, warn};

const KV_WB_SHRINK_SIZE: usize = 1024 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 10 * 1024 * 1024;

/// Notify the event to the specified region.
pub trait Notifier: Clone + Send + 'static {
    fn notify_persisted(&self, region_id: u64, peer_id: u64, ready_number: u64, now: Instant);
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
        Self { router }
    }
}

impl<EK, ER> Notifier for RegionNotifier<EK, ER>
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
        if let Err(e) = self.router.force_send(
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
            if from <= last_index {
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
}

/// AsyncWriteBatch is used for combining several AsyncWriteTask into one.
struct AsyncWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub kv_wb: EK::WriteBatch,
    pub raft_wb: ER::LogBatch,
    // region_id -> (peer_id, ready_number)
    pub readies: HashMap<u64, (u64, u64)>,
    // Write raft state once for a region everytime writing to disk
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
    fn new(kv_wb: EK::WriteBatch, raft_wb: ER::LogBatch, waterfall_metrics: bool) -> Self {
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

    /// Add write task to this batch
    fn add_write_task(&mut self, mut task: AsyncWriteTask<EK, ER>) {
        if let Err(e) = task.valid() {
            panic!("task is not valid: {:?}", e);
        }
        self.readies
            .insert(task.region_id, (task.peer_id, task.ready_number));
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
        self.readies.clear();
        self.raft_states.clear();
        self.tasks.clear();
        self.state_size = 0;
    }

    #[inline]
    fn get_raft_size(&self) -> usize {
        self.state_size + self.raft_wb.persist_size()
    }

    fn before_write_to_db(&mut self, now: Instant) {
        // Put raft state to raft writebatch
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
        let mut stopped = false;
        while !stopped {
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
                        Err(TryRecvError::Disconnected) => {
                            stopped = true;
                            break;
                        }
                    }
                };
                match msg {
                    AsyncWriteMsg::Shutdown => {
                        stopped = true;
                        break;
                    }
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
                .observe(duration_to_sec(handle_begin.elapsed()) as f64);
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
        for (region_id, (peer_id, ready_number)) in &self.wb.readies {
            self.notifier
                .notify_persisted(*region_id, *peer_id, *ready_number, now);
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

    use crate::store::{Config, Transport};
    use crate::Result;
    use engine_rocks::RocksWriteBatch;
    use engine_test::kv::{new_engine, KvTestEngine};
    use engine_traits::{Mutable, Peekable, WriteBatchExt, ALL_CFS};
    use kvproto::raft_serverpb::RaftMessage;
    use tempfile::{Builder, TempDir};

    use super::*;

    fn create_tmp_engine(path: &str) -> (TempDir, KvTestEngine) {
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

    fn must_have_entries_and_state(
        raft_engine: &KvTestEngine,
        entries_state: Vec<(u64, Vec<Entry>, RaftLocalState)>,
    ) {
        let snapshot = raft_engine.snapshot();
        for (region_id, entries, state) in entries_state {
            for e in entries {
                let key = keys::raft_log_key(region_id, e.get_index());
                let val = snapshot.get_msg::<Entry>(&key).unwrap().unwrap();
                assert_eq!(val, e);
            }
            let val = snapshot
                .get_msg::<RaftLocalState>(&keys::raft_state_key(region_id))
                .unwrap()
                .unwrap();
            assert_eq!(val, state);
            let key = keys::raft_log_key(region_id, state.get_last_index() + 1);
            // last_index + 1 entry should not exist
            assert!(snapshot.get_msg::<Entry>(&key).unwrap().is_none());
        }
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
        e.set_index(index);
        e.set_term(term);
        e
    }

    fn new_raft_state(term: u64, vote: u64, commit: u64, last_index: u64) -> RaftLocalState {
        let mut raft_state = RaftLocalState::new();
        raft_state.mut_hard_state().set_term(term);
        raft_state.mut_hard_state().set_vote(vote);
        raft_state.mut_hard_state().set_commit(commit);
        raft_state.set_last_index(last_index);
        raft_state
    }

    #[derive(Clone)]
    struct TestNotifier {
        tx: Sender<(u64, (u64, u64))>,
    }

    impl Notifier for TestNotifier {
        fn notify_persisted(&self, region_id: u64, peer_id: u64, ready_number: u64, _now: Instant) {
            self.tx.send((region_id, (peer_id, ready_number))).unwrap()
        }
        fn notify_unreachable(&self, _region_id: u64, _to_peer_id: u64) {
            panic!("unimplemented");
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
        fn flush(&mut self) {}
    }

    fn must_have_same_count_msg(msg_count: u32, msg_rx: &Receiver<RaftMessage>) {
        let mut count = 0;
        while msg_rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, msg_count);
    }

    fn must_have_same_notifies(
        notifies: Vec<(u64, (u64, u64))>,
        notify_rx: &Receiver<(u64, (u64, u64))>,
    ) {
        let mut notify_set = HashSet::default();
        for n in notifies {
            notify_set.insert(n);
        }
        while let Ok(n) = notify_rx.try_recv() {
            if !notify_set.remove(&n) {
                panic!("{:?} not in expected notify", n);
            }
        }
        assert!(
            notify_set.is_empty(),
            "remaining expected notify {:?} not exist",
            notify_set
        );
    }

    fn must_wait_same_notifies(
        notifies: Vec<(u64, (u64, u64))>,
        notify_rx: &Receiver<(u64, (u64, u64))>,
    ) {
        let mut notify_map = HashMap::default();
        for (region_id, n) in notifies {
            notify_map.insert(region_id, n);
        }
        let timer = Instant::now();
        loop {
            match notify_rx.recv() {
                Ok((region_id, n)) => {
                    if let Some(n2) = notify_map.get(&region_id) {
                        if n == *n2 {
                            notify_map.remove(&region_id);
                            if notify_map.is_empty() {
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    panic!("recv error: {:?}", e);
                }
            }

            if timer.elapsed() > Duration::from_secs(5) {
                panic!("wait some notifies after 3 seconds")
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Help function for less code
    /// Option must not be none
    fn put_kv(wb: &mut Option<RocksWriteBatch>, key: &[u8], value: &[u8]) {
        wb.as_mut().unwrap().put(key, value).unwrap();
    }

    /// Help function for less code
    /// Option must not be none
    fn delete_kv(wb: &mut Option<RocksWriteBatch>, key: &[u8]) {
        wb.as_mut().unwrap().delete(key).unwrap();
    }

    struct TestAsyncWriteWorker {
        worker: AsyncWriteWorker<KvTestEngine, KvTestEngine, TestTransport, TestNotifier>,
        msg_rx: Receiver<RaftMessage>,
        notify_rx: Receiver<(u64, (u64, u64))>,
    }

    impl TestAsyncWriteWorker {
        fn new(cfg: &Config, kv_engine: &KvTestEngine, raft_engine: &KvTestEngine) -> Self {
            let (_, task_rx) = channel();
            let (msg_tx, msg_rx) = channel();
            let trans = TestTransport { tx: msg_tx };
            let (notify_tx, notify_rx) = channel();
            let notifier = TestNotifier { tx: notify_tx };
            Self {
                worker: AsyncWriteWorker::new(
                    1,
                    "writer".to_string(),
                    kv_engine.clone(),
                    raft_engine.clone(),
                    task_rx,
                    notifier,
                    trans,
                    cfg,
                ),
                msg_rx,
                notify_rx,
            }
        }
    }

    struct TestAsyncWriters {
        writers: AsyncWriters<KvTestEngine, KvTestEngine>,
        msg_rx: Receiver<RaftMessage>,
        notify_rx: Receiver<(u64, (u64, u64))>,
    }

    impl TestAsyncWriters {
        fn new(cfg: &Config, kv_engine: &KvTestEngine, raft_engine: &KvTestEngine) -> Self {
            let (msg_tx, msg_rx) = channel();
            let trans = TestTransport { tx: msg_tx };
            let (notify_tx, notify_rx) = channel();
            let notifier = TestNotifier { tx: notify_tx };
            let mut writers = AsyncWriters::new();
            writers
                .spawn(1, kv_engine, raft_engine, &notifier, &trans, &cfg)
                .unwrap();
            Self {
                writers,
                msg_rx,
                notify_rx,
            }
        }

        fn async_write_sender(
            &self,
            id: usize,
        ) -> &Sender<AsyncWriteMsg<KvTestEngine, KvTestEngine>> {
            &self.writers.senders()[id]
        }
    }

    #[test]
    fn test_batch_write() {
        let (_dir, kv_engine) = create_tmp_engine("async-io-batch-kv");
        let (_dir, raft_engine) = create_tmp_engine("async-io-batch-raft");
        let mut t = TestAsyncWriteWorker::new(&Config::default(), &kv_engine, &raft_engine);

        let mut task_1 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 10);
        task_1.kv_wb = Some(kv_engine.write_batch());
        put_kv(&mut task_1.kv_wb, b"kv_k1", b"kv_v1");
        task_1.raft_wb = Some(raft_engine.write_batch());
        put_kv(&mut task_1.raft_wb, b"raft_k1", b"raft_v1");
        task_1.entries.append(&mut vec![
            new_entry(5, 5),
            new_entry(6, 5),
            new_entry(7, 5),
            new_entry(8, 5),
        ]);
        task_1.raft_state = Some(new_raft_state(5, 123, 6, 8));
        task_1.messages.append(&mut vec![RaftMessage::default()]);

        t.worker.wb.add_write_task(task_1);

        let mut task_2 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(2, 2, 15);
        task_2.kv_wb = Some(kv_engine.write_batch());
        put_kv(&mut task_2.kv_wb, b"kv_k2", b"kv_v2");
        task_2.raft_wb = Some(raft_engine.write_batch());
        put_kv(&mut task_2.raft_wb, b"raft_k2", b"raft_v2");
        task_2
            .entries
            .append(&mut vec![new_entry(20, 15), new_entry(21, 15)]);
        task_2.raft_state = Some(new_raft_state(15, 234, 20, 21));
        task_2
            .messages
            .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

        t.worker.wb.add_write_task(task_2);

        let mut task_3 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 11);
        task_3.kv_wb = Some(kv_engine.write_batch());
        put_kv(&mut task_3.kv_wb, b"kv_k3", b"kv_v3");
        task_3.raft_wb = Some(raft_engine.write_batch());
        put_kv(&mut task_3.raft_wb, b"raft_k3", b"raft_v3");
        delete_kv(&mut task_3.raft_wb, b"raft_k1");
        task_3
            .entries
            .append(&mut vec![new_entry(6, 6), new_entry(7, 7)]);
        task_3.cut_logs = Some((8, 9));
        task_3.raft_state = Some(new_raft_state(7, 124, 6, 7));
        task_3
            .messages
            .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

        t.worker.wb.add_write_task(task_3);

        t.worker.sync_write();

        let snapshot = kv_engine.snapshot();
        assert_eq!(snapshot.get_value(b"kv_k1").unwrap().unwrap(), b"kv_v1");
        assert_eq!(snapshot.get_value(b"kv_k2").unwrap().unwrap(), b"kv_v2");
        assert_eq!(snapshot.get_value(b"kv_k3").unwrap().unwrap(), b"kv_v3");

        let snapshot = raft_engine.snapshot();
        assert!(snapshot.get_value(b"raft_k1").unwrap().is_none());
        assert_eq!(snapshot.get_value(b"raft_k2").unwrap().unwrap(), b"raft_v2");
        assert_eq!(snapshot.get_value(b"raft_k3").unwrap().unwrap(), b"raft_v3");

        must_have_same_notifies(vec![(1, (1, 11)), (2, (2, 15))], &t.notify_rx);

        must_have_entries_and_state(
            &raft_engine,
            vec![
                (
                    1,
                    vec![new_entry(5, 5), new_entry(6, 6), new_entry(7, 7)],
                    new_raft_state(7, 124, 6, 7),
                ),
                (
                    2,
                    vec![new_entry(20, 15), new_entry(21, 15)],
                    new_raft_state(15, 234, 20, 21),
                ),
            ],
        );

        must_have_same_count_msg(5, &t.msg_rx);
    }

    #[test]
    fn test_basic_flow() {
        let (_dir, kv_engine) = create_tmp_engine("async-io-basic-kv");
        let (_dir, raft_engine) = create_tmp_engine("async-io-basic-raft");
        let mut cfg = Config::default();
        cfg.store_io_pool_size = 2;
        let mut t = TestAsyncWriters::new(&cfg, &kv_engine, &raft_engine);

        let mut task_1 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 10);
        task_1.kv_wb = Some(kv_engine.write_batch());
        put_kv(&mut task_1.kv_wb, b"kv_k1", b"kv_v1");
        task_1.raft_wb = Some(raft_engine.write_batch());
        put_kv(&mut task_1.raft_wb, b"raft_k1", b"raft_v1");
        task_1
            .entries
            .append(&mut vec![new_entry(5, 5), new_entry(6, 5), new_entry(7, 5)]);
        task_1.raft_state = Some(new_raft_state(5, 234, 6, 7));
        task_1
            .messages
            .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

        t.async_write_sender(0)
            .send(AsyncWriteMsg::WriteTask(task_1))
            .unwrap();

        let mut task_2 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(2, 2, 20);
        task_2.kv_wb = Some(kv_engine.write_batch());
        put_kv(&mut task_2.kv_wb, b"kv_k2", b"kv_v2");
        task_2.raft_wb = Some(raft_engine.write_batch());
        put_kv(&mut task_2.raft_wb, b"raft_k2", b"raft_v2");
        task_2
            .entries
            .append(&mut vec![new_entry(50, 12), new_entry(51, 13)]);
        task_2.raft_state = Some(new_raft_state(13, 567, 49, 51));
        task_2
            .messages
            .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

        t.async_write_sender(1)
            .send(AsyncWriteMsg::WriteTask(task_2))
            .unwrap();

        let mut task_3 = AsyncWriteTask::<KvTestEngine, KvTestEngine>::new(1, 1, 15);
        task_3.kv_wb = Some(kv_engine.write_batch());
        put_kv(&mut task_3.kv_wb, b"kv_k3", b"kv_v3");
        delete_kv(&mut task_3.kv_wb, b"kv_k1");
        task_3.raft_wb = Some(raft_engine.write_batch());
        put_kv(&mut task_3.raft_wb, b"raft_k3", b"raft_v3");
        delete_kv(&mut task_3.raft_wb, b"raft_k2");
        task_3.entries.append(&mut vec![new_entry(6, 6)]);
        task_3.cut_logs = Some((7, 8));
        task_3.raft_state = Some(new_raft_state(6, 345, 6, 6));
        task_3
            .messages
            .append(&mut vec![RaftMessage::default(), RaftMessage::default()]);

        t.async_write_sender(0)
            .send(AsyncWriteMsg::WriteTask(task_3))
            .unwrap();

        must_wait_same_notifies(vec![(1, (1, 15)), (2, (2, 20))], &t.notify_rx);

        must_have_entries_and_state(
            &raft_engine,
            vec![
                (
                    1,
                    vec![new_entry(5, 5), new_entry(6, 6)],
                    new_raft_state(6, 345, 6, 6),
                ),
                (
                    2,
                    vec![new_entry(50, 12), new_entry(51, 13)],
                    new_raft_state(13, 567, 49, 51),
                ),
            ],
        );

        must_have_same_count_msg(6, &t.msg_rx);

        t.writers.shutdown();
    }
}
