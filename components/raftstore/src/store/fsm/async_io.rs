// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
use std::marker::PhantomData;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::{collections::VecDeque, time::Instant};

use crate::store::config::Config;
use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::AsyncWriterStoreMetrics;
use crate::store::metrics::*;
use crate::store::transport::Transport;
use crate::store::util::PerfContextStatistics;
use crate::store::PeerMsg;
use crate::{observe_perf_context_type, report_perf_context, Result};

use engine_rocks::{PerfContext, PerfLevel};
use engine_traits::{KvEngine, Mutable, RaftEngine, RaftLogBatch, WriteBatch, WriteOptions};
use error_code::ErrorCodeExt;
use kvproto::raft_serverpb::{RaftLocalState, RaftMessage};
use raft::eraftpb::Entry;

use std::time::Duration;
use tikv_util::collections::HashMap;
use tikv_util::time::{duration_to_micros, duration_to_sec, Instant as UtilInstant};

const KV_WB_SHRINK_SIZE: usize = 256 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 1024 * 1024;

pub struct SampleWindow {
    count: usize,
    buckets: VecDeque<f64>,
    buckets_val_cnt: VecDeque<usize>,
    bucket_factor: f64,
}

impl SampleWindow {
    pub fn new() -> Self {
        Self {
            count: 0,
            buckets: VecDeque::default(),
            buckets_val_cnt: VecDeque::default(),
            bucket_factor: 2.0,
        }
    }

    pub fn observe(&mut self, value: f64) {
        // For P99, P999
        self.count += 1;
        if self.buckets.is_empty() {
            self.buckets.push_back(value);
            self.buckets_val_cnt.push_back(0);
        } else {
            let mut bucket_pos = self.buckets.len() / 2;
            loop {
                let bucket_val = self.buckets[bucket_pos];
                if value < bucket_val {
                    if bucket_pos == 0 {
                        self.buckets.push_front(bucket_val / self.bucket_factor);
                        self.buckets_val_cnt.push_front(0);
                    } else {
                        bucket_pos -= 1;
                    }
                    continue;
                }
                let bucket_val_ub = bucket_val * self.bucket_factor;
                if value < bucket_val_ub {
                    break;
                }
                if bucket_pos + 1 >= self.buckets.len() {
                    self.buckets.push_back(bucket_val_ub);
                    self.buckets_val_cnt.push_back(0);
                }
                bucket_pos += 1;
            }
            self.buckets_val_cnt[bucket_pos] += 1;
        }
    }

    pub fn quantile(&mut self, quantile: f64) -> f64 {
        let mut cnt_sum = 0;
        let mut index = self.buckets_val_cnt.len() - 1;
        let sum_target = (self.count as f64 * quantile) as usize;
        for i in 0..self.buckets_val_cnt.len() {
            cnt_sum += self.buckets_val_cnt[i];
            if cnt_sum >= sum_target {
                index = i;
                break;
            }
        }
        self.buckets[index] * self.bucket_factor
    }
}

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
            error!(
                "failed to send noop to trigger persisted ready";
                "region_id" => region_id,
                "peer_id" => self.peer_id,
                "ready_number" => self.number,
                "error" => ?e,
            );
        }
    }
}

pub struct AsyncWriteTask {
    region_id: u64,
    pub entries: Vec<Entry>,
    pub cut_logs: Option<(u64, u64)>,
    pub unsynced_ready: Option<UnsyncedReady>,
    pub raft_state: Option<RaftLocalState>,
    pub proposal_times: Vec<Instant>,
    pub messages: Vec<RaftMessage>,
    pub msg_seq_id: usize,
    pub size: usize,
}

impl AsyncWriteTask {
    pub fn new(region_id: u64) -> Self {
        Self {
            region_id,
            entries: vec![],
            cut_logs: None,
            unsynced_ready: None,
            raft_state: None,
            proposal_times: vec![],
            messages: vec![],
            msg_seq_id: 0,
            size: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
            && self.cut_logs.is_none()
            && self.unsynced_ready.is_none()
            && self.raft_state.is_none()
            && self.proposal_times.is_empty()
            && self.messages.is_empty()
    }
}

pub enum AsyncWriteMsg {
    WriteTask(AsyncWriteTask),
    Shutdown,
}

pub struct AsyncWriteBatch<EK, WK, WR>
where
    EK: KvEngine,
    WK: WriteBatch<EK>,
    WR: RaftLogBatch,
{
    pub kv_wb: WK,
    pub raft_wb: WR,
    pub begin: Option<UtilInstant>,
    pub unsynced_readies: HashMap<u64, UnsyncedReady>,
    pub raft_states: HashMap<u64, RaftLocalState>,
    pub state_size: usize,
    pub tasks: Vec<AsyncWriteTask>,
    _phantom: PhantomData<EK>,
}

impl<EK, WK, WR> AsyncWriteBatch<EK, WK, WR>
where
    EK: KvEngine,
    WK: WriteBatch<EK>,
    WR: RaftLogBatch,
{
    fn new(kv_wb: WK, raft_wb: WR) -> Self {
        Self {
            kv_wb,
            raft_wb,
            begin: None,
            unsynced_readies: HashMap::default(),
            raft_states: HashMap::default(),
            tasks: vec![],
            state_size: 0,
            _phantom: PhantomData,
        }
    }

    fn on_taken_for_write(&mut self) {
        if self.is_empty() {
            self.begin = Some(UtilInstant::now());
        }
    }

    fn add_write_task(&mut self, mut task: AsyncWriteTask) {
        let entries = std::mem::take(&mut task.entries);
        self.raft_wb.append(task.region_id, entries).unwrap();
        if let Some((from, to)) = task.cut_logs {
            self.raft_wb.cut_logs(task.region_id, from, to);
        }
        if let Some(ready) = task.unsynced_ready.take() {
            self.unsynced_readies.insert(task.region_id, ready);
        }
        if let Some(raft_state) = task.raft_state.take() {
            if !self.raft_states.contains_key(&task.region_id) {
                self.state_size += 4 * 8;
            }
            self.raft_states.insert(task.region_id, raft_state);
        }
        self.tasks.push(task);
    }

    fn is_empty(&self) -> bool {
        self.raft_wb.is_empty()
            && self.unsynced_readies.is_empty()
            && self.raft_states.is_empty()
            && self.kv_wb.is_empty()
            && self.tasks.is_empty()
    }

    fn clear(&mut self) {
        // raft_wb doesn't have clear interface but it should be consumed by raft engine before
        self.kv_wb.clear();
        self.begin = None;
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

pub struct AsyncWriteAdaptiveQueue<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    wbs: VecDeque<AsyncWriteBatch<EK, EK::WriteBatch, ER::LogBatch>>,
    metrics: AsyncWriterStoreMetrics,
    size_limits: Vec<usize>,
    current_idx: usize,
    adaptive_idx: usize,
    adaptive_gain: usize,
    sample_window: SampleWindow,
    sample_quantile: f64,
    task_suggest_bytes_cache: usize,
    adaptive_size: bool,
}

impl<EK, ER> AsyncWriteAdaptiveQueue<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(
        kv_engine: &EK,
        raft_engine: &ER,
        queue_size: usize,
        queue_init_bytes: usize,
        queue_bytes_step: f64,
        queue_adaptive_gain: usize,
        queue_sample_quantile: f64,
        adaptive_size: bool,
    ) -> Self {
        let mut wbs = VecDeque::default();
        for _ in 0..queue_size {
            wbs.push_back(AsyncWriteBatch::new(
                kv_engine.write_batch(),
                raft_engine.log_batch(4 * 1024),
            ));
        }
        let mut size_limits = vec![];
        let mut size_limit = queue_init_bytes;
        for _ in 0..(queue_size * 2 + queue_adaptive_gain) {
            size_limits.push(size_limit);
            size_limit = (size_limit as f64 * queue_bytes_step) as usize;
        }
        Self {
            wbs,
            metrics: AsyncWriterStoreMetrics::default(),
            size_limits,
            current_idx: 0,
            adaptive_idx: 0,
            adaptive_gain: queue_adaptive_gain,
            sample_window: SampleWindow::new(),
            sample_quantile: queue_sample_quantile,
            task_suggest_bytes_cache: 0,
            adaptive_size,
        }
    }

    pub fn prepare_current_for_write(
        &mut self,
    ) -> &mut AsyncWriteBatch<EK, EK::WriteBatch, ER::LogBatch> {
        if self.adaptive_size {
            let current_size = self.wbs[self.current_idx].get_raft_size();
            if current_size
                >= self.size_limits[self.adaptive_gain + self.adaptive_idx + self.current_idx]
            {
                if self.current_idx + 1 < self.wbs.len() {
                    self.current_idx += 1;
                } else {
                    // do nothing, adaptive IO size
                }
            }
        }
        self.wbs[self.current_idx].on_taken_for_write();
        &mut self.wbs[self.current_idx]
    }

    fn detach_task(&mut self) -> AsyncWriteBatch<EK, EK::WriteBatch, ER::LogBatch> {
        self.metrics.queue_size.observe(self.current_idx as f64);
        self.metrics.adaptive_idx.observe(self.adaptive_idx as f64);

        let task = self.wbs.pop_front().unwrap();

        let task_bytes = task.get_raft_size();
        self.metrics.task_real_bytes.observe(task_bytes as f64);

        if self.adaptive_size {
            let limit_bytes =
                self.size_limits[self.adaptive_gain + self.adaptive_idx + self.current_idx];
            self.metrics.task_limit_bytes.observe(limit_bytes as f64);

            self.sample_window.observe(task_bytes as f64);
            let task_suggest_bytes = self.sample_window.quantile(self.sample_quantile);
            self.task_suggest_bytes_cache = task_suggest_bytes as usize;
            self.metrics.task_suggest_bytes.observe(task_suggest_bytes);

            let current_target_bytes =
                self.size_limits[self.adaptive_idx + self.current_idx] as f64;
            if task_suggest_bytes >= current_target_bytes {
                if self.adaptive_idx + (self.wbs.len() - 1) + 1 < self.size_limits.len() {
                    self.adaptive_idx += 1;
                }
            } else if self.adaptive_idx > 0
                && task_suggest_bytes
                    < (self.size_limits[self.adaptive_idx + self.current_idx - 1] as f64)
            {
                self.adaptive_idx -= 1;
            }
        }

        if self.current_idx != 0 {
            self.current_idx -= 1;
        }
        task
    }

    fn push_back_done_task(&mut self, mut task: AsyncWriteBatch<EK, EK::WriteBatch, ER::LogBatch>) {
        task.clear();
        self.wbs.push_back(task);
    }

    fn has_task(&self) -> bool {
        !self.wbs.front().unwrap().is_empty()
    }

    fn has_writable_task(&self) -> bool {
        if self.current_idx > 0 {
            return true;
        }
        let first_task = self.wbs.front().unwrap();
        self.task_suggest_bytes_cache == 0
            || first_task.get_raft_size() >= self.task_suggest_bytes_cache
    }

    fn flush_metrics(&mut self) {
        self.metrics.flush();
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
    receiver: Receiver<Vec<AsyncWriteMsg>>,
    queue: AsyncWriteAdaptiveQueue<EK, ER>,
    trans: T,
    perf_context_statistics: PerfContextStatistics,
    io_max_wait_us: i64,
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
        receiver: Receiver<Vec<AsyncWriteMsg>>,
        trans: T,
        config: &Config,
    ) -> Self {
        let queue = AsyncWriteAdaptiveQueue::new(
            &kv_engine,
            &raft_engine,
            config.store_batch_system.io_queue_size + 1,
            config.store_batch_system.io_queue_init_bytes,
            config.store_batch_system.io_queue_bytes_step,
            config.store_batch_system.io_queue_adaptive_gain,
            config.store_batch_system.io_queue_sample_quantile,
            config.store_batch_system.io_adaptive_size,
        );
        Self {
            store_id,
            tag,
            kv_engine,
            raft_engine,
            router,
            receiver,
            queue,
            trans,
            perf_context_statistics: PerfContextStatistics::new(config.perf_level),
            io_max_wait_us: config.store_batch_system.io_max_wait_us as i64,
        }
    }

    fn run(&mut self) {
        let mut msgs = vec![];
        loop {
            let loop_begin = UtilInstant::now();

            if !self.queue.has_task() {
                let msg = match self.receiver.recv() {
                    Ok(msg) => msg,
                    Err(_) => return,
                };
                msgs.push(msg);
            }
            STORE_WRITE_TASK_GEN_DURATION_HISTOGRAM.observe(duration_to_sec(loop_begin.elapsed()));

            let begin = UtilInstant::now();
            let len = self.receiver.len();
            for _ in 0..len {
                msgs.push(self.receiver.try_recv().unwrap());
            }
            STORE_WRITE_RECEIVE_MSG_DURATION_HISTOGRAM.observe(duration_to_sec(begin.elapsed()));
            let begin = UtilInstant::now();
            for msg_vec in msgs.drain(..) {
                for msg in msg_vec {
                    match msg {
                        AsyncWriteMsg::Shutdown => return,
                        AsyncWriteMsg::WriteTask(task) => {
                            let current = self.queue.prepare_current_for_write();
                            current.add_write_task(task);
                        }
                    }
                }
            }
            STORE_WRITE_HANDLE_MSG_DURATION_HISTOGRAM.observe(duration_to_sec(begin.elapsed()));

            if !self.queue.has_task() {
                continue;
            }

            let mut task = self.queue.detach_task();
            self.queue.flush_metrics();

            STORE_WRITE_TIME_TRIGGER_SIZE_HISTOGRAM.observe(task.get_raft_size() as f64);

            STORE_WRITE_WAIT_DURATION_HISTOGRAM
                .observe(duration_to_sec(task.begin.unwrap().elapsed()));

            self.sync_write(&mut task);

            self.queue.push_back_done_task(task);

            STORE_WRITE_LOOP_DURATION_HISTOGRAM
                .observe(duration_to_sec(loop_begin.elapsed()) as f64);
        }
    }

    fn sync_write(&mut self, batch: &mut AsyncWriteBatch<EK, EK::WriteBatch, ER::LogBatch>) {
        batch.before_write_to_db();

        self.perf_context_statistics.start();
        fail_point!("raft_before_save");
        if !batch.kv_wb.is_empty() {
            let now = UtilInstant::now();
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.kv_engine
                .write_opt(&batch.kv_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to write to kv engine: {:?}", self.tag, e);
                });
            if batch.kv_wb.data_size() > KV_WB_SHRINK_SIZE {
                batch.kv_wb = self.kv_engine.write_batch_with_cap(4 * 1024);
            }

            STORE_WRITE_KVDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }
        batch.after_write_to_kv_db();
        fail_point!("raft_between_save");
        if !batch.raft_wb.is_empty() {
            fail_point!("raft_before_save_on_store_1", self.store_id == 1, |_| {});
            let now = UtilInstant::now();
            self.raft_engine
                .consume_and_shrink(&mut batch.raft_wb, true, RAFT_WB_SHRINK_SIZE, 4 * 1024)
                .unwrap_or_else(|e| {
                    panic!("{} failed to write to raft engine: {:?}", self.tag, e);
                });

            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }
        report_perf_context!(
            self.perf_context_statistics,
            STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC
        );
        batch.after_write_to_db();

        let send_begin = UtilInstant::now();
        for task in &mut batch.tasks {
            for msg in task.messages.drain(..) {
                let region_id = msg.get_region_id();
                let from_peer_id = msg.get_from_peer().get_id();
                let to_peer_id = msg.get_to_peer().get_id();
                let to_store_id = msg.get_to_peer().get_store_id();
                if let Err(e) = self.trans.send(Some(task.msg_seq_id), msg) {
                    warn!(
                        "failed to send msg to other peer in async-writer";
                        "region_id" => region_id,
                        "peer_id" => from_peer_id,
                        "target_peer_id" => to_peer_id,
                        "target_store_id" => to_store_id,
                        "err" => ?e,
                        "error_code" => %e.error_code(),
                    );
                    // TODO: send msg to this region
                }
            }
        }
        self.trans.flush();
        STORE_WRITE_SEND_DURATION_HISTOGRAM.observe(duration_to_sec(send_begin.elapsed()));

        let callback_begin = UtilInstant::now();
        for (region_id, r) in &batch.unsynced_readies {
            r.flush(*region_id, &self.router);
        }
        STORE_WRITE_CALLBACK_DURATION_HISTOGRAM.observe(duration_to_sec(callback_begin.elapsed()));

        fail_point!("raft_after_save");
    }
}

pub struct AsyncWriters {
    writers: Vec<Sender<Vec<AsyncWriteMsg>>>,
    handlers: Vec<JoinHandle<()>>,
}

impl AsyncWriters {
    pub fn new() -> Self {
        Self {
            writers: vec![],
            handlers: vec![],
        }
    }

    pub fn senders(&self) -> &Vec<Sender<Vec<AsyncWriteMsg>>> {
        &self.writers
    }

    pub fn spawn<EK: KvEngine, ER: RaftEngine, T: Transport + 'static>(
        &mut self,
        store_id: u64,
        kv_engine: &EK,
        raft_engine: &ER,
        router: &RaftRouter<EK, ER>,
        trans: &T,
        config: &Config,
    ) -> Result<()> {
        for i in 0..config.store_batch_system.io_pool_size {
            let tag = format!("store-writer-{}", i);
            let (tx, rx) = unbounded();
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
            self.writers[i].send(vec![AsyncWriteMsg::Shutdown]).unwrap();
            handler.join().unwrap();
        }
    }
}
