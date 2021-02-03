// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::store::config::Config;
use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::AsyncWriterStoreMetrics;
use crate::store::metrics::*;
use crate::store::PeerMsg;
use crate::Result;
use engine_traits::{KvEngine, Mutable, RaftEngine, RaftLogBatch, WriteBatch, WriteOptions};
use tikv_util::collections::HashMap;
use tikv_util::time::{duration_to_sec, Instant};

const KV_WB_SHRINK_SIZE: usize = 256 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 1024 * 1024;

pub struct SampleWindow {
    count: usize,
    buckets: VecDeque<f64>,
    buckets_val_cnt: VecDeque<usize>,
    bucket_factor: f64,
}

impl SampleWindow {
    pub fn new() -> SampleWindow {
        SampleWindow {
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
    pub number: u64,
    pub region_id: u64,
    pub notifier: Arc<AtomicU64>,
}

impl UnsyncedReady {
    fn new(number: u64, region_id: u64, notifier: Arc<AtomicU64>) -> UnsyncedReady {
        UnsyncedReady {
            number,
            region_id,
            notifier,
        }
    }

    fn flush<EK, ER>(&self, router: &RaftRouter<EK, ER>)
    where
        EK: KvEngine,
        ER: RaftEngine,
    {
        loop {
            let pre_number = self.notifier.load(Ordering::Acquire);
            // TODO: reduce duplicated messages
            //assert_ne!(pre_number, self.number);
            if pre_number >= self.number {
                break;
            }
            if pre_number
                == self
                    .notifier
                    .compare_and_swap(pre_number, self.number, Ordering::AcqRel)
            {
                if let Err(e) = router.force_send(self.region_id, PeerMsg::Noop) {
                    error!(
                        "failed to send noop to trigger persisted ready";
                        "region_id" => self.region_id,
                        "ready_number" => self.number,
                        "error" => ?e,
                    );
                }
                break;
            }
        }
    }
}

pub struct AsyncWriteTask<EK, WK, WR>
where
    EK: KvEngine,
    WK: WriteBatch<EK>,
    WR: RaftLogBatch,
{
    pub kv_wb: WK,
    pub raft_wb: WR,
    pub unsynced_readies: HashMap<u64, UnsyncedReady>,
    pub begin: Option<Instant>,
    _phantom: PhantomData<EK>,
}

impl<EK, WK, WR> AsyncWriteTask<EK, WK, WR>
where
    EK: KvEngine,
    WK: WriteBatch<EK>,
    WR: RaftLogBatch,
{
    fn new(kv_wb: WK, raft_wb: WR) -> AsyncWriteTask<EK, WK, WR> {
        Self {
            kv_wb,
            raft_wb,
            unsynced_readies: HashMap::default(),
            begin: None,
            _phantom: PhantomData,
        }
    }

    pub fn on_taken_for_write(&mut self) {
        if self.is_empty() {
            self.begin = Some(Instant::now_coarse());
        }
    }

    pub fn on_wb_written(
        &mut self,
        region_id: u64,
        ready_number: u64,
        region_notifier: Arc<AtomicU64>,
    ) {
        self.unsynced_readies.insert(
            region_id,
            UnsyncedReady::new(ready_number, region_id, region_notifier),
        );
    }

    pub fn is_empty(&self) -> bool {
        self.raft_wb.is_empty() && self.unsynced_readies.is_empty() && self.kv_wb.is_empty()
    }

    pub fn clear(&mut self) {
        // raft_wb doesn't have clear interface but it should be consumed by raft engine before
        self.kv_wb.clear();
        self.unsynced_readies.clear();
        self.begin = None;
    }
}

pub struct AsyncWriteAdaptiveTasks<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    stop: bool,
    wbs: VecDeque<AsyncWriteTask<EK, EK::WriteBatch, ER::LogBatch>>,
    metrics: AsyncWriterStoreMetrics,
    queue_size: usize,
    queue_init_bytes: usize,
    queue_bytes_step: f64,
    size_limits: Vec<usize>,
    current_idx: usize,
    adaptive_idx: usize,
    adaptive_gain: usize,
    sample_window: SampleWindow,
    sample_quantile: f64,
    task_suggest_bytes_cache: usize,
    io_wait_max: Duration,
}

impl<EK, ER> AsyncWriteAdaptiveTasks<EK, ER>
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
        io_wait_max: Duration,
    ) -> Self {
        let mut wbs = VecDeque::default();
        for _ in 0..queue_size {
            wbs.push_back(AsyncWriteTask::new(
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
            stop: false,
            wbs,
            metrics: AsyncWriterStoreMetrics::default(),
            queue_size,
            queue_init_bytes,
            queue_bytes_step,
            size_limits,
            current_idx: 0,
            adaptive_idx: 0,
            adaptive_gain: queue_adaptive_gain,
            sample_window: SampleWindow::new(),
            sample_quantile: queue_sample_quantile,
            task_suggest_bytes_cache: 0,
            io_wait_max,
        }
    }

    pub fn prepare_current_for_write(
        &mut self,
    ) -> (&mut AsyncWriteTask<EK, EK::WriteBatch, ER::LogBatch>, bool) {
        let current_size = self.wbs[self.current_idx].raft_wb.persist_size();
        if current_size
            >= self.size_limits[self.adaptive_gain + self.adaptive_idx + self.current_idx]
        {
            if self.current_idx + 1 < self.wbs.len() {
                self.current_idx += 1;
            } else {
                // do nothing, adaptive IO size
            }
        }
        let current = &mut self.wbs[self.current_idx];
        current.on_taken_for_write();
        (current, self.current_idx == 0)
    }

    pub fn detach_task(&mut self) -> AsyncWriteTask<EK, EK::WriteBatch, ER::LogBatch> {
        self.metrics.queue_size.observe(self.current_idx as f64);
        self.metrics.adaptive_idx.observe(self.adaptive_idx as f64);

        let task = self.wbs.pop_front().unwrap();

        let task_bytes = task.raft_wb.persist_size();
        self.metrics.task_real_bytes.observe(task_bytes as f64);

        let limit_bytes =
            self.size_limits[self.adaptive_gain + self.adaptive_idx + self.current_idx];
        self.metrics.task_limit_bytes.observe(limit_bytes as f64);

        self.sample_window.observe(task_bytes as f64);
        let task_suggest_bytes = self.sample_window.quantile(self.sample_quantile);
        self.task_suggest_bytes_cache = task_suggest_bytes as usize;
        self.metrics.task_suggest_bytes.observe(task_suggest_bytes);

        let current_target_bytes = self.size_limits[self.adaptive_idx + self.current_idx] as f64;
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

        if self.current_idx != 0 {
            self.current_idx -= 1;
        }
        task
    }

    pub fn push_back_done_task(
        &mut self,
        mut task: AsyncWriteTask<EK, EK::WriteBatch, ER::LogBatch>,
    ) {
        task.clear();
        self.wbs.push_back(task);
    }

    pub fn should_write_first_task(&self) -> bool {
        let first_task = self.wbs.front().unwrap();
        if first_task.is_empty() {
            return false;
        }
        self.task_suggest_bytes_cache == 0
            || first_task.raft_wb.persist_size() >= self.task_suggest_bytes_cache
            || first_task.begin.unwrap().elapsed() >= self.io_wait_max
    }

    pub fn flush_metrics(&mut self) {
        self.metrics.flush();
    }
}

pub type AsyncWriter<ER, EK> = Arc<(Mutex<AsyncWriteAdaptiveTasks<ER, EK>>, Condvar)>;

struct AsyncWriteWorker<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    store_id: u64,
    tag: String,
    kv_engine: EK,
    raft_engine: ER,
    router: RaftRouter<EK, ER>,
    pub writer: AsyncWriter<EK, ER>,
    cv_wait: Duration,
}

impl<EK, ER> AsyncWriteWorker<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(
        store_id: u64,
        tag: String,
        kv_engine: EK,
        raft_engine: ER,
        router: RaftRouter<EK, ER>,
        config: &Config,
    ) -> AsyncWriteWorker<EK, ER> {
        let writer = Arc::new((
            Mutex::new(AsyncWriteAdaptiveTasks::new(
                &kv_engine,
                &raft_engine,
                config.store_io_queue_size + 1,
                config.store_io_queue_init_bytes,
                config.store_io_queue_bytes_step,
                config.store_io_queue_adaptive_gain,
                config.store_io_queue_sample_quantile,
                Duration::from_micros(config.store_io_max_wait_us),
            )),
            Condvar::new(),
        ));
        Self {
            store_id,
            tag,
            kv_engine,
            raft_engine,
            router,
            writer,
            cv_wait: Duration::from_micros(config.store_io_max_wait_us / 2),
        }
    }

    fn run(&mut self) {
        loop {
            let mut task = {
                let mut w = self.writer.0.lock().unwrap();
                while !w.stop && !w.should_write_first_task() {
                    w = self.writer.1.wait_timeout(w, self.cv_wait).unwrap().0;
                }
                if w.stop {
                    return;
                }
                w.detach_task()
            };

            // TODO: metric change name?
            STORE_WRITE_RAFTDB_TICK_DURATION_HISTOGRAM
                .observe(duration_to_sec(task.begin.unwrap().elapsed()) as f64);

            self.sync_write(&mut task);

            // TODO: block if too many tasks
            {
                let mut tasks = self.writer.0.lock().unwrap();
                tasks.push_back_done_task(task);
                tasks.flush_metrics();
            }
        }
    }

    fn sync_write(&mut self, task: &mut AsyncWriteTask<EK, EK::WriteBatch, ER::LogBatch>) {
        fail_point!("raft_before_save");
        if !task.kv_wb.is_empty() {
            let now = Instant::now_coarse();
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.kv_engine
                .write_opt(&task.kv_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save append state result: {:?}", self.tag, e);
                });
            if task.kv_wb.data_size() > KV_WB_SHRINK_SIZE {
                task.kv_wb = self.kv_engine.write_batch_with_cap(4 * 1024);
            }

            STORE_WRITE_KVDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }
        fail_point!("raft_between_save");
        if !task.raft_wb.is_empty() {
            fail_point!("raft_before_save_on_store_1", self.store_id == 1, |_| {});
            let now = Instant::now_coarse();
            self.raft_engine
                .consume_and_shrink(&mut task.raft_wb, true, RAFT_WB_SHRINK_SIZE, 4 * 1024)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save raft append result: {:?}", self.tag, e);
                });

            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }

        for (_, r) in &task.unsynced_readies {
            r.flush(&self.router);
        }

        fail_point!("raft_after_save");
    }
}

pub struct AsyncWriters<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub writers: Vec<AsyncWriter<EK, ER>>,
    handlers: Vec<JoinHandle<()>>,
}

impl<EK, ER> AsyncWriters<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new() -> AsyncWriters<EK, ER> {
        Self {
            writers: vec![],
            handlers: vec![],
        }
    }

    pub fn spawn(
        &mut self,
        store_id: u64,
        kv_engine: &EK,
        raft_engine: &ER,
        router: &RaftRouter<EK, ER>,
        config: &Config,
    ) -> Result<()> {
        for i in 0..config.store_batch_system.io_pool_size {
            let tag = format!("raftdb-async-writer-{}", i);
            let mut worker = AsyncWriteWorker::new(
                store_id,
                tag.clone(),
                kv_engine.clone(),
                raft_engine.clone(),
                router.clone(),
                config,
            );
            let writer = worker.writer.clone();
            let t = thread::Builder::new().name(thd_name!(tag)).spawn(move || {
                worker.run();
            })?;
            self.writers.push(writer);
            self.handlers.push(t);
        }
        Ok(())
    }

    pub fn shutdown(&mut self) {
        assert_eq!(self.writers.len(), self.handlers.len());
        for (i, handler) in self.handlers.drain(..).enumerate() {
            let mut tasks = self.writers[i].0.lock().unwrap();
            tasks.stop = true;
            drop(tasks);
            self.writers[i].1.notify_one();
            handler.join().unwrap();
        }
    }
}
