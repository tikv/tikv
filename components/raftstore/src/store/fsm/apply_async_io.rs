// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::coprocessor::CoprocessorHost;
use crate::store::config::Config;
use crate::store::fsm::apply::{ApplyCallback, ApplyRes};
use crate::store::fsm::async_io::SampleWindow;
use crate::store::fsm::ApplyNotifier;
use crate::store::local_metrics::AsyncWriterApplyMetrics;
use crate::store::metrics::*;
use crate::store::util::PerfContextStatistics;
use crate::store::PeerMsg;
use crate::{observe_perf_context_type, report_perf_context, Result};

use engine_rocks::{PerfContext, PerfLevel};
use engine_traits::{KvEngine, Mutable, RaftEngine, RaftLogBatch, WriteBatch, WriteOptions};

use tikv_util::time::{duration_to_sec, Instant};

const DEFAULT_APPLY_WB_SIZE: usize = 4 * 1024;
const APPLY_WB_SHRINK_SIZE: usize = 1024 * 1024;

pub struct ApplyAsyncWriteTask<EK, WK>
where
    EK: KvEngine,
    WK: WriteBatch<EK>,
{
    pub kv_wb: WK,
    pub sync_log: bool,
    pub begin: Option<Instant>,
    _phantom: PhantomData<EK>,
    pub cbs: Vec<ApplyCallback<EK>>,
    pub apply_res: Vec<ApplyRes<EK::Snapshot>>,
    pub destroy_res: Vec<(u64, PeerMsg<EK>)>,
}

impl<EK, WK> ApplyAsyncWriteTask<EK, WK>
where
    EK: KvEngine,
    WK: WriteBatch<EK>,
{
    fn new(kv_wb: WK) -> Self {
        Self {
            kv_wb,
            sync_log: false,
            begin: None,
            _phantom: PhantomData,
            cbs: vec![],
            apply_res: vec![],
            destroy_res: vec![],
        }
    }

    pub fn on_taken_for_write(&mut self) {
        if self.is_empty() {
            self.begin = Some(Instant::now_coarse());
        }
    }

    pub fn update_apply(
        &mut self,
        sync_log: bool,
        cb: ApplyCallback<EK>,
        apply_res: ApplyRes<EK::Snapshot>,
    ) {
        self.sync_log |= sync_log;
        self.cbs.push(cb);
        self.apply_res.push(apply_res);
    }

    pub fn update_destroy(&mut self, region_id: u64, msg: PeerMsg<EK>) {
        self.destroy_res.push((region_id, msg));
    }

    pub fn is_empty(&self) -> bool {
        self.kv_wb.is_empty()
            && self.cbs.is_empty()
            && self.apply_res.is_empty()
            && self.destroy_res.is_empty()
    }

    pub fn clear(&mut self) {
        // TODO: need shrink
        self.kv_wb.clear();
        self.begin = None;
        self.cbs.clear();
        self.apply_res.clear();
        self.destroy_res.clear();
    }
}

pub struct ApplyAsyncWriteTasks<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    stop: bool,
    wbs: VecDeque<ApplyAsyncWriteTask<EK, W>>,
    metrics: AsyncWriterApplyMetrics,
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

impl<EK, W> ApplyAsyncWriteTasks<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    pub fn new(
        kv_engine: &EK,
        queue_size: usize,
        queue_init_bytes: usize,
        queue_bytes_step: f64,
        queue_adaptive_gain: usize,
        queue_sample_quantile: f64,
        io_wait_max: Duration,
    ) -> Self {
        let mut wbs = VecDeque::default();
        for _ in 0..queue_size {
            wbs.push_back(ApplyAsyncWriteTask::new(W::with_capacity(
                &kv_engine,
                DEFAULT_APPLY_WB_SIZE,
            )));
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
            metrics: AsyncWriterApplyMetrics::default(),
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

    pub fn prepare_current_for_write(&mut self) {
        let current_size = self.wbs[self.current_idx].kv_wb.data_size();
        if current_size
            >= self.size_limits[self.adaptive_gain + self.adaptive_idx + self.current_idx]
        {
            if self.current_idx + 1 < self.wbs.len() {
                self.current_idx += 1;
            } else {
                // do nothing, adaptive IO size
            }
        }
        self.wbs[self.current_idx].on_taken_for_write();
    }

    pub fn get_current_task(&mut self) -> &mut ApplyAsyncWriteTask<EK, W> {
        &mut self.wbs[self.current_idx]
    }

    pub fn should_notify(&self) -> bool {
        self.current_idx == 0 && self.should_write_first_task()
    }

    fn detach_task(&mut self) -> ApplyAsyncWriteTask<EK, W> {
        self.metrics.queue_size.observe(self.current_idx as f64);
        self.metrics.adaptive_idx.observe(self.adaptive_idx as f64);

        let task = self.wbs.pop_front().unwrap();

        let task_bytes = task.kv_wb.data_size();
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

    fn push_back_done_task(&mut self, mut task: ApplyAsyncWriteTask<EK, W>) {
        task.clear();
        self.wbs.push_back(task);
    }

    fn should_write_first_task(&self) -> bool {
        let first_task = self.wbs.front().unwrap();
        if first_task.is_empty() {
            return false;
        }
        self.task_suggest_bytes_cache == 0
            || first_task.kv_wb.data_size() >= self.task_suggest_bytes_cache
            || first_task.begin.unwrap().elapsed() >= self.io_wait_max
    }

    fn flush_metrics(&mut self) {
        self.metrics.flush();
    }
}

pub type ApplyAsyncWriter<EK, W> = Arc<(Mutex<ApplyAsyncWriteTasks<EK, W>>, Condvar)>;

struct ApplyAsyncWriteWorker<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    store_id: u64,
    tag: String,
    kv_engine: EK,
    pub writer: ApplyAsyncWriter<EK, W>,
    cv_wait: Duration,
    notifier: Box<dyn ApplyNotifier<EK>>,
    coprocessor_host: CoprocessorHost<EK>,
    perf_context_statistics: PerfContextStatistics,
}

impl<EK, W> ApplyAsyncWriteWorker<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    fn new(
        store_id: u64,
        tag: String,
        kv_engine: EK,
        notifier: Box<dyn ApplyNotifier<EK>>,
        coprocessor_host: CoprocessorHost<EK>,
        config: &Config,
    ) -> Self {
        let writer = Arc::new((
            Mutex::new(ApplyAsyncWriteTasks::new(
                &kv_engine,
                config.apply_batch_system.io_queue_size + 1,
                config.apply_batch_system.io_queue_init_bytes,
                config.apply_batch_system.io_queue_bytes_step,
                config.apply_batch_system.io_queue_adaptive_gain,
                config.apply_batch_system.io_queue_sample_quantile,
                Duration::from_micros(config.apply_batch_system.io_max_wait_us),
            )),
            Condvar::new(),
        ));
        Self {
            store_id,
            tag,
            kv_engine,
            writer,
            cv_wait: Duration::from_micros(config.apply_batch_system.io_max_wait_us / 2),
            notifier,
            coprocessor_host,
            perf_context_statistics: PerfContextStatistics::new(config.perf_level),
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
            APPLY_WRITE_WAIT_DURATION_HISTOGRAM
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

    fn sync_write(&mut self, task: &mut ApplyAsyncWriteTask<EK, W>) {
        if !task.kv_wb.is_empty() {
            let now = Instant::now_coarse();
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(task.sync_log);
            task.kv_wb
                .write_to_engine(&self.kv_engine, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to write to kv engine: {:?}", self.tag, e);
                });
            if task.kv_wb.data_size() > APPLY_WB_SHRINK_SIZE {
                task.kv_wb = W::with_capacity(&self.kv_engine, DEFAULT_APPLY_WB_SIZE);
            }
            report_perf_context!(
                self.perf_context_statistics,
                APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC
            );

            APPLY_WRITE_KVDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }
        self.coprocessor_host.on_flush_apply(self.kv_engine.clone());

        for cb in task.cbs.drain(..) {
            cb.invoke_all(&self.coprocessor_host);
        }

        let apply_res = std::mem::replace(&mut task.apply_res, vec![]);
        self.notifier.notify(apply_res);

        for (region_id, msg) in task.destroy_res.drain(..) {
            self.notifier.notify_one(region_id, msg);
        }
    }
}

pub struct ApplyAsyncWriters<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK>,
{
    writers: Vec<ApplyAsyncWriter<EK, W>>,
    handlers: Vec<JoinHandle<()>>,
}

impl<EK, W> ApplyAsyncWriters<EK, W>
where
    EK: KvEngine,
    W: WriteBatch<EK> + 'static,
{
    pub fn new() -> Self {
        Self {
            writers: vec![],
            handlers: vec![],
        }
    }

    pub fn writers(&self) -> &Vec<ApplyAsyncWriter<EK, W>> {
        &self.writers
    }

    pub fn spawn(
        &mut self,
        store_id: u64,
        kv_engine: &EK,
        notifier: Box<dyn ApplyNotifier<EK>>,
        coprocessor_host: &CoprocessorHost<EK>,
        config: &Config,
    ) -> Result<()> {
        for i in 0..config.store_batch_system.io_pool_size {
            let tag = format!("apply-writer-{}", i);
            let mut worker = ApplyAsyncWriteWorker::new(
                store_id,
                tag.clone(),
                kv_engine.clone(),
                notifier.clone_box(),
                coprocessor_host.clone(),
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
