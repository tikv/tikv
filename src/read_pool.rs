// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::TryFrom,
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::SyncSender,
        Arc, Mutex,
    },
    time::Duration,
};

use file_system::{set_io_type, IoType};
use futures::{
    channel::oneshot,
    future::{FutureExt, TryFutureExt},
};
use kvproto::{errorpb, kvrpcpb::CommandPri};
use online_config::{ConfigChange, ConfigManager, ConfigValue, Result as CfgResult};
use prometheus::{core::Metric, Histogram, IntCounter, IntGauge};
use resource_control::{
    with_resource_limiter, ControlledFuture, ResourceController, ResourceLimiter, TaskMetadata,
};
use thiserror::Error;
use tikv_util::{
    sys::{cpu_time::ProcessStat, SysQuota},
    time::Instant,
    worker::{Runnable, RunnableWithTimer, Scheduler, Worker},
    yatp_pool::{self, CleanupMethod, FuturePool, PoolTicker, YatpPoolBuilder},
};
use tracker::TrackedFuture;
use yatp::{
    metrics::MULTILEVEL_LEVEL_ELAPSED, pool::Remote, queue::Extras, task::future::TaskCell,
};

use self::metrics::*;
use crate::{
    config::{UnifiedReadPoolConfig, UNIFIED_READPOOL_MIN_CONCURRENCY},
    storage::kv::{destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter},
};

// the duration to check auto-scale unified-thread-pool's thread
const READ_POOL_THREAD_CHECK_DURATION: Duration = Duration::from_secs(10);
// consider scale out read pool size if the average thread cpu usage is higher
// than this threshold.
const READ_POOL_THREAD_HIGH_THRESHOLD: f64 = 0.8;
// consider scale in read pool size if the average thread cpu usage is lower
// than this threshold.
const READ_POOL_THREAD_LOW_THRESHOLD: f64 = 0.7;
// avg running tasks per-thread that indicates read-pool is busy
const RUNNING_TASKS_PER_THREAD_THRESHOLD: i64 = 3;

pub enum ReadPool {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        pool: yatp::ThreadPool<TaskCell>,
        running_tasks: IntGauge,
        running_threads: IntGauge,
        max_tasks: usize,
        pool_size: usize,
        resource_ctl: Option<Arc<ResourceController>>,
        time_slice_inspector: Arc<TimeSliceInspector>,
    },
}

impl ReadPool {
    pub fn handle(&self) -> ReadPoolHandle {
        match self {
            ReadPool::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => ReadPoolHandle::FuturePools {
                read_pool_high: read_pool_high.clone(),
                read_pool_normal: read_pool_normal.clone(),
                read_pool_low: read_pool_low.clone(),
            },
            ReadPool::Yatp {
                pool,
                running_tasks,
                running_threads,
                max_tasks,
                pool_size,
                resource_ctl,
                time_slice_inspector,
            } => ReadPoolHandle::Yatp {
                remote: pool.remote().clone(),
                running_tasks: running_tasks.clone(),
                running_threads: running_threads.clone(),
                max_tasks: *max_tasks,
                pool_size: *pool_size,
                resource_ctl: resource_ctl.clone(),
                time_slice_inspector: time_slice_inspector.clone(),
            },
        }
    }
}

#[derive(Clone)]
pub enum ReadPoolHandle {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        remote: Remote<TaskCell>,
        running_tasks: IntGauge,
        running_threads: IntGauge,
        max_tasks: usize,
        pool_size: usize,
        resource_ctl: Option<Arc<ResourceController>>,
        time_slice_inspector: Arc<TimeSliceInspector>,
    },
}

impl ReadPoolHandle {
    pub fn spawn<F>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
    ) -> Result<(), ReadPoolError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => {
                let pool = match priority {
                    CommandPri::High => read_pool_high,
                    CommandPri::Normal => read_pool_normal,
                    CommandPri::Low => read_pool_low,
                };

                pool.spawn(f)?;
            }
            ReadPoolHandle::Yatp {
                remote,
                running_tasks,
                max_tasks,
                resource_ctl,
                ..
            } => {
                let running_tasks = running_tasks.clone();
                // Note that the running task number limit is not strict.
                // If several tasks are spawned at the same time while the running task number
                // is close to the limit, they may all pass this check and the number of running
                // tasks may exceed the limit.
                if running_tasks.get() as usize >= *max_tasks {
                    return Err(ReadPoolError::UnifiedReadPoolFull);
                }

                running_tasks.inc();
                let fixed_level = match priority {
                    CommandPri::High => Some(0),
                    CommandPri::Normal => None,
                    CommandPri::Low => Some(2),
                };
                let group_name = metadata.group_name().to_owned();
                let mut extras = Extras::new_multilevel(task_id, fixed_level);
                extras.set_metadata(metadata.to_vec());
                let task_cell = if let Some(resource_ctl) = resource_ctl {
                    TaskCell::new(
                        TrackedFuture::new(with_resource_limiter(
                            ControlledFuture::new(
                                f.map(move |_| {
                                    running_tasks.dec();
                                }),
                                resource_ctl.clone(),
                                group_name,
                            ),
                            resource_limiter,
                        )),
                        extras,
                    )
                } else {
                    TaskCell::new(
                        TrackedFuture::new(f.map(move |_| {
                            running_tasks.dec();
                        })),
                        extras,
                    )
                };
                remote.spawn(task_cell);
            }
        }
        Ok(())
    }

    pub fn spawn_handle<F, T>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
    ) -> impl Future<Output = Result<T, ReadPoolError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<T>();
        let res = self.spawn(
            f.map(move |res| {
                let _ = tx.send(res);
            }),
            priority,
            task_id,
            metadata,
            resource_limiter,
        );
        async move {
            res?;
            rx.map_err(ReadPoolError::from).await
        }
    }

    pub fn get_normal_pool_size(&self) -> usize {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_normal, ..
            } => read_pool_normal.get_pool_size(),
            ReadPoolHandle::Yatp { pool_size, .. } => *pool_size,
        }
    }

    pub fn get_queue_size_per_worker(&self) -> usize {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_normal, ..
            } => read_pool_normal.get_running_task_count() / read_pool_normal.get_pool_size(),
            ReadPoolHandle::Yatp {
                running_tasks,
                pool_size,
                ..
            } => running_tasks.get() as usize / *pool_size,
        }
    }

    pub fn scale_pool_size(&mut self, max_thread_count: usize) {
        match self {
            ReadPoolHandle::FuturePools { .. } => {
                unreachable!()
            }
            ReadPoolHandle::Yatp {
                remote,
                running_threads,
                max_tasks,
                pool_size,
                ..
            } => {
                remote.scale_workers(max_thread_count);
                *max_tasks = max_tasks
                    .saturating_div(*pool_size)
                    .saturating_mul(max_thread_count);
                running_threads.set(max_thread_count as i64);
                *pool_size = max_thread_count;
            }
        }
    }

    pub fn get_ewma_time_slice(&self) -> Option<Duration> {
        match self {
            ReadPoolHandle::FuturePools { .. } => None,
            ReadPoolHandle::Yatp {
                time_slice_inspector,
                ..
            } => Some(time_slice_inspector.get_ewma_time_slice()),
        }
    }

    pub fn update_ewma_time_slice(&self) {
        if let ReadPoolHandle::Yatp {
            time_slice_inspector,
            ..
        } = self
        {
            time_slice_inspector.update();
        }
    }

    pub fn get_estimated_wait_duration(&self) -> Option<Duration> {
        self.get_ewma_time_slice()
            .map(|s| s * (self.get_queue_size_per_worker() as u32))
    }

    pub fn check_busy_threshold(
        &self,
        busy_threshold: Duration,
    ) -> Result<(), errorpb::ServerIsBusy> {
        if busy_threshold.is_zero() {
            return Ok(());
        }
        let estimated_wait = match self.get_estimated_wait_duration() {
            Some(estimated_wait) if estimated_wait > busy_threshold => estimated_wait,
            _ => return Ok(()),
        };
        // TODO: Get applied_index from the raftstore and check memory locks. Then, we
        // can skip read index in replica read. But now the difficulty is that we don't
        // have access to the the local reader in gRPC threads.
        let mut busy_err = errorpb::ServerIsBusy::default();
        busy_err.set_reason("estimated wait time exceeds threshold".to_owned());
        busy_err.estimated_wait_ms = u32::try_from(estimated_wait.as_millis()).unwrap_or(u32::MAX);
        warn!("Already many pending tasks in the read queue, task is rejected";
            "busy_threshold" => ?&busy_threshold,
            "busy_err" => ?&busy_err,
        );
        Err(busy_err)
    }
}

pub const UPDATE_EWMA_TIME_SLICE_INTERVAL: Duration = Duration::from_millis(200);

pub struct TimeSliceInspector {
    // `atomic_ewma_nanos` is a mirror of `inner.ewma` provided for fast access. It is updated in
    // the `update` method.
    atomic_ewma_nanos: AtomicU64,
    inner: Mutex<TimeSliceInspectorInner>,
}

struct TimeSliceInspectorInner {
    time_slice_hist: [Histogram; 3],
    ewma: Duration,

    last_sum: Duration,
    last_count: u64,
}

impl TimeSliceInspector {
    pub fn new(name: &str) -> Self {
        let time_slice_hist = [
            yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "0"]),
            yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "1"]),
            yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "2"]),
        ];
        let inner = TimeSliceInspectorInner {
            time_slice_hist,
            ewma: Duration::default(),
            last_sum: Duration::default(),
            last_count: 0,
        };
        Self {
            atomic_ewma_nanos: AtomicU64::default(),
            inner: Mutex::new(inner),
        }
    }

    pub fn update(&self) {
        // new_ewma = WEIGHT * new_val + (1 - WEIGHT) * old_ewma
        const WEIGHT: f64 = 0.3;
        // If the accumulated time slice is less than 100ms, the EWMA is not updated.
        const MIN_TIME_DIFF: Duration = Duration::from_millis(100);

        let mut inner = self.inner.lock().unwrap();
        let mut new_sum = Duration::default();
        let mut new_count = 0;
        // Now, we simplify the problem by merging samples from all levels. If we want
        // more accurate answer in the future, calculate for each level separately.
        for hist in &inner.time_slice_hist {
            // Call `metric` to get a consistent snapshot of sum and count.
            let metric_proto = hist.metric();
            let hist_proto = metric_proto.get_histogram();
            new_sum += Duration::from_secs_f64(hist_proto.get_sample_sum());
            new_count += hist_proto.get_sample_count();
        }
        let time_diff = new_sum.saturating_sub(inner.last_sum);
        let count_diff = new_count.saturating_sub(inner.last_count);
        if time_diff < MIN_TIME_DIFF || count_diff == 0 {
            return;
        }
        let new_val = time_diff / ((new_count - inner.last_count) as u32);
        let new_ewma = new_val.mul_f64(WEIGHT) + inner.ewma.mul_f64(1.0 - WEIGHT);
        inner.ewma = new_ewma;
        inner.last_sum = new_sum;
        inner.last_count = new_count;

        self.atomic_ewma_nanos
            .store(new_ewma.as_nanos() as u64, Ordering::Release);
    }

    pub fn get_ewma_time_slice(&self) -> Duration {
        Duration::from_nanos(self.atomic_ewma_nanos.load(Ordering::Acquire))
    }
}

#[derive(Clone)]
pub struct ReporterTicker<R: FlowStatsReporter> {
    reporter: R,
}

impl<R: FlowStatsReporter> PoolTicker for ReporterTicker<R> {
    fn on_tick(&mut self) {
        self.flush_metrics_on_tick();
    }
}

impl<R: FlowStatsReporter> ReporterTicker<R> {
    fn flush_metrics_on_tick(&mut self) {
        crate::storage::metrics::tls_flush(&self.reporter);
        crate::coprocessor::metrics::tls_flush(&self.reporter);
    }
}

#[cfg(test)]
fn get_unified_read_pool_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "unified-read-pool-test-{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(not(test))]
fn get_unified_read_pool_name() -> String {
    "unified-read-pool".to_string()
}

#[inline]
pub fn build_yatp_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &UnifiedReadPoolConfig,
    reporter: R,
    engine: E,
    resource_ctl: Option<Arc<ResourceController>>,
    cleanup_method: CleanupMethod,
    metric_idx_from_task_meta_fn: Option<Arc<dyn Fn(&[u8]) -> usize + Send + Sync + 'static>>,
) -> ReadPool {
    let unified_read_pool_name = get_unified_read_pool_name();
    build_yatp_read_pool_with_name(
        config,
        reporter,
        engine,
        resource_ctl,
        cleanup_method,
        unified_read_pool_name,
        metric_idx_from_task_meta_fn,
    )
}

pub fn build_yatp_read_pool_with_name<E: Engine, R: FlowStatsReporter>(
    config: &UnifiedReadPoolConfig,
    reporter: R,
    engine: E,
    resource_ctl: Option<Arc<ResourceController>>,
    cleanup_method: CleanupMethod,
    unified_read_pool_name: String,
    metric_idx_from_task_meta_fn: Option<Arc<dyn Fn(&[u8]) -> usize + Send + Sync + 'static>>,
) -> ReadPool {
    let raftkv = Arc::new(Mutex::new(engine));
    let mut builder = YatpPoolBuilder::new(ReporterTicker { reporter })
        .name_prefix(&unified_read_pool_name)
        .cleanup_method(cleanup_method)
        .stack_size(config.stack_size.0 as usize)
        .thread_count(
            config.min_thread_count,
            config.max_thread_count,
            std::cmp::max(
                std::cmp::max(
                    UNIFIED_READPOOL_MIN_CONCURRENCY,
                    SysQuota::cpu_cores_quota() as usize,
                ),
                config.max_thread_count,
            ),
        )
        .after_start(move || {
            let engine = raftkv.lock().unwrap().clone();
            set_tls_engine(engine);
            set_io_type(IoType::ForegroundRead);
        })
        .before_stop(|| unsafe {
            destroy_tls_engine::<E>();
        });
    if let Some(metric_idx_from_task_meta_fn) = metric_idx_from_task_meta_fn {
        builder = builder
            .enable_task_wait_metrics()
            .metric_idx_from_task_meta(metric_idx_from_task_meta_fn);
    }

    let pool = if let Some(ref r) = resource_ctl {
        builder.build_priority_pool(r.clone())
    } else {
        builder.build_multi_level_pool()
    };
    let time_slice_inspector = Arc::new(TimeSliceInspector::new(&unified_read_pool_name));
    ReadPool::Yatp {
        pool,
        running_tasks: UNIFIED_READ_POOL_RUNNING_TASKS
            .with_label_values(&[&unified_read_pool_name]),
        running_threads: UNIFIED_READ_POOL_RUNNING_THREADS
            .with_label_values(&[&unified_read_pool_name]),
        max_tasks: config
            .max_tasks_per_worker
            .saturating_mul(config.max_thread_count),
        pool_size: config.max_thread_count,
        resource_ctl,
        time_slice_inspector,
    }
}

impl From<Vec<FuturePool>> for ReadPool {
    fn from(mut v: Vec<FuturePool>) -> ReadPool {
        assert_eq!(v.len(), 3);
        let read_pool_high = v.remove(2);
        let read_pool_normal = v.remove(1);
        let read_pool_low = v.remove(0);
        ReadPool::FuturePools {
            read_pool_high,
            read_pool_normal,
            read_pool_low,
        }
    }
}

struct ReadPoolCpuTimeTracker {
    yatp_total_time_elapsed: IntCounter,
    // the total time duration of each thread busy with handling tasks. This time also includes
    // the time when the threads are off-cpu, so it might be much higher than the actual cpu time.
    prev_total_task_handling_time_us: u64,
    prev_check_time: Instant,
    prev_cpu_per_sec: f64,
}

impl ReadPoolCpuTimeTracker {
    fn new(pool_name: &str) -> Self {
        let prev_check_time = Instant::now_coarse();
        let yatp_total_time_elapsed = MULTILEVEL_LEVEL_ELAPSED
            .get_metric_with_label_values(&[pool_name, "total"])
            .unwrap();
        let prev_total_task_handling_time_us = yatp_total_time_elapsed.get();
        Self {
            yatp_total_time_elapsed,
            prev_total_task_handling_time_us,
            prev_check_time,
            prev_cpu_per_sec: 0.0,
        }
    }

    fn prev_avg_cpu_used(&mut self) -> f64 {
        let check_time = Instant::now_coarse();
        let duration = check_time.saturating_duration_since(self.prev_check_time);
        // if the check duration is too small, just return the latest cached value.
        if duration < Duration::from_millis(100) {
            return self.prev_cpu_per_sec;
        }
        let total_cpu_time = self.yatp_total_time_elapsed.get();
        let total_cpu_per_sec = (total_cpu_time - self.prev_total_task_handling_time_us) as f64
            / duration.as_micros() as f64;
        self.prev_total_task_handling_time_us = total_cpu_time;
        self.prev_check_time = check_time;
        self.prev_cpu_per_sec = total_cpu_per_sec;
        total_cpu_per_sec
    }
}
struct ReadPoolConfigRunner {
    interval: Duration,
    sender: SyncSender<usize>,
    handle: ReadPoolHandle,
    cpu_time_tracker: ReadPoolCpuTimeTracker,
    process_stats: ProcessStat,
    // configed thread pool size, it's the min thread count to be scale
    core_thread_count: usize,
    // the max thread count can be scaled
    max_thread_count: usize,
    // the current active thread count
    cur_thread_count: usize,
    auto_adjust: bool,
}

impl Runnable for ReadPoolConfigRunner {
    type Task = Task;
    fn run(&mut self, task: Self::Task) {
        match task {
            Task::PoolSize(s) => {
                if s != self.core_thread_count {
                    self.handle.scale_pool_size(s);
                    self.core_thread_count = s;
                    self.cur_thread_count = s;
                    self.notify_pool_size_change(s);
                }
            }
            Task::AutoAdjust(s) => {
                self.auto_adjust = s;
                // when auto adjust is disabled, reset to the config pool size.
                if !s && self.cur_thread_count != self.core_thread_count {
                    self.handle.scale_pool_size(self.core_thread_count);
                    self.cur_thread_count = self.core_thread_count;
                }
            }
        }
    }
}

impl RunnableWithTimer for ReadPoolConfigRunner {
    fn get_interval(&self) -> Duration {
        self.interval
    }

    fn on_timeout(&mut self) {
        self.adjust_pool_size();
    }
}

impl ReadPoolConfigRunner {
    fn running_tasks(&self) -> i64 {
        match &self.handle {
            ReadPoolHandle::Yatp { running_tasks, .. } => running_tasks.get(),
            _ => unreachable!(),
        }
    }

    fn adjust_pool_size(&mut self) {
        if !self.auto_adjust
            || (self.cur_thread_count == self.max_thread_count
                && self.core_thread_count == self.max_thread_count)
        {
            return;
        }

        let read_pool_cpu = self.cpu_time_tracker.prev_avg_cpu_used();
        let running_tasks = self.running_tasks();
        let process_cpu = match self.process_stats.cpu_usage() {
            Ok(p) => p,
            Err(e) => {
                warn!("fetch process cpu usage failed"; "err" => ?e);
                return;
            }
        };
        let cpu_quota = SysQuota::cpu_cores_quota();

        // scale out the thread pool size by 1 iff:
        // - current thread count is small than the maximum thread count
        // - process cpu is not overloaded after scaling out one more thread
        // - all read pool threads are busy handling tasks(thread busy time >= 80%)
        // - there are enough tasks waiting in the scheduling queue.
        // scale in the thread pool size by 1 iff:
        // - current thread count is bigger than the configed thread count
        // - the average thread usage percent is under the low water mark(70%)
        // - the running tasks in the scheduling queue is under the threshold
        let new_thread_count = if self.cur_thread_count < self.max_thread_count
            && process_cpu * (self.cur_thread_count as f64 + 1.0) / (self.cur_thread_count as f64)
                < cpu_quota
            && read_pool_cpu > self.cur_thread_count as f64 * READ_POOL_THREAD_HIGH_THRESHOLD
            && running_tasks > self.cur_thread_count as i64 * RUNNING_TASKS_PER_THREAD_THRESHOLD
        {
            self.cur_thread_count + 1
        } else if self.cur_thread_count > self.core_thread_count
            && read_pool_cpu < (self.cur_thread_count - 1) as f64 * READ_POOL_THREAD_LOW_THRESHOLD
            && running_tasks < self.cur_thread_count as i64 * RUNNING_TASKS_PER_THREAD_THRESHOLD
        {
            self.cur_thread_count - 1
        } else {
            self.cur_thread_count
        };

        if new_thread_count != self.cur_thread_count {
            self.handle.scale_pool_size(new_thread_count);
            self.notify_pool_size_change(new_thread_count);
            self.cur_thread_count = new_thread_count;
        }
    }

    fn notify_pool_size_change(&self, new_thread_count: usize) {
        // it's unlikely to send failed.
        if let Err(e) = self.sender.try_send(new_thread_count) {
            warn!("notify read pool thread count change failed"; "err" => ?e);
        }
    }
}

enum Task {
    PoolSize(usize),
    AutoAdjust(bool),
}

impl std::fmt::Display for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Task::PoolSize(s) => write!(f, "PoolSize({})", *s),
            Task::AutoAdjust(s) => write!(f, "AutoAdjust({})", *s),
        }
    }
}

pub struct ReadPoolConfigManager {
    scheduler: Scheduler<Task>,
}

impl ReadPoolConfigManager {
    pub fn new(
        handle: ReadPoolHandle,
        sender: SyncSender<usize>,
        worker: &Worker,
        thread_count: usize,
        auto_adjust: bool,
    ) -> Self {
        let max_thread_count = std::cmp::max(
            UNIFIED_READPOOL_MIN_CONCURRENCY,
            SysQuota::cpu_cores_quota().round() as usize,
        );
        let runner = ReadPoolConfigRunner {
            interval: READ_POOL_THREAD_CHECK_DURATION,
            sender,
            handle,
            cpu_time_tracker: ReadPoolCpuTimeTracker::new(&get_unified_read_pool_name()),
            process_stats: ProcessStat::cur_proc_stat().unwrap(),
            core_thread_count: thread_count,
            cur_thread_count: thread_count,
            max_thread_count,
            auto_adjust,
        };
        let scheduler = worker.start_with_timer("read-pool-config-worker", runner);

        Self { scheduler }
    }
}

impl Drop for ReadPoolConfigManager {
    fn drop(&mut self) {
        self.scheduler.stop();
    }
}

impl ConfigManager for ReadPoolConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> CfgResult<()> {
        if let Some(ConfigValue::Module(unified)) = change.get("unified") {
            if let Some(ConfigValue::Usize(max_thread_count)) = unified.get("max_thread_count") {
                self.scheduler.schedule(Task::PoolSize(*max_thread_count))?;
            }
            if let Some(ConfigValue::Bool(b)) = unified.get("auto_adjust_pool_size") {
                self.scheduler.schedule(Task::AutoAdjust(*b))?;
            }
        }
        info!(
            "readpool config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ReadPoolError {
    #[error("{0}")]
    FuturePoolFull(#[from] yatp_pool::Full),

    #[error("Unified read pool is full")]
    UnifiedReadPoolFull,

    #[error("{0}")]
    Canceled(#[from] oneshot::Canceled),
}

mod metrics {
    use prometheus::*;

    lazy_static! {
        pub static ref UNIFIED_READ_POOL_RUNNING_TASKS: IntGaugeVec = register_int_gauge_vec!(
            "tikv_unified_read_pool_running_tasks",
            "The number of running tasks in the unified read pool",
            &["name"]
        )
        .unwrap();
        pub static ref UNIFIED_READ_POOL_RUNNING_THREADS: IntGaugeVec = register_int_gauge_vec!(
            "tikv_unified_read_pool_thread_count",
            "The number of running threads in the unified read pool",
            &["name"]
        )
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use futures::channel::oneshot;
    use raftstore::store::{ReadStats, WriteStats};
    use resource_control::ResourceGroupManager;

    use super::*;
    use crate::storage::TestEngineBuilder;

    #[derive(Clone)]
    struct DummyReporter;

    impl FlowStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}
        fn report_write_stats(&self, _write_stats: WriteStats) {}
    }

    #[test]
    fn test_yatp_full() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 1,
            ..Default::default()
        };
        // max running tasks number should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            None,
            CleanupMethod::InPlace,
            None,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let handle = pool.handle();
        let (task1, tx1) = gen_task();
        let (task2, _tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();

        handle
            .spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None)
            .unwrap();
        handle
            .spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None)
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task3, CommandPri::Normal, 3, TaskMetadata::default(), None) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
        tx1.send(()).unwrap();

        thread::sleep(Duration::from_millis(300));
        handle
            .spawn(task4, CommandPri::Normal, 4, TaskMetadata::default(), None)
            .unwrap();
    }

    #[test]
    fn test_yatp_scale_up() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 1,
            ..Default::default()
        };
        // max running tasks number should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            None,
            CleanupMethod::InPlace,
            None,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let mut handle = pool.handle();
        let (task1, _tx1) = gen_task();
        let (task2, _tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();
        let (task5, _tx5) = gen_task();

        handle
            .spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None)
            .unwrap();
        handle
            .spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None)
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task3, CommandPri::Normal, 3, TaskMetadata::default(), None) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }

        handle.scale_pool_size(3);
        assert_eq!(handle.get_normal_pool_size(), 3);

        handle
            .spawn(task4, CommandPri::Normal, 4, TaskMetadata::default(), None)
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task5, CommandPri::Normal, 5, TaskMetadata::default(), None) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
    }

    #[test]
    fn test_yatp_scale_down() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 1,
            ..Default::default()
        };
        // max running tasks number should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            None,
            CleanupMethod::InPlace,
            None,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let mut handle = pool.handle();
        let (task1, tx1) = gen_task();
        let (task2, tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();
        let (task5, _tx5) = gen_task();

        handle
            .spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None)
            .unwrap();
        handle
            .spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None)
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task3, CommandPri::Normal, 3, TaskMetadata::default(), None) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }

        tx1.send(()).unwrap();
        tx2.send(()).unwrap();
        thread::sleep(Duration::from_millis(300));

        handle.scale_pool_size(1);
        assert_eq!(handle.get_normal_pool_size(), 1);

        handle
            .spawn(task4, CommandPri::Normal, 4, TaskMetadata::default(), None)
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task5, CommandPri::Normal, 5, TaskMetadata::default(), None) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
    }

    #[test]
    fn test_time_slice_inspector_ewma() {
        const MARGIN: f64 = 1e-5; // 10us

        let name = "test_time_slice_inspector_ewma";
        let inspector = TimeSliceInspector::new(name);
        let hist = yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "0"]);

        // avg: 0.055, prev_ewma: 0 => new_ewma = 0.0165
        for i in 1..=10 {
            hist.observe(i as f64 * 0.01);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.0165).abs() < MARGIN);

        // avg: 0.0125, prev_ewma: 0.0165 => new_ewma = 0.0153
        for i in 5..=20 {
            hist.observe(i as f64 * 0.001);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.0153).abs() < MARGIN);

        // sum: 55ms, don't update ewma
        for i in 1..=10 {
            hist.observe(i as f64 * 0.001);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.0153).abs() < MARGIN);

        // avg: 0.00786, prev_ewma: 0.0153 => new_ewma = 0.01307
        for i in 5..=15 {
            hist.observe(i as f64 * 0.001);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.01307).abs() < MARGIN);
    }

    #[test]
    fn test_yatp_task_poll_duration_metric() {
        let count_metric = |name: &str| -> u64 {
            let mut sum = 0;
            for i in 0..=2 {
                let hist =
                    yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, &format!("{}", i)]);
                sum += hist.get_sample_count();
            }
            sum
        };

        for control in [false, true] {
            let name = format!("test_yatp_task_poll_duration_metric_{}", control);
            let resource_manager = if control {
                let resource_manager = ResourceGroupManager::default();
                let resource_ctl = resource_manager.derive_controller(name.clone(), true);
                Some(resource_ctl)
            } else {
                None
            };
            let config = UnifiedReadPoolConfig {
                min_thread_count: 1,
                max_thread_count: 2,
                max_tasks_per_worker: 1,
                ..Default::default()
            };

            let engine = TestEngineBuilder::new().build().unwrap();

            let pool = build_yatp_read_pool_with_name(
                &config,
                DummyReporter,
                engine,
                resource_manager,
                CleanupMethod::InPlace,
                name.clone(),
                None,
            );

            let gen_task = || {
                let (tx, rx) = oneshot::channel::<()>();
                let task = async move {
                    // sleep the thread 100ms to trigger flushing the metrics.
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    let _ = rx.await;
                };
                (task, tx)
            };

            let handle = pool.handle();
            let (task1, tx1) = gen_task();
            let (task2, tx2) = gen_task();

            handle
                .spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None)
                .unwrap();
            handle
                .spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None)
                .unwrap();

            tx1.send(()).unwrap();
            tx2.send(()).unwrap();

            thread::sleep(Duration::from_millis(300));
            assert_eq!(count_metric(&name), 2);
            drop(pool);
        }
    }
}
