// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    sync::{mpsc::SyncSender, Arc, Mutex},
    time::Duration,
};

use file_system::{set_io_type, IoType};
use futures::{channel::oneshot, future::TryFutureExt};
use kvproto::kvrpcpb::CommandPri;
use online_config::{ConfigChange, ConfigManager, ConfigValue, Result as CfgResult};
use prometheus::IntGauge;
use thiserror::Error;
use tikv_util::{
    metrics::ThreadInfoStatistics,
    sys::SysQuota,
    worker::{Runnable, RunnableWithTimer, Scheduler, Worker},
    yatp_pool::{self, FuturePool, PoolTicker, YatpPoolBuilder},
};
use tracker::TrackedFuture;
use yatp::{pool::Remote, queue::Extras, task::future::TaskCell};

use self::metrics::*;
use crate::{
    config::{UnifiedReadPoolConfig, UNIFIED_READPOOL_MIN_CONCURRENCY},
    storage::kv::{destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter},
};

// the duration to check auto-scale unified-thread-pool's thread
const READ_POOL_THREAD_CHECK_DURATION: Duration = Duration::from_secs(1);
// total cpu usage percentage that indicate process is overloaded, do not scale
// up thread pool size.
const CPU_BUSY_THRESHOLD: f64 = 0.9;
// total cpu usage percentage that indicate process is high loaded.
const CPU_HIGH_THRESHOLD: f64 = 0.7;
// consider scale up read pool size if the average thread cpu usage is higher
// than this threahold.
const READ_POOL_THREAD_HIGH_THREAHOLD: f64 = 0.7;
// consider scale down read pool size if the average thread cpu usage is lower
// than this threahold.
const READ_POOL_THREAD_LOW_THREAHOLD: f64 = 0.5;
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
        max_tasks: usize,
        pool_size: usize,
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
                max_tasks,
                pool_size,
            } => ReadPoolHandle::Yatp {
                remote: pool.remote().clone(),
                running_tasks: running_tasks.clone(),
                max_tasks: *max_tasks,
                pool_size: *pool_size,
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
        max_tasks: usize,
        pool_size: usize,
    },
}

impl ReadPoolHandle {
    pub fn spawn<F>(&self, f: F, priority: CommandPri, task_id: u64) -> Result<(), ReadPoolError>
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
                let extras = Extras::new_multilevel(task_id, fixed_level);
                let task_cell = TaskCell::new(
                    TrackedFuture::new(async move {
                        f.await;
                        running_tasks.dec();
                    }),
                    extras,
                );
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
    ) -> impl Future<Output = Result<T, ReadPoolError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<T>();
        let res = self.spawn(
            async move {
                let res = f.await;
                let _ = tx.send(res);
            },
            priority,
            task_id,
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
            } => {
                read_pool_normal.get_running_task_count() as usize
                    / read_pool_normal.get_pool_size()
            }
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
                running_tasks: _,
                max_tasks,
                pool_size,
            } => {
                remote.scale_workers(max_thread_count);
                *max_tasks = max_tasks
                    .saturating_div(*pool_size)
                    .saturating_mul(max_thread_count);
                *pool_size = max_thread_count;
            }
        }
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
    use std::sync::atomic::{AtomicU64, Ordering};

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

pub fn build_yatp_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &UnifiedReadPoolConfig,
    reporter: R,
    engine: E,
) -> ReadPool {
    let unified_read_pool_name = get_unified_read_pool_name();
    let mut builder = YatpPoolBuilder::new(ReporterTicker { reporter });
    let raftkv = Arc::new(Mutex::new(engine));
    let pool = builder
        .name_prefix(&unified_read_pool_name)
        .stack_size(config.stack_size.0 as usize)
        .thread_count(
            config.min_thread_count,
            config.max_thread_count,
            std::cmp::max(
                UNIFIED_READPOOL_MIN_CONCURRENCY,
                SysQuota::cpu_cores_quota() as usize,
            ),
        )
        .after_start(move || {
            let engine = raftkv.lock().unwrap().clone();
            set_tls_engine(engine);
            set_io_type(IoType::ForegroundRead);
        })
        .before_stop(|| unsafe {
            destroy_tls_engine::<E>();
        })
        .build_multi_level_pool();
    ReadPool::Yatp {
        pool,
        running_tasks: UNIFIED_READ_POOL_RUNNING_TASKS
            .with_label_values(&[&unified_read_pool_name]),
        max_tasks: config
            .max_tasks_per_worker
            .saturating_mul(config.max_thread_count),
        pool_size: config.max_thread_count,
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
struct ReadPoolConfigRunner {
    interval: Duration,
    sender: SyncSender<usize>,
    handle: ReadPoolHandle,
    thread_stats: ThreadInfoStatistics,
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
                    self.notify_pool_size_change(new_thread_count);
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
        let cpu_quota = SysQuota::cpu_cores_quota();
        self.thread_stats.record();
        let thread_stats = self.thread_stats.get_cpu_usages();
        let total_cpu = thread_stats.values().sum::<u64>() as f64 / 100.0;
        let read_pool_cpu = thread_stats
            .iter()
            .filter_map(|(t, u)| {
                if t.contains("unified-read-po") {
                    Some(*u)
                } else {
                    None
                }
            })
            .sum::<u64>() as f64
            / 100.0;

        let running_tasks = self.running_tasks();

        let new_thread_count = if self.cur_thread_count < self.max_thread_count
            && total_cpu / cpu_quota < CPU_BUSY_THRESHOLD
            && read_pool_cpu
                >= (total_cpu * CPU_HIGH_THRESHOLD)
                    .max(self.cur_thread_count as f64 * READ_POOL_THREAD_HIGH_THREAHOLD)
            && running_tasks > self.cur_thread_count as i64 * RUNNING_TASKS_PER_THREAD_THRESHOLD
        {
            self.cur_thread_count + 1
        } else if self.cur_thread_count > self.core_thread_count
            && read_pool_cpu <= self.cur_thread_count as f64 * READ_POOL_THREAD_LOW_THREAHOLD
            && running_tasks < self.cur_thread_count as i64 * RUNNING_TASKS_PER_THREAD_THRESHOLD
        {
            self.cur_thread_count - 1
        } else {
            self.cur_thread_count
        };

        if new_thread_count != self.cur_thread_count {
            self.handle.scale_pool_size(new_thread_count);
            info!("auto adjust read pool thread"; "cpu_quota" => cpu_quota, 
                "total_cpu" => total_cpu, "read_pool_cpu" => read_pool_cpu, 
                "running_tasks" => running_tasks, "current" => self.cur_thread_count, 
                "new" => new_thread_count);
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
            thread_stats: ThreadInfoStatistics::new(),
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
                self.scheduler.schedule(Task::AutoAdjust(*b));
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
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use futures::channel::oneshot;
    use raftstore::store::{ReadStats, WriteStats};

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
        let pool = build_yatp_read_pool(&config, DummyReporter, engine);

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

        handle.spawn(task1, CommandPri::Normal, 1).unwrap();
        handle.spawn(task2, CommandPri::Normal, 2).unwrap();

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task3, CommandPri::Normal, 3) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
        tx1.send(()).unwrap();

        thread::sleep(Duration::from_millis(300));
        handle.spawn(task4, CommandPri::Normal, 4).unwrap();
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
        let pool = build_yatp_read_pool(&config, DummyReporter, engine);

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

        handle.spawn(task1, CommandPri::Normal, 1).unwrap();
        handle.spawn(task2, CommandPri::Normal, 2).unwrap();

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task3, CommandPri::Normal, 3) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }

        handle.scale_pool_size(3);
        assert_eq!(handle.get_normal_pool_size(), 3);

        handle.spawn(task4, CommandPri::Normal, 4).unwrap();

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task5, CommandPri::Normal, 5) {
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
        let pool = build_yatp_read_pool(&config, DummyReporter, engine);

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

        handle.spawn(task1, CommandPri::Normal, 1).unwrap();
        handle.spawn(task2, CommandPri::Normal, 2).unwrap();

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task3, CommandPri::Normal, 3) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }

        tx1.send(()).unwrap();
        tx2.send(()).unwrap();
        thread::sleep(Duration::from_millis(300));

        handle.scale_pool_size(1);
        assert_eq!(handle.get_normal_pool_size(), 1);

        handle.spawn(task4, CommandPri::Normal, 4).unwrap();

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task5, CommandPri::Normal, 5) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
    }
}
