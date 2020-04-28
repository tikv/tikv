// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use futures::sync::oneshot;
use futures::{future, Future};
use kvproto::kvrpcpb::CommandPri;
use std::cell::Cell;
use std::future::Future as StdFuture;
use std::time::Duration;
use tikv_util::future_pool::{self, FuturePool};
use tikv_util::time::Instant;
use yatp::pool::{CloneRunnerBuilder, Local, Runner};
use yatp::queue::{multilevel, Extras, QueueType};
use yatp::task::future::{Runner as FutureRunner, TaskCell};
use yatp::Remote;

use self::metrics::*;
use crate::config::UnifiedReadPoolConfig;
use crate::storage::kv::{destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter};
use prometheus::IntGauge;

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
            } => ReadPoolHandle::Yatp {
                remote: pool.remote().clone(),
                running_tasks: running_tasks.clone(),
                max_tasks: *max_tasks,
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
    },
}

impl ReadPoolHandle {
    pub fn spawn<F>(&self, f: F, priority: CommandPri, task_id: u64) -> Result<(), ReadPoolError>
    where
        F: StdFuture<Output = ()> + Send + 'static,
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
            } => {
                let running_tasks = running_tasks.clone();
                // Note that the running task number limit is not strict.
                // If several tasks are spawned at the same time while the running task number
                // is close to the limit, they may all pass this check and the number of running
                // tasks may exceed the limit.
                if running_tasks.get() as usize >= *max_tasks {
                    return Err(ReadPoolError::UnifiedReadPoolFull);
                }

                let fixed_level = match priority {
                    CommandPri::High => Some(0),
                    CommandPri::Normal => None,
                    CommandPri::Low => Some(2),
                };
                let extras = Extras::new_multilevel(task_id, fixed_level);
                let task_cell = TaskCell::new(
                    async move {
                        running_tasks.inc();
                        f.await;
                        running_tasks.dec();
                    },
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
    ) -> impl Future<Item = T, Error = ReadPoolError>
    where
        F: StdFuture<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<T>();
        let spawn_res = self.spawn(
            async move {
                let _ = tx.send(f.await);
            },
            priority,
            task_id,
        );
        if let Err(e) = spawn_res {
            future::Either::A(future::err(e))
        } else {
            future::Either::B(rx.map_err(ReadPoolError::from))
        }
    }
}

#[derive(Clone)]
pub struct ReadPoolRunner<E: Engine, R: FlowStatsReporter> {
    engine: Option<E>,
    reporter: R,
    inner: FutureRunner,
}

impl<E: Engine, R: FlowStatsReporter> Runner for ReadPoolRunner<E, R> {
    type TaskCell = TaskCell;

    fn start(&mut self, local: &mut Local<Self::TaskCell>) {
        set_tls_engine(self.engine.take().unwrap());
        self.inner.start(local)
    }

    fn handle(&mut self, local: &mut Local<Self::TaskCell>, task_cell: Self::TaskCell) -> bool {
        let finished = self.inner.handle(local, task_cell);
        if finished {
            self.flush_metrics_on_tick();
        }
        finished
    }

    fn pause(&mut self, local: &mut Local<Self::TaskCell>) -> bool {
        self.inner.pause(local)
    }

    fn resume(&mut self, local: &mut Local<Self::TaskCell>) {
        self.inner.resume(local)
    }

    fn end(&mut self, local: &mut Local<Self::TaskCell>) {
        self.inner.end(local);
        self.flush_metrics();
        unsafe { destroy_tls_engine::<E>() }
    }
}

impl<E: Engine, R: FlowStatsReporter> ReadPoolRunner<E, R> {
    pub fn new(engine: E, inner: FutureRunner, reporter: R) -> Self {
        ReadPoolRunner {
            engine: Some(engine),
            reporter,
            inner,
        }
    }

    // Do nothing if no tick passed
    fn flush_metrics_on_tick(&self) {
        const TICK_INTERVAL: Duration = Duration::from_secs(1);

        thread_local! {
            static THREAD_LAST_TICK_TIME: Cell<Instant> = Cell::new(Instant::now_coarse());
        }

        THREAD_LAST_TICK_TIME.with(|tls_last_tick| {
            let now = Instant::now_coarse();
            let last_tick = tls_last_tick.get();
            if now.duration_since(last_tick) < TICK_INTERVAL {
                return;
            }
            tls_last_tick.set(now);
            self.flush_metrics();
        })
    }

    fn flush_metrics(&self) {
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

    let mut builder = yatp::Builder::new(&unified_read_pool_name);
    builder
        .stack_size(config.stack_size.0 as usize)
        .min_thread_count(config.min_thread_count)
        .max_thread_count(config.max_thread_count);
    let multilevel_builder =
        multilevel::Builder::new(multilevel::Config::default().name(Some(&unified_read_pool_name)));
    let read_pool_runner = ReadPoolRunner::new(engine, Default::default(), reporter);
    let runner_builder = multilevel_builder.runner_builder(CloneRunnerBuilder(read_pool_runner));
    let pool = builder
        .build_with_queue_and_runner(QueueType::Multilevel(multilevel_builder), runner_builder);
    ReadPool::Yatp {
        pool,
        running_tasks: UNIFIED_READ_POOL_RUNNING_TASKS
            .with_label_values(&[&unified_read_pool_name]),
        max_tasks: config
            .max_tasks_per_worker
            .saturating_mul(config.max_thread_count),
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

quick_error! {
    #[derive(Debug)]
    pub enum ReadPoolError {
        FuturePoolFull(err: future_pool::Full) {
            from()
            cause(err)
            description(err.description())
        }
        UnifiedReadPoolFull {
            description("Unified read pool is full")
        }
        Canceled(err: oneshot::Canceled) {
            from()
            cause(err)
            description(err.description())
        }
    }
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
    use super::*;
    use crate::storage::TestEngineBuilder;
    use futures03::channel::oneshot;
    use raftstore::store::ReadStats;
    use std::thread;

    #[derive(Clone)]
    struct DummyReporter;

    impl FlowStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}
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

        assert!(handle.spawn(task1, CommandPri::Normal, 1).is_ok());
        assert!(handle.spawn(task2, CommandPri::Normal, 2).is_ok());

        thread::sleep(Duration::from_millis(300));
        match handle.spawn(task3, CommandPri::Normal, 3) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
        tx1.send(()).unwrap();

        thread::sleep(Duration::from_millis(300));
        assert!(handle.spawn(task4, CommandPri::Normal, 4).is_ok());
    }
}
