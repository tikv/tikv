use futures::sync::oneshot;
use futures::{future, Future};
use futures03::prelude::*;
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

use crate::config::UnifiedReadPoolConfig;
use crate::storage::kv::{destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter};

#[derive(Clone)]
pub enum ReadPool {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp(Remote<TaskCell>),
}

impl ReadPool {
    pub fn spawn<F>(&self, f: F, priority: CommandPri, task_id: u64) -> Result<(), ReadPoolError>
    where
        F: StdFuture<Output = ()> + Send + 'static,
    {
        match self {
            ReadPool::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => {
                let pool = match priority {
                    CommandPri::High => read_pool_high,
                    CommandPri::Normal => read_pool_normal,
                    CommandPri::Low => read_pool_low,
                };

                pool.spawn(move || Box::pin(f.never_error()).compat())?;
            }
            ReadPool::Yatp(remote) => {
                let fixed_level = match priority {
                    CommandPri::High => Some(0),
                    CommandPri::Normal => None,
                    CommandPri::Low => Some(2),
                };
                let extras = Extras::new_multilevel(task_id, fixed_level);
                let task_cell = TaskCell::new(f, extras);
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
            self.maybe_flush_metrics();
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

    // Only flush metrics by tick
    fn maybe_flush_metrics(&self) {
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

pub fn build_yatp_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &UnifiedReadPoolConfig,
    reporter: R,
    engine: E,
) -> yatp::ThreadPool<TaskCell> {
    let pool_name = "unified-read-pool";
    let mut builder = yatp::Builder::new(pool_name);
    builder
        .min_thread_count(config.min_thread_count)
        .max_thread_count(config.max_thread_count);
    let multilevel_builder =
        multilevel::Builder::new(multilevel::Config::default().name(Some(pool_name)));
    let read_pool_runner = ReadPoolRunner::new(engine, Default::default(), reporter);
    let runner_builder = multilevel_builder.runner_builder(CloneRunnerBuilder(read_pool_runner));
    builder.build_with_queue_and_runner(QueueType::Multilevel(multilevel_builder), runner_builder)
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

impl From<Remote<TaskCell>> for ReadPool {
    fn from(yatp_remote: Remote<TaskCell>) -> Self {
        ReadPool::Yatp(yatp_remote)
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
        Canceled(err: oneshot::Canceled) {
            from()
            cause(err)
            description(err.description())
        }
    }
}
