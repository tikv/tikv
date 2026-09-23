// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

mod future_pool;
pub mod metrics;

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use fail::fail_point;
pub use future_pool::{Full, FuturePool};
use futures::{StreamExt, compat::Stream01CompatExt};
use prometheus::local::LocalHistogram;
use yatp::{
    ThreadPool,
    pool::{CloneRunnerBuilder, Local, Remote, Runner},
    queue::{Extras, QueueType, TaskCell as _, multilevel, priority},
    task::future::{Runner as FutureRunner, TaskCell},
};

use crate::{
    resource_control::{TaskPriority, priority_from_task_meta},
    thread_group::GroupProperties,
    time::{Duration, Instant},
    timer::GLOBAL_TIMER_HANDLE,
};

const DEFAULT_CLEANUP_INTERVAL: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(10)
};

fn background_cleanup_task<F>(cleanup: F) -> TaskCell
where
    F: Fn() -> Option<std::time::Instant> + Send + 'static,
{
    let mut interval = GLOBAL_TIMER_HANDLE
        .interval(
            std::time::Instant::now() + DEFAULT_CLEANUP_INTERVAL,
            DEFAULT_CLEANUP_INTERVAL,
        )
        .compat();
    TaskCell::new(
        async move {
            while let Some(Ok(_)) = interval.next().await {
                cleanup();
            }
        },
        Extras::multilevel_default(),
    )
}

/// CleanupMethod describes how a pool cleanup its internal task-elapsed map. A
/// task-elapsed map is used for tracking how long each task has been running,
/// so that the pool can adjust the level of a task according to its running
/// time. To prevent a task-elapsed map from growing too large, the following
/// strategies are provided for cleaning up it periodically.
pub enum CleanupMethod {
    /// Cleanup in place on spawning.
    InPlace,
    /// Cleanup in this pool (the one to be built) locally.
    Local,
    /// Cleanup in the given remote pool.
    Remote(Remote<TaskCell>),
}

impl CleanupMethod {
    /// Returns the perferred cleanup interval used for creating a queue
    /// builder.
    fn preferred_interval(&self) -> Option<std::time::Duration> {
        match self {
            Self::InPlace => Some(DEFAULT_CLEANUP_INTERVAL),
            _ => None,
        }
    }

    /// Tries to create a task from the cleanup function and spawn it if
    /// possible, returns Some(task) if there is a task shall be spawned but
    /// hasn't been spawned (that is, need to be spawned locally later).
    fn try_spawn<F>(&self, cleanup: F) -> Option<TaskCell>
    where
        F: Fn() -> Option<std::time::Instant> + Send + 'static,
    {
        match self {
            Self::InPlace => None,
            Self::Local => Some(background_cleanup_task(cleanup)),
            Self::Remote(remote) => {
                remote.spawn(background_cleanup_task(cleanup));
                None
            }
        }
    }
}

pub(crate) const TICK_INTERVAL: Duration = Duration::from_secs(1);

fn tick_interval() -> Duration {
    fail_point!("mock_tick_interval", |_| { Duration::from_millis(1) });
    TICK_INTERVAL
}

pub trait PoolTicker: Send + Clone + 'static {
    fn on_tick(&mut self);
}

#[derive(Clone)]
pub struct TickerWrapper<T: PoolTicker> {
    ticker: T,
    last_tick_time: Instant,
}

impl<T: PoolTicker> TickerWrapper<T> {
    pub fn new(ticker: T) -> Self {
        Self {
            ticker,
            last_tick_time: Instant::now_coarse(),
        }
    }

    // Returns whether tick has been triggered.
    pub fn try_tick(&mut self) -> bool {
        let now = Instant::now_coarse();
        if now.saturating_duration_since(self.last_tick_time) < tick_interval() {
            return false;
        }
        self.last_tick_time = now;
        self.ticker.on_tick();
        true
    }

    pub fn on_tick(&mut self) {
        self.ticker.on_tick();
    }
}

#[derive(Clone, Default)]
pub struct DefaultTicker {}

impl PoolTicker for DefaultTicker {
    fn on_tick(&mut self) {}
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub workers: usize,
    pub max_tasks_per_worker: usize,
    pub stack_size: usize,
}

impl Config {
    pub fn default_for_test() -> Self {
        Self {
            workers: 2,
            max_tasks_per_worker: usize::MAX,
            stack_size: 2_000_000,
        }
    }
}

#[derive(Clone)]
struct TaskScheduleHistograms(Option<[LocalHistogram; TaskPriority::PRIORITY_COUNT]>);

impl TaskScheduleHistograms {
    fn new(enable: bool, name: &str, metric_vec: &prometheus::HistogramVec) -> Self {
        if enable {
            let histograms = TaskPriority::priorities()
                .map(|p| metric_vec.with_label_values(&[name, p.as_str()]).local());
            TaskScheduleHistograms(Some(histograms))
        } else {
            TaskScheduleHistograms(None)
        }
    }

    fn enabled(&self) -> bool {
        self.0.is_some()
    }

    fn observe(&mut self, priority: TaskPriority, duration: Duration) {
        if let Some(histograms) = &mut self.0 {
            let idx = priority as usize;
            histograms[idx].observe(duration.as_secs_f64());
        }
    }

    fn flush(&mut self) {
        if let Some(histograms) = &mut self.0 {
            for hist in histograms.iter_mut() {
                hist.flush();
            }
        }
    }
}

/// Owns the per-thread state a yatp worker thread registers in
/// [`YatpPoolRunner::start`] and releases it when dropped.
///
/// yatp runs `Runner::end` only when the worker's task loop exits by itself: a
/// panicking task unwinds out of yatp's `WorkerThread::run`, which does not
/// catch it, so `end` is skipped. yatp drops the runner while that unwind
/// leaves `run`, so the state is owned by a guard dropped with the runner
/// instead of being released inline in `end`: whichever way the worker exits,
/// the release happens on the worker thread itself — which it must, since
/// `remove_thread_memory_accessor` deregisters the calling thread.
///
/// `end`'s remaining steps (`ticker.on_tick()` and the inner runner's `end`)
/// are not part of this guard: they need the worker's `Local`, which a `Drop`
/// has no access to. On the unwind path they are skipped, as they always were
/// when `end` did not run at all; the inner runner's `end` is a no-op for the
/// future runner and the ticker only flushes per-thread metrics.
struct ThreadCleanup {
    /// The `before_stop` hook, taken when it runs so it runs at most once.
    before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl ThreadCleanup {
    /// Runs the `before_stop` hook. On the normal shutdown path `end` calls
    /// this while the thread's registered state is still alive, just like
    /// `drop` does when the worker unwinds.
    fn run_before_stop(&mut self) {
        if let Some(f) = self.before_stop.take() {
            f();
        }
    }
}

impl Drop for ThreadCleanup {
    fn drop(&mut self) {
        // `Drop` is also the path taken when a task panicked: the worker unwinds
        // out of `WorkerThread::run` without `end` ever running. `before_stop` is
        // arbitrary pool-owner code (thread-local engine teardown, metric
        // flushes), and a panic escaping a drop while the thread unwinds aborts
        // the whole process, so on that path a panicking hook is reported instead
        // of propagated. On the normal path `end` runs the hook itself, where a
        // panic still propagates as it always did.
        if std::thread::panicking() {
            if catch_unwind(AssertUnwindSafe(|| self.run_before_stop())).is_err() {
                error!("`before_stop` hook panicked while a yatp worker thread was unwinding");
            }
        } else {
            self.run_before_stop();
        }
        // `add_thread_memory_accessor` requires the deregistration to happen
        // before the thread exits; with a stale entry, the allocator registry
        // readers (`dump_stats` behind the debug `GetMetrics{all}` RPC, and
        // `iterate_thread_allocation_stats` behind the always-on allocator
        // metrics collector) read the freed jemalloc counters of the dead thread.
        // Both removals are no-ops for a thread that registered nothing, so they
        // hold wherever `start` panicked.
        tikv_alloc::remove_thread_memory_accessor();
        crate::sys::thread::remove_thread_name_from_map();
    }
}

pub struct YatpPoolRunner<T: PoolTicker> {
    inner: FutureRunner,
    ticker: TickerWrapper<T>,
    props: Option<GroupProperties>,
    after_start: Option<Arc<dyn Fn() + Send + Sync>>,
    before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    before_pause: Option<Arc<dyn Fn() + Send + Sync>>,

    // Statistics about the schedule wait/exec duration.
    // local histogram for high,medium,low priority tasks.
    schedule_wait_durations: TaskScheduleHistograms,
    schedule_exec_durations: TaskScheduleHistograms,

    // `Some` from `start` until the worker thread's state has been released; see
    // `ThreadCleanup`.
    thread_cleanup: Option<ThreadCleanup>,
}

/// A clone is a runner for a new worker thread, which registers its own state
/// in `start`: the per-thread state owned by `thread_cleanup` belongs to the
/// thread running the runner being cloned and must not be carried over.
impl<T: PoolTicker> Clone for YatpPoolRunner<T> {
    fn clone(&self) -> Self {
        YatpPoolRunner {
            inner: self.inner.clone(),
            ticker: self.ticker.clone(),
            props: self.props.clone(),
            after_start: self.after_start.clone(),
            before_stop: self.before_stop.clone(),
            before_pause: self.before_pause.clone(),
            schedule_wait_durations: self.schedule_wait_durations.clone(),
            schedule_exec_durations: self.schedule_exec_durations.clone(),
            thread_cleanup: None,
        }
    }
}

impl<T: PoolTicker> Runner for YatpPoolRunner<T> {
    type TaskCell = TaskCell;

    fn start(&mut self, local: &mut Local<Self::TaskCell>) {
        // From here on the worker thread owns per-thread state that has to be
        // released before the thread exits, whichever way it exits, so it is
        // handed to the guard right away: arming it first also covers a panic
        // thrown by anything registered below. A panic before `after_start` ran
        // therefore still calls `before_stop`; every hook in the tree (destroying
        // a thread-local engine, flushing thread-local metrics, deregistering)
        // tolerates state that was never set up.
        self.thread_cleanup = Some(ThreadCleanup {
            before_stop: self.before_stop.take(),
        });
        crate::sys::thread::call_thread_start_hooks();
        crate::sys::thread::add_thread_name_to_map();
        if let Some(props) = self.props.take() {
            crate::thread_group::set_properties(Some(props));
        }
        self.inner.start(local);
        if let Some(f) = self.after_start.take() {
            f();
        }
        // SAFETY: `self.thread_cleanup` releases the accessor: from `end` on a
        // clean shutdown, or from its `Drop` when a task panics and yatp unwinds
        // out of `WorkerThread::run` before `end`.
        unsafe {
            tikv_alloc::add_thread_memory_accessor();
            tikv_alloc::thread_allocate_exclusive_arena().unwrap();
        }
    }

    fn handle(&mut self, local: &mut Local<Self::TaskCell>, mut task_cell: Self::TaskCell) -> bool {
        let extras = task_cell.mut_extras();
        let priority = priority_from_task_meta(extras.metadata());
        let start_time =
            if self.schedule_wait_durations.enabled() || self.schedule_exec_durations.enabled() {
                Some(std::time::Instant::now())
            } else {
                None
            };
        if let Some(dur) = start_time
            .zip(extras.schedule_time())
            .map(|(t1, t2)| t1.saturating_duration_since(t2))
        {
            self.schedule_wait_durations.observe(priority, dur);
        }
        let finished = self.inner.handle(local, task_cell);
        let end_time = if self.schedule_exec_durations.enabled() {
            Some(std::time::Instant::now())
        } else {
            None
        };
        if let Some(dur) = end_time
            .zip(start_time)
            .map(|(t1, t2)| t1.saturating_duration_since(t2))
        {
            self.schedule_exec_durations.observe(priority, dur);
        }
        if self.ticker.try_tick() {
            self.schedule_wait_durations.flush();
            self.schedule_exec_durations.flush();
        }
        finished
    }

    fn pause(&mut self, local: &mut Local<Self::TaskCell>) -> bool {
        if let Some(f) = self.before_pause.as_ref() {
            f();
        }
        self.inner.pause(local)
    }

    fn resume(&mut self, local: &mut Local<Self::TaskCell>) {
        self.inner.resume(local)
    }

    fn end(&mut self, local: &mut Local<Self::TaskCell>) {
        // The hook runs with the thread's registered state still held, as it
        // always did.
        if let Some(cleanup) = self.thread_cleanup.as_mut() {
            cleanup.run_before_stop();
        }
        self.ticker.on_tick();
        self.inner.end(local);
        // Releases the thread's jemalloc accessor and its thread-name entry — the
        // same disposal the guard performs if the worker unwinds first. The hook
        // has been taken above, so it cannot run a second time.
        drop(self.thread_cleanup.take());
    }
}

impl<T: PoolTicker> YatpPoolRunner<T> {
    fn new(
        inner: FutureRunner,
        ticker: TickerWrapper<T>,
        after_start: Option<Arc<dyn Fn() + Send + Sync>>,
        before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
        before_pause: Option<Arc<dyn Fn() + Send + Sync>>,
        schedule_wait_durations: TaskScheduleHistograms,
        schedule_exec_durations: TaskScheduleHistograms,
    ) -> Self {
        YatpPoolRunner {
            inner,
            ticker,
            props: crate::thread_group::current_properties(),
            after_start,
            before_stop,
            before_pause,
            schedule_wait_durations,
            schedule_exec_durations,
            thread_cleanup: None,
        }
    }
}

pub struct YatpPoolBuilder<T: PoolTicker> {
    name_prefix: Option<String>,
    ticker: TickerWrapper<T>,
    after_start: Option<Arc<dyn Fn() + Send + Sync>>,
    before_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    before_pause: Option<Arc<dyn Fn() + Send + Sync>>,
    min_thread_count: usize,
    core_thread_count: usize,
    max_thread_count: usize,
    stack_size: usize,
    max_tasks: usize,
    cleanup_method: CleanupMethod,

    // whether to tracker task scheduling wait/exec duration
    enable_task_wait_metrics: bool,
    enable_task_exec_metrics: bool,
    metric_idx_from_task_meta: Option<Arc<dyn Fn(&[u8]) -> usize + Send + Sync>>,

    #[cfg(test)]
    background_cleanup_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl<T: PoolTicker> YatpPoolBuilder<T> {
    pub fn new(ticker: T) -> Self {
        Self {
            ticker: TickerWrapper::new(ticker),
            name_prefix: None,
            after_start: None,
            before_stop: None,
            before_pause: None,
            min_thread_count: 1,
            core_thread_count: 1,
            max_thread_count: 1,
            stack_size: 0,
            max_tasks: usize::MAX,
            cleanup_method: CleanupMethod::InPlace,

            enable_task_wait_metrics: false,
            enable_task_exec_metrics: false,
            metric_idx_from_task_meta: None,

            #[cfg(test)]
            background_cleanup_hook: None,
        }
    }

    pub fn config(self, config: Config) -> Self {
        // TODO: maybe we should use (1, num_cpu) for min and max thread count.
        self.thread_count(config.workers, config.workers, config.workers)
            .stack_size(config.stack_size)
            .max_tasks(config.workers.saturating_mul(config.max_tasks_per_worker))
    }

    pub fn stack_size(mut self, val: usize) -> Self {
        self.stack_size = val;
        self
    }

    pub fn name_prefix(mut self, val: impl Into<String>) -> Self {
        let name = val.into();
        self.name_prefix = Some(name);
        self
    }

    pub fn thread_count(
        mut self,
        min_thread_count: usize,
        core_thread_count: usize,
        max_thread_count: usize,
    ) -> Self {
        self.min_thread_count = min_thread_count;
        self.core_thread_count = core_thread_count;
        self.max_thread_count = max_thread_count;
        self
    }

    pub fn max_tasks(mut self, tasks: usize) -> Self {
        self.max_tasks = tasks;
        self
    }

    pub fn cleanup_method(mut self, method: CleanupMethod) -> Self {
        self.cleanup_method = method;
        self
    }

    pub fn before_stop<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.before_stop = Some(Arc::new(f));
        self
    }

    pub fn after_start<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.after_start = Some(Arc::new(f));
        self
    }

    pub fn before_pause<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.before_pause = Some(Arc::new(f));
        self
    }

    pub fn enable_task_wait_metrics(mut self, enable: bool) -> Self {
        self.enable_task_wait_metrics = enable;
        self
    }

    pub fn enable_task_exec_metrics(mut self, enable: bool) -> Self {
        self.enable_task_exec_metrics = enable;
        self
    }

    pub fn metric_idx_from_task_meta(
        mut self,
        f: Arc<dyn Fn(&[u8]) -> usize + Send + Sync>,
    ) -> Self {
        self.metric_idx_from_task_meta = Some(f);
        self
    }

    pub fn build_future_pool(self) -> FuturePool {
        let name = self
            .name_prefix
            .clone()
            .unwrap_or_else(|| "yatp_pool".to_string());
        let size = self.core_thread_count;
        let task = self.max_tasks;
        let pool = self.build_single_level_pool();
        FuturePool::from_pool(pool, &name, size, task)
    }

    pub fn build_priority_future_pool(
        self,
        priority_provider: Arc<dyn priority::TaskPriorityProvider>,
    ) -> FuturePool {
        let name = self
            .name_prefix
            .clone()
            .unwrap_or_else(|| "yatp_pool".to_string());
        let size = self.core_thread_count;
        let task = self.max_tasks;
        let pool = self.build_priority_pool(priority_provider);
        FuturePool::from_pool(pool, &name, size, task)
    }

    pub fn build_single_level_pool(self) -> ThreadPool<TaskCell> {
        let (builder, runner) = self.create_builder();
        builder.build_with_queue_and_runner(
            yatp::queue::QueueType::SingleLevel,
            yatp::pool::CloneRunnerBuilder(runner),
        )
    }

    pub fn build_multi_level_pool(self) -> ThreadPool<TaskCell> {
        let name = self
            .name_prefix
            .clone()
            .unwrap_or_else(|| "yatp_pool".to_string());
        let multilevel_builder = multilevel::Builder::new(
            multilevel::Config::default()
                .name(Some(name))
                .cleanup_interval(self.cleanup_method.preferred_interval()),
        );
        let pending_task = self.try_spawn_cleanup(multilevel_builder.cleanup_fn());
        let (builder, read_pool_runner) = self.create_builder();
        let runner_builder =
            multilevel_builder.runner_builder(CloneRunnerBuilder(read_pool_runner));
        let pool = builder
            .build_with_queue_and_runner(QueueType::Multilevel(multilevel_builder), runner_builder);
        if let Some(task) = pending_task {
            pool.spawn(task);
        }
        pool
    }

    pub fn build_priority_pool(
        self,
        priority_provider: Arc<dyn priority::TaskPriorityProvider>,
    ) -> ThreadPool<TaskCell> {
        let name = self
            .name_prefix
            .clone()
            .unwrap_or_else(|| "yatp_pool".to_string());
        let priority_builder = priority::Builder::new(
            priority::Config::default()
                .name(Some(name))
                .cleanup_interval(self.cleanup_method.preferred_interval()),
            priority_provider,
        );
        let pending_task = self.try_spawn_cleanup(priority_builder.cleanup_fn());
        let (builder, read_pool_runner) = self.create_builder();
        let runner_builder = priority_builder.runner_builder(CloneRunnerBuilder(read_pool_runner));
        let pool = builder
            .build_with_queue_and_runner(QueueType::Priority(priority_builder), runner_builder);
        if let Some(task) = pending_task {
            pool.spawn(task);
        }
        pool
    }

    #[cfg(test)]
    fn background_cleanup_hook<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.background_cleanup_hook = Some(Arc::new(f));
        self
    }

    #[cfg(test)]
    fn try_spawn_cleanup<F>(&self, cleanup: F) -> Option<TaskCell>
    where
        F: Fn() -> Option<std::time::Instant> + Send + 'static,
    {
        if let Some(hook) = &self.background_cleanup_hook {
            let on_cleanup = hook.clone();
            self.cleanup_method.try_spawn(move || {
                on_cleanup();
                cleanup()
            })
        } else {
            self.cleanup_method.try_spawn(cleanup)
        }
    }

    #[cfg(not(test))]
    fn try_spawn_cleanup<F>(&self, cleanup: F) -> Option<TaskCell>
    where
        F: Fn() -> Option<std::time::Instant> + Send + 'static,
    {
        self.cleanup_method.try_spawn(cleanup)
    }

    fn create_builder(mut self) -> (yatp::Builder, YatpPoolRunner<T>) {
        let name = self.name_prefix.unwrap_or_else(|| "yatp_pool".to_string());
        let mut builder = yatp::Builder::new(thd_name!(name));
        builder
            .stack_size(self.stack_size)
            .min_thread_count(self.min_thread_count)
            .core_thread_count(self.core_thread_count)
            .max_thread_count(self.max_thread_count);

        let after_start = self.after_start.take();
        let before_stop = self.before_stop.take();
        let before_pause = self.before_pause.take();
        let schedule_wait_durations = TaskScheduleHistograms::new(
            self.enable_task_wait_metrics,
            &name,
            &metrics::YATP_POOL_SCHEDULE_WAIT_DURATION_VEC,
        );
        let schedule_exec_durations = TaskScheduleHistograms::new(
            self.enable_task_exec_metrics,
            &name,
            &metrics::YATP_POOL_SCHEDULE_EXEC_DURATION_VEC,
        );
        let read_pool_runner = YatpPoolRunner::new(
            Default::default(),
            self.ticker.clone(),
            after_start,
            before_stop,
            before_pause,
            schedule_wait_durations,
            schedule_exec_durations,
        );
        (builder, read_pool_runner)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{self, AssertUnwindSafe},
        sync::{atomic, mpsc},
        thread,
    };

    use futures::compat::Future01CompatExt;

    use super::*;
    use crate::{
        sys::thread::{THREAD_NAME_HASHMAP, thread_id},
        timer::GLOBAL_TIMER_HANDLE,
        worker,
    };

    /// Thread names of all registered threads whose name contains `prefix`.
    fn registered_thread_names(prefix: &str) -> Vec<String> {
        THREAD_NAME_HASHMAP
            .lock()
            .unwrap()
            .values()
            .filter(|name| name.contains(prefix))
            .cloned()
            .collect()
    }

    /// Whether this build's allocator keeps the per-thread registry that
    /// `dump_stats` walks. The stub allocators used without the jemalloc
    /// feature keep none, and then the registry assertions below cannot
    /// observe anything.
    fn allocator_tracks_threads() -> bool {
        match tikv_alloc::fetch_stats() {
            // Stub allocator: there is no per-thread registry to assert on.
            Ok(None) => false,
            Ok(Some(_)) => true,
            // An allocator that tracks stats but cannot report them would make the
            // assertions below pass vacuously, so fail instead of skipping.
            Err(e) => panic!("cannot tell whether the allocator tracks threads: {e:?}"),
        }
    }

    /// Asserts whether the worker `name` is present in the allocator's
    /// per-thread registry. Where that registry exists, `dump_stats`
    /// dereferences the counters of every thread registered in it, so an
    /// entry of a dead worker is a use-after-free read. The control test
    /// asserts the positive case first, which is what keeps this from
    /// silently passing vacuously.
    fn assert_allocator_registry(name: &str, registered: bool) {
        if !allocator_tracks_threads() {
            return;
        }
        let stats = tikv_alloc::dump_stats();
        let present = stats.contains(name);
        assert!(
            present == registered,
            "worker {name} is {} the allocator registry, expected {}:\n{stats}",
            if present { "in" } else { "not in" },
            if registered { "in" } else { "not in" },
        );
    }

    /// Records which thread ran the pool's lifecycle hooks.
    #[derive(Default)]
    struct HookLog {
        started_on: atomic::AtomicI64,
        stopped_on: atomic::AtomicI64,
        stops: atomic::AtomicU32,
    }

    fn build_pool_with_logged_hooks(name: &str, log: &Arc<HookLog>) -> FuturePool {
        let (after_start, before_stop) = (log.clone(), log.clone());
        YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(name)
            .thread_count(1, 1, 1)
            .after_start(move || {
                after_start
                    .started_on
                    .store(thread_id() as i64, atomic::Ordering::SeqCst);
            })
            .before_stop(move || {
                before_stop
                    .stopped_on
                    .store(thread_id() as i64, atomic::Ordering::SeqCst);
                before_stop.stops.fetch_add(1, atomic::Ordering::SeqCst);
            })
            .build_future_pool()
    }

    /// Asserts that every per-thread resource a worker registers in
    /// `Runner::start` was released: the thread-name entry, the allocator
    /// accessor and the `before_stop` hook, the latter on the worker thread
    /// itself.
    fn assert_worker_state_released(name: &str, log: &HookLog) {
        let leaked = registered_thread_names(name);
        assert!(
            leaked.is_empty(),
            "thread-name entries of dead workers leaked: {leaked:?}"
        );
        assert_allocator_registry(name, false);
        assert_eq!(
            log.stops.load(atomic::Ordering::SeqCst),
            1,
            "`before_stop` must run exactly once per worker thread"
        );
        let (started_on, stopped_on) = (
            log.started_on.load(atomic::Ordering::SeqCst),
            log.stopped_on.load(atomic::Ordering::SeqCst),
        );
        assert_eq!(
            started_on, stopped_on,
            "`before_stop` must run on the worker thread that `after_start` ran on"
        );
    }

    /// Control case: a worker that stops normally releases its per-thread state
    /// from `Runner::end`. This is what makes the stale state observable at
    /// all.
    #[test]
    fn test_worker_releases_thread_state_on_clean_shutdown() {
        let name = "test_worker_clean_shutdown";
        let log = Arc::new(HookLog::default());
        let pool = build_pool_with_logged_hooks(name, &log);

        let (tx, rx) = mpsc::sync_channel(1);
        pool.spawn(async move { tx.send(()).unwrap() }).unwrap();
        rx.recv().unwrap();
        assert!(
            !registered_thread_names(name).is_empty(),
            "the running worker must have registered its thread name"
        );
        // Anchors the negative allocator check below: where the allocator keeps a
        // per-thread registry, the live worker must be observable in it.
        assert_allocator_registry(name, true);

        drop(pool);
        assert_worker_state_released(name, &log);
    }

    /// A panicking task unwinds out of yatp's `WorkerThread::run`, which calls
    /// `Runner::end` only on a normal exit of the task loop. The worker must
    /// release the state registered by `Runner::start` anyway, and it must
    /// release it on the worker thread: `remove_thread_memory_accessor`
    /// deregisters the calling thread, so a foreign thread cannot do it.
    ///
    /// The registries are only inspected after `drop(pool)` has joined the dead
    /// worker: once the task panics, the worker unwinds on its own schedule, so
    /// reading them earlier would race the release. The control test pins that
    /// a live worker is observable in the same registries.
    #[test]
    fn test_worker_releases_thread_state_on_task_panic() {
        let name = "test_worker_task_panic";
        let log = Arc::new(HookLog::default());
        let pool = build_pool_with_logged_hooks(name, &log);

        let (tx, rx) = mpsc::sync_channel(1);
        pool.spawn(async move {
            tx.send(()).unwrap();
            panic!("deliberate panic to unwind out of WorkerThread::run");
        })
        .unwrap();
        rx.recv().unwrap();

        // Shutdown joins the worker, whose panic is therefore re-raised here.
        let drop_res = panic::catch_unwind(AssertUnwindSafe(move || drop(pool)));
        assert!(
            drop_res.is_err(),
            "the task panic must reach the pool owner instead of being swallowed"
        );

        assert_worker_state_released(name, &log);
    }

    /// The `before_stop` hook is part of the same cleanup and runs while a task
    /// panic unwinds the worker. A panic escaping the guard's drop there would
    /// abort the whole process, killing every other thread's in-flight work, so
    /// the hook's panic must be contained — while the registrations below it
    /// are still released.
    #[test]
    fn test_panicking_before_stop_hook_keeps_cleanup_on_unwind() {
        let name = "test_worker_panicking_hook";
        let stops = Arc::new(atomic::AtomicU32::new(0));
        let logged = stops.clone();
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(name)
            .thread_count(1, 1, 1)
            .before_stop(move || {
                logged.fetch_add(1, atomic::Ordering::SeqCst);
                panic!("deliberate panic in the `before_stop` hook");
            })
            .build_future_pool();

        let (tx, rx) = mpsc::sync_channel(1);
        pool.spawn(async move {
            tx.send(()).unwrap();
            panic!("deliberate panic to unwind out of WorkerThread::run");
        })
        .unwrap();
        rx.recv().unwrap();

        let drop_res = panic::catch_unwind(AssertUnwindSafe(move || drop(pool)));
        assert!(drop_res.is_err(), "the task panic must escape `drop`");
        assert_eq!(
            stops.load(atomic::Ordering::SeqCst),
            1,
            "the hook must run exactly once"
        );
        assert_allocator_registry(name, false);
        let leaked = registered_thread_names(name);
        assert!(
            leaked.is_empty(),
            "a panicking hook must not leave the dead worker registered: {leaked:?}"
        );
    }

    #[test]
    fn test_record_schedule_wait_duration() {
        let name = "test_record_schedule_wait_duration";
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(name)
            .enable_task_wait_metrics(true)
            .build_single_level_pool();
        let (tx, rx) = mpsc::channel();
        for _ in 0..3 {
            let tx = tx.clone();
            pool.spawn(async move {
                GLOBAL_TIMER_HANDLE
                    .delay(std::time::Instant::now() + Duration::from_millis(100))
                    .compat()
                    .await
                    .unwrap();
                tx.send(()).unwrap();
            });
        }
        for _ in 0..3 {
            rx.recv().unwrap();
        }
        // Drop the pool so the local metrics are flushed.
        drop(pool);
        let histogram =
            metrics::YATP_POOL_SCHEDULE_WAIT_DURATION_VEC.with_label_values(&[name, "medium"]);
        assert_eq!(histogram.get_sample_count() as u32, 6, "{:?}", histogram);
    }

    #[test]
    fn test_cleanup_in_place_by_default() {
        let name = "test_cleanup_default";
        let count = Arc::new(atomic::AtomicU32::new(0));
        let n = count.clone();
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(name)
            .background_cleanup_hook(move || {
                n.fetch_add(1, atomic::Ordering::SeqCst);
            })
            .build_multi_level_pool();

        thread::sleep(3 * DEFAULT_CLEANUP_INTERVAL);
        drop(pool);
        assert_eq!(0, count.load(atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_cleanup_in_local_pool() {
        let name = "test_cleanup_local";
        let count = Arc::new(atomic::AtomicU32::new(0));
        let n = count.clone();
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(name)
            .cleanup_method(CleanupMethod::Local)
            .background_cleanup_hook(move || {
                n.fetch_add(1, atomic::Ordering::SeqCst);
                let t = thread::current();
                assert!(t.name().unwrap().starts_with(name));
            })
            .build_multi_level_pool();

        thread::sleep(3 * DEFAULT_CLEANUP_INTERVAL + DEFAULT_CLEANUP_INTERVAL / 2);
        drop(pool);
        thread::sleep(2 * DEFAULT_CLEANUP_INTERVAL);
        assert!(3 == count.load(atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_cleanup_in_remote_pool() {
        let name = "test_cleanup_remote";
        let bg_name = "test_background";
        let bg_pool = worker::Builder::new(bg_name).create();
        let count = Arc::new(atomic::AtomicU32::new(0));
        let n = count.clone();
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(name)
            .cleanup_method(CleanupMethod::Remote(bg_pool.remote()))
            .background_cleanup_hook(move || {
                n.fetch_add(1, atomic::Ordering::SeqCst);
                let t = thread::current();
                assert!(t.name().unwrap().starts_with(bg_name));
            })
            .build_multi_level_pool();

        thread::sleep(3 * DEFAULT_CLEANUP_INTERVAL + DEFAULT_CLEANUP_INTERVAL / 2);
        drop(pool);
        thread::sleep(2 * DEFAULT_CLEANUP_INTERVAL);
        assert!(5 == count.load(atomic::Ordering::SeqCst));
        drop(bg_pool);
        thread::sleep(2 * DEFAULT_CLEANUP_INTERVAL);
        assert!(5 == count.load(atomic::Ordering::SeqCst));
    }
}
