// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
    thread,
};

use batch_system::{BatchRouter, Fsm, FsmTypes, HandlerBuilder, Poller, PoolState, Priority};
use file_system::{set_io_type, IoType};
use tikv_util::{
    debug, error, info, safe_panic, sys::thread::StdThreadBuildWrapper, thd_name, warn,
    worker::Runnable, yatp_pool::FuturePool,
};

use crate::store::{
    async_io::write::{StoreWriters, StoreWritersContext},
    fsm::{
        apply::{ApplyFsm, ControlFsm},
        store::{RaftRouter, StoreFsm},
        PeerFsm,
    },
    transport::Transport,
    PersistedNotifier,
};

pub struct PoolController<N: Fsm, C: Fsm, H: HandlerBuilder<N, C>> {
    pub router: BatchRouter<N, C>,
    pub state: PoolState<N, C, H>,
}

impl<N, C, H> PoolController<N, C, H>
where
    N: Fsm,
    C: Fsm,
    H: HandlerBuilder<N, C>,
{
    pub fn new(router: BatchRouter<N, C>, state: PoolState<N, C, H>) -> Self {
        PoolController { router, state }
    }
}

impl<N, C, H> PoolController<N, C, H>
where
    N: Fsm + std::marker::Send + 'static,
    C: Fsm + std::marker::Send + 'static,
    H: HandlerBuilder<N, C>,
{
    pub fn decrease_by(&mut self, size: usize) {
        for _ in 0..size {
            if let Err(e) = self.state.fsm_sender.send(FsmTypes::Empty, None) {
                error!(
                    "failed to decrease thread pool";
                    "decrease_to" => size,
                    "err" => %e,
                );
                return;
            }
        }
    }

    pub fn increase_by(&mut self, size: usize) {
        let name_prefix = self.state.name_prefix.clone();
        let mut workers = self.state.workers.lock().unwrap();
        for i in 0..size {
            let handler = self.state.handler_builder.build(Priority::Normal);
            let mut poller = Poller {
                router: self.router.clone(),
                fsm_receiver: self.state.fsm_receiver.clone(),
                handler,
                max_batch_size: self.state.max_batch_size,
                reschedule_duration: self.state.reschedule_duration,
                joinable_workers: Some(Arc::clone(&self.state.joinable_workers)),
            };
            let props = tikv_util::thread_group::current_properties();
            let t = thread::Builder::new()
                .name(thd_name!(format!(
                    "{}-{}",
                    name_prefix,
                    i + self.state.id_base,
                )))
                .spawn_wrapper(move || {
                    tikv_util::thread_group::set_properties(props);
                    set_io_type(IoType::ForegroundWrite);
                    poller.poll();
                })
                .unwrap();
            workers.push(t);
        }
        self.state.id_base += size;
    }

    fn cleanup_poller_threads(&mut self) {
        // When a Poller executes drop, its tid is inserted into joinable_workers,
        // and then the thread exits.
        // When the batch system executes shutdown, it will traverse workers and
        // execute join in turn.
        // Maintaining this lock order is to prevent the shutdown thread from waiting
        // for the Poller thread to be unable to exit after the cleanup_poller_thread
        // thread gets the joinable_workers lock causing a deadlock problem.
        let mut workers = self.state.workers.lock().unwrap();
        let mut joinable_workers = self.state.joinable_workers.lock().unwrap();
        let mut last_error = None;
        for tid in joinable_workers.drain(..) {
            if let Some(i) = workers.iter().position(|v| v.thread().id() == tid) {
                let h = workers.swap_remove(i);
                debug!("cleanup poller waiting for {}", h.thread().name().unwrap());
                if let Err(e) = h.join() {
                    error!("failed to join worker thread: {:?}", e);
                    last_error = Some(e);
                }
            }
        }
        if let Some(e) = last_error {
            safe_panic!("failed to join worker thread: {:?}", e);
        }
    }
}

pub struct WriterContoller<EK, ER, T, N>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    T: Transport + 'static,
    N: PersistedNotifier,
{
    writer_meta: StoreWritersContext<EK, ER, T, N>,
    store_writers: StoreWriters<EK, ER>,
    expected_writers_size: usize,
}

impl<EK, ER, T, N> WriterContoller<EK, ER, T, N>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    T: Transport + 'static,
    N: PersistedNotifier,
{
    pub fn new(
        writer_meta: StoreWritersContext<EK, ER, T, N>,
        store_writers: StoreWriters<EK, ER>,
    ) -> Self {
        let writers_size = store_writers.size();
        Self {
            writer_meta,
            store_writers,
            expected_writers_size: writers_size,
        }
    }

    pub fn expected_writers_size(&self) -> usize {
        self.expected_writers_size
    }

    pub fn set_expected_writers_size(&mut self, size: usize) {
        self.expected_writers_size = size;
    }

    pub fn mut_store_writers(&mut self) -> &mut StoreWriters<EK, ER> {
        &mut self.store_writers
    }

    pub fn writer_meta(&self) -> &StoreWritersContext<EK, ER, T, N> {
        &self.writer_meta
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BatchComponent {
    Store,
    Apply,
}

impl Display for BatchComponent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            BatchComponent::Store => {
                write!(f, "raft")
            }
            BatchComponent::Apply => {
                write!(f, "apply")
            }
        }
    }
}

#[derive(Debug)]
pub enum Task {
    ScalePool(BatchComponent, usize),
    ScaleBatchSize(BatchComponent, usize),
    ScaleWriters(usize),
    ScaleAsyncReader(usize),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            Task::ScalePool(pool, size) => {
                write!(f, "Scale pool ajusts {}: {} ", pool, size)
            }
            Task::ScaleBatchSize(component, size) => {
                write!(f, "Scale max_batch_size adjusts {}: {} ", component, size)
            }
            Task::ScaleWriters(size) => {
                write!(f, "Scale store_io_pool_size adjusts {} ", size)
            }
            Task::ScaleAsyncReader(size) => {
                write!(f, "Scale snap_generator_pool_size adjusts {} ", size)
            }
        }
    }
}

pub struct Runner<EK, ER, AH, RH, T>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm>,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK>>,
    T: Transport + 'static,
{
    writer_ctrl: WriterContoller<EK, ER, T, RaftRouter<EK, ER>>,
    apply_pool: PoolController<ApplyFsm<EK>, ControlFsm, AH>,
    raft_pool: PoolController<PeerFsm<EK, ER>, StoreFsm<EK>, RH>,
    snap_generator_pool: FuturePool,
}

impl<EK, ER, AH, RH, T> Runner<EK, ER, AH, RH, T>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm>,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK>>,
    T: Transport + 'static,
{
    pub fn new(
        writer_meta: StoreWritersContext<EK, ER, T, RaftRouter<EK, ER>>,
        store_writers: StoreWriters<EK, ER>,
        apply_router: BatchRouter<ApplyFsm<EK>, ControlFsm>,
        raft_router: BatchRouter<PeerFsm<EK, ER>, StoreFsm<EK>>,
        apply_pool_state: PoolState<ApplyFsm<EK>, ControlFsm, AH>,
        raft_pool_state: PoolState<PeerFsm<EK, ER>, StoreFsm<EK>, RH>,
        snap_generator_pool: FuturePool,
    ) -> Self {
        let writer_ctrl = WriterContoller::new(writer_meta, store_writers);
        let apply_pool = PoolController::new(apply_router, apply_pool_state);
        let raft_pool = PoolController::new(raft_router, raft_pool_state);

        Runner {
            writer_ctrl,
            apply_pool,
            raft_pool,
            snap_generator_pool,
        }
    }

    fn resize_raft_pool(&mut self, size: usize) {
        let current_pool_size = self.raft_pool.state.expected_pool_size;
        self.raft_pool.state.expected_pool_size = size;
        match current_pool_size.cmp(&size) {
            std::cmp::Ordering::Greater => self.raft_pool.decrease_by(current_pool_size - size),
            std::cmp::Ordering::Less => self.raft_pool.increase_by(size - current_pool_size),
            std::cmp::Ordering::Equal => return,
        }
        self.raft_pool.cleanup_poller_threads();
        info!(
            "resize raft pool";
            "from" => current_pool_size,
            "to" => self.raft_pool.state.expected_pool_size
        );
    }

    fn resize_apply_pool(&mut self, size: usize) {
        let current_pool_size = self.apply_pool.state.expected_pool_size;
        self.apply_pool.state.expected_pool_size = size;
        match current_pool_size.cmp(&size) {
            std::cmp::Ordering::Greater => self.apply_pool.decrease_by(current_pool_size - size),
            std::cmp::Ordering::Less => self.apply_pool.increase_by(size - current_pool_size),
            std::cmp::Ordering::Equal => return,
        }
        self.apply_pool.cleanup_poller_threads();
        info!(
            "resize apply pool";
            "from" => current_pool_size,
            "to" => self.apply_pool.state.expected_pool_size
        );
    }

    /// Resizes the count of background threads in store_writers.
    fn resize_store_writers(&mut self, size: usize) {
        // The resizing of store writers will not directly update the local cached
        // store writers in each poller. Each poller will timely correct its local
        // cached in its next `poller.begin()` after the resize operation completed.
        let current_size = self.writer_ctrl.expected_writers_size;
        self.writer_ctrl.expected_writers_size = size;
        match current_size.cmp(&size) {
            std::cmp::Ordering::Greater => {
                if let Err(e) = self.writer_ctrl.store_writers.decrease_to(size) {
                    error!("failed to decrease store writers size"; "err_msg" => ?e);
                }
            }
            std::cmp::Ordering::Less => {
                let writer_meta = self.writer_ctrl.writer_meta.clone();
                if let Err(e) = self
                    .writer_ctrl
                    .store_writers
                    .increase_to(size, writer_meta)
                {
                    error!("failed to increase store writers size"; "err_msg" => ?e);
                }
            }
            std::cmp::Ordering::Equal => return,
        }
        info!(
            "resize store writers pool";
            "from" => current_size,
            "to" => size
        );
    }

    fn resize_snap_generator_read_pool(&mut self, size: usize) {
        let current_pool_size = self.snap_generator_pool.get_pool_size();
        // It may not take effect immediately. See comments of
        // ThreadPool::scale_workers.
        // Also, the size will be clamped between min_thread_count and the max_pool_size
        // set when the pool is initialized. This is fine as max_pool_size
        // is relatively a large value.
        self.snap_generator_pool.scale_pool_size(size);
        let (min_thread_count, max_thread_count) = self.snap_generator_pool.thread_count_limit();
        if size > max_thread_count || size < min_thread_count {
            warn!(
                "apply pool scale size is out of bound, and the size is clamped";
                "size" => size,
                "min_thread_limit" => min_thread_count,
                "max_thread_count" => max_thread_count,
            );
        } else {
            info!(
                "resize apply pool";
                "from" => current_pool_size,
                "to" => size,
            );
        }
    }
}

impl<EK, ER, AH, RH, T> Runnable for Runner<EK, ER, AH, RH, T>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm> + std::marker::Send,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK>> + std::marker::Send,
    T: Transport + 'static,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::ScalePool(component, size) => match component {
                BatchComponent::Store => self.resize_raft_pool(size),
                BatchComponent::Apply => self.resize_apply_pool(size),
            },
            Task::ScaleBatchSize(component, size) => match component {
                BatchComponent::Store => {
                    self.raft_pool.state.max_batch_size = size;
                }
                BatchComponent::Apply => {
                    self.apply_pool.state.max_batch_size = size;
                }
            },
            Task::ScaleWriters(size) => self.resize_store_writers(size),
            Task::ScaleAsyncReader(size) => {
                self.resize_snap_generator_read_pool(size);
            }
        }
    }
}
