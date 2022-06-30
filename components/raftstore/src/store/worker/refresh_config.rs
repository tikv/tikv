// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
    thread,
};

use batch_system::{BatchRouter, Fsm, FsmTypes, HandlerBuilder, Poller, PoolState, Priority};
use file_system::{set_io_type, IOType};
use tikv_util::{
    debug, error, info, safe_panic, sys::thread::StdThreadBuildWrapper, thd_name, worker::Runnable,
};

use crate::store::fsm::{
    apply::{ApplyFsm, ControlFsm},
    store::StoreFsm,
    PeerFsm,
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
            if let Err(e) = self.state.fsm_sender.send(FsmTypes::Empty) {
                error!(
                    "failed to decrese thread pool";
                    "decrease to" => size,
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
                    set_io_type(IOType::ForegroundWrite);
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
        }
    }
}

pub struct Runner<EK, ER, AH, RH>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm>,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK>>,
{
    apply_pool: PoolController<ApplyFsm<EK>, ControlFsm, AH>,
    raft_pool: PoolController<PeerFsm<EK, ER>, StoreFsm<EK>, RH>,
}

impl<EK, ER, AH, RH> Runner<EK, ER, AH, RH>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm>,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK>>,
{
    pub fn new(
        apply_router: BatchRouter<ApplyFsm<EK>, ControlFsm>,
        raft_router: BatchRouter<PeerFsm<EK, ER>, StoreFsm<EK>>,
        apply_pool_state: PoolState<ApplyFsm<EK>, ControlFsm, AH>,
        raft_pool_state: PoolState<PeerFsm<EK, ER>, StoreFsm<EK>, RH>,
    ) -> Self {
        let apply_pool = PoolController::new(apply_router, apply_pool_state);
        let raft_pool = PoolController::new(raft_router, raft_pool_state);

        Runner {
            apply_pool,
            raft_pool,
        }
    }

    fn resize_raft_pool(&mut self, size: usize) {
        let current_pool_size = self.raft_pool.state.expected_pool_size;
        self.raft_pool.state.expected_pool_size = size;
        match current_pool_size.cmp(&size) {
            std::cmp::Ordering::Greater => self.raft_pool.decrease_by(current_pool_size - size),
            std::cmp::Ordering::Less => self.raft_pool.increase_by(size - current_pool_size),
            std::cmp::Ordering::Equal => (),
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
            std::cmp::Ordering::Equal => (),
        }
        self.apply_pool.cleanup_poller_threads();
        info!(
            "resize apply pool";
            "from" => current_pool_size,
            "to" => self.apply_pool.state.expected_pool_size
        );
    }
}

impl<EK, ER, AH, RH> Runnable for Runner<EK, ER, AH, RH>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm> + std::marker::Send,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK>> + std::marker::Send,
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
        }
    }
}
