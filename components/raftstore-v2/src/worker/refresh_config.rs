// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    sync::Arc,
    thread,
};

use batch_system::{BatchRouter, Fsm, FsmTypes, HandlerBuilder, Poller, PoolState, Priority};
use file_system::{set_io_type, IoType};
use slog::{error, info, Logger};
use tikv_util::{sys::thread::StdThreadBuildWrapper, thd_name, worker::Runnable};

use crate::fsm::{PeerFsm, StoreFsm};

pub struct PoolController<N: Fsm, C: Fsm, H: HandlerBuilder<N, C>> {
    pub logger: Logger,
    pub router: BatchRouter<N, C>,
    pub state: PoolState<N, C, H>,
}

impl<N, C, H> PoolController<N, C, H>
where
    N: Fsm,
    C: Fsm,
    H: HandlerBuilder<N, C>,
{
    pub fn new(logger: Logger, router: BatchRouter<N, C>, state: PoolState<N, C, H>) -> Self {
        PoolController {
            logger,
            router,
            state,
        }
    }

    pub fn decrease_by(&mut self, size: usize) {
        for _ in 0..size {
            if let Err(e) = self.state.fsm_sender.send(FsmTypes::Empty, None) {
                error!(
                    self.logger,
                    "failed to decrease thread pool";
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
                    set_io_type(IoType::ForegroundWrite);
                    poller.poll();
                })
                .unwrap();
            workers.push(t);
        }
        self.state.id_base += size;
    }
}

#[derive(Debug)]
pub enum Task {
    ScalePool(usize),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self {
            Task::ScalePool(size) => {
                write!(f, "Scale pool ajusts: {} ", size)
            }
        }
    }
}

pub struct Runner<EK, ER, H>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    H: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm>,
{
    logger: Logger,
    raft_pool: PoolController<PeerFsm<EK, ER>, StoreFsm, H>,
}

impl<EK, ER, H> Runner<EK, ER, H>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    H: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm>,
{
    pub fn new(
        logger: Logger,
        router: BatchRouter<PeerFsm<EK, ER>, StoreFsm>,
        raft_pool_state: PoolState<PeerFsm<EK, ER>, StoreFsm, H>,
    ) -> Self {
        let raft_pool = PoolController::new(logger.clone(), router, raft_pool_state);
        Runner { logger, raft_pool }
    }

    fn resize_raft_pool(&mut self, size: usize) {
        let current_pool_size = self.raft_pool.state.expected_pool_size;
        self.raft_pool.state.expected_pool_size = size;
        match current_pool_size.cmp(&size) {
            std::cmp::Ordering::Greater => self.raft_pool.decrease_by(current_pool_size - size),
            std::cmp::Ordering::Less => self.raft_pool.increase_by(size - current_pool_size),
            std::cmp::Ordering::Equal => return,
        }

        info!(
            self.logger,
            "resize raft pool";
            "from" => current_pool_size,
            "to" => self.raft_pool.state.expected_pool_size
        );
    }
}

impl<EK, ER, H> Runnable for Runner<EK, ER, H>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    H: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm> + std::marker::Send,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::ScalePool(size) => {
                self.resize_raft_pool(size);
            }
        }
    }
}
