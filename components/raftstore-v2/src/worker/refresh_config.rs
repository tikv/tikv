// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread};

use batch_system::{BatchRouter, Fsm, FsmTypes, HandlerBuilder, Poller, PoolState, Priority};
use file_system::{set_io_type, IoType};
use raftstore::store::{BatchComponent, RefreshConfigTask, Transport, WriterContoller};
use slog::{error, info, warn, Logger};
use tikv_util::{
    sys::thread::StdThreadBuildWrapper, thd_name, worker::Runnable, yatp_pool::FuturePool,
};

use crate::{
    fsm::{PeerFsm, StoreFsm},
    StoreRouter,
};

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
}

pub struct Runner<EK, ER, H, T>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    H: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm>,
    T: Transport + 'static,
{
    logger: Logger,
    raft_pool: PoolController<PeerFsm<EK, ER>, StoreFsm, H>,
    writer_ctrl: WriterContoller<EK, ER, T, StoreRouter<EK, ER>>,
    apply_pool: FuturePool,
}

impl<EK, ER, H, T> Runner<EK, ER, H, T>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    H: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm>,
    T: Transport + 'static,
{
    pub fn new(
        logger: Logger,
        router: BatchRouter<PeerFsm<EK, ER>, StoreFsm>,
        raft_pool_state: PoolState<PeerFsm<EK, ER>, StoreFsm, H>,
        writer_ctrl: WriterContoller<EK, ER, T, StoreRouter<EK, ER>>,
        apply_pool: FuturePool,
    ) -> Self {
        let raft_pool = PoolController::new(logger.clone(), router, raft_pool_state);
        Runner {
            logger,
            raft_pool,
            writer_ctrl,
            apply_pool,
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

        info!(
            self.logger,
            "resize raft pool";
            "from" => current_pool_size,
            "to" => self.raft_pool.state.expected_pool_size
        );
    }

    fn resize_apply_pool(&mut self, size: usize) {
        let current_pool_size = self.apply_pool.get_pool_size();
        if current_pool_size == size {
            return;
        }

        // It may not take effect immediately. See comments of
        // ThreadPool::scale_workers.
        // Also, the size will be clamped between min_thread_count and the max_pool_size
        // set when the apply_pool is initialized. This is fine as max_pool_size
        // is relatively a large value and there's no use case to set a value
        // larger than that.
        self.apply_pool.scale_pool_size(size);
        let (min_thread_count, max_thread_count) = self.apply_pool.thread_count_limit();
        if size > max_thread_count || size < min_thread_count {
            warn!(
                self.logger,
                "apply pool scale size is out of bound, and the size is clamped";
                "size" => size,
                "min_thread_limit" => min_thread_count,
                "max_thread_count" => max_thread_count,
            );
        } else {
            info!(
                self.logger,
                "resize apply pool";
                "from" => current_pool_size,
                "to" => size
            );
        }
    }

    /// Resizes the count of background threads in store_writers.
    fn resize_store_writers(&mut self, size: usize) {
        // The resizing of store writers will not directly update the local
        // cached store writers in each poller. Each poller will timely
        // correct its local cached in its next `poller.begin()` after
        // the resize operation completed.
        let current_size = self.writer_ctrl.expected_writers_size();
        self.writer_ctrl.set_expected_writers_size(size);
        match current_size.cmp(&size) {
            std::cmp::Ordering::Greater => {
                if let Err(e) = self.writer_ctrl.mut_store_writers().decrease_to(size) {
                    error!(
                        self.logger,
                        "failed to decrease store writers size";
                        "err_msg" => ?e
                    );
                }
            }
            std::cmp::Ordering::Less => {
                let writer_meta = self.writer_ctrl.writer_meta().clone();
                if let Err(e) = self
                    .writer_ctrl
                    .mut_store_writers()
                    .increase_to(size, writer_meta)
                {
                    error!(
                        self.logger,
                        "failed to increase store writers size";
                        "err_msg" => ?e
                    );
                }
            }
            std::cmp::Ordering::Equal => return,
        }
        info!(
            self.logger,
            "resize store writers pool";
            "from" => current_size,
            "to" => size
        );
    }
}

impl<EK, ER, H, T> Runnable for Runner<EK, ER, H, T>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    H: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm> + std::marker::Send,
    T: Transport + 'static,
{
    type Task = RefreshConfigTask;

    fn run(&mut self, task: Self::Task) {
        match task {
            RefreshConfigTask::ScalePool(component, size) => {
                match component {
                    BatchComponent::Store => self.resize_raft_pool(size),
                    BatchComponent::Apply => self.resize_apply_pool(size),
                };
            }
            RefreshConfigTask::ScaleWriters(size) => self.resize_store_writers(size),
            _ => {
                warn!(
                    self.logger,
                    "not supported now";
                    "config_change" => ?task,
                );
            }
        }
    }
}
