// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use crate::store::config::{PoolController, RaftstoreConfigTask};
use crate::store::fsm::apply::{ApplyFsm, ControlFsm};
use crate::store::fsm::store::StoreFsm;
use crate::store::fsm::PeerFsm;
use batch_system::{BatchRouter, HandlerBuilder, PoolState, SCALE_FINISHED_CHANNEL};
use tikv_util::info;
use tikv_util::worker::Runnable;

pub struct Runner<EK, ER, AH, RH>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm>,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK, ER>>,
{
    apply_pool: PoolController<ApplyFsm<EK>, ControlFsm, AH>,
    raft_pool: PoolController<PeerFsm<EK, ER>, StoreFsm<EK, ER>, RH>,
}

impl<EK, ER, AH, RH> Runner<EK, ER, AH, RH>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm>,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK, ER>>,
{
    pub fn new(
        apply_router: BatchRouter<ApplyFsm<EK>, ControlFsm>,
        raft_router: BatchRouter<PeerFsm<EK, ER>, StoreFsm<EK, ER>>,
        apply_pool_state: PoolState<ApplyFsm<EK>, ControlFsm, AH>,
        raft_pool_state: PoolState<PeerFsm<EK, ER>, StoreFsm<EK, ER>, RH>,
    ) -> Self {
        let apply_pool = PoolController::new(apply_router, apply_pool_state);
        let raft_pool = PoolController::new(raft_router, raft_pool_state);

        Runner {
            apply_pool,
            raft_pool,
        }
    }

    fn resize_raft_pool(&mut self, size: usize) {
        match self.raft_pool.resize_to(size) {
            (_, 0) => {
                SCALE_FINISHED_CHANNEL.0.send(()).unwrap();
                return;
            }
            (true, s) => self.raft_pool.decrease_by(s),
            (false, s) => self.raft_pool.increase_by(s),
        }
        info!("resize_raft_pool");
    }

    fn resize_apply_pool(&mut self, size: usize) {
        match self.apply_pool.resize_to(size) {
            (_, 0) => {
                SCALE_FINISHED_CHANNEL.0.send(()).unwrap();
                return;
            }
            (true, s) => self.apply_pool.decrease_by(s),
            (false, s) => self.apply_pool.increase_by(s),
        }
        info!("resize_apply_pool");
    }

    fn cleanup_poller_threads(&mut self, type_: &str) {
        println!("*** type_: {}", type_);
        match type_ {
            "apply" => self.apply_pool.cleanup_poller_threads(),
            "raft" => self.raft_pool.cleanup_poller_threads(),
            _ => unreachable!(),
        };
        SCALE_FINISHED_CHANNEL.0.send(()).unwrap();
        info!("cleanup_poller_threads");
    }
}

impl<EK, ER, AH, RH> Runnable for Runner<EK, ER, AH, RH>
where
    EK: engine_traits::KvEngine,
    ER: engine_traits::RaftEngine,
    AH: HandlerBuilder<ApplyFsm<EK>, ControlFsm> + std::marker::Send,
    RH: HandlerBuilder<PeerFsm<EK, ER>, StoreFsm<EK, ER>> + std::marker::Send,
{
    type Task = RaftstoreConfigTask;

    fn run(&mut self, task: RaftstoreConfigTask) {
        match task {
            RaftstoreConfigTask::ScalePool(type_, size) => {
                match type_.as_str() {
                    "apply" => self.resize_apply_pool(size),
                    "raft" => self.resize_raft_pool(size),
                    _ => unreachable!(),
                }
                info!("run ScalePool");
            }
            RaftstoreConfigTask::CleanupPollerThreads(type_) => {
                self.cleanup_poller_threads(type_.as_str());
                info!("run CleanupPollerThreads");
            }
        }
    }
}
