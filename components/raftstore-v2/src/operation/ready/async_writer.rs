// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_serverpb::RaftMessage;
use raftstore::store::{
    local_metrics::RaftMetrics, Config, PersistedNotifier, WriteRouter, WriteRouterContext,
    WriteSenders, WriteTask,
};
use slog::{warn, Logger};

use crate::{
    batch::{StoreContext, StoreRouter},
    router::PeerMsg,
};

#[derive(Debug)]
struct UnpersistedReady {
    /// Number of ready.
    number: u64,
    /// Max number of following ready whose data to be persisted is empty.
    max_empty_number: u64,
    raft_msgs: Vec<Vec<RaftMessage>>,
}

/// A writer that handles asynchronous writes.
pub struct AsyncWriter<EK: KvEngine, ER: RaftEngine> {
    write_router: WriteRouter<EK, ER>,
    unpersisted_readies: VecDeque<UnpersistedReady>,
    persisted_number: u64,
}

impl<EK: KvEngine, ER: RaftEngine> AsyncWriter<EK, ER> {
    pub fn new(region_id: u64, peer_id: u64) -> Self {
        let write_router = WriteRouter::new(format!("[region {}] {}", region_id, peer_id));
        Self {
            write_router,
            unpersisted_readies: VecDeque::new(),
            persisted_number: 0,
        }
    }

    /// Execute the task.
    ///
    /// If the task takes some time to finish, `None` is returned. Otherwise,
    pub fn write(
        &mut self,
        ctx: &mut impl WriteRouterContext<EK, ER>,
        task: WriteTask<EK, ER>,
    ) -> Option<WriteTask<EK, ER>> {
        if task.has_data() {
            self.send(ctx, task);
            None
        } else {
            self.merge(task)
        }
    }

    pub fn known_largest_number(&self) -> u64 {
        self.unpersisted_readies
            .back()
            .map(|r| r.number)
            .unwrap_or(self.persisted_number)
    }

    fn send(&mut self, ctx: &mut impl WriteRouterContext<EK, ER>, task: WriteTask<EK, ER>) {
        let ready_number = task.ready_number();
        self.write_router.send_write_msg(
            ctx,
            self.unpersisted_readies.back().map(|r| r.number),
            raftstore::store::WriteMsg::WriteTask(task),
        );
        self.unpersisted_readies.push_back(UnpersistedReady {
            number: ready_number,
            max_empty_number: ready_number,
            raft_msgs: vec![],
        });
    }

    fn merge(&mut self, task: WriteTask<EK, ER>) -> Option<WriteTask<EK, ER>> {
        let ready_number = task.ready_number();
        if self.unpersisted_readies.is_empty() {
            // If this ready don't need to be persisted and there is no previous unpersisted
            // ready, we can safely consider it is persisted so the persisted msgs can be
            // sent immediately.
            self.persisted_number = task.ready_number();
            return Some(task);
        }

        // Attach to the last unpersisted ready so that it can be considered to be
        // persisted with the last ready at the same time.
        let last = self.unpersisted_readies.back_mut().unwrap();
        last.max_empty_number = task.ready_number();
        if !task.messages.is_empty() {
            last.raft_msgs.push(task.messages);
        }
        None
    }

    /// Called when an asynchronous write has finished.
    pub fn on_persisted(
        &mut self,
        ctx: &mut impl WriteRouterContext<EK, ER>,
        ready_number: u64,
        logger: &Logger,
    ) -> Vec<Vec<RaftMessage>> {
        if self.persisted_number >= ready_number {
            return vec![];
        }

        let last_unpersisted = self.unpersisted_readies.back();
        if last_unpersisted.map_or(true, |u| u.number < ready_number) {
            panic!(
                "{:?} ready number is too large {:?} vs {}",
                logger.list(),
                last_unpersisted,
                ready_number
            );
        }

        let mut raft_messages = vec![];
        // There must be a match in `self.unpersisted_readies`.
        loop {
            let Some(v) = self.unpersisted_readies.pop_front() else {
                panic!("{:?} ready number not found {}", logger.list(), ready_number);
            };
            if v.number > ready_number {
                panic!(
                    "{:?} ready number not matched {:?} vs {}",
                    logger.list(),
                    v,
                    ready_number
                );
            }
            if raft_messages.is_empty() {
                raft_messages = v.raft_msgs;
            } else {
                raft_messages.extend(v.raft_msgs);
            }
            if v.number == ready_number {
                self.persisted_number = v.max_empty_number;
                break;
            }
        }

        self.write_router
            .check_new_persisted(ctx, self.persisted_number);

        raft_messages
    }

    pub fn persisted_number(&self) -> u64 {
        self.persisted_number
    }

    pub fn all_ready_persisted(&self) -> bool {
        self.unpersisted_readies.is_empty()
    }
}

impl<EK, ER, T> WriteRouterContext<EK, ER> for StoreContext<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn write_senders(&self) -> &WriteSenders<EK, ER> {
        &self.write_senders
    }

    fn config(&self) -> &Config {
        &self.cfg
    }

    fn raft_metrics(&self) -> &RaftMetrics {
        &self.raft_metrics
    }
}

impl<EK: KvEngine, ER: RaftEngine> PersistedNotifier for StoreRouter<EK, ER> {
    fn notify(&self, region_id: u64, peer_id: u64, ready_number: u64) {
        if let Err(e) = self.force_send(
            region_id,
            PeerMsg::Persisted {
                peer_id,
                ready_number,
            },
        ) {
            warn!(
                self.logger(),
                "failed to send noop to trigger persisted ready";
                "region_id" => region_id,
                "peer_id" => peer_id,
                "ready_number" => ready_number,
                "error" => ?e,
            );
        }
    }
}
