// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::RaftMessage;
use raft::eraftpb::MessageType;
use std::sync::mpsc::Sender;
use std::sync::{Arc, RwLock};

use super::metrics::*;
use super::resolve::StoreAddrResolver;
use super::snap::Task as SnapTask;
use raft::SnapshotStatus;
use raftstore::Result as RaftStoreResult;
use raftstore::store::{BatchReadCallback, Callback, Msg as StoreMsg, SignificantMsg, Transport};
use server::Result;
use server::raft_client::RaftClient;
use util::HandyRwLock;
use util::transport::SendCh;
use util::worker::Scheduler;

pub trait RaftStoreRouter: Send + Clone {
    /// Send StoreMsg, retry if failed. Try times may vary from implementation.
    fn send(&self, msg: StoreMsg) -> RaftStoreResult<()>;

    /// Send StoreMsg.
    fn try_send(&self, msg: StoreMsg) -> RaftStoreResult<()>;

    // Send RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::RaftMessage(msg))
    }

    // Send RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::new_raft_cmd(req, cb))
    }

    // Send a batch of RaftCmdRequests to local store.
    fn send_batch_commands(
        &self,
        batch: Vec<RaftCmdRequest>,
        on_finished: BatchReadCallback,
    ) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::new_batch_raft_snapshot_cmd(batch, on_finished))
    }

    // Send significant message. We should guarantee that the message can't be dropped.
    fn significant_send(&self, msg: SignificantMsg) -> RaftStoreResult<()>;

    // Report the peer of the region is unreachable.
    fn report_unreachable(&self, region_id: u64, to_peer_id: u64) -> RaftStoreResult<()> {
        self.significant_send(SignificantMsg::Unreachable {
            region_id,
            to_peer_id,
        })
    }

    // Report the sending snapshot status to the peer of the region.
    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    ) -> RaftStoreResult<()> {
        self.significant_send(SignificantMsg::SnapshotStatus {
            region_id,
            to_peer_id,
            status,
        })
    }
}

#[derive(Clone)]
pub struct ServerRaftStoreRouter {
    pub ch: SendCh<StoreMsg>,
    pub significant_msg_sender: Sender<SignificantMsg>,
}

impl ServerRaftStoreRouter {
    pub fn new(
        ch: SendCh<StoreMsg>,
        significant_msg_sender: Sender<SignificantMsg>,
    ) -> ServerRaftStoreRouter {
        ServerRaftStoreRouter {
            ch,
            significant_msg_sender,
        }
    }
}

impl RaftStoreRouter for ServerRaftStoreRouter {
    fn try_send(&self, msg: StoreMsg) -> RaftStoreResult<()> {
        self.ch.try_send(msg)?;
        Ok(())
    }

    fn send(&self, msg: StoreMsg) -> RaftStoreResult<()> {
        self.ch.send(msg)?;
        Ok(())
    }

    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::RaftMessage(msg))
    }

    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::new_raft_cmd(req, cb))
    }

    fn send_batch_commands(
        &self,
        batch: Vec<RaftCmdRequest>,
        on_finished: BatchReadCallback,
    ) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::new_batch_raft_snapshot_cmd(batch, on_finished))
    }

    fn significant_send(&self, msg: SignificantMsg) -> RaftStoreResult<()> {
        if let Err(e) = self.significant_msg_sender.send(msg) {
            return Err(box_err!("failed to sendsignificant msg {:?}", e));
        }

        Ok(())
    }
}

pub struct ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    raft_client: Arc<RwLock<RaftClient<S>>>,
    snap_scheduler: Scheduler<SnapTask>,
    pub raft_router: T,
}

impl<T, S> Clone for ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    fn clone(&self) -> Self {
        ServerTransport {
            raft_client: Arc::clone(&self.raft_client),
            snap_scheduler: self.snap_scheduler.clone(),
            raft_router: self.raft_router.clone(),
        }
    }
}

impl<T: RaftStoreRouter + 'static, S: StoreAddrResolver + 'static> ServerTransport<T, S> {
    pub fn new(
        raft_client: Arc<RwLock<RaftClient<S>>>,
        snap_scheduler: Scheduler<SnapTask>,
        raft_router: T,
    ) -> ServerTransport<T, S> {
        ServerTransport {
            raft_client,
            snap_scheduler,
            raft_router,
        }
    }

    fn send_store(&self, store_id: u64, msg: RaftMessage) {
        if msg.get_message().has_snapshot() {
            return self.send_snapshot_sock(store_id, msg);
        }
        if let Err(e) = self.raft_client.wl().send(store_id, msg) {
            error!("send raft msg err {:?}", e);
        }
    }

    fn send_snapshot_sock(&self, store_id: u64, msg: RaftMessage) {
        let rep = self.new_snapshot_reporter(&msg);
        let cb = box move |res: Result<()>| {
            if res.is_err() {
                rep.report(SnapshotStatus::Failure);
            } else {
                rep.report(SnapshotStatus::Finish);
            }
        };
        let region_id = msg.get_region_id();
        let from = msg.get_from_peer().get_id();
        let target = msg.get_to_peer().to_owned();
        let addr = self.raft_client.wl().addr(store_id);
        if let Err(e) = self.snap_scheduler.schedule(SnapTask::Send {
            target,
            addr,
            msg,
            cb,
        }) {
            if let SnapTask::Send { cb, target, .. } = e.into_inner() {
                error!(
                    "[region {}] {} channel is unavaliable, failed to schedule snapshot to {:?}",
                    region_id, from, target
                );
                cb(Err(box_err!("failed to schedule snapshot")));
            }
        }
    }

    fn new_snapshot_reporter(&self, msg: &RaftMessage) -> SnapshotReporter<T> {
        let region_id = msg.get_region_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let to_store_id = msg.get_to_peer().get_store_id();

        SnapshotReporter {
            raft_router: self.raft_router.clone(),
            region_id,
            to_peer_id,
            to_store_id,
        }
    }

    pub fn report_unreachable(&self, msg: RaftMessage) {
        let region_id = msg.get_region_id();
        let to_peer_id = msg.get_to_peer().get_id();
        let store_id = msg.get_to_peer().get_store_id();

        // Report snapshot failure.
        if msg.get_message().get_msg_type() == MessageType::MsgSnapshot {
            self.new_snapshot_reporter(&msg)
                .report(SnapshotStatus::Failure);
        }

        if let Err(e) = self.raft_router.report_unreachable(region_id, to_peer_id) {
            error!(
                "report peer {} on store {} unreachable for region {} failed {:?}",
                to_peer_id, store_id, region_id, e
            );
        }
    }

    pub fn flush_raft_client(&mut self) {
        self.raft_client.wl().flush();
    }
}

impl<T, S> Transport for ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();
        self.send_store(to_store_id, msg);
        Ok(())
    }

    fn flush(&mut self) {
        self.flush_raft_client();
    }
}

struct SnapshotReporter<T: RaftStoreRouter + 'static> {
    raft_router: T,
    region_id: u64,
    to_peer_id: u64,
    to_store_id: u64,
}

impl<T: RaftStoreRouter + 'static> SnapshotReporter<T> {
    pub fn report(&self, status: SnapshotStatus) {
        debug!(
            "send snapshot to {} for {} {:?}",
            self.to_peer_id, self.region_id, status
        );

        if status == SnapshotStatus::Failure {
            let store = self.to_store_id.to_string();
            REPORT_FAILURE_MSG_COUNTER
                .with_label_values(&["snapshot", &*store])
                .inc();
        };

        if let Err(e) =
            self.raft_router
                .report_snapshot_status(self.region_id, self.to_peer_id, status)
        {
            error!(
                "report snapshot to peer {} in store {} with region {} err {:?}",
                self.to_peer_id, self.to_store_id, self.region_id, e
            );
        }
    }
}
