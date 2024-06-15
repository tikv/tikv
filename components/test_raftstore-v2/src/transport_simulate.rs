// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use engine_traits::{KvEngine, RaftEngine};
use futures::future::{BoxFuture, FutureExt};
use kvproto::{
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RaftMessage,
};
use raft::SnapshotStatus;
use raftstore::{
    router::handle_send_error,
    store::{RegionSnapshot, Transport},
    Result, Result as RaftStoreResult,
};
use raftstore_v2::router::{PeerMsg, RaftRouter};
use test_raftstore::{filter_send, Filter};
use tikv_util::HandyRwLock;

#[derive(Clone)]
pub struct SimulateTransport<C> {
    filters: Arc<RwLock<Vec<Box<dyn Filter>>>>,
    ch: C,
}

impl<C> SimulateTransport<C> {
    pub fn new(ch: C) -> SimulateTransport<C> {
        Self {
            filters: Arc::new(RwLock::new(vec![])),
            ch,
        }
    }

    pub fn clear_filters(&mut self) {
        self.filters.wl().clear();
    }

    pub fn add_filter(&mut self, filter: Box<dyn Filter>) {
        self.filters.wl().push(filter);
    }

    pub fn filters(&self) -> &Arc<RwLock<Vec<Box<dyn Filter>>>> {
        &self.filters
    }
}

impl<C: Transport> Transport for SimulateTransport<C> {
    fn send(&mut self, m: RaftMessage) -> Result<()> {
        let ch = &mut self.ch;
        filter_send(&self.filters, m, |m| ch.send(m))
    }

    fn set_store_allowlist(&mut self, allowlist: Vec<u64>) {
        self.ch.set_store_allowlist(allowlist);
    }

    fn need_flush(&self) -> bool {
        self.ch.need_flush()
    }

    fn flush(&mut self) {
        self.ch.flush();
    }
}

pub trait SnapshotRouter<EK: KvEngine> {
    fn snapshot(
        &mut self,
        req: RaftCmdRequest,
    ) -> BoxFuture<'static, std::result::Result<RegionSnapshot<EK::Snapshot>, RaftCmdResponse>>;
}

impl<EK: KvEngine, ER: RaftEngine> SnapshotRouter<EK> for RaftRouter<EK, ER> {
    fn snapshot(
        &mut self,
        req: RaftCmdRequest,
    ) -> BoxFuture<'static, std::result::Result<RegionSnapshot<EK::Snapshot>, RaftCmdResponse>>
    {
        self.snapshot(req).boxed()
    }
}

impl<EK: KvEngine, C: SnapshotRouter<EK>> SnapshotRouter<EK> for SimulateTransport<C> {
    fn snapshot(
        &mut self,
        req: RaftCmdRequest,
    ) -> BoxFuture<'static, std::result::Result<RegionSnapshot<EK::Snapshot>, RaftCmdResponse>>
    {
        self.ch.snapshot(req).boxed()
    }
}

pub trait RaftStoreRouter {
    fn send_peer_msg(&self, region_id: u64, msg: PeerMsg) -> Result<()>;

    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()>;

    /// Reports the sending snapshot status to the peer of the Region.
    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    ) -> RaftStoreResult<()>;
}

impl<EK: KvEngine, ER: RaftEngine> RaftStoreRouter for RaftRouter<EK, ER> {
    fn send_peer_msg(&self, region_id: u64, msg: PeerMsg) -> RaftStoreResult<()> {
        self.send(region_id, msg)
            .map_err(|e| handle_send_error(region_id, e))
    }

    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let region_id = msg.get_region_id();
        self.send_raft_message(Box::new(msg))
            .map_err(|e| handle_send_error(region_id, e))
    }

    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    ) -> RaftStoreResult<()> {
        self.send_peer_msg(region_id, PeerMsg::SnapshotSent { to_peer_id, status })
    }
}

impl<C: RaftStoreRouter> RaftStoreRouter for SimulateTransport<C> {
    fn send_peer_msg(&self, region_id: u64, msg: PeerMsg) -> RaftStoreResult<()> {
        self.ch.send_peer_msg(region_id, msg)
    }

    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        filter_send(&self.filters, msg, |m| self.ch.send_raft_msg(m))
    }

    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    ) -> RaftStoreResult<()> {
        self.ch
            .report_snapshot_status(region_id, to_peer_id, status)
    }
}
