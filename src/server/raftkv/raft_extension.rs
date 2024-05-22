// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use futures::future::BoxFuture;
use kvproto::{
    metapb::{Region, RegionEpoch},
    raft_cmdpb::{AdminCmdType, RaftCmdRequest},
    raft_serverpb::RaftMessage,
};
use raft::SnapshotStatus;
use raftstore::{
    router::RaftStoreRouter,
    store::{
        region_meta::{RaftStateRole, RegionMeta},
        CasualMessage,
    },
};
use tikv_util::future::paired_future_callback;

use crate::storage::kv;

#[derive(Clone)]
pub struct RaftRouterWrap<S, E> {
    router: S,
    _phantom: PhantomData<E>,
}

impl<S, E> RaftRouterWrap<S, E> {
    pub fn new(router: S) -> Self {
        Self {
            router,
            _phantom: PhantomData,
        }
    }
}

impl<S, E> Deref for RaftRouterWrap<S, E> {
    type Target = S;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.router
    }
}

impl<S, E> DerefMut for RaftRouterWrap<S, E> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.router
    }
}

impl<S, E> tikv_kv::RaftExtension for RaftRouterWrap<S, E>
where
    S: RaftStoreRouter<E> + 'static,
    E: engine_traits::KvEngine,
{
    #[inline]
    fn feed(&self, msg: RaftMessage, key_message: bool) {
        let region_id = msg.get_region_id();
        let msg_ty = msg.get_message().get_msg_type();
        // Channel full and region not found are ignored unless it's a key message.
        if let Err(e) = self.router.send_raft_msg(msg) && key_message {
            error!("failed to send raft message"; "region_id" => region_id, "msg_ty" => ?msg_ty, "err" => ?e);
        }
    }

    #[inline]
    fn report_reject_message(&self, region_id: u64, from_peer_id: u64) {
        let m = CasualMessage::RejectRaftAppend {
            peer_id: from_peer_id,
        };
        let _ = self.router.send_casual_msg(region_id, m);
    }

    #[inline]
    fn report_peer_unreachable(&self, region_id: u64, to_peer_id: u64) {
        let _ = self.router.report_unreachable(region_id, to_peer_id);
    }

    #[inline]
    fn report_store_unreachable(&self, store_id: u64) {
        self.router.broadcast_unreachable(store_id);
    }

    #[inline]
    fn report_snapshot_status(&self, region_id: u64, to_peer_id: u64, status: SnapshotStatus) {
        if let Err(e) = self
            .router
            .report_snapshot_status(region_id, to_peer_id, status)
        {
            error!(?e;
                "report snapshot to peer failes";
                "to_peer_id" => to_peer_id,
                "status" => ?status,
                "region_id" => region_id,
            );
        }
    }

    #[inline]
    fn report_resolved(&self, store_id: u64, group_id: u64) {
        self.router.report_resolved(store_id, group_id);
    }

    #[inline]
    fn split(
        &self,
        region_id: u64,
        region_epoch: RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: String,
    ) -> BoxFuture<'static, kv::Result<Vec<Region>>> {
        let (cb, rx) = paired_future_callback();
        let req = CasualMessage::SplitRegion {
            region_epoch,
            split_keys,
            callback: raftstore::store::Callback::write(cb),
            source: source.into(),
            share_source_region_size: false,
        };
        let res = self.router.send_casual_msg(region_id, req);
        Box::pin(async move {
            res?;
            let mut admin_resp = box_try!(rx.await);
            super::check_raft_cmd_response(&mut admin_resp.response)?;
            let regions = admin_resp
                .response
                .mut_admin_response()
                .mut_splits()
                .take_regions();
            Ok(regions.into())
        })
    }

    /// Get the region meta of the given region.
    #[inline]
    fn query_region(&self, region_id: u64) -> BoxFuture<'static, kv::Result<RegionMeta>> {
        let (cb, rx) = paired_future_callback();
        let res = self
            .router
            .send_casual_msg(region_id, CasualMessage::AccessPeer(cb));
        Box::pin(async move {
            res?;
            Ok(box_try!(rx.await))
        })
    }

    /// Ask the raft group to do a consistency check.
    fn check_consistency(&self, region_id: u64) -> BoxFuture<'static, kv::Result<()>> {
        let region = self.query_region(region_id);
        let router = self.router.clone();
        Box::pin(async move {
            let meta: RegionMeta = region.await?;
            let leader_id = meta.raft_status.soft_state.leader_id;
            let mut leader = None;
            for peer in meta.region_state.peers {
                if peer.id == leader_id {
                    leader = Some(peer.into());
                }
            }
            if meta.raft_status.soft_state.raft_state != RaftStateRole::Leader {
                return Err(raftstore::Error::NotLeader(region_id, leader).into());
            }
            let mut req = RaftCmdRequest::default();
            req.mut_header().set_region_id(region_id);
            req.mut_header().set_peer(leader.unwrap());
            req.mut_admin_request()
                .set_cmd_type(AdminCmdType::ComputeHash);
            let f = super::exec_admin(&router, req);
            f.await
        })
    }
}
