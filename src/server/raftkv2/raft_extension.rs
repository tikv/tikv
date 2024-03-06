// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_serverpb::RaftMessage;
use raftstore_v2::{
    router::{DebugInfoChannel, PeerMsg, StoreMsg},
    StoreRouter,
};

#[derive(Clone)]
pub struct Extension<EK: KvEngine, ER: RaftEngine> {
    router: StoreRouter<EK, ER>,
}

impl<EK: KvEngine, ER: RaftEngine> Extension<EK, ER> {
    pub fn new(router: StoreRouter<EK, ER>) -> Self {
        Extension { router }
    }
}

impl<EK: KvEngine, ER: RaftEngine> tikv_kv::RaftExtension for Extension<EK, ER> {
    #[inline]
    fn feed(&self, msg: RaftMessage, key_message: bool) {
        let region_id = msg.get_region_id();
        let msg_ty = msg.get_message().get_msg_type();
        // Channel full and region not found are ignored unless it's a key message.
        if let Err(e) = self.router.send_raft_message(Box::new(msg)) && key_message {
            error!("failed to send raft message"; "region_id" => region_id, "msg_ty" => ?msg_ty, "err" => ?e);
        }
    }

    #[inline]
    fn report_reject_message(&self, _region_id: u64, _from_peer_id: u64) {
        // TODO：reject the message on connection side instead of go through
        // raft layer.
    }

    #[inline]
    fn report_peer_unreachable(&self, region_id: u64, to_peer_id: u64) {
        let _ = self
            .router
            .send(region_id, PeerMsg::PeerUnreachable { to_peer_id });
    }

    #[inline]
    fn report_store_unreachable(&self, to_store_id: u64) {
        let _ = self
            .router
            .send_control(StoreMsg::StoreUnreachable { to_store_id });
    }

    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: raft::SnapshotStatus,
    ) {
        let _ = self
            .router
            .force_send(region_id, PeerMsg::SnapshotSent { to_peer_id, status });
    }

    fn report_resolved(&self, _store_id: u64, _group_id: u64) {
        // TODO: support commit group
    }

    fn split(
        &self,
        region_id: u64,
        region_epoch: kvproto::metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: String,
    ) -> futures::future::BoxFuture<'static, tikv_kv::Result<Vec<kvproto::metapb::Region>>> {
        let (msg, sub) = PeerMsg::request_split(region_epoch, split_keys, source);
        let res = self.router.check_send(region_id, msg);
        Box::pin(async move {
            res?;
            let mut resp = match sub.result().await {
                Some(r) => r,
                None => return Err(box_err!("split is aborted")),
            };
            if !resp.get_header().has_error() {
                let regions = resp.mut_admin_response().mut_splits().take_regions();
                Ok(regions.into())
            } else {
                Err(tikv_kv::Error::from(resp.mut_header().take_error()))
            }
        })
    }

    fn query_region(
        &self,
        region_id: u64,
    ) -> futures::future::BoxFuture<
        'static,
        tikv_kv::Result<raftstore::store::region_meta::RegionMeta>,
    > {
        let (ch, sub) = DebugInfoChannel::pair();
        let msg = PeerMsg::QueryDebugInfo(ch);
        let res = self.router.check_send(region_id, msg);
        Box::pin(async move {
            res?;
            match sub.result().await {
                Some(res) => Ok(res),
                None => Err(box_err!("query region is aborted")),
            }
        })
    }
}
