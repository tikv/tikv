// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    sync::{Arc, Mutex},
};

use crossbeam::channel::{SendError, TrySendError};
use engine_traits::{KvEngine, RaftEngine};
use futures::Future;
use kvproto::{
    kvrpcpb::ExtraOp,
    metapb::RegionEpoch,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
    raft_serverpb::RaftMessage,
};
use raftstore::{
    router::CdcHandle,
    store::{
        fsm::ChangeObserver, AsyncReadNotifier, Callback, FetchedLogs, GenSnapRes, RegionSnapshot,
    },
};
use slog::warn;

use super::{build_any_channel, message::CaptureChange, PeerMsg, QueryResChannel, QueryResult};
use crate::{batch::StoreRouter, operation::LocalReader, StoreMeta};

impl<EK: KvEngine, ER: RaftEngine> AsyncReadNotifier for StoreRouter<EK, ER> {
    fn notify_logs_fetched(&self, region_id: u64, fetched_logs: FetchedLogs) {
        let _ = self.force_send(region_id, PeerMsg::LogsFetched(fetched_logs));
    }

    fn notify_snapshot_generated(&self, region_id: u64, snapshot: GenSnapRes) {
        let _ = self.force_send(region_id, PeerMsg::SnapshotGenerated(snapshot));
    }
}

impl<EK: KvEngine, ER: RaftEngine> raftstore::coprocessor::StoreHandle for StoreRouter<EK, ER> {
    fn update_approximate_size(&self, region_id: u64, size: u64) {
        let _ = self.send(region_id, PeerMsg::UpdateRegionSize { size });
    }

    fn update_approximate_keys(&self, region_id: u64, keys: u64) {
        let _ = self.send(region_id, PeerMsg::UpdateRegionKeys { keys });
    }

    fn ask_split(
        &self,
        region_id: u64,
        region_epoch: kvproto::metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        source: Cow<'static, str>,
    ) {
        let (msg, _) = PeerMsg::request_split(region_epoch, split_keys, source.to_string());
        let res = self.send(region_id, msg);
        if let Err(e) = res {
            warn!(
                self.logger(),
                "failed to send ask split";
                "region_id" => region_id,
                "err" => %e,
            );
        }
    }

    fn refresh_region_buckets(
        &self,
        region_id: u64,
        region_epoch: kvproto::metapb::RegionEpoch,
        buckets: Vec<raftstore::store::Bucket>,
        bucket_ranges: Option<Vec<raftstore::store::BucketRange>>,
    ) {
        let res = self.send(
            region_id,
            PeerMsg::RefreshRegionBuckets {
                region_epoch,
                buckets,
                bucket_ranges,
            },
        );
        if let Err(e) = res {
            warn!(
                self.logger(),
                "failed to refresh region buckets";
                "err" => %e,
            );
        }
    }

    fn update_compute_hash_result(
        &self,
        _region_id: u64,
        _index: u64,
        _context: Vec<u8>,
        _hash: Vec<u8>,
    ) {
        // TODO
    }
}

/// A router that routes messages to the raftstore
pub struct RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    router: StoreRouter<EK, ER>,
    local_reader: LocalReader<EK, StoreRouter<EK, ER>>,
}

impl<EK, ER> Clone for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn clone(&self) -> Self {
        RaftRouter {
            router: self.router.clone(),
            local_reader: self.local_reader.clone(),
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> RaftRouter<EK, ER> {
    pub fn new(store_id: u64, router: StoreRouter<EK, ER>) -> Self {
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(store_id)));

        let logger = router.logger().clone();
        RaftRouter {
            router: router.clone(),
            local_reader: LocalReader::new(store_meta, router, logger),
        }
    }

    pub fn store_router(&self) -> &StoreRouter<EK, ER> {
        &self.router
    }

    pub fn send(&self, addr: u64, msg: PeerMsg) -> Result<(), TrySendError<PeerMsg>> {
        self.router.send(addr, msg)
    }

    #[inline]
    pub fn check_send(&self, addr: u64, msg: PeerMsg) -> crate::Result<()> {
        self.router.check_send(addr, msg)
    }

    pub fn store_meta(&self) -> &Arc<Mutex<StoreMeta<EK>>> {
        self.local_reader.store_meta()
    }

    pub fn send_raft_message(
        &self,
        msg: Box<RaftMessage>,
    ) -> std::result::Result<(), TrySendError<Box<RaftMessage>>> {
        self.router.send_raft_message(msg)
    }

    pub fn snapshot(
        &mut self,
        req: RaftCmdRequest,
    ) -> impl Future<Output = std::result::Result<RegionSnapshot<EK::Snapshot>, RaftCmdResponse>>
    + Send
    + 'static {
        self.local_reader.snapshot(req)
    }

    #[cfg(any(test, feature = "testexport"))]
    pub fn new_with_store_meta(
        router: StoreRouter<EK, ER>,
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
    ) -> Self {
        let logger = router.logger().clone();
        RaftRouter {
            router: router.clone(),
            local_reader: LocalReader::new(store_meta, router, logger),
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> CdcHandle<EK> for RaftRouter<EK, ER> {
    fn capture_change(
        &self,
        region_id: u64,
        region_epoch: RegionEpoch,
        observer: ChangeObserver,
        callback: Callback<EK::Snapshot>,
    ) -> crate::Result<()> {
        let (snap_cb, _) = build_any_channel(Box::new(move |args| {
            let (resp, snap) = (&args.0, args.1.take());
            if let Some(snap) = snap {
                let snapshot: RegionSnapshot<EK::Snapshot> = match snap.downcast() {
                    Ok(s) => *s,
                    Err(t) => unreachable!("snapshot type should be the same: {:?}", t),
                };
                callback.invoke_read(raftstore::store::ReadResponse {
                    response: Default::default(),
                    snapshot: Some(snapshot),
                    txn_extra_op: ExtraOp::Noop,
                })
            } else {
                callback.invoke_read(raftstore::store::ReadResponse {
                    response: resp.clone(),
                    snapshot: None,
                    txn_extra_op: ExtraOp::Noop,
                });
            }
        }));
        if let Err(SendError(msg)) = self.router.force_send(
            region_id,
            PeerMsg::CaptureChange(CaptureChange {
                observer,
                region_epoch,
                snap_cb,
            }),
        ) {
            warn!(self.router.logger(), "failed to send capture change msg"; "msg" => ?msg);
            return Err(crate::Error::RegionNotFound(region_id));
        }
        Ok(())
    }

    fn check_leadership(
        &self,
        region_id: u64,
        callback: Callback<EK::Snapshot>,
    ) -> crate::Result<()> {
        let (ch, _) = QueryResChannel::with_callback(Box::new(|res| {
            let resp = match res {
                QueryResult::Read(_) => raftstore::store::ReadResponse {
                    response: Default::default(),
                    snapshot: None,
                    txn_extra_op: ExtraOp::Noop,
                },
                QueryResult::Response(resp) => raftstore::store::ReadResponse {
                    response: resp.clone(),
                    snapshot: None,
                    txn_extra_op: ExtraOp::Noop,
                },
            };
            callback.invoke_read(resp);
        }));
        if let Err(SendError(msg)) = self
            .router
            .force_send(region_id, PeerMsg::LeaderCallback(ch))
        {
            warn!(self.router.logger(), "failed to send capture change msg"; "msg" => ?msg);
            return Err(crate::Error::RegionNotFound(region_id));
        }
        Ok(())
    }
}
