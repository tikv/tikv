// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod node;

use std::sync::Arc;

use collections::HashMap;
use crossbeam::channel::TrySendError;
use engine_traits::{CfName, KvEngine, MvccProperties, RaftEngine};
use futures::{future::BoxFuture, Future};
use keys::NoPrefix;
use kvproto::{
    kvrpcpb::Context,
    metapb::{Region, RegionEpoch},
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, Request},
    raft_serverpb::RaftMessage,
};
pub use node::NodeV2;
use raft::SnapshotStatus;
use raftstore::{
    store::{cmd_resp, RegionSnapshot},
    DiscardReason,
};
use raftstore_v2::{
    router::{CmdResChannel, CmdResSubscriber, PeerMsg, RaftRequest, RaftRouter},
    StoreRouter,
};
use tikv_kv::{
    raft_extension::RaftExtension, Engine, Modify, OnReturnCallback, SnapContext, WriteData,
    WriteSubscriber,
};
use tikv_util::{codec::number::NumberEncoder, time::Instant};
use txn_types::{TimeStamp, TxnExtra, TxnExtraScheduler, WriteBatchFlags};

use super::raftkv::{check_raft_cmd_response, new_request_header};
use crate::{
    server::{
        metrics::{ASYNC_REQUESTS_COUNTER_VEC, ASYNC_REQUESTS_DURATIONS_VEC},
        raftkv::{get_status_kind_from_engine_error, invalid_resp_type},
    },
    storage::kv,
};

struct Wrap(CmdResSubscriber);

impl WriteSubscriber for Wrap {
    type ProposedWaiter<'a> = impl Future<Output = bool> + Send + 'a where Self: 'a;
    fn wait_proposed(&mut self) -> Self::ProposedWaiter<'_> {
        self.0.wait_proposed()
    }

    type CommittedWaiter<'a> = impl Future<Output = bool> + Send + 'a where Self: 'a;
    fn wait_committed(&mut self) -> Self::CommittedWaiter<'_> {
        self.0.wait_committed()
    }

    type ResultWaiter = impl Future<Output = Option<kv::Result<()>>>;
    fn result(self) -> Self::ResultWaiter {
        async move {
            self.0
                .result()
                .await
                .map(|mut res| check_raft_cmd_response(&mut res).map_err(|e| e.into()))
        }
    }
}

#[derive(Clone)]
pub struct RouterWrap<EK: KvEngine, ER: RaftEngine> {
    router: StoreRouter<EK, ER>,
}

impl<EK: KvEngine, ER: RaftEngine> RouterWrap<EK, ER> {
    pub fn new(router: StoreRouter<EK, ER>) -> Self {
        Self { router }
    }
}

impl<EK: KvEngine, ER: RaftEngine> RaftExtension for RouterWrap<EK, ER> {
    #[inline]
    fn feed(&self, msg: RaftMessage, key_message: bool) {
        // Channel full and region not found are ignored.
        let region_id = msg.get_region_id();
        let msg_ty = msg.get_message().get_msg_type();
        if let Err(e) = self.router.send_raft_message(Box::new(msg)) && key_message {
            error!("failed to send raft message"; "region_id" => region_id, "msg_ty" => ?msg_ty, "err" => ?e);
        }
    }

    #[inline]
    fn report_reject_message(&self, _region_id: u64, _from_peer_id: u64) {
        // TODO
    }

    #[inline]
    fn report_peer_unreachable(&self, _region_id: u64, _to_peer_id: u64) {
        // TODO
    }

    #[inline]
    fn report_store_unreachable(&self, _store_id: u64) {
        // TODO
    }

    #[inline]
    fn report_snapshot_status(&self, _region_id: u64, _to_peer_id: u64, _status: SnapshotStatus) {
        // TODO
    }

    #[inline]
    fn report_resolved(&self, _store_id: u64, _group_id: u64) {
        // TODO
    }

    #[inline]
    fn split(
        &self,
        _region_id: u64,
        _region_epoch: RegionEpoch,
        _split_keys: Vec<Vec<u8>>,
        _source: String,
    ) -> BoxFuture<'static, kv::Result<Vec<Region>>> {
        // TODO
        unimplemented!()
    }

    type ReadIndexRes = impl Future<Output = kv::Result<u64>>;
    fn read_index(&self, _ctx: &Context) -> Self::ReadIndexRes {
        async move { unimplemented!() }
    }
}

#[derive(Clone)]
pub struct RaftKvV2<EK: KvEngine, ER: RaftEngine> {
    extension: RouterWrap<EK, ER>,
    router: RaftRouter<EK, ER>,
    txn_extra_scheduler: Option<Arc<dyn TxnExtraScheduler>>,
}

impl<EK, ER> RaftKvV2<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(router: RaftRouter<EK, ER>) -> Self {
        Self {
            extension: RouterWrap::new(router.store_router().clone()),
            router,
            txn_extra_scheduler: None,
        }
    }

    pub fn set_txn_extra_scheduler(&mut self, txn_extra_scheduler: Arc<dyn TxnExtraScheduler>) {
        self.txn_extra_scheduler = Some(txn_extra_scheduler);
    }
}

impl<EK, ER> Engine for RaftKvV2<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Snap = RegionSnapshot<EK::Snapshot, NoPrefix>;
    type Local = EK;

    fn kv_engine(&self) -> Option<EK> {
        None
    }

    fn modify_on_kv_engine(&self, _: HashMap<u64, Vec<Modify>>) -> kv::Result<()> {
        // TODO
        Ok(())
    }

    type RaftExtension = RouterWrap<EK, ER>;

    fn raft_extension(&self) -> &Self::RaftExtension {
        &self.extension
    }

    fn precheck_write_with_ctx(&self, _ctx: &Context) -> kv::Result<()> {
        // TODO
        Ok(())
    }

    type WriteSubscriber = impl WriteSubscriber;
    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        _subscribed_event: u8,
        on_return: Option<OnReturnCallback<()>>,
    ) -> Self::WriteSubscriber {
        fail_point!("raftkv_async_write");
        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let timer = Instant::now_coarse();

        let reqs: Vec<Request> = batch.modifies.into_iter().map(Into::into).collect();
        let txn_extra = batch.extra;
        let region_id = ctx.get_region_id();
        let mut header = new_request_header(ctx);
        let mut flags = 0;
        if txn_extra.one_pc {
            flags |= WriteBatchFlags::ONE_PC.bits();
        }
        if txn_extra.for_flashback {
            flags |= WriteBatchFlags::FLASHBACK.bits();
        }
        header.set_flags(flags);

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(reqs.into());

        self.schedule_txn_extra(txn_extra);

        let mut res;
        #[cfg(feature = "failpoints")]
        {
            // If rid is some, only the specified region reports error.
            // If rid is None, all regions report error.
            let raftkv_early_error_report_fp = || -> raftstore_v2::Result<()> {
                fail_point!("raftkv_early_error_report", |rid| {
                    let region_id = ctx.get_region_id();
                    rid.and_then(|rid| {
                        let rid: u64 = rid.parse().unwrap();
                        if rid == region_id { None } else { Some(()) }
                    })
                    .ok_or_else(|| raftstore_v2::Error::RegionNotFound(region_id))
                });
                Ok(())
            };
            res = raftkv_early_error_report_fp().map_err(kv::Error::from);
        }
        #[cfg(not(feature = "failpoints"))]
        {
            res = Ok(());
        }
        if res.is_ok() && cmd.get_requests().is_empty() {
            res = Err(kv::Error::from(kv::ErrorInner::EmptyRequest))
        }

        let (ch, sub) = match on_return {
            Some(cb) => {
                let preset = Box::new(move |resp: &mut RaftCmdResponse| {
                    if !resp.get_header().has_error() {
                        cb(&mut Ok(()));
                    } else {
                        // Although clone, but it's unlikely happen.
                        let mut err =
                            check_raft_cmd_response(&mut resp.clone()).map_err(From::from);
                        cb(&mut err);
                    }
                });
                CmdResChannel::with_pre_set(preset)
            }
            None => CmdResChannel::pair(),
        };
        if res.is_ok() {
            let msg = PeerMsg::RaftCommand(RaftRequest::new(cmd, ch));
            if let Err(e) = self.router.send(region_id, msg) {
                match e {
                    TrySendError::Full(PeerMsg::RaftCommand(req)) => {
                        req.ch
                            .set_result(cmd_resp::new_error(raftstore_v2::Error::Transport(
                                DiscardReason::Full,
                            )));
                    }
                    TrySendError::Disconnected(PeerMsg::RaftCommand(req)) => {
                        req.ch.set_result(cmd_resp::new_error(
                            raftstore_v2::Error::RegionNotFound(region_id),
                        ));
                    }
                    _ => unreachable!(),
                }
            }
        }

        Wrap(sub).map(move |r| {
            if res.is_ok() {
                res = r;
            }
            match &res {
                Ok(_) => {
                    ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .write
                        .observe(timer.saturating_elapsed_secs());
                    fail_point!("raftkv_async_write_finish");
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(e);
                    ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                }
            }
            res
        })
    }

    type SnapshotRes = impl Future<Output = kv::Result<Self::Snap>> + Send;
    fn async_snapshot(&mut self, mut ctx: SnapContext<'_>) -> Self::SnapshotRes {
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        if !ctx.key_ranges.is_empty() && ctx.start_ts.map_or(false, |ts| !ts.is_zero()) {
            req.mut_read_index()
                .set_start_ts(ctx.start_ts.as_ref().unwrap().into_inner());
            req.mut_read_index()
                .set_key_ranges(std::mem::take(&mut ctx.key_ranges).into());
        }
        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let begin_instant = Instant::now_coarse();
        let mut header = new_request_header(ctx.pb_ctx);
        let mut flags = 0;
        if ctx.pb_ctx.get_stale_read() && ctx.start_ts.map_or(true, |ts| !ts.is_zero()) {
            let mut data = [0u8; 8];
            (&mut data[..])
                .encode_u64(ctx.start_ts.unwrap_or_default().into_inner())
                .unwrap();
            flags |= WriteBatchFlags::STALE_READ.bits();
            header.set_flag_data(data.into());
        }
        if ctx.for_flashback {
            flags |= WriteBatchFlags::FLASHBACK.bits();
        }
        header.set_flags(flags);

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(vec![req].into());

        let res = self.router.snapshot(cmd);
        async move {
            fail_point!("raftkv_async_snapshot_err", |_| Err(box_err!(
                "injected error for async_snapshot"
            )));

            match res.await {
                Ok(snap) => {
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .snapshot
                        .observe(begin_instant.saturating_elapsed_secs());
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    Ok(snap)
                }
                Err(mut resp) => {
                    let resps = resp.mut_responses();
                    let e = if resps
                        .get(0)
                        .map(|resp| resp.get_read_index().has_locked())
                        .unwrap_or(false)
                    {
                        let locked = resps[0].take_read_index().take_locked();
                        kv::Error::from(kv::ErrorInner::KeyIsLocked(locked))
                    } else {
                        invalid_resp_type(CmdType::Snap, resps[0].get_cmd_type()).into()
                    };
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                    Err(e)
                }
            }
        }
    }

    fn release_snapshot(&mut self) {
        // No snapshot cache in v2.
    }

    fn get_mvcc_properties_cf(
        &self,
        _: CfName,
        _: TimeStamp,
        _: &[u8],
        _: &[u8],
    ) -> Option<MvccProperties> {
        None
    }

    fn schedule_txn_extra(&self, txn_extra: TxnExtra) {
        if let Some(tx) = self.txn_extra_scheduler.as_ref() {
            if !txn_extra.is_empty() {
                tx.schedule(txn_extra);
            }
        }
    }

    fn start_flashback(&self, _ctx: &Context) -> BoxFuture<'static, kv::Result<()>> {
        // TODO
        unimplemented!()
    }

    fn end_flashback(&self, _ctx: &Context) -> BoxFuture<'static, kv::Result<()>> {
        // TODO
        unimplemented!()
    }
}
