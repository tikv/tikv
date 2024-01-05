// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

mod node;
mod raft_extension;

use std::{
    mem,
    pin::Pin,
    sync::{Arc, RwLock},
    task::Poll,
};

use collections::HashSet;
use engine_traits::{KvEngine, RaftEngine, CF_LOCK};
use futures::{future::BoxFuture, Future, Stream, StreamExt, TryFutureExt};
use kvproto::{
    kvrpcpb::Context,
    raft_cmdpb::{AdminCmdType, CmdType, RaftCmdRequest, Request},
};
pub use node::NodeV2;
pub use raft_extension::Extension;
use raftstore::{
    store::{
        cmd_resp, msg::ErrorCallback, util::encode_start_ts_into_flag_data, RaftCmdExtraOpts,
        RegionSnapshot,
    },
    Error,
};
use raftstore_v2::{
    router::{
        message::SimpleWrite, CmdResChannelBuilder, CmdResEvent, CmdResStream, PeerMsg, RaftRouter,
    },
    SimpleWriteBinary, SimpleWriteEncoder,
};
use tikv_kv::{Modify, WriteEvent};
use tikv_util::time::Instant;
use tracker::{get_tls_tracker_token, GLOBAL_TRACKERS};
use txn_types::{TxnExtra, TxnExtraScheduler, WriteBatchFlags};

use super::{
    metrics::{ASYNC_REQUESTS_COUNTER_VEC, ASYNC_REQUESTS_DURATIONS_VEC},
    raftkv::{
        check_raft_cmd_response, get_status_kind_from_engine_error, new_flashback_req,
        new_request_header,
    },
};

struct Transform {
    resp: CmdResStream,
    early_err: Option<tikv_kv::Error>,
}

impl Stream for Transform {
    type Item = WriteEvent;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        if stream.early_err.is_some() {
            return Poll::Ready(Some(WriteEvent::Finished(Err(stream
                .early_err
                .take()
                .unwrap()))));
        }
        match stream.resp.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(CmdResEvent::Proposed)) => Poll::Ready(Some(WriteEvent::Proposed)),
            Poll::Ready(Some(CmdResEvent::Committed)) => Poll::Ready(Some(WriteEvent::Committed)),
            Poll::Ready(Some(CmdResEvent::Finished(mut resp))) => {
                let res = if !resp.get_header().has_error() {
                    Ok(())
                } else {
                    Err(tikv_kv::Error::from(resp.take_header().take_error()))
                };
                Poll::Ready(Some(WriteEvent::Finished(res)))
            }
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

fn modifies_to_simple_write(modifies: Vec<Modify>) -> SimpleWriteBinary {
    let mut encoder = SimpleWriteEncoder::with_capacity(128);
    let modifies_len = modifies.len();
    let mut ssts = vec![];
    for m in modifies {
        match m {
            Modify::Put(cf, k, v) => encoder.put(cf, k.as_encoded(), &v),
            Modify::Delete(cf, k) => encoder.delete(cf, k.as_encoded()),
            Modify::PessimisticLock(k, lock) => {
                encoder.put(CF_LOCK, k.as_encoded(), &lock.into_lock().to_bytes())
            }
            Modify::DeleteRange(cf, start_key, end_key, notify_only) => encoder.delete_range(
                cf,
                start_key.as_encoded(),
                end_key.as_encoded(),
                notify_only,
            ),
            Modify::Ingest(sst) => {
                if ssts.capacity() == 0 {
                    ssts.reserve(modifies_len);
                }
                ssts.push(*sst);
            }
        }
    }
    if !ssts.is_empty() {
        encoder.ingest(ssts);
    }
    encoder.encode()
}

#[derive(Clone)]
pub struct RaftKv2<EK: KvEngine, ER: RaftEngine> {
    router: RaftRouter<EK, ER>,
    txn_extra_scheduler: Option<Arc<dyn TxnExtraScheduler>>,
    region_leaders: Arc<RwLock<HashSet<u64>>>,
}

impl<EK: KvEngine, ER: RaftEngine> RaftKv2<EK, ER> {
    #[allow(unused)]
    pub fn new(
        router: RaftRouter<EK, ER>,
        region_leaders: Arc<RwLock<HashSet<u64>>>,
    ) -> RaftKv2<EK, ER> {
        RaftKv2 {
            router,
            region_leaders,
            txn_extra_scheduler: None,
        }
    }

    pub fn set_txn_extra_scheduler(&mut self, txn_extra_scheduler: Arc<dyn TxnExtraScheduler>) {
        self.txn_extra_scheduler = Some(txn_extra_scheduler);
    }

    // for test only
    pub fn router(&self) -> &RaftRouter<EK, ER> {
        &self.router
    }
}

impl<EK: KvEngine, ER: RaftEngine> tikv_kv::Engine for RaftKv2<EK, ER> {
    type Snap = RegionSnapshot<EK::Snapshot>;
    type Local = EK;

    #[inline]
    fn kv_engine(&self) -> Option<Self::Local> {
        None
    }

    type RaftExtension = raft_extension::Extension<EK, ER>;
    #[inline]
    fn raft_extension(&self) -> Self::RaftExtension {
        raft_extension::Extension::new(self.router.store_router().clone())
    }

    fn modify_on_kv_engine(
        &self,
        region_modifies: collections::HashMap<u64, Vec<tikv_kv::Modify>>,
    ) -> tikv_kv::Result<()> {
        for (region_id, batch) in region_modifies {
            let bin = modifies_to_simple_write(batch);
            let _ = self.router.send(region_id, PeerMsg::unsafe_write(bin));
        }
        Ok(())
    }

    type SnapshotRes = impl Future<Output = tikv_kv::Result<Self::Snap>> + Send + 'static;
    fn async_snapshot(&mut self, mut ctx: tikv_kv::SnapContext<'_>) -> Self::SnapshotRes {
        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        if !ctx.key_ranges.is_empty() && ctx.start_ts.map_or(false, |ts| !ts.is_zero()) {
            req.mut_read_index()
                .set_start_ts(ctx.start_ts.as_ref().unwrap().into_inner());
            req.mut_read_index()
                .set_key_ranges(mem::take(&mut ctx.key_ranges).into());
        }
        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let begin_instant = Instant::now();

        let mut header = new_request_header(ctx.pb_ctx);
        let mut flags = 0;
        let need_encoded_start_ts = ctx.start_ts.map_or(true, |ts| !ts.is_zero());
        if ctx.pb_ctx.get_stale_read() && need_encoded_start_ts {
            flags |= WriteBatchFlags::STALE_READ.bits();
        }
        if ctx.allowed_in_flashback {
            flags |= WriteBatchFlags::FLASHBACK.bits();
        }
        header.set_flags(flags);
        // Encode `start_ts` in `flag_data` for the check of stale read and flashback.
        if need_encoded_start_ts {
            encode_start_ts_into_flag_data(
                &mut header,
                ctx.start_ts.unwrap_or_default().into_inner(),
            );
        }

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(vec![req].into());
        let res: tikv_kv::Result<()> = (|| {
            fail_point!("raftkv_async_snapshot_err", |_| {
                Err(box_err!("injected error for async_snapshot"))
            });
            Ok(())
        })();
        let f = if res.is_err() {
            None
        } else {
            Some(self.router.snapshot(cmd))
        };

        async move {
            res?;
            let res = f.unwrap().await;
            match res {
                Ok(snap) => {
                    let elapse = begin_instant.saturating_elapsed_secs();
                    let tracker = get_tls_tracker_token();
                    GLOBAL_TRACKERS.with_tracker(tracker, |tracker| {
                        if tracker.metrics.read_index_propose_wait_nanos > 0 {
                            ASYNC_REQUESTS_DURATIONS_VEC
                                .snapshot_read_index_propose_wait
                                .observe(
                                    tracker.metrics.read_index_propose_wait_nanos as f64
                                        / 1_000_000_000.0,
                                );
                            // snapshot may be handled by lease read in raftstore
                            if tracker.metrics.read_index_confirm_wait_nanos > 0 {
                                ASYNC_REQUESTS_DURATIONS_VEC
                                    .snapshot_read_index_confirm
                                    .observe(
                                        tracker.metrics.read_index_confirm_wait_nanos as f64
                                            / 1_000_000_000.0,
                                    );
                            }
                        } else if tracker.metrics.local_read {
                            ASYNC_REQUESTS_DURATIONS_VEC
                                .snapshot_local_read
                                .observe(elapse);
                        }
                    });
                    ASYNC_REQUESTS_DURATIONS_VEC.snapshot.observe(elapse);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    Ok(snap)
                }
                Err(mut resp) => {
                    if resp
                        .get_responses()
                        .first()
                        .map_or(false, |r| r.get_read_index().has_locked())
                    {
                        let locked = resp.mut_responses()[0].mut_read_index().take_locked();
                        Err(tikv_kv::Error::from(tikv_kv::ErrorInner::KeyIsLocked(
                            locked,
                        )))
                    } else if resp.get_header().has_error() {
                        let err = tikv_kv::Error::from(resp.take_header().take_error());
                        let status_kind = get_status_kind_from_engine_error(&err);
                        ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                        Err(err)
                    } else {
                        Err(box_err!("unexpected response: {:?}", resp))
                    }
                }
            }
        }
    }

    type WriteRes = impl Stream<Item = WriteEvent> + Send + Unpin;
    fn async_write(
        &self,
        ctx: &kvproto::kvrpcpb::Context,
        batch: tikv_kv::WriteData,
        subscribed: u8,
        on_applied: Option<tikv_kv::OnAppliedCb>,
    ) -> Self::WriteRes {
        fail_point!("raftkv_async_write");

        let region_id = ctx.region_id;
        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();

        let inject_region_not_found = (|| {
            // If rid is some, only the specified region reports error.
            // If rid is None, all regions report error.
            fail_point!("raftkv_early_error_report", |rid| -> bool {
                rid.and_then(|rid| rid.parse().ok())
                    .map_or(true, |rid: u64| rid == region_id)
            });
            false
        })();

        let begin_instant = Instant::now_coarse();
        let mut header = Box::new(new_request_header(ctx));
        let mut flags = 0;
        if batch.extra.one_pc {
            flags |= WriteBatchFlags::ONE_PC.bits();
        }
        if batch.extra.allowed_in_flashback {
            flags |= WriteBatchFlags::FLASHBACK.bits();
        }
        header.set_flags(flags);

        self.schedule_txn_extra(batch.extra);
        let mut data = modifies_to_simple_write(batch.modifies);
        if batch.avoid_batch {
            data.freeze();
        }
        let mut builder = CmdResChannelBuilder::default();
        if WriteEvent::subscribed_proposed(subscribed) {
            builder.subscribe_proposed();
        }
        if WriteEvent::subscribed_committed(subscribed) {
            builder.subscribe_committed();
        }
        if let Some(cb) = on_applied {
            builder.before_set(move |resp| {
                let mut res = if !resp.get_header().has_error() {
                    Ok(())
                } else {
                    Err(tikv_kv::Error::from(resp.get_header().get_error().clone()))
                };
                cb(&mut res);
            });
        }
        let (ch, sub) = builder.build();
        let res = if inject_region_not_found {
            ch.report_error(cmd_resp::new_error(Error::RegionNotFound(region_id)));
            Err(tikv_kv::Error::from(Error::RegionNotFound(region_id)))
        } else {
            let msg = PeerMsg::SimpleWrite(SimpleWrite {
                header,
                data,
                ch,
                send_time: Instant::now_coarse(),
                extra_opts: RaftCmdExtraOpts {
                    deadline: batch.deadline,
                    disk_full_opt: batch.disk_full_opt,
                },
            });
            self.router
                .store_router()
                .check_send(region_id, msg)
                .map_err(tikv_kv::Error::from)
        };
        (Transform {
            resp: CmdResStream::new(sub),
            early_err: res.err(),
        })
        .inspect(move |ev| {
            let WriteEvent::Finished(res) = ev else {
                return;
            };
            match res {
                Ok(()) => {
                    ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .write
                        .observe(begin_instant.saturating_elapsed_secs());
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(e);
                    ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                }
            }
        })
    }

    #[inline]
    fn precheck_write_with_ctx(&self, ctx: &kvproto::kvrpcpb::Context) -> tikv_kv::Result<()> {
        let region_id = ctx.get_region_id();
        match self.region_leaders.read().unwrap().get(&region_id) {
            Some(_) => Ok(()),
            None => Err(raftstore_v2::Error::NotLeader(region_id, None).into()),
        }
    }

    #[inline]
    fn schedule_txn_extra(&self, txn_extra: TxnExtra) {
        if let Some(tx) = self.txn_extra_scheduler.as_ref() {
            if !txn_extra.is_empty() {
                tx.schedule(txn_extra);
            }
        }
    }

    fn start_flashback(
        &self,
        ctx: &Context,
        start_ts: u64,
    ) -> BoxFuture<'static, tikv_kv::Result<()>> {
        // Send an `AdminCmdType::PrepareFlashback` to prepare the raftstore for the
        // later flashback. Once invoked, we will update the persistent region meta and
        // the memory state of the flashback in Peer FSM to reject all read, write
        // and scheduling operations for this region when propose/apply before we
        // start the actual data flashback transaction command in the next phase.
        let mut req = new_flashback_req(ctx, AdminCmdType::PrepareFlashback);
        req.mut_admin_request()
            .mut_prepare_flashback()
            .set_start_ts(start_ts);
        exec_admin(&self.router, req)
    }

    fn end_flashback(&self, ctx: &Context) -> BoxFuture<'static, tikv_kv::Result<()>> {
        // Send an `AdminCmdType::FinishFlashback` to unset the persistence state
        // in `RegionLocalState` and region's meta, and when that admin cmd is applied,
        // will update the memory state of the flashback
        let req = new_flashback_req(ctx, AdminCmdType::FinishFlashback);
        exec_admin(&self.router, req)
    }
}

fn exec_admin<EK: KvEngine, ER: RaftEngine>(
    router: &RaftRouter<EK, ER>,
    req: RaftCmdRequest,
) -> BoxFuture<'static, tikv_kv::Result<()>> {
    let region_id = req.get_header().get_region_id();
    let peer_id = req.get_header().get_peer().get_id();
    let term = req.get_header().get_term();
    let epoch = req.get_header().get_region_epoch().clone();
    let admin_type = req.get_admin_request().get_cmd_type();
    let (msg, sub) = PeerMsg::admin_command(req);
    let res = router.check_send(region_id, msg);
    Box::pin(
        async move {
            res?;
            let mut resp = sub.result().await.ok_or_else(|| -> tikv_kv::Error {
                box_err!(
                    "region {} exec_admin {:?} without response",
                    region_id,
                    admin_type
                )
            })?;
            check_raft_cmd_response(&mut resp)?;
            Ok(())
        }
        .map_err(move |e| {
            warn!("failed to execute admin command";
                "err" => ?e,
                "admin_type" => ?admin_type,
                "term" => term,
                "region_epoch" => ?epoch,
                "peer_id" => peer_id,
                "region_id" => region_id,
            );
            e
        }),
    )
}
