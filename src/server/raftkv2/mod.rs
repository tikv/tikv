// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{mem, pin::Pin, task::Poll};

use engine_traits::{KvEngine, RaftEngine, CF_LOCK};
use futures::{Future, Stream, StreamExt};
use kvproto::{
    raft_cmdpb::{CmdType, RaftCmdRequest, Request},
    raft_serverpb::RaftMessage,
};
use raftstore::store::RegionSnapshot;
use raftstore_v2::{
    router::{message::SimpleWrite, CmdResChannel, CmdResEvent, CmdResStream, PeerMsg, RaftRouter},
    SimpleWriteEncoder, StoreRouter,
};
use tikv_kv::{Modify, RaftExtension, WriteEvent};
use tikv_util::{codec::number::NumberEncoder, time::Instant};
use txn_types::WriteBatchFlags;

use super::{
    metrics::{ASYNC_REQUESTS_COUNTER_VEC, ASYNC_REQUESTS_DURATIONS_VEC},
    raftkv::{get_status_kind_from_engine_error, new_request_header},
};

#[derive(Clone)]
pub struct RaftExtensionImpl<EK: KvEngine, ER: RaftEngine> {
    router: StoreRouter<EK, ER>,
}

impl<EK: KvEngine, ER: RaftEngine> RaftExtension for RaftExtensionImpl<EK, ER> {
    #[inline]
    fn feed(&self, msg: RaftMessage, key_message: bool) {
        let region_id = msg.get_region_id();
        let msg_ty = msg.get_message().get_msg_type();
        // Channel full and region not found are ignored unless it's a key message.
        if let Err(e) = self.router.send_raft_message(Box::new(msg)) && key_message {
            error!("failed to send raft message"; "region_id" => region_id, "msg_ty" => ?msg_ty, "err" => ?e);
        }
    }

    fn report_reject_message(&self, _region_id: u64, _from_peer_id: u64) {
        // TODO：reject the message on connection side instead of go through
        // raft layer.
    }

    fn report_peer_unreachable(&self, region_id: u64, to_peer_id: u64) {
        let _ = self
            .router
            .send(region_id, PeerMsg::PeerUnreachable { to_peer_id });
    }

    fn report_store_unreachable(&self, _store_id: u64) {}

    fn report_snapshot_status(
        &self,
        _region_id: u64,
        _to_peer_id: u64,
        _status: raft::SnapshotStatus,
    ) {
    }

    fn report_resolved(&self, _store_id: u64, _group_id: u64) {}

    fn split(
        &self,
        _region_id: u64,
        _region_epoch: kvproto::metapb::RegionEpoch,
        _split_keys: Vec<Vec<u8>>,
        _source: String,
    ) -> futures::future::BoxFuture<'static, tikv_kv::Result<Vec<kvproto::metapb::Region>>> {
        Box::pin(async move { Err(box_err!("raft split is not supported")) })
    }

    fn query_region(
        &self,
        _region_id: u64,
    ) -> futures::future::BoxFuture<
        'static,
        tikv_kv::Result<raftstore::store::region_meta::RegionMeta>,
    > {
        Box::pin(async move { Err(box_err!("query region is not supported")) })
    }
}

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

#[derive(Clone)]
pub struct RaftKv2<EK: KvEngine, ER: RaftEngine> {
    router: RaftRouter<EK, ER>,
}

impl<EK: KvEngine, ER: RaftEngine> RaftKv2<EK, ER> {
    #[allow(unused)]
    pub fn new(router: RaftRouter<EK, ER>) -> RaftKv2<EK, ER> {
        RaftKv2 { router }
    }
}

impl<EK: KvEngine, ER: RaftEngine> tikv_kv::Engine for RaftKv2<EK, ER> {
    type Snap = RegionSnapshot<EK::Snapshot>;
    type Local = EK;

    #[inline]
    fn kv_engine(&self) -> Option<Self::Local> {
        None
    }

    type RaftExtension = RaftExtensionImpl<EK, ER>;

    fn modify_on_kv_engine(
        &self,
        _region_modifies: collections::HashMap<u64, Vec<tikv_kv::Modify>>,
    ) -> tikv_kv::Result<()> {
        // TODO
        Ok(())
    }

    type SnapshotRes = impl Future<Output = tikv_kv::Result<Self::Snap>> + Send;
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
        if ctx.allowed_in_flashback {
            flags |= WriteBatchFlags::FLASHBACK.bits();
        }
        header.set_flags(flags);

        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.set_requests(vec![req].into());
        let f = self.router.snapshot(cmd);
        async move {
            let res = f.await;
            match res {
                Ok(snap) => {
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .snapshot
                        .observe(begin_instant.saturating_elapsed_secs());
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    Ok(snap)
                }
                Err(mut resp) => {
                    if resp
                        .get_responses()
                        .get(0)
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
        _subscribed: u8,
        on_applied: Option<tikv_kv::OnAppliedCb>,
    ) -> Self::WriteRes {
        let region_id = ctx.region_id;
        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
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
        let mut encoder = SimpleWriteEncoder::with_capacity(128);
        for m in batch.modifies {
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
            }
        }
        let data = encoder.encode();
        let (ch, sub) = match on_applied {
            Some(cb) => {
                let (ch, sub) = CmdResChannel::with_callback(move |resp| {
                    let mut res = if !resp.get_header().has_error() {
                        Ok(())
                    } else {
                        Err(tikv_kv::Error::from(resp.get_header().get_error().clone()))
                    };
                    cb(&mut res);
                });
                (ch, sub)
            }
            None => CmdResChannel::pair(),
        };
        let msg = PeerMsg::SimpleWrite(SimpleWrite {
            header,
            data,
            ch,
            send_time: Instant::now_coarse(),
        });
        let res = self
            .router
            .store_router()
            .send(region_id, msg)
            .map_err(|e| tikv_kv::Error::from(raftstore_v2::Error::from(e)));
        (Transform {
            resp: CmdResStream::new(sub),
            early_err: res.err(),
        })
        .inspect(move |ev| {
            let WriteEvent::Finished(res) = ev else { return };
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
}
