// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    kvrpcpb::ExtraOp as TxnExtraOp,
    raft_cmdpb::{self, RaftCmdRequest, RaftCmdResponse},
};
use raftstore::{
    store::{
        cmd_resp, fsm::apply::notify_stale_req, metrics::RAFT_READ_INDEX_PENDING_COUNT,
        peer::RaftPeer, read_queue::ReadIndexRequest, util::check_region_epoch, ReadCallback,
    },
    Error,
};
use slog::{debug, error, info, o, warn, Logger};
use tikv_util::codec::number::decode_u64;
use txn_types::WriteBatchFlags;

use crate::{
    batch::StoreContext,
    raft::Peer,
    router::{
        message::RaftRequest,
        response_channel::{QueryResChannel, QueryResult, ReadResponse},
    },
    Result,
};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub(crate) fn response_replica_read<T>(
        &self,
        read_index_req: &mut ReadIndexRequest<QueryResChannel>,
        ctx: &mut StoreContext<EK, ER, T>,
    ) {
        debug!(
            self.logger,
            "handle replica reads with a read index";
            "request_id" => ?read_index_req.id,
        );
        RAFT_READ_INDEX_PENDING_COUNT.sub(read_index_req.cmds().len() as i64);
        for (req, ch, mut read_index) in read_index_req.take_cmds().drain(..) {
            // leader reports key is locked
            if let Some(locked) = read_index_req.locked.take() {
                let mut response = raft_cmdpb::Response::default();
                response.mut_read_index().set_locked(*locked);
                let mut cmd_resp = RaftCmdResponse::default();
                cmd_resp.mut_responses().push(response);
                let read_resp = ReadResponse {
                    response: cmd_resp,
                    txn_extra_op: TxnExtraOp::Noop,
                };
                ch.set_result(QueryResult::Read(read_resp));
                continue;
            }
            if req.get_header().get_replica_read() {
                // We should check epoch since the range could be changed.
                ch.set_result(self.can_replica_read(req, true, read_index_req.read_index));
            } else {
                // The request could be proposed when the peer was leader.
                // TODO: figure out that it's necessary to notify stale or not.
                let term = self.term();
                notify_stale_req(term, ch);
            }
        }
    }

    pub(crate) fn can_replica_read(
        &self,
        req: RaftCmdRequest,
        check_epoch: bool,
        read_index: Option<u64>,
    ) -> QueryResult {
        let region = self.region().clone();
        if check_epoch {
            if let Err(e) = check_region_epoch(&req, &region, true) {
                debug!(self.logger, "epoch not match"; "err" => ?e);
                let mut response = cmd_resp::new_error(e);
                cmd_resp::bind_term(&mut response, self.term());
                return QueryResult::Response(response);
            }
        }
        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
            let safe_ts = self.read_progress().safe_ts();
            if safe_ts < read_ts {
                warn!(
                    self.logger,
                    "read rejected by safe timestamp";
                    "safe ts" => safe_ts,
                    "read ts" => read_ts,
                    "tag" => self.tag(),
                );
                let mut response = cmd_resp::new_error(Error::DataIsNotReady {
                    region_id: region.get_id(),
                    peer_id: self.peer_id(),
                    safe_ts,
                });
                cmd_resp::bind_term(&mut response, self.term());
                return QueryResult::Response(response);
            }
        }

        QueryResult::Read(ReadResponse::new(read_index.unwrap_or(0)))
    }
}
