// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::{
    kvrpcpb::ExtraOp as TxnExtraOp,
    raft_cmdpb::{self, RaftCmdRequest, RaftCmdResponse},
};
use raftstore::{
    store::{cmd_resp, msg::ErrorCallback, util::check_region_epoch},
    Error,
};
use slog::{debug, error, info, o, warn, Logger};
use tikv_util::codec::number::decode_u64;

use crate::{
    batch::StoreContext,
    raft::Peer,
    router::{QueryResChannel, QueryResult, ReadResponse},
    Result,
};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub(crate) fn respond_stale_read(
        &self,
        req: RaftCmdRequest,
        check_epoch: bool,
        read_index: Option<u64>,
        ch: QueryResChannel,
    ) {
        let region = self.region().clone();
        if check_epoch {
            if let Err(e) = check_region_epoch(&req, &region, true) {
                let mut response = cmd_resp::new_error(e);
                cmd_resp::bind_term(&mut response, self.term());
                ch.report_error(response);
                return;
            }
        }
        let read_ts = decode_u64(&mut req.get_header().get_flag_data()).unwrap();
        let safe_ts = self.read_progress().safe_ts();
        if safe_ts < read_ts {
            debug!(
                self.logger,
                "read rejected by safe timestamp";
                "safe ts" => safe_ts,
                "read ts" => read_ts,
            );
            let mut response = cmd_resp::new_error(Error::DataIsNotReady {
                region_id: region.get_id(),
                peer_id: self.peer_id(),
                safe_ts,
            });
            cmd_resp::bind_term(&mut response, self.term());
            ch.report_error(response);
            return;
        }

        ch.set_result(QueryResult::Read(ReadResponse::new(
            read_index.unwrap_or(0),
        )))
    }
}
