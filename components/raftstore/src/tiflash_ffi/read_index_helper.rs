use crate::router::RaftStoreRouter;
use crate::store::{Callback, RaftRouter};
use crate::{DiscardReason, Error as RaftStoreError};
use engine_rocks::RocksEngine;
use engine_traits::RaftEngine;
use futures::executor::block_on;
use kvproto::kvrpcpb::{ReadIndexRequest, ReadIndexResponse};
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request as RaftRequest};
use tikv_util::future::paired_future_callback;
use txn_types::Key;

pub trait ReadIndex {
    fn read_index(&self, req: ReadIndexRequest) -> ReadIndexResponse;
}

pub struct ReadIndexClient<ER: RaftEngine> {
    pub router: RaftRouter<RocksEngine, ER>,
}

impl<ER: RaftEngine> ReadIndex for ReadIndexClient<ER> {
    fn read_index(&self, req: ReadIndexRequest) -> ReadIndexResponse {
        let region_id = req.get_context().get_region_id();

        let mut cmd = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        let mut inner_req = RaftRequest::default();
        inner_req.set_cmd_type(CmdType::ReadIndex);
        inner_req.mut_read_index().set_start_ts(req.get_start_ts());
        for r in req.get_ranges() {
            let mut range = kvproto::kvrpcpb::KeyRange::default();
            range.set_start_key(Key::from_raw(r.get_start_key()).into_encoded());
            range.set_end_key(Key::from_raw(r.get_end_key()).into_encoded());
            inner_req.mut_read_index().mut_key_ranges().push(range);
        }
        header.set_region_id(region_id);
        header.set_peer(req.get_context().get_peer().clone());
        header.set_region_epoch(req.get_context().get_region_epoch().clone());
        cmd.set_header(header);
        cmd.set_requests(vec![inner_req].into());

        let (cb, f) = paired_future_callback();

        if let Err(e) = self.router.send_command(cmd, Callback::Read(cb)) {
            // Retrun region error instead a gRPC error.
            let mut resp = ReadIndexResponse::default();
            let region_error = if let RaftStoreError::Transport(DiscardReason::Disconnected) = e {
                RaftStoreError::RegionNotFound(region_id).into()
            } else {
                e.into()
            };
            resp.set_region_error(region_error);
            return resp;
        }

        {
            let mut res = block_on(f).unwrap();
            let mut resp = ReadIndexResponse::default();
            if res.response.get_header().has_error() {
                resp.set_region_error(res.response.mut_header().take_error());
            } else {
                let mut raft_resps = res.response.take_responses();
                if raft_resps.len() != 1 {
                    error!(
                        "invalid read index response";
                        "region_id" => region_id,
                        "response" => ?raft_resps
                    );
                    resp.mut_region_error().set_message(format!(
                        "Internal Error: invalid response: {:?}",
                        raft_resps
                    ));
                } else {
                    let mut read_index_resp = raft_resps[0].take_read_index();
                    if read_index_resp.has_locked() {
                        resp.set_locked(read_index_resp.take_locked());
                    } else {
                        resp.set_read_index(read_index_resp.get_read_index());
                    }
                }
            }
            resp
        }
    }
}
