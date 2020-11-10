use crate::router::RaftStoreRouter;
use crate::store::{Callback, RaftRouter};
use crate::{DiscardReason, Error as RaftStoreError};
use engine_rocks::RocksEngine;
use engine_traits::RaftEngine;
use futures::executor::block_on;
use kvproto::kvrpcpb::{ReadIndexRequest, ReadIndexResponse};
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request as RaftRequest};
use tikv_util::future::paired_future_callback;

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
        header.set_region_id(region_id);
        header.set_peer(req.get_context().get_peer().clone());
        header.set_region_epoch(req.get_context().get_region_epoch().clone());
        cmd.set_header(header);
        cmd.set_requests(vec![inner_req].into());

        let (cb, f) = paired_future_callback();

        // We must deal with all requests which acquire read-quorum in raftstore-thread,
        // so just send it as an command.
        if let Err(e) = self.router.send_command(cmd, Callback::Read(cb)) {
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
                let raft_resps = res.response.get_responses();
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
                    let read_index = raft_resps[0].get_read_index().get_read_index();
                    resp.set_read_index(read_index);
                }
            }
            resp
        }
    }
}
