use crate::server::transport::RaftStoreRouter;
use kvproto::tikvpb;
use kvproto::kvrpcpb::{GetCommittedIndexAndTsRequest, GetCommittedIndexAndTsResponse};
use grpcio::{RpcContext, UnarySink};

#[derive(Clone)]
pub struct Service<T: RaftStoreRouter> {
    raft_router: T,
}

impl<T: RaftStoreRouter + 'static> tikvpb::DcProxy for Service<T> {
    fn get_committed_index_and_ts(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetCommittedIndexAndTsRequest,
        sink: UnarySink<GetCommittedIndexAndTsResponse>,
    ) {
        unimplemented!()
    }
}
