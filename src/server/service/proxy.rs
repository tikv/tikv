use crate::server::{StoreAddrResolver, Error};

use kvproto::tikvpb;
use kvproto::kvrpcpb::{
    GetCommittedIndexAndTsRequest,
    GetCommittedIndexAndTsResponse,
    ReadIndexRequest,
};
use kvproto::tikvpb::TikvClient;

use grpcio::{RpcContext, UnarySink, ChannelBuilder, Environment};

use futures::prelude::*;
use futures::future;
use std::sync::Arc;

use tikv_util::future::{paired_future_callback, AndThenWith};

#[derive(Clone)]
pub struct Service<S: StoreAddrResolver> {
    store_resolver: S,
}

impl<S: StoreAddrResolver + 'static> tikvpb::DcProxy for Service<S> {
    fn get_committed_index_and_ts(
        &mut self,
        ctx: RpcContext<'_>,
        req: GetCommittedIndexAndTsRequest,
        sink: UnarySink<GetCommittedIndexAndTsResponse>,
    ) {
        let mut fs = Vec::with_capacity(req.get_contexts().len());
        let regions = req.get_contexts().to_vec();
        for region_ctx in regions {
            let store_id = region_ctx.get_peer().get_store_id();
            let (cb, f) = paired_future_callback();
            let res = self.store_resolver.resolve(store_id, cb);

            let f = AndThenWith::new(res, f.map_err(Error::from))
                .and_then(move |addr| {
                    let addr = addr.unwrap(); // FIXME: unwrap
                    let env = Arc::new(Environment::new(1));
                    let channel = ChannelBuilder::new(env).connect(&addr);
                    let client = TikvClient::new(channel);
                    let mut read_index_req = ReadIndexRequest::default();
                    read_index_req.set_context(region_ctx.clone());
                    client.read_index_async(&read_index_req).unwrap().map_err(Error::from)
                })
                .map(|res| {
                    res.get_read_index()
                });
            fs.push(Box::new(f));
        }

        let f = future::join_all(fs).and_then(|indexes| {
            let mut res = GetCommittedIndexAndTsResponse::default();
            res.set_committed_index(*indexes.iter().max().unwrap()); // FIXME: handle region error.
            sink.success(res).map_err(Error::from)
        }).map_err(|_| {});

        ctx.spawn(f)
    }
}
