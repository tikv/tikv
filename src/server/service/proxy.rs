use crate::server::{Error, StoreAddrResolver};

use kvproto::kvrpcpb::{
    GetCommittedIndexAndTsRequest, GetCommittedIndexAndTsResponse, ReadIndexRequest, 
    TransactionWriteRequest, TransactionWriteResponse, CommitRequest
};
use kvproto::tikvpb;
use kvproto::tikvpb::TikvClient;

use grpcio::{ChannelBuilder, Environment, RpcContext, UnarySink};

use futures::future;
use futures::prelude::*;
use std::sync::Arc;

use tikv_util::future::{paired_future_callback, AndThenWith};
use pd_client::PdClient;

pub struct Service<S: StoreAddrResolver, P: PdClient> {
    store_resolver: S,
    pd: Arc<P>,
}

impl<S: StoreAddrResolver + 'static, P: PdClient + 'static>  std::clone::Clone for Service<S, P> {
    fn clone(&self) -> Self {
        Service {
            store_resolver: self.store_resolver.clone(),
            pd: self.pd.clone(),
        }
    }
}

impl<S: StoreAddrResolver + 'static, P: PdClient + 'static> Service<S, P> {
    pub fn new(store_resolver: S, pd: Arc<P>) -> Self {
        Service {
            store_resolver,
            pd,
        }
    }
}

impl<S: StoreAddrResolver + 'static, P: PdClient + 'static> tikvpb::DcProxy for Service<S, P> {
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
                    client
                        .read_index_async(&read_index_req)
                        .unwrap()
                        .map_err(Error::from)
                })
                .map(|res| res.get_read_index());
            fs.push(Box::new(f));
        }

        let f1 = future::join_all(fs);

        let f2 = self.pd.tso(1).map(move |(count, ts)| {
            assert_eq!(count, 1); // FIXME
            ts
        }).map_err(Error::from);

        let f = Future::join(f1, f2).and_then(|(indexes, ts)| {
            let mut res = GetCommittedIndexAndTsResponse::default();
            res.set_committed_index(*indexes.iter().max().unwrap());
            res.set_timestamp(ts);
            sink.success(res).map_err(Error::from)
        }).map_err(|_| {});

        ctx.spawn(f)
    }

    fn transaction_write(
        &mut self,
        ctx: RpcContext<'_>,
        req: TransactionWriteRequest,
        sink: UnarySink<TransactionWriteResponse>,
    ) {
        let mut fs1 = Vec::with_capacity(req.get_pre_writes().len());
        let mut fs2 = Vec::with_capacity(req.get_pre_writes().len());
        let pre_writes = req.get_pre_writes().to_vec();
        for pre_write_req in pre_writes.clone() {
            let proxy_ctx = pre_write_req.get_context();
            let store_id = proxy_ctx.get_peer().get_store_id();
            let (cb, f) = paired_future_callback();
            let res = self.store_resolver.resolve(store_id, cb);
            let future = AndThenWith::new(res, f.map_err(Error::from))
                .and_then(move |addr| {
                    let addr = addr.unwrap(); // FIXME: unwrap
                    let env = Arc::new(Environment::new(1));
                    let channel = ChannelBuilder::new(env).connect(&addr);
                    let client = TikvClient::new(channel);
                    client
                        .kv_prewrite_async(&pre_write_req)
                        .unwrap()
                        .map_err(Error::from)
                })
                .map(|resp| resp.clone());
            fs1.push(Box::new(future));
        }
        let f1 = future::join_all(fs1);

        for pre_write_req in pre_writes.clone() {
            let proxy_ctx = pre_write_req.get_context();
            let store_id = proxy_ctx.get_peer().get_store_id();
            let (cb, f) = paired_future_callback();
            let res = self.store_resolver.resolve(store_id, cb);
            let commit_future = AndThenWith::new(res, f.map_err(Error::from))
                .and_then(move |addr| {
                    let addr = addr.unwrap(); // FIXME: unwrap
                    let env = Arc::new(Environment::new(1));
                    let channel = ChannelBuilder::new(env).connect(&addr);
                    let client = TikvClient::new(channel);
                    let commit_req = CommitRequest::default();
                    client
                        .kv_commit_async(&commit_req)
                        .unwrap()
                        .map_err(Error::from)
                })
                .map(|resp| resp.clone());
            fs2.push(Box::new(commit_future));
        }

        let f2 = future::join_all(fs2);

        let f = Future::join(f1, f2).and_then(|(pre_write_resps, commit_resps)| {
            let mut res = TransactionWriteResponse::default();
            res.set_pre_write_resps(protobuf::RepeatedField::from_vec(pre_write_resps));
            res.set_commit_resps(protobuf::RepeatedField::from_vec(commit_resps));
            sink.success(res).map_err(Error::from)
        }).map_err(|_| {});

        ctx.spawn(f)
    }
}