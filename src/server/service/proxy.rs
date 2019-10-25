use std::sync::{Arc, RwLock};

use futures::{future, stream, Future, Stream};
use grpcio::{
    ChannelBuilder, Environment, Error as GrpcError, RpcContext, RpcStatus, RpcStatusCode,
    UnarySink,
};
use kvproto::errorpb;
use kvproto::kvrpcpb::*;
use kvproto::tikvpb::{self, TikvClient};
use tokio_threadpool::Builder as ThreadPoolBuilder;

use crate::server::StoreAddrResolver;
use pd_client::PdClient;
use tikv_util::collections::HashMap;
use tikv_util::future::paired_future_callback;

pub struct Service<S: StoreAddrResolver, P: PdClient> {
    store_resolver: S,
    pool: Arc<tokio_threadpool::ThreadPool>,
    clients: Arc<RwLock<HashMap<u64, TikvClient>>>,

    #[allow(dead_code)]
    pd: Arc<P>,
}

impl<S: StoreAddrResolver + 'static, P: PdClient + 'static> std::clone::Clone for Service<S, P> {
    fn clone(&self) -> Self {
        Service {
            store_resolver: self.store_resolver.clone(),
            pd: self.pd.clone(),
            pool: Arc::clone(&self.pool),
            clients: Arc::clone(&self.clients),
        }
    }
}

impl<S: StoreAddrResolver + 'static, P: PdClient + 'static> Service<S, P> {
    pub fn new(store_resolver: S, pd: Arc<P>) -> Self {
        Service {
            store_resolver,
            pd,
            pool: Arc::new(
                ThreadPoolBuilder::new()
                    .pool_size(4)
                    .name_prefix("dcproxy")
                    .build(),
            ),
            clients: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    fn get_client(&self, store_id: u64) -> Option<TikvClient> {
        let clients = self.clients.read().unwrap();
        match clients.get(&store_id) {
            Some(c) => Some(c.clone()),
            None => None,
        }
    }
}

impl<S: StoreAddrResolver + 'static, P: PdClient + 'static> tikvpb::DcProxy for Service<S, P> {
    fn get_committed_index_and_ts(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: GetCommittedIndexAndTsRequest,
        sink: UnarySink<GetCommittedIndexAndTsResponse>,
    ) {
        enum Res {
            GrpcError((GrpcError, u64)),
            RegionError(errorpb::Error),
            Index(u64),
        }

        info!("GetCommittedIndexAndTs is called");
        if req.get_ts_required() {
            panic!("GetCommittedIndexAndTs ts_required currently not supported");
        }

        let mut indices = Vec::new();
        for region_ctx in req.take_contexts().into_iter() {
            let store_id = region_ctx.get_peer().get_store_id();
            info!("getting connection for {}", store_id);
            let client = match self.get_client(store_id) {
                Some(c) => c,
                None => {
                    let (cb, f) = paired_future_callback();
                    let addr = match self
                        .store_resolver
                        .resolve(store_id, cb)
                        .and_then(|_| f.wait().unwrap())
                    {
                        Ok(a) => a,
                        Err(e) => {
                            warn!("GetCommittedIndexAndTs resolve {} fail: {:?}", store_id, e);
                            let code = RpcStatusCode::INTERNAL;
                            let status = RpcStatus::new(code, Some(format!("{}", e)));
                            ctx.spawn(sink.fail(status).map_err(|_| ()));
                            return;
                        }
                    };
                    info!("GetCommittedIndexAndTs resolved {} to {}", store_id, addr);
                    let env = Arc::new(Environment::new(4));
                    let channel = ChannelBuilder::new(env).connect(&addr);
                    let c = TikvClient::new(channel);
                    info!(
                        "GetCommittedIndexAndTs connected to [{}, {}]",
                        store_id, addr
                    );
                    let mut clients = self.clients.write().unwrap();
                    clients.insert(store_id, c.clone());
                    drop(clients);
                    c
                }
            };

            let mut req = ReadIndexRequest::default();
            req.set_context(region_ctx);
            let f = future::result(client.read_index_async(&req))
                .flatten()
                .map(|mut resp| {
                    let idx = resp.get_read_index();
                    if idx != 0 {
                        return Res::Index(idx);
                    }
                    return Res::RegionError(resp.take_region_error());
                })
                .or_else(move |e| future::ok::<_, ()>(Res::GrpcError((e, store_id))));
            indices.push(f);
        }

        let f = stream::futures_ordered(indices)
            .collect()
            .and_then(|res_vec| {
                let mut resp = GetCommittedIndexAndTsResponse::new();
                for res in res_vec {
                    match res {
                        Res::Index(i) => resp.mut_committed_indices().push(i),
                        Res::RegionError(e) => {
                            warn!("GetCommittedIndexAndTs region error: {:?}", e);
                            resp.mut_committed_indices().push(0);
                            let mut proxy_e = ProxyError::new();
                            proxy_e.set_region_error(e);
                            resp.mut_errors().push(proxy_e);
                        }
                        Res::GrpcError(e) => {
                            // TODO: update peer cache.
                            warn!("GetCommittedIndexAndTs grpc error: {:?}", e);
                            resp.mut_committed_indices().push(0);
                            let mut proxy_e = ProxyError::new();
                            proxy_e.set_grpc_error(format!("{:?}", e));
                            resp.mut_errors().push(proxy_e);
                        }
                    }
                }
                Ok(resp)
            });
        let h = self.pool.spawn_handle(f);
        ctx.spawn(h.and_then(|resp| {
            sink.success(resp)
                .map_err(|e| {
                    warn!("GetCommittedIndexAndTs sends response fail: {:?}", e);
                })
                .map(|_| {
                    info!("GetCommittedIndexAndTs sends response success");
                })
        }));
    }

    fn transaction_write(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: TransactionWriteRequest,
        _sink: UnarySink<TransactionWriteResponse>,
    ) {
        unimplemented!();
        /***********
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
        ***********/
    }
}
