// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! `meta_storage` is the API set for storing generic KV pairs.
//! It is a trimmed version of the KV service of etcd, along with some metrics.

use futures::{stream::BoxStream, FutureExt, Stream};
use grpcio::RpcStatusCode;
use kvproto::meta_storagepb as pb;

use crate::{Error, PdFuture, Result, RpcClient};

pub trait MetaStorageClient {
    type WatchStream<T>: Stream<Item = grpcio::Result<T>>;

    fn get(&self, req: pb::GetRequest) -> PdFuture<Result<pb::GetResponse>>;
    fn put(&self, req: pb::PutRequest) -> PdFuture<Result<pb::PutResponse>>;
    fn watch(
        &self,
        req: pb::WatchRequest,
    ) -> PdFuture<Result<Self::WatchStream<pb::WatchResponse>>>;
}
