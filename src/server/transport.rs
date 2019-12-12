// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_serverpb::RaftMessage;

use crate::raftstore::router::RaftStoreRouter;
use crate::raftstore::store::Transport;
use crate::raftstore::Result as RaftStoreResult;
use crate::server::raft_client2::RaftStreamPool;
use crate::server::resolve::StoreAddrResolver;

pub struct ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    stream_pool: RaftStreamPool<S, T>,
}

impl<T, S> Clone for ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    fn clone(&self) -> Self {
        ServerTransport {
            stream_pool: self.stream_pool.clone(),
        }
    }
}

impl<T: RaftStoreRouter + 'static, S: StoreAddrResolver + 'static> ServerTransport<T, S> {
    pub fn new(stream_pool: RaftStreamPool<S, T>) -> ServerTransport<T, S> {
        ServerTransport { stream_pool }
    }
}

impl<T, S> Transport for ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    fn send(&mut self, msg: RaftMessage) -> RaftStoreResult<()> {
        self.stream_pool.send(msg)
    }

    fn flush(&mut self) {
        self.stream_pool.flush();
    }
}
