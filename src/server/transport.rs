// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_serverpb::RaftMessage;

use crate::server::raft_client::RaftClient;
use crate::server::resolve::StoreAddrResolver;
use engine_rocks::RocksEngine;
use raftstore::router::RaftStoreRouter;
use raftstore::store::Transport;
use raftstore::Result as RaftStoreResult;

pub struct ServerTransport<T, S>
where
    T: RaftStoreRouter<RocksEngine> + 'static,
    S: StoreAddrResolver + 'static,
{
    raft_client: RaftClient<S, T>,
}

impl<T, S> Clone for ServerTransport<T, S>
where
    T: RaftStoreRouter<RocksEngine> + 'static,
    S: StoreAddrResolver + 'static,
{
    fn clone(&self) -> Self {
        ServerTransport {
            raft_client: self.raft_client.clone(),
        }
    }
}

impl<T: RaftStoreRouter<RocksEngine> + 'static, S: StoreAddrResolver + 'static>
    ServerTransport<T, S>
{
    pub fn new(raft_client: RaftClient<S, T>) -> ServerTransport<T, S> {
        ServerTransport { raft_client }
    }
}

impl<T, S> Transport for ServerTransport<T, S>
where
    T: RaftStoreRouter<RocksEngine> + Unpin + 'static,
    S: StoreAddrResolver + Unpin + 'static,
{
    fn send(&mut self, msg: RaftMessage) -> RaftStoreResult<()> {
        match self.raft_client.send(msg) {
            Ok(()) => Ok(()),
            Err(reason) => Err(raftstore::Error::Transport(reason)),
        }
    }

    fn flush(&mut self) {
        self.raft_client.flush();
    }
}
