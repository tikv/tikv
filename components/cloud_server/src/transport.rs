// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_serverpb::RaftMessage;
use rfstore::{router::RaftStoreRouter, store::Transport, Result as RaftStoreResult};
use tikv::server::resolve::StoreAddrResolver;

use crate::raft_client::RaftClient;

pub struct ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    raft_client: RaftClient<S, T>,
}

impl<T, S> Clone for ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    fn clone(&self) -> Self {
        ServerTransport {
            raft_client: self.raft_client.clone(),
        }
    }
}

impl<T, S> ServerTransport<T, S>
where
    T: RaftStoreRouter + 'static,
    S: StoreAddrResolver + 'static,
{
    pub fn new(raft_client: RaftClient<S, T>) -> ServerTransport<T, S> {
        ServerTransport { raft_client }
    }
}

impl<T, S> Transport for ServerTransport<T, S>
where
    T: RaftStoreRouter + Unpin + 'static,
    S: StoreAddrResolver + Unpin + 'static,
{
    fn send(&mut self, msg: RaftMessage) -> RaftStoreResult<()> {
        match self.raft_client.send(msg) {
            Ok(()) => Ok(()),
            Err(reason) => Err(rfstore::Error::Transport(reason)),
        }
    }

    fn need_flush(&self) -> bool {
        self.raft_client.need_flush()
    }

    fn flush(&mut self) {
        self.raft_client.flush();
    }
}
