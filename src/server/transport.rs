// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_serverpb::RaftMessage;

use crate::server::raft_client::RaftClient;
use crate::server::resolve::StoreAddrResolver;
use raftstore::store::Transport;
use raftstore::Result as RaftStoreResult;

pub struct ServerTransport<S>
where
    S: StoreAddrResolver + 'static,
{
    raft_client: RaftClient<S>,
}

impl<S> Clone for ServerTransport<S>
where
    S: StoreAddrResolver + 'static,
{
    fn clone(&self) -> Self {
        ServerTransport {
            raft_client: self.raft_client.clone(),
        }
    }
}

impl<S: StoreAddrResolver + 'static> ServerTransport<S> {
    pub fn new(raft_client: RaftClient<S>) -> ServerTransport<S> {
        ServerTransport { raft_client }
    }
}

impl<S> Transport for ServerTransport<S>
where
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
