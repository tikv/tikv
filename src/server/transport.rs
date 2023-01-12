// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_serverpb::RaftMessage;
use raftstore::{store::Transport, Result as RaftStoreResult};
use tikv_kv::RaftExtension;

use crate::server::{raft_client::RaftClient, resolve::StoreAddrResolver};

pub struct ServerTransport<T, S>
where
    T: RaftExtension + 'static,
    S: StoreAddrResolver + 'static,
{
    raft_client: RaftClient<S, T>,
}

impl<T, S> Clone for ServerTransport<T, S>
where
    T: RaftExtension + 'static,
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
    T: RaftExtension + 'static,
    S: StoreAddrResolver + 'static,
{
    pub fn new(raft_client: RaftClient<S, T>) -> Self {
        ServerTransport { raft_client }
    }
}

impl<T, S> Transport for ServerTransport<T, S>
where
    T: RaftExtension + Unpin + 'static,
    S: StoreAddrResolver + Unpin + 'static,
{
    fn send(&mut self, msg: RaftMessage) -> RaftStoreResult<()> {
        match self.raft_client.send(msg) {
            Ok(()) => Ok(()),
            Err(reason) => Err(raftstore::Error::Transport(reason)),
        }
    }

    fn set_store_allowlist(&mut self, stores: Vec<u64>) {
        self.raft_client.set_store_allowlist(stores)
    }

    fn need_flush(&self) -> bool {
        self.raft_client.need_flush()
    }

    fn flush(&mut self) {
        self.raft_client.flush();
    }
}
