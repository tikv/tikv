// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::raft_serverpb::RaftMessage;

use crate::server::raft_client::RaftClient;
use crate::server::resolve::StoreAddrResolver;
use engine_traits::KvEngine;
use raftstore::router::RaftStoreRouter;
use raftstore::store::Transport;
use raftstore::Result as RaftStoreResult;

pub struct ServerTransport<T, S, E>
where
    T: RaftStoreRouter<E> + 'static,
    S: StoreAddrResolver + 'static,
    E: KvEngine,
{
    raft_client: RaftClient<S, T, E>,
}

impl<T, S, E> Clone for ServerTransport<T, S, E>
where
    T: RaftStoreRouter<E> + 'static,
    S: StoreAddrResolver + 'static,
    E: KvEngine,
{
    fn clone(&self) -> Self {
        ServerTransport {
            raft_client: self.raft_client.clone(),
        }
    }
}

impl<T, S, E> ServerTransport<T, S, E>
where
    T: RaftStoreRouter<E> + 'static,
    S: StoreAddrResolver + 'static,
    E: KvEngine,
{
    pub fn new(raft_client: RaftClient<S, T, E>) -> ServerTransport<T, S, E> {
        ServerTransport { raft_client }
    }
}

impl<T, S, E> Transport for ServerTransport<T, S, E>
where
    T: RaftStoreRouter<E> + Unpin + 'static,
    S: StoreAddrResolver + Unpin + 'static,
    E: KvEngine + Unpin,
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
