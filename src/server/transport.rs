// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use engine_traits::KvEngine;
use kvproto::raft_serverpb::RaftMessage;
use raftstore::{router::RaftStoreRouter, store::Transport, Result as RaftStoreResult};

use crate::server::{raft_client::RaftClient, resolve::StoreAddrResolver};

pub struct ServerTransport<T, S, E>
where
    T: RaftStoreRouter<E> + 'static,
    S: StoreAddrResolver + 'static,
    E: KvEngine,
{
    raft_client: RaftClient<S, T, E>,
    engine: PhantomData<E>,
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
            engine: PhantomData,
        }
    }
}

impl<T, S, E> ServerTransport<T, S, E>
where
    E: KvEngine,
    T: RaftStoreRouter<E> + 'static,
    S: StoreAddrResolver + 'static,
{
    pub fn new(raft_client: RaftClient<S, T, E>) -> ServerTransport<T, S, E> {
        ServerTransport {
            raft_client,
            engine: PhantomData,
        }
    }
}

impl<T, S, E> Transport for ServerTransport<T, S, E>
where
    T: RaftStoreRouter<E> + Unpin + 'static,
    S: StoreAddrResolver + Unpin + 'static,
    E: KvEngine,
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
