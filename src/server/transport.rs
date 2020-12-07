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
    need_flush: bool,
}

impl<T, S> Clone for ServerTransport<T, S>
where
    T: RaftStoreRouter<RocksEngine> + 'static,
    S: StoreAddrResolver + 'static,
{
    fn clone(&self) -> Self {
        ServerTransport {
            raft_client: self.raft_client.clone(),
            need_flush: false,
        }
    }
}

impl<T: RaftStoreRouter<RocksEngine> + 'static, S: StoreAddrResolver + 'static>
    ServerTransport<T, S>
{
    pub fn new(raft_client: RaftClient<S, T>) -> ServerTransport<T, S> {
        ServerTransport {
            raft_client,
            need_flush: false,
        }
    }
}

impl<T, S> Transport for ServerTransport<T, S>
where
    T: RaftStoreRouter<RocksEngine> + Unpin + 'static,
    S: StoreAddrResolver + Unpin + 'static,
{
    fn send(&mut self, msg: RaftMessage) -> RaftStoreResult<()> {
        match self.raft_client.send(msg) {
            Ok(()) => {
                self.need_flush = true;
                Ok(())
            }
            Err(reason) => Err(raftstore::Error::Transport(reason)),
        }
    }

    fn need_flush(&self) -> bool {
        self.need_flush
    }

    fn flush(&mut self) {
        self.need_flush = false;
        self.raft_client.flush();
    }
}
