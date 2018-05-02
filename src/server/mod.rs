// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

mod metrics;
mod raft_client;
mod service;

pub mod config;
pub mod debug;
pub mod errors;
pub mod node;
pub mod readpool;
pub mod resolve;
pub mod server;
pub mod snap;
pub mod transport;

use std::fmt::{Debug, Formatter, Result as FormatResult};

use futures::sync::{mpsc, oneshot};
use futures::{stream, Future, Sink, Stream};

pub use self::config::{Config, DEFAULT_CLUSTER_ID, DEFAULT_LISTENING_ADDR};
pub use self::errors::{Error, Result};
pub use self::node::{create_raft_storage, Node};
pub use self::raft_client::RaftClient;
pub use self::resolve::{PdStoreAddrResolver, StoreAddrResolver};
pub use self::server::Server;
pub use self::transport::{ServerRaftStoreRouter, ServerTransport};

pub enum OnResponse<T> {
    Unary(oneshot::Sender<T>),
    Streaming(mpsc::Sender<T>),
}

impl<T: Send + Debug + 'static> OnResponse<T> {
    pub fn is_streaming(&self) -> bool {
        match *self {
            OnResponse::Unary(_) => false,
            OnResponse::Streaming(_) => true,
        }
    }

    pub fn respond(self, resp: T) {
        match self {
            OnResponse::Unary(sender) => {
                let _ = sender.send(resp);
            }
            OnResponse::Streaming(sender) => {
                // `stream::once` never blocks, and our sender is a mpsc::channel::Sender so
                // that we can safely `wait()` here.
                let _ = sender.send_all(stream::once(Ok(resp))).wait();
            }
        }
    }

    pub fn respond_stream<S>(self, s: S) -> impl Future<Item = (), Error = mpsc::SendError<T>>
    where
        S: Stream<Item = T, Error = mpsc::SendError<T>>,
    {
        match self {
            OnResponse::Unary(_) => unreachable!(),
            OnResponse::Streaming(sender) => sender.send_all(s).map(|_| ()),
        }
    }
}

impl<T> Debug for OnResponse<T> {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        match *self {
            OnResponse::Unary(_) => write!(f, "Unary"),
            OnResponse::Streaming(_) => write!(f, "Streaming"),
        }
    }
}
