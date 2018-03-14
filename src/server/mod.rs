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
mod service;
mod raft_client;

pub mod readpool;
pub mod config;
pub mod errors;
pub mod server;
pub mod transport;
pub mod node;
pub mod resolve;
pub mod snap;
pub mod debug;

use std::fmt::{Debug, Formatter, Result as FormatResult};
use std::boxed::FnBox;

use futures::{stream, Stream};
use futures::sync::mpsc;

use util::futurepool::{self, FuturePool};

pub use self::config::{Config, DEFAULT_CLUSTER_ID, DEFAULT_LISTENING_ADDR};
pub use self::errors::{Error, Result};
pub use self::server::Server;
pub use self::transport::{ServerRaftStoreRouter, ServerTransport};
pub use self::node::{create_raft_storage, Node};
pub use self::resolve::{PdStoreAddrResolver, StoreAddrResolver};
pub use self::raft_client::RaftClient;

pub type CopStream<T> = Box<Stream<Item = T, Error = mpsc::SendError<T>> + Send>;

#[allow(type_complexity)]
pub enum OnResponse<T, C: futurepool::Context + 'static> {
    Unary(Box<FnBox(T) + Send>),
    Streaming(Box<FnBox(CopStream<T>, Option<FuturePool<C>>) + Send>),
}

impl<T: Send + Debug + 'static, C: futurepool::Context + 'static> OnResponse<T, C> {
    pub fn is_streaming(&self) -> bool {
        match *self {
            OnResponse::Unary(_) => false,
            OnResponse::Streaming(_) => true,
        }
    }

    pub fn respond(self, resp: T) {
        match self {
            OnResponse::Unary(cb) => cb(resp),
            OnResponse::Streaming(cb) => cb(box stream::once(Ok(resp)), None),
        }
    }

    pub fn respond_stream(self, s: CopStream<T>, executor: FuturePool<C>) {
        match self {
            OnResponse::Streaming(cb) => cb(s, Some(executor)),
            OnResponse::Unary(_) => unreachable!(),
        }
    }
}

impl<T, C: futurepool::Context + 'static> Debug for OnResponse<T, C> {
    fn fmt(&self, f: &mut Formatter) -> FormatResult {
        match *self {
            OnResponse::Unary(_) => write!(f, "Unary"),
            OnResponse::Streaming(_) => write!(f, "Streaming"),
        }
    }
}
