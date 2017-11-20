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

pub mod config;
pub mod errors;
pub mod server;
pub mod transport;
pub mod node;
pub mod resolve;
pub mod snap;
pub mod debug;

use std::fmt;

use grpc::{ServerStreamingSink, UnarySink};
#[cfg(test)]
use futures::sync::mpsc::Sender;

use kvproto::coprocessor::Response;

pub use self::config::{Config, DEFAULT_CLUSTER_ID, DEFAULT_LISTENING_ADDR};
pub use self::errors::{Error, Result};
pub use self::server::Server;
pub use self::transport::{ServerRaftStoreRouter, ServerTransport};
pub use self::node::{create_raft_storage, Node};
pub use self::resolve::{PdStoreAddrResolver, StoreAddrResolver};
pub use self::raft_client::RaftClient;

pub enum CopResponseSink {
    Unary(UnarySink<Response>),
    Streaming(ServerStreamingSink<Response>),
    #[cfg(test)]
    TestChannel(Sender<Response>),
}

impl CopResponseSink {
    pub fn is_streaming(&self) -> bool {
        match *self {
            CopResponseSink::Unary(_) => false,
            _ => true,
        }
    }
}

impl From<UnarySink<Response>> for CopResponseSink {
    fn from(s: UnarySink<Response>) -> CopResponseSink {
        CopResponseSink::Unary(s)
    }
}

impl From<ServerStreamingSink<Response>> for CopResponseSink {
    fn from(s: ServerStreamingSink<Response>) -> CopResponseSink {
        CopResponseSink::Streaming(s)
    }
}

impl From<CopResponseSink> for UnarySink<Response> {
    fn from(s: CopResponseSink) -> UnarySink<Response> {
        match s {
            CopResponseSink::Unary(sink) => sink,
            _ => unreachable!(),
        }
    }
}

impl From<CopResponseSink> for ServerStreamingSink<Response> {
    fn from(s: CopResponseSink) -> ServerStreamingSink<Response> {
        match s {
            CopResponseSink::Streaming(sink) => sink,
            _ => unreachable!(),
        }
    }
}

impl fmt::Debug for CopResponseSink {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CopResponseSink::Unary(_) => write!(f, "Grpc UnarySink"),
            CopResponseSink::Streaming(_) => write!(f, "Grpc ServerStreamingSink"),
            #[cfg(test)]
            CopResponseSink::TestChannel(_) => write!(f, "Test Sender"),
        }
    }
}
