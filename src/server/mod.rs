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
use std::boxed::FnBox;

use grpc::Error as GrpcError;
use futures::{stream, Stream};

use kvproto::coprocessor::Response;

pub use self::config::{Config, DEFAULT_CLUSTER_ID, DEFAULT_LISTENING_ADDR};
pub use self::errors::{Error, Result};
pub use self::server::Server;
pub use self::transport::{ServerRaftStoreRouter, ServerTransport};
pub use self::node::{create_raft_storage, Node};
pub use self::resolve::{PdStoreAddrResolver, StoreAddrResolver};
pub use self::raft_client::RaftClient;

pub type ResponseStream = Box<Stream<Item = Response, Error = GrpcError> + Send>;

pub enum OnResponse {
    Unary(Box<FnBox(Response) + Send>),
    Streaming(Box<FnBox(ResponseStream) + Send>),
}

impl OnResponse {
    pub fn is_streaming(&self) -> bool {
        match *self {
            OnResponse::Unary(_) => false,
            OnResponse::Streaming(_) => true,
        }
    }

    pub fn respond(self, resp: Response) {
        match self {
            OnResponse::Unary(cb) => cb(resp),
            OnResponse::Streaming(cb) => {
                let s = box stream::once::<_, GrpcError>(Ok(resp));
                cb(s as ResponseStream);
            }
        }
    }

    pub fn respond_stream(self, s: ResponseStream) {
        match self {
            OnResponse::Streaming(cb) => cb(s),
            OnResponse::Unary(_) => unreachable!(),
        }
    }
}

impl fmt::Debug for OnResponse {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OnResponse::Unary(_) => write!(f, "Unary"),
            OnResponse::Streaming(_) => write!(f, "Streaming"),
        }
    }
}
