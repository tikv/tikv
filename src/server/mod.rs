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

use std::boxed::FnBox;
use std::ops::FnMut;
use kvproto::coprocessor::Response;
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

pub use self::config::{Config, DEFAULT_CLUSTER_ID, DEFAULT_LISTENING_ADDR};
pub use self::errors::{Error, Result};
pub use self::server::Server;
pub use self::transport::{ServerRaftStoreRouter, ServerTransport};
pub use self::node::{create_raft_storage, Node};
pub use self::resolve::{PdStoreAddrResolver, StoreAddrResolver};
pub use self::raft_client::RaftClient;

pub enum OnResponse {
    Unary(Box<FnBox(Response) + Send>),
    Stream(Box<FnMut(Option<Response>) + Send>),
}

impl OnResponse {
    pub fn on_finish(mut self, res: Response) {
        if self.is_stream() {
            self.finish_stream(Some(res))
        } else {
            self.finish_unary(res)
        }
    }

    pub fn resp(&mut self, res: Response) {
        match *self {
            OnResponse::Unary(_) => {}
            OnResponse::Stream(ref mut cb) => cb(Some(res)),
        }
    }

    pub fn finish_unary(self, res: Response) {
        if let OnResponse::Unary(cb) = self {
            cb(res)
        }
    }

    pub fn finish_stream(&mut self, res: Option<Response>) {
        if let OnResponse::Stream(ref mut cb) = *self {
            if res.is_some() {
                cb(res);
            }
            cb(None)
        }
    }

    pub fn is_stream(&self) -> bool {
        match *self {
            OnResponse::Unary(_) => false,
            OnResponse::Stream(_) => true,
        }
    }
}
