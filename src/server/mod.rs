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

use std::boxed::{Box, FnBox};

use kvproto::coprocessor::Response;
mod metrics;
mod grpc_service;
mod raft_client;

pub mod config;
pub mod errors;
pub mod server;
pub mod coprocessor;
pub mod transport;
pub mod node;
pub mod resolve;
pub mod snap;

pub use self::config::{Config, DEFAULT_LISTENING_ADDR, DEFAULT_CLUSTER_ID};
pub use self::errors::{Result, Error};
pub use self::server::Server;
pub use self::transport::{ServerTransport, ServerRaftStoreRouter};
pub use self::node::{Node, create_raft_storage};
pub use self::resolve::{StoreAddrResolver, PdStoreAddrResolver};
pub use self::raft_client::RaftClient;

pub type OnResponse = Box<FnBox(Response) + Send>;
