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

use std::net::SocketAddr;
use std::fmt::{self, Formatter, Display, Debug};
use std::boxed::{Box, FnBox};
use std::io::Write;

use mio::Token;

use util::codec::rpc;
use kvproto::eraftpb::MessageType as RaftMessageType;
use kvproto::raft_serverpb::RaftMessage;
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
pub use self::server::{ServerChannel, Server, create_event_loop};
pub use self::transport::{ServerTransport, ServerRaftStoreRouter, MockRaftStoreRouter};
pub use self::node::{Node, create_raft_storage};
pub use self::resolve::{StoreAddrResolver, PdStoreAddrResolver};
pub use self::raft_client::RaftClient;

pub type OnResponse = Box<FnBox(Response) + Send>;

pub struct ConnData {
    msg_id: u64,
    msg: RaftMessage,
}

impl ConnData {
    pub fn new(msg_id: u64, msg: RaftMessage) -> ConnData {
        ConnData {
            msg_id: msg_id,
            msg: msg,
        }
    }

    pub fn is_snapshot(&self) -> bool {
        self.msg.get_message().get_msg_type() == RaftMessageType::MsgSnapshot
    }

    pub fn encode_to<T: Write>(&self, w: &mut T) -> Result<()> {
        try!(rpc::encode_msg(w, self.msg_id, &self.msg));
        Ok(())
    }
}

impl Display for ConnData {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let from_peer = self.msg.get_from_peer();
        let to_peer = self.msg.get_to_peer();
        let msg_type = self.msg.get_message().get_msg_type();
        write!(f,
               "[{}] raft {:?} from {:?} to {:?}",
               self.msg_id,
               msg_type,
               from_peer.get_id(),
               to_peer.get_id())
    }
}

impl Debug for ConnData {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug)]
pub enum Msg {
    // Quit event loop.
    Quit,
    // Send data to remote store.
    SendStore { store_id: u64, data: ConnData },
    // Resolve store address result.
    ResolveResult {
        store_id: u64,
        sock_addr: Result<SocketAddr>,
        data: ConnData,
    },
    CloseConn { token: Token },
}
