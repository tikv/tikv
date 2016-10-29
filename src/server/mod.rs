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

use kvproto::msgpb::{self, MessageType};
use kvproto::eraftpb::MessageType as RaftMessageType;

use util::codec::rpc;

mod conn;
mod kv;
mod metrics;

pub mod config;
pub mod errors;
pub mod server;
pub mod coprocessor;
pub mod transport;
pub mod client;
pub mod node;
pub mod resolve;
pub mod snap;

pub use self::config::{Config, DEFAULT_LISTENING_ADDR, DEFAULT_CLUSTER_ID};
pub use self::errors::{Result, Error};
pub use self::server::{Server, create_event_loop, bind};
pub use self::transport::{ServerTransport, ServerRaftStoreRouter, MockRaftStoreRouter};
pub use self::node::{Node, create_raft_storage};
pub use self::resolve::{StoreAddrResolver, PdStoreAddrResolver, MockStoreAddrResolver};
pub use self::client::{TikvRpcWorker, SimpleClient};

pub type OnResponse = Box<FnBox(msgpb::Message) + Send>;

pub struct ConnData {
    msg_id: u64,
    msg: msgpb::Message,
}

impl ConnData {
    pub fn new(msg_id: u64, msg: msgpb::Message) -> ConnData {
        ConnData {
            msg_id: msg_id,
            msg: msg,
        }
    }

    pub fn is_snapshot(&self) -> bool {
        if !self.msg.has_raft() {
            return false;
        }

        self.msg.get_raft().get_message().get_msg_type() == RaftMessageType::MsgSnapshot
    }

    pub fn encode_to<T: Write>(&self, w: &mut T) -> Result<()> {
        try!(rpc::encode_msg(w, self.msg_id, &self.msg));
        Ok(())
    }
}

impl Display for ConnData {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.msg.get_msg_type() {
            MessageType::Cmd => write!(f, "[{}] raft command request", self.msg_id),
            MessageType::CmdResp => write!(f, "[{}] raft command response", self.msg_id),
            MessageType::Raft => {
                let from_peer = self.msg.get_raft().get_from_peer();
                let to_peer = self.msg.get_raft().get_to_peer();
                let msg_type = self.msg.get_raft().get_message().get_msg_type();
                write!(f,
                       "[{}] raft {:?} from {:?} to {:?}",
                       self.msg_id,
                       msg_type,
                       from_peer.get_id(),
                       to_peer.get_id())
            }
            MessageType::KvReq => {
                write!(f,
                       "[{}] kv command request {:?}",
                       self.msg_id,
                       self.msg.get_kv_req().get_field_type())
            }
            MessageType::KvResp => {
                write!(f,
                       "[{}] kv command resposne {:?}",
                       self.msg_id,
                       self.msg.get_kv_resp().get_field_type())
            }
            MessageType::CopReq => write!(f, "[{}] coprocessor request", self.msg_id),
            MessageType::CopResp => write!(f, "[{}] coprocessor response", self.msg_id),
            MessageType::PdReq => write!(f, "[{}] pd request", self.msg_id),
            MessageType::PdResp => write!(f, "[{}] pd response", self.msg_id),
            MessageType::None => write!(f, "[{}] invalid message", self.msg_id),
        }
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
    // Write data to connection.
    WriteData { token: Token, data: ConnData },
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
