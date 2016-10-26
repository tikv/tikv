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

use std::thread;
use std::time::Duration;
use std::net::{TcpStream, SocketAddr};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::msgpb::{Message, MessageType};

use raftstore::Result;
use util::make_std_tcp_conn;
use util::codec::rpc;


const MAX_RAFT_RPC_SEND_RETRY_COUNT: u64 = 2;
const RAFT_RPC_RETRY_TIME_MILLIS: u64 = 50;
const SOCKET_READ_TIMEOUT: u64 = 3;
const SOCKET_WRITE_TIMEOUT: u64 = 3;


fn rpc_connect(address: SocketAddr) -> Result<TcpStream> {
    let stream = try!(make_std_tcp_conn(address));
    try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));
    Ok(stream)
}

fn send_message(stream: &mut TcpStream, msg_id: u64, message: &Message) -> Result<(u64, Message)> {
    try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));
    try!(rpc::encode_msg(stream, msg_id, message));

    try!(stream.set_read_timeout(Some(Duration::from_secs(SOCKET_READ_TIMEOUT))));
    let mut resp = Message::new();
    let id = try!(rpc::decode_msg(stream, &mut resp));

    Ok((id, resp))
}


pub trait Client {
    fn send(&self, msg: &Message) -> Result<Message>;

    fn send_cmd(&self, request: &RaftCmdRequest) -> Result<RaftCmdResponse> {
        let mut message = Message::new();
        message.set_msg_type(MessageType::Cmd);
        message.set_cmd_req(request.clone());

        let mut resp = try!(self.send(&message));
        if resp.get_msg_type() != MessageType::CmdResp {
            return Err(box_err!("invalid cmd response type {:?}", resp.get_msg_type()));
        }
        Ok((resp.take_cmd_resp()))
    }

    // TODO add convenient methods for other message types
}

#[derive(Debug)]
struct KVClientCore {
    address: SocketAddr,
    stream: Option<TcpStream>,
}

impl KVClientCore {
    pub fn new(address: SocketAddr) -> KVClientCore {
        KVClientCore {
            address: address,
            stream: None,
        }
    }

    fn try_connect(&mut self) -> Result<()> {
        let stream = try!(rpc_connect(self.address));
        self.stream = Some(stream);
        Ok(())
    }

    fn send(&mut self, msg_id: u64, msg: &Message) -> Result<Message> {
        for _ in 0..MAX_RAFT_RPC_SEND_RETRY_COUNT {
            if self.stream.is_none() {
                if let Err(e) = self.try_connect() {
                    warn!("connect tikv failed {:?}", e);
                    thread::sleep(Duration::from_millis(RAFT_RPC_RETRY_TIME_MILLIS));
                    continue;
                }
            }

            let mut stream = self.stream.take().unwrap();

            let (id, resp) = match send_message(&mut stream, msg_id, msg) {
                Err(e) => {
                    warn!("send message to tikv failed {:?}", e);
                    thread::sleep(Duration::from_millis(RAFT_RPC_RETRY_TIME_MILLIS));
                    continue;
                }
                Ok((id, resp)) => (id, resp),
            };

            if id != msg_id {
                return Err(box_err!("tikv response msg_id not match, want {}, got {}",
                                    msg_id,
                                    id));
            }

            self.stream = Some(stream);

            return Ok(resp);
        }
        Err(box_err!("send message to tikv failed, address: {}", self.address))
    }
}

pub struct KVClient {
    msg_id: AtomicUsize,
    core: Mutex<KVClientCore>,
}

impl KVClient {
    pub fn new(address: SocketAddr) -> KVClient {
        KVClient {
            msg_id: AtomicUsize::new(0),
            core: Mutex::new(KVClientCore::new(address)),
        }
    }


    pub fn alloc_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) as u64
    }
}

impl Client for KVClient {
    fn send(&self, req: &Message) -> Result<Message> {
        let msg_id = self.alloc_msg_id();
        let resp = try!(self.core.lock().unwrap().send(msg_id, req));
        Ok(resp)
    }
}
