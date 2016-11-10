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

use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use util::codec::rpc;
use util::make_std_tcp_conn;

use rand::{self, Rng};

use kvproto::pdpb::{Request, Response};
use kvproto::msgpb::{Message, MessageType};

use super::{Result, PdClient};
use super::metrics::*;

const MAX_PD_SEND_RETRY_COUNT: usize = 100;
const SOCKET_READ_TIMEOUT: u64 = 3;
const SOCKET_WRITE_TIMEOUT: u64 = 3;

const PD_RPC_PREFIX: &'static str = "/pd/rpc";

#[derive(Debug)]
struct RpcClientCore {
    endpoints: String,
    stream: Option<TcpStream>,
}

fn send_msg(stream: &mut TcpStream, msg_id: u64, message: &Request) -> Result<(u64, Response)> {
    let timer = PD_SEND_MSG_HISTOGRAM.start_timer();

    let mut req = Message::new();
    req.set_msg_type(MessageType::PdReq);
    // TODO: optimize clone later in HTTP refactor.
    req.set_pd_req(message.clone());

    try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));
    try!(rpc::encode_msg(stream, msg_id, &req));

    try!(stream.set_read_timeout(Some(Duration::from_secs(SOCKET_READ_TIMEOUT))));
    let mut resp = Message::new();
    let id = try!(rpc::decode_msg(stream, &mut resp));
    if resp.get_msg_type() != MessageType::PdResp {
        return Err(box_err!("invalid pd response type {:?}", resp.get_msg_type()));
    }
    timer.observe_duration();

    Ok((id, resp.take_pd_resp()))
}

fn rpc_connect(endpoints: &str) -> Result<TcpStream> {
    // Randomize hosts.
    let mut hosts: Vec<String> = endpoints.split(',').map(|s| s.into()).collect();
    rand::thread_rng().shuffle(&mut hosts);

    for host in &hosts {
        let mut stream = match make_std_tcp_conn(host.as_str()) {
            Ok(stream) => stream,
            Err(_) => continue,
        };
        try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));

        // Send a HTTP header to tell PD to hijack this connection for RPC.
        let header_str = format!("GET {} HTTP/1.0\r\n\r\n", PD_RPC_PREFIX);
        let header = header_str.as_bytes();
        match stream.write_all(header) {
            Ok(_) => return Ok(stream),
            Err(_) => continue,
        }
    }

    Err(box_err!("failed to connect to {:?}", hosts))
}

impl RpcClientCore {
    fn new(endpoints: &str) -> RpcClientCore {
        RpcClientCore {
            endpoints: endpoints.into(),
            stream: None,
        }
    }

    fn try_connect(&mut self) -> Result<()> {
        let stream = try!(rpc_connect(&self.endpoints));
        self.stream = Some(stream);
        Ok(())
    }

    fn send(&mut self, msg_id: u64, req: &Request) -> Result<Response> {
        // If we post failed, we should retry.
        for _ in 0..MAX_PD_SEND_RETRY_COUNT {
            // If no stream, try connect first.
            if self.stream.is_none() {
                if let Err(_) = self.try_connect() {
                    // TODO: figure out a better way to do backoff
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }
            }

            let mut stream = self.stream.take().unwrap();
            // We may send message to a not leader pd, retry.

            let (id, resp) = match send_msg(&mut stream, msg_id, req) {
                Err(e) => {
                    warn!("send message to pd failed {:?}", e);
                    // TODO: figure out a better way to do backoff
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }
                Ok((id, resp)) => (id, resp),
            };

            if id != msg_id {
                return Err(box_err!("pd response msg_id not match, want {}, got {}", msg_id, id));
            }

            self.stream = Some(stream);

            return Ok(resp);
        }

        Err(box_err!("send message to pd failed"))
    }
}

#[derive(Debug)]
pub struct RpcClient {
    msg_id: AtomicUsize,
    core: Mutex<RpcClientCore>,
    pub cluster_id: u64,
}

impl RpcClient {
    pub fn new(endpoints: &str) -> Result<RpcClient> {
        let mut client = RpcClient {
            msg_id: AtomicUsize::new(0),
            core: Mutex::new(RpcClientCore::new(endpoints)),
            cluster_id: 0,
        };

        for _ in 0..MAX_PD_SEND_RETRY_COUNT {
            match client.get_cluster_id() {
                Ok(id) => {
                    client.cluster_id = id;
                    return Ok(client);
                }
                Err(e) => {
                    warn!("failed to get cluster id from pd: {:?}", e);
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
        Err(box_err!("failed to get cluster id from pd"))
    }

    pub fn send(&self, req: &Request) -> Result<Response> {
        let msg_id = self.alloc_msg_id();
        let resp = try!(self.core.lock().unwrap().send(msg_id, req));
        Ok(resp)
    }

    fn alloc_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) as u64
    }
}
