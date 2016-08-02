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
use std::time::{Duration, Instant};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use util::codec::rpc;
use util::make_std_tcp_conn;

use hyper::Url;

use kvproto::pdpb::{Request, Response};
use kvproto::msgpb::{Message, MessageType};

use super::Result;
use super::etcd::EtcdPdClient;

const MAX_PD_SEND_RETRY_COUNT: usize = 100;
const SOCKET_READ_TIMEOUT: u64 = 3;
const SOCKET_WRITE_TIMEOUT: u64 = 3;

#[derive(Debug)]
struct RpcClientCore {
    client: EtcdPdClient,
    stream: Option<TcpStream>,
}

fn send_msg(stream: &mut TcpStream, msg_id: u64, message: &Request) -> Result<(u64, Response)> {
    let ts = Instant::now();
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
    metric_time!("pd.send_msg", ts.elapsed());
    Ok((id, resp.take_pd_resp()))
}

fn parse_urls(addr: &str) -> Result<Vec<String>> {
    let mut hosts = Vec::new();
    for a in addr.split(',') {
        let url = match Url::parse(a) {
            Ok(url) => url,
            Err(e) => return Err(box_err!("parse url failed: {:?}", e)),
        };
        if url.host_str().is_none() || url.port().is_none() {
            return Err(box_err!("invalid url {:?}", url));
        }
        hosts.push(format!("{}:{}", url.host_str().unwrap(), url.port().unwrap()));
    }
    Ok(hosts)
}

fn rpc_connect(addr: &str) -> Result<TcpStream> {
    let hosts = try!(parse_urls(addr));

    for host in &hosts {
        let mut stream = match make_std_tcp_conn(host.as_str()) {
            Ok(stream) => stream,
            Err(_) => continue,
        };

        // Send a HTTP header to tell PD to hijack this connection for RPC.
        let header = b"GET /pd/rpc HTTP/1.0\r\n\r\n";
        try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));
        match stream.write(header) {
            Ok(n) if n == header.len() => {
                return Ok(stream);
            }
            Ok(n) => {
                return Err(box_err!("write header failed: header len {} written len {}",
                                    header.len(),
                                    n));
            }
            Err(e) => {
                return Err(box_err!("write header failed: {:?}", e));
            }
        }
    }

    Err(box_err!("connect to {} failed", addr))
}

impl RpcClientCore {
    fn new(client: EtcdPdClient) -> RpcClientCore {
        RpcClientCore {
            client: client,
            stream: None,
        }
    }

    fn try_connect(&mut self) -> Result<()> {
        let addr = box_try!(self.client.get_leader_addr());
        info!("get pd leader {}", addr);

        let stream = try!(rpc_connect(&addr));
        self.stream = Some(stream);
        Ok(())
    }

    fn send(&mut self, msg_id: u64, req: &Request) -> Result<Response> {
        // If we post failed, we should retry.
        for _ in 0..MAX_PD_SEND_RETRY_COUNT {
            // If no stream, try connect first.
            if self.stream.is_none() {
                if let Err(e) = self.try_connect() {
                    warn!("connect pd failed {:?}", e);
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
    pub fn new(client: EtcdPdClient, cluster_id: u64) -> Result<RpcClient> {
        Ok(RpcClient {
            msg_id: AtomicUsize::new(0),
            core: Mutex::new(RpcClientCore::new(client)),
            cluster_id: cluster_id,
        })
    }

    pub fn send(&self, req: &Request) -> Result<Response> {
        let msg_id = self.alloc_msg_id();;
        let resp = try!(self.core.lock().unwrap().send(msg_id, req));
        Ok(resp)
    }

    fn alloc_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) as u64
    }
}
