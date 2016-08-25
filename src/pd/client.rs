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

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use util::codec::rpc;
use util::make_std_tcp_conn;

use rand::{self, Rng};
use hyper::{Url, Client};
use rustc_serialize::json::Json;

use kvproto::pdpb::{Request, Response};
use kvproto::msgpb::{Message, MessageType};

use super::Result;

const MAX_PD_SEND_RETRY_COUNT: usize = 100;
const SOCKET_READ_TIMEOUT: u64 = 3;
const SOCKET_WRITE_TIMEOUT: u64 = 3;

const PD_RPC_PREFIX: &'static str = "/pd/rpc";
const PD_API_PREFIX: &'static str = "/pd/api/v1";

#[derive(Debug)]
struct RpcClientCore {
    endpoints: String,
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
        let url = box_try!(Url::parse(a));
        let host = url.host_str();
        let port = url.port_or_known_default();
        if host.is_none() || port.is_none() {
            return Err(box_err!("invalid url {:?}", url));
        }
        hosts.push(format!("{}:{}", host.unwrap(), port.unwrap()));
    }
    Ok(hosts)
}

fn parse_pd_members(json: &Json) -> Result<Vec<String>> {
    let mut hosts = Vec::new();

    if let Some(&Json::Array(ref members)) = json.find("members") {
        for m in members {
            if let Some(&Json::Array(ref client_urls)) = m.find("client-urls") {
                for url in client_urls {
                    if let &Json::String(ref u) = url {
                        hosts.extend(try!(parse_urls(u)))
                    }
                }
            }
        }
    }

    if hosts.len() > 0 {
        Ok(hosts)
    } else {
        Err(box_err!("invalid pd members object"))
    }
}

fn get_pd_members(endpoints: &str) -> Result<Vec<String>> {
    let mut c = Client::new();
    c.set_read_timeout(Some(Duration::from_secs(SOCKET_READ_TIMEOUT)));
    c.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT)));

    for host in endpoints.split(',') {
        // Request for pd members.
        let url = format!("http://{}/{}/members", host, PD_API_PREFIX);
        let req = c.get(&url);
        let mut resp = match req.send() {
            Ok(resp) => resp,
            Err(e) => {
                warn!("request pd members failed: {:?}", e);
                continue;
            }
        };

        // Read response body.
        let mut data = String::new();
        if let Err(e) = resp.read_to_string(&mut data) {
            warn!("read pd members failed: {:?}", e);
            continue;
        }

        // Parse response body as JSON.
        let json = match Json::from_str(&data) {
            Ok(json) => json,
            Err(e) => {
                warn!("parse pd members failed: {:?}", e);
                continue;
            }
        };

        return parse_pd_members(&json);
    }

    Err(box_err!("get pd members failed"))
}

fn rpc_connect(hosts: &[String]) -> Result<TcpStream> {
    // Randomize hosts.
    let mut random_hosts = hosts.to_owned();
    rand::thread_rng().shuffle(&mut random_hosts);

    for host in &random_hosts {
        let mut stream = match make_std_tcp_conn(host.as_str()) {
            Ok(stream) => stream,
            Err(_) => continue,
        };

        // Send a HTTP header to tell PD to hijack this connection for RPC.
        let header_str = format!("GET {} HTTP/1.0\r\n\r\n", PD_RPC_PREFIX);
        let header = header_str.as_bytes();
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

    Err(box_err!("failed connect to {:?}", hosts))
}

impl RpcClientCore {
    fn new(endpoints: &str) -> RpcClientCore {
        RpcClientCore {
            endpoints: endpoints.into(),
            stream: None,
        }
    }

    fn try_connect(&mut self) -> Result<()> {
        let hosts = try!(get_pd_members(&self.endpoints));
        let stream = try!(rpc_connect(&hosts));
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
    pub fn new(endpoints: &str, cluster_id: u64) -> Result<RpcClient> {
        Ok(RpcClient {
            msg_id: AtomicUsize::new(0),
            core: Mutex::new(RpcClientCore::new(endpoints)),
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

#[cfg(test)]
mod test {
    use std::env;

    use rustc_serialize::json::Json;

    use super::parse_urls;
    use super::parse_pd_members;
    use super::get_pd_members;

    #[test]
    fn test_parse_urls() {
        assert!(parse_urls("example.com").is_err());

        // test default port.
        let hosts = parse_urls("http://example.com").unwrap();
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0], "example.com:80");
        let hosts = parse_urls("https://example.com").unwrap();
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0], "example.com:443");

        // test non-default port.
        let hosts = parse_urls("http://example.com:1234").unwrap();
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0], "example.com:1234");
        let hosts = parse_urls("https://example.com:4321").unwrap();
        assert_eq!(hosts.len(), 1);
        assert_eq!(hosts[0], "example.com:4321");

        // test multiple urls.
        let hosts = parse_urls("http://127.0.0.1:8080,http://example.com:2379,https://example.com")
            .unwrap();
        assert_eq!(hosts.len(), 3);
        assert_eq!(hosts[0], "127.0.0.1:8080");
        assert_eq!(hosts[1], "example.com:2379");
        assert_eq!(hosts[2], "example.com:443");
    }

    #[test]
    fn test_parse_pd_members() {
        let valid_case = r#"{
            "members": [{
                "name": "pd1",
                "peer-urls": ["http://localhost:1001"],
                "client-urls": ["http://localhost:1001"]
            }, {
                "name": "pd2",
                "peer-urls": ["http://localhost:2001"],
                "client-urls": ["http://localhost:2001","http://localhost:2002"]
            }, {
                "name": "pd3",
                "peer-urls": ["http://localhost:3001"],
                "client-urls": ["http://localhost:3001","http://localhost:3002","http://localhost:3003"]
            }]}"#;

        let json = Json::from_str(valid_case).unwrap();
        let hosts = parse_pd_members(&json).unwrap();
        assert_eq!(hosts.len(), 6);
        assert_eq!(hosts[0], "localhost:1001");
        assert_eq!(hosts[1], "localhost:2001");
        assert_eq!(hosts[2], "localhost:2002");
        assert_eq!(hosts[3], "localhost:3001");
        assert_eq!(hosts[4], "localhost:3002");
        assert_eq!(hosts[5], "localhost:3003");

        let invalid_cases = vec![
            r#"{}"#,
            r#"{"example": ""}"#,
            r#"{"members": ""}"#,
            r#"{"members": []}"#,
            r#"{"members": [{}]}"#,
            r#"{"members": [{"client-urls": []}]}"#,
        ];

        for s in &invalid_cases {
            let json = Json::from_str(s).unwrap();
            assert!(parse_pd_members(&json).is_err());
        }
    }

    #[test]
    fn test_get_pd_members() {
        // We need to set all members in PD_ENDPOINTS to pass this test.
        let endpoints = match env::var("PD_ENDPOINTS") {
            Ok(v) => v,
            Err(_) => return,
        };

        let mut members = get_pd_members(&endpoints).unwrap();
        members.sort();
        println!("get pd members: {:?}", members);

        let mut hosts: Vec<_> = endpoints.split(',').collect();
        hosts.sort();

        assert_eq!(hosts.len(), members.len());
        for i in 0..hosts.len() {
            assert_eq!(hosts[i], members[i]);
        }
    }
}
