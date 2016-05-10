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

use std::net::TcpStream;
use std::time::Duration;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use util::codec::rpc;
use util::make_std_tcp_conn;

use kvproto::pdpb::{Request, Response};

use super::Result;

const MAX_PD_SEND_RETRY_COUNT: usize = 100;
const SOCKET_READ_TIMEOUT: u64 = 3;
const SOCKET_WRITE_TIMEOUT: u64 = 3;

#[derive(Debug)]
struct RpcClientCore {
    addrs: Vec<String>,
    // Try to connect pd with round-robin.
    next_index: usize,
    stream: Option<TcpStream>,
}

fn send_msg(stream: &mut TcpStream, msg_id: u64, message: &Request) -> Result<(u64, Response)> {
    try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));
    try!(rpc::encode_msg(stream, msg_id, message));

    try!(stream.set_read_timeout(Some(Duration::from_secs(SOCKET_READ_TIMEOUT))));
    let mut resp = Response::new();
    let id = try!(rpc::decode_msg(stream, &mut resp));
    Ok((id, resp))
}

impl RpcClientCore {
    fn new(dsn: &str) -> RpcClientCore {
        let addrs: Vec<String> = dsn.split(',').map(From::from).collect();
        RpcClientCore {
            addrs: addrs,
            next_index: 0,
            stream: None,
        }
    }

    fn try_connect(&mut self) -> Result<()> {
        let index = self.next_index;
        self.next_index = (self.next_index + 1) % self.addrs.len();

        let addr = self.addrs.get(index).unwrap();
        let stream = try!(make_std_tcp_conn(&**addr));
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
}

impl RpcClient {
    pub fn new(dsn: &str) -> Result<RpcClient> {
        Ok(RpcClient {
            msg_id: AtomicUsize::new(0),
            core: Mutex::new(RpcClientCore::new(dsn)),
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
mod tests {
    use std::net::TcpListener;
    use std::thread;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};

    use super::*;
    use util::codec::rpc;
    use kvproto::pdpb;
    use rand;
    use util::make_std_tcp_conn;

    fn start_pd_server(index: usize,
                       leader_index: Arc<AtomicUsize>,
                       quit: Arc<AtomicBool>)
                       -> (thread::JoinHandle<()>, String) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();

        let addr = format!("{}", l.local_addr().unwrap());

        let h = thread::spawn(move || {
            loop {
                let (mut stream, _) = l.accept().unwrap();

                if quit.load(Ordering::SeqCst) {
                    return;
                }

                stream.set_nodelay(true).unwrap();

                let leader = leader_index.load(Ordering::SeqCst);
                if leader != index {
                    continue;
                }

                let (id, data) = rpc::decode_data(&mut stream).unwrap();
                rpc::encode_data(&mut stream, id, &data).unwrap();
            }
        });

        (h, addr)
    }

    #[test]
    fn test_rpc_client() {
        let leader = Arc::new(AtomicUsize::new(0));
        let quit = Arc::new(AtomicBool::new(false));
        let mut handlers = vec![];
        let mut addrs = vec![];

        let count = 3;
        for i in 0..count {
            let (h, addr) = start_pd_server(i, leader.clone(), quit.clone());
            handlers.push(h);
            addrs.push(addr);
        }

        let dsn = addrs.join(",");

        let msg = pdpb::Request::new();
        let client = RpcClient::new(&dsn).unwrap();

        for _ in 0..10 {
            client.send(&msg).unwrap();

            // select a leader randomly
            leader.store(rand::random::<usize>() % 3, Ordering::SeqCst);
        }

        quit.store(true, Ordering::SeqCst);

        // connect the server so that we can close the thread.
        for addr in addrs {
            make_std_tcp_conn(&*addr).unwrap();
        }

        for h in handlers {
            h.join().unwrap();
        }
    }
}
