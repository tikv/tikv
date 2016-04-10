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
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use util::codec::rpc;
use util::make_std_tcp_conn;
use protobuf::{self, MessageStatic};
use super::Result;

const MAX_PD_SEND_RETRY_COUNT: usize = 100;

pub trait TRpcClient: Sync + Send {
    fn send<M, P>(&self, msg_id: u64, message: &M) -> Result<P>
        where M: protobuf::Message,
              P: protobuf::Message + MessageStatic;
    fn post<M: protobuf::Message + ?Sized>(&self, msg_id: u64, message: &M) -> Result<()>;
}

#[derive(Debug)]
struct RpcClientCore {
    addrs: Vec<String>,
    // Try to connect pd with round-robin.
    next_index: usize,
    stream: Option<TcpStream>,
}

fn send_msg<M: protobuf::Message + ?Sized>(stream: &mut TcpStream,
                                           msg_id: u64,
                                           message: &M)
                                           -> Result<(u64, Vec<u8>)> {
    try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));
    try!(rpc::encode_msg(stream, msg_id, message));

    try!(stream.set_read_timeout(Some(Duration::from_secs(SOCKET_READ_TIMEOUT))));
    let (id, data) = try!(rpc::decode_data(stream));
    Ok((id, data))
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

    fn post_msg<M: protobuf::Message + ?Sized>(&mut self,
                                               msg_id: u64,
                                               message: &M)
                                               -> Result<Vec<u8>> {
        // If we post failed, we should retry.
        for _ in 0..MAX_PD_SEND_RETRY_COUNT {
            // If no stream, try connect first.
            if self.stream.is_none() {
                if let Err(e) = self.try_connect() {
                    warn!("connect pd failed {:?}", e);
                    continue;
                }
            }

            let mut stream = self.stream.take().unwrap();
            // We may send message to a not leader pd, retry.

            let (id, data) = match send_msg(&mut stream, msg_id, message) {
                Err(e) => {
                    warn!("send message to pd failed {:?}", e);
                    // TODO: figure out a better way to do backoff
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }
                Ok((id, data)) => (id, data),
            };

            if id != msg_id {
                return Err(box_err!("pd response msg_id not match, want {}, got {}", msg_id, id));
            }

            self.stream = Some(stream);
            return Ok(data);
        }

        Err(box_err!("post message to pd failed"))
    }
}

#[derive(Debug)]
pub struct RpcClient {
    core: Mutex<RpcClientCore>,
}

const SOCKET_READ_TIMEOUT: u64 = 3;
const SOCKET_WRITE_TIMEOUT: u64 = 3;

impl RpcClient {
    pub fn new(dsn: &str) -> Result<RpcClient> {
        Ok(RpcClient { core: Mutex::new(RpcClientCore::new(dsn)) })
    }
}

impl TRpcClient for RpcClient {
    fn send<M, P>(&self, msg_id: u64, message: &M) -> Result<P>
        where M: protobuf::Message,
              P: protobuf::Message + MessageStatic
    {
        let mut resp = P::new();
        let data = try!(self.core.lock().unwrap().post_msg(msg_id, message));
        try!(rpc::decode_body(&data, &mut resp));
        Ok(resp)
    }

    fn post<M: protobuf::Message + ?Sized>(&self, msg_id: u64, message: &M) -> Result<()> {
        let _ = try!(self.core.lock().unwrap().post_msg(msg_id, message));
        Ok(())
    }
}

#[derive(Debug)]
enum Msg {
    Rpc(u64, Box<protobuf::Message + Send>),
    Quit,
}

pub struct Client<C: TRpcClient + 'static> {
    msg_id: AtomicUsize,
    rpc_client: Arc<C>,
    tx: Mutex<Sender<Msg>>,
    handle: Option<JoinHandle<()>>,
}

impl<C: TRpcClient + 'static> Client<C> {
    pub fn new(rpc_client: C) -> Result<Client<C>> {
        let shared = Arc::new(rpc_client);
        let (tx, rx) = mpsc::channel();
        let handle = {
            let rpc_client = shared.clone();
            try!(thread::Builder::new()
                     .name("pd-client background worker".to_owned())
                     .spawn(move || {
                         info!("pd-client background worker starting");
                         while let Msg::Rpc(msg_id, msg) = rx.recv().unwrap() {
                             if let Err(e) = rpc_client.post(msg_id, msg.as_ref()) {
                                 error!("pd post error: {}", e);
                             } else {
                                 debug!("pd post done: {}", msg_id);
                             }
                         }
                         info!("pd-client background worker closing");
                     }))
        };
        Ok(Client {
            msg_id: AtomicUsize::new(0),
            rpc_client: shared.clone(),
            tx: Mutex::new(tx),
            handle: Some(handle),
        })
    }

    fn close(&mut self) {
        if let Err(e) = self.clone_tx().send(Msg::Quit) {
            error!("failed to send quit message to pd-client background worker: {}",
                   e);
        }
        let handle = self.handle.take().unwrap();
        if handle.join().is_err() {
            error!("failed to wait pd-client background worker quit");
        }
        info!("pd-client closed.");
    }

    pub fn send<M, P>(&self, message: &M) -> Result<P>
        where M: protobuf::Message,
              P: protobuf::Message + MessageStatic
    {
        let id = self.alloc_msg_id();
        self.rpc_client.send(id, message)
    }

    pub fn post<M: protobuf::Message + Send>(&self, message: M) -> Result<()> {
        let id = self.alloc_msg_id();
        if let Err(e) = self.clone_tx().send(Msg::Rpc(id, box message)) {
            return Err(box_err!(format!("SendError: {:?}", e)));
        }
        Ok(())
    }

    fn alloc_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) as u64
    }

    fn clone_tx(&self) -> Sender<Msg> {
        let tx = self.tx.lock().unwrap();
        tx.clone()
    }
}

impl<C: TRpcClient + 'static> Drop for Client<C> {
    fn drop(&mut self) {
        self.close()
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::thread;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};

    use super::super::{Result, PdClient};
    use super::*;
    use protobuf;
    use bytes::ByteBuf;
    use util::codec::rpc;
    use kvproto::{metapb, pdpb};
    use rand;
    use util::make_std_tcp_conn;

    struct MockRpcClient;

    impl MockRpcClient {
        fn reply(&self, req: &pdpb::Request) -> pdpb::Response {
            assert!(req.has_header());
            let header = req.get_header();
            let (uuid, cluster_id) = (header.get_uuid(), header.get_cluster_id());

            let mut resp = pdpb::Response::new();
            let mut header = pdpb::ResponseHeader::new();
            header.set_uuid(uuid.to_vec());
            header.set_cluster_id(cluster_id);

            match req.get_cmd_type() {
                pdpb::CommandType::AllocId => {
                    let mut alloc = pdpb::AllocIdResponse::new();
                    alloc.set_id(42);
                    resp.set_alloc_id(alloc);
                }
                pdpb::CommandType::PutMeta => {
                    let meta = req.get_put_meta();
                    if meta.get_meta_type() == pdpb::MetaType::StoreType {
                        let store = meta.get_store();
                        assert_eq!(store.get_id(), 233);
                        assert_eq!(store.get_address(), "localhost");
                    }
                }
                _ => {
                    let mut err = pdpb::Error::new();
                    err.set_message(String::from("I've got a bad feeling about this"));
                    header.set_error(err);
                }
            }

            resp.set_header(header);
            resp.set_cmd_type(req.get_cmd_type());
            resp
        }
    }

    impl TRpcClient for MockRpcClient {
        fn send<M, P>(&self, msg_id: u64, message: &M) -> Result<P>
            where M: protobuf::Message,
                  P: protobuf::Message + Default
        {
            let mut buf = ByteBuf::mut_with_capacity(128);
            rpc::encode_msg(&mut buf, msg_id, message).unwrap();
            let mut req = pdpb::Request::new();
            rpc::decode_msg(&mut buf.flip(), &mut req).unwrap();

            let resp = self.reply(&req);
            let mut buf = ByteBuf::mut_with_capacity(128);
            rpc::encode_msg(&mut buf, msg_id, &resp).unwrap();
            let mut msg = P::default();
            rpc::decode_msg(&mut buf.flip(), &mut msg).unwrap();
            Ok(msg)
        }

        fn post<M: protobuf::Message + ?Sized>(&self, _: u64, _: &M) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_pd_client() {
        let mut client = Client::new(MockRpcClient).unwrap();
        assert_eq!(client.alloc_id().unwrap(), 42u64);
        assert!(client.is_cluster_bootstrapped(1).is_err());

        let mut store = metapb::Store::new();
        store.set_id(233);
        store.set_address("localhost".to_owned());
        assert!(client.put_store(1, store).is_ok());
    }

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

        for i in 0..10 {
            client.post(i as u64, &msg).unwrap();

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
