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

use std::env;
use std::net::TcpListener;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::BTreeMap;
use std::io::Read;

use rustc_serialize::json::Json;
use protobuf::Message;

use kvproto::pdpb;
use kvproto::msgpb::{Message as PbMessage, MessageType};

use kvproto::pdpb::Leader;

use tikv::util::make_std_tcp_conn;
use tikv::util::codec::rpc;
use tikv::pd::RpcClient;
use tikv::pd::etcd::*;

fn put_leader_addr(client: &mut EtcdPdClient, addr: &str) {
    let mut leader = Leader::new();
    leader.set_addr(addr.to_owned());

    let key = format!("{}/leader", client.root_path());

    let value = leader.write_to_bytes().unwrap();
    let mut obj = BTreeMap::new();
    obj.insert("key".to_owned(), b64encode_to_json(key.as_bytes()));
    obj.insert("value".to_owned(), b64encode_to_json(&value));

    client.request("/v3alpha/kv/put", Json::Object(obj)).unwrap();
}

#[test]
fn test_get_leader() {
    // If no ETCD_ENDPOINTS, skip this test.
    let endpoints = match env::var("ETCD_ENDPOINTS") {
        Err(_) => return,
        Ok(v) => v,
    };

    let mut client = EtcdPdClient::new(&endpoints, 0).unwrap();

    put_leader_addr(&mut client, "http://127.0.0.1:2379");
    let addr = client.get_leader_addr().unwrap();
    assert_eq!(&addr, "http://127.0.0.1:2379");
}

fn start_pd_server(client: &mut EtcdPdClient,
                   quit: Arc<AtomicBool>)
                   -> (thread::JoinHandle<()>, String) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();

    let addr = format!("{}", l.local_addr().unwrap());
    let urls = format!("http://{}", addr);
    put_leader_addr(client, &urls);

    let h = thread::spawn(move || {
        loop {
            let (mut stream, _) = l.accept().unwrap();

            if quit.load(Ordering::SeqCst) {
                return;
            }

            stream.set_nodelay(true).unwrap();

            // Read a HTTP header and hijack the stream.
            let header = b"GET /pd/rpc HTTP/1.0\r\n\r\n";
            let mut buffer = vec![0; header.len()];
            let n = stream.read(&mut buffer).unwrap();
            assert_eq!(n, header.len());
            assert_eq!(buffer, header);

            let (id, _) = rpc::decode_data(&mut stream).unwrap();
            let mut msg = PbMessage::new();
            msg.set_msg_type(MessageType::PdResp);
            msg.set_pd_resp(pdpb::Response::new());
            rpc::encode_msg(&mut stream, id, &msg).unwrap();
        }
    });

    (h, addr)
}

#[test]
fn test_rpc_client() {
    // If no ETCD_ENDPOINTS, skip this test.
    let endpoints = match env::var("ETCD_ENDPOINTS") {
        Err(_) => return,
        Ok(v) => v,
    };

    let quit = Arc::new(AtomicBool::new(false));

    let mut etcd_client = EtcdPdClient::new(&endpoints, 1).unwrap();
    let (h, addr) = start_pd_server(&mut etcd_client, quit.clone());

    let client = RpcClient::new(etcd_client, 0).unwrap();

    let msg = pdpb::Request::new();
    for _ in 0..10 {
        client.send(&msg).unwrap();
    }

    quit.store(true, Ordering::SeqCst);

    // connect the server so that we can close the thread.
    make_std_tcp_conn(&*addr).unwrap();

    h.join().unwrap();
}
