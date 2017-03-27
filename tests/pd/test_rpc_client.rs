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
use std::sync::Arc;
use std::time::Duration;

use kvproto::metapb;

use tikv::pd::{PdClient, RpcClient, validate_endpoints};

use super::mock::mocker::*;
use super::mock::Server as MockServer;

#[test]
fn test_rpc_client() {
    let eps = "http://127.0.0.1:3079".to_owned();

    let se = Arc::new(Service::new(vec![eps.clone()]));
    let _server = MockServer::run("127.0.0.1:3079", se.clone(), Some(se.clone()));

    thread::sleep(Duration::from_secs(1));

    let client = RpcClient::new(&eps).unwrap();
    assert_ne!(client.get_cluster_id().unwrap(), 0);

    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::new();
    store.set_id(store_id);
    debug!("bootstrap store {:?}", store);

    let peer_id = client.alloc_id().unwrap();
    let mut peer = metapb::Peer::new();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::new();
    region.set_id(region_id);
    region.mut_peers().push(peer.clone());
    debug!("bootstrap region {:?}", region);

    client.bootstrap_cluster(store.clone(), region.clone()).unwrap();
    assert_eq!(client.is_cluster_bootstrapped().unwrap(), true);

    let tmp_store = client.get_store(store_id).unwrap();
    assert_eq!(tmp_store.get_id(), store.get_id());

    let tmp_region = client.get_region_by_id(region_id).unwrap().unwrap();
    assert_eq!(tmp_region.get_id(), region.get_id());

    let mut prev_id = 0;
    for _ in 0..100 {
        let client = RpcClient::new(&eps).unwrap();
        let alloc_id = client.alloc_id().unwrap();
        assert!(alloc_id > prev_id);
        prev_id = alloc_id;
    }
}

#[test]
fn test_validate_endpoints() {
    let eps = vec![
        "http://127.0.0.1:13079".to_owned(),
        "http://127.0.0.1:23079".to_owned(),
        "http://127.0.0.1:33079".to_owned(),
    ];

    let se = Arc::new(Service::new(eps.clone()));
    let sp = Arc::new(Split::new(eps.clone()));

    let _server_a = MockServer::run("127.0.0.1:13079", se.clone(), Some(sp.clone()));
    let _server_b = MockServer::run("127.0.0.1:23079", se.clone(), Some(sp.clone()));
    let _server_c = MockServer::run("127.0.0.1:33079", se.clone(), Some(sp.clone()));

    thread::sleep(Duration::from_secs(1));

    assert!(validate_endpoints(&eps).is_err());
}

#[test]
fn test_change_leader() {
    let mut eps = vec![
        "http://127.0.0.1:43079".to_owned(),
        "http://127.0.0.1:53079".to_owned(),
        "http://127.0.0.1:63079".to_owned(),
    ];

    let se = Arc::new(Service::new(eps.clone()));
    let lc = Arc::new(LeaderChange::new(eps.clone()));

    let _server_a = MockServer::run("127.0.0.1:43079", se.clone(), Some(lc.clone()));
    let _server_b = MockServer::run("127.0.0.1:53079", se.clone(), Some(lc.clone()));
    let _server_a = MockServer::run("127.0.0.1:63079", se.clone(), Some(lc.clone()));

    thread::sleep(Duration::from_secs(1));

    let client = RpcClient::new(&eps.pop().unwrap()).unwrap();
    let leader = client.get_leader();

    for _ in 0..5 {
        client.is_cluster_bootstrapped().unwrap();
        let new = client.get_leader();
        if new != leader {
            return;
        }
        thread::sleep(LeaderChange::get_leader_interval());
    }

    panic!("failed, leader should changed");
}
