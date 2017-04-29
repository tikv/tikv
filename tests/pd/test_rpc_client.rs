// Copyright 2017 PingCAP, Inc.
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

use futures::Future;
use kvproto::metapb;
use kvproto::pdpb;

use tikv::pd::{PdClient, RpcClient, validate_endpoints, Error as PdError, RegionStat};

use super::mock::mocker::*;
use super::mock::Server as MockServer;

#[test]
fn test_rpc_client() {
    let eps = "http://127.0.0.1:52729".to_owned();

    let se = Arc::new(Service::new(vec![eps.clone()]));
    let _server = MockServer::run("127.0.0.1:52729", se.clone(), Some(se.clone()));

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

    let tmp_region = client.get_region_by_id(region_id).wait().unwrap().unwrap();
    assert_eq!(tmp_region.get_id(), region.get_id());

    let mut prev_id = 0;
    for _ in 0..100 {
        let client = RpcClient::new(&eps).unwrap();
        let alloc_id = client.alloc_id().unwrap();
        assert!(alloc_id > prev_id);
        prev_id = alloc_id;
    }

    // Only check if it works.
    client.region_heartbeat(metapb::Region::new(),
                          metapb::Peer::new(),
                          vec![],
                          vec![],
                          RegionStat::default())
        .wait()
        .unwrap();
    client.store_heartbeat(pdpb::StoreStats::new()).wait().unwrap();
    client.ask_split(metapb::Region::new()).wait().unwrap();
    client.report_split(metapb::Region::new(), metapb::Region::new()).wait().unwrap();
}

#[test]
fn test_reboot() {
    let mut eps = vec![
        "http://127.0.0.1:52730".to_owned(),
    ];

    let se = Arc::new(Service::new(eps.clone()));
    let lc = Arc::new(AlreadyBootstrap::new());

    let _server = MockServer::run("127.0.0.1:52730", se.clone(), Some(lc.clone()));
    thread::sleep(Duration::from_secs(1));

    let client = RpcClient::new(&eps.pop().unwrap()).unwrap();

    assert!(!client.is_cluster_bootstrapped().unwrap());

    match client.bootstrap_cluster(metapb::Store::new(), metapb::Region::new()) {
        Err(PdError::ClusterBootstrapped(_)) => (),
        _ => {
            panic!("failed, should return ClusterBootstrapped");
        }
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
fn test_retry_async() {
    let mut eps = vec![
        "http://127.0.0.1:63080".to_owned(),
    ];

    let se = Arc::new(Service::new(eps.clone()));
    // Retry mocker returns `Err(_)` for most request, here two thirds are `Err(_)`.
    let lc = Arc::new(Retry::new(3));

    let _server_a = MockServer::run("127.0.0.1:63080", se.clone(), Some(lc.clone()));

    thread::sleep(Duration::from_secs(1));

    let client = RpcClient::new(&eps.pop().unwrap()).unwrap();

    for _ in 0..5 {
        let region = client.get_region_by_id(1);
        region.wait().unwrap();
    }
}

#[test]
fn test_restart_leader() {
    let eps = vec![
        "http://127.0.0.1:42978".to_owned(),
        "http://127.0.0.1:52978".to_owned(),
        "http://127.0.0.1:62978".to_owned(),
    ];

    // Service has only one GetMembersResponse, so the leader never changes.
    let se = Arc::new(Service::new(eps.clone()));
    // Start mock servers.
    let _server_a = MockServer::run("127.0.0.1:42978", se.clone(), Some(se.clone()));
    let _server_b = MockServer::run("127.0.0.1:52978", se.clone(), Some(se.clone()));
    let _server_c = MockServer::run("127.0.0.1:62978", se.clone(), Some(se.clone()));

    thread::sleep(Duration::from_secs(1));

    let client = RpcClient::new(&eps[0]).unwrap();
    // Put a region.
    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::new();
    store.set_id(store_id);

    let peer_id = client.alloc_id().unwrap();
    let mut peer = metapb::Peer::new();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::new();
    region.set_id(region_id);
    region.mut_peers().push(peer);
    client.bootstrap_cluster(store.clone(), region.clone()).unwrap();

    let region = client.get_region_by_id(1);
    region.wait().unwrap();

    // Kill servers.
    drop(_server_a);
    drop(_server_b);
    drop(_server_c);
    // Restart them again.
    let _server_a = MockServer::run("127.0.0.1:42978", se.clone(), Some(se.clone()));
    let _server_b = MockServer::run("127.0.0.1:52978", se.clone(), Some(se.clone()));
    let _server_c = MockServer::run("127.0.0.1:62978", se.clone(), Some(se.clone()));
    thread::sleep(Duration::from_secs(1));

    let region = client.get_region_by_id(1);
    region.wait().unwrap();
}

#[test]
fn test_change_leader_async() {
    let mut eps = vec![
        "http://127.0.0.1:42979".to_owned(),
        "http://127.0.0.1:52979".to_owned(),
        "http://127.0.0.1:62979".to_owned(),
    ];

    let se = Arc::new(Service::new(eps.clone()));
    let lc = Arc::new(LeaderChange::new(eps.clone()));

    let _server_a = MockServer::run("127.0.0.1:42979", se.clone(), Some(lc.clone()));
    let _server_b = MockServer::run("127.0.0.1:52979", se.clone(), Some(lc.clone()));
    let _server_c = MockServer::run("127.0.0.1:62979", se.clone(), Some(lc.clone()));

    thread::sleep(Duration::from_secs(1));

    let client = RpcClient::new(&eps.pop().unwrap()).unwrap();
    let leader = client.get_leader();

    for _ in 0..5 {
        let region = client.get_region_by_id(1);
        region.wait().ok();

        let new = client.get_leader();
        if new != leader {
            return;
        }
        thread::sleep(LeaderChange::get_leader_interval());
    }

    panic!("failed, leader should changed");
}
