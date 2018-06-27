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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use futures::Future;
use futures_cpupool::Builder;
use grpc::EnvBuilder;
use kvproto::metapb;
use kvproto::pdpb;

use tikv::pd::{validate_endpoints, Config, Error as PdError, PdClient, RegionStat, RpcClient};
use tikv::util::security::{SecurityConfig, SecurityManager};

use super::mock::mocker::*;
use super::mock::Server as MockServer;
use util;

fn new_config(eps: Vec<(String, u16)>) -> Config {
    let mut cfg = Config::default();
    cfg.endpoints = eps
        .into_iter()
        .map(|addr| format!("{}:{}", addr.0, addr.1))
        .collect();
    cfg
}

fn new_client(eps: Vec<(String, u16)>, mgr: Option<Arc<SecurityManager>>) -> RpcClient {
    let cfg = new_config(eps);
    let mgr =
        mgr.unwrap_or_else(|| Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()));
    RpcClient::new(&cfg, mgr).unwrap()
}

#[test]
fn test_rpc_client() {
    let eps_count = 1;
    let server = MockServer::new(eps_count);
    let eps = server.bind_addrs();

    let client = new_client(eps.clone(), None);
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

    client
        .bootstrap_cluster(store.clone(), region.clone())
        .unwrap();
    assert_eq!(client.is_cluster_bootstrapped().unwrap(), true);

    let tmp_stores = client.get_all_stores().unwrap();
    assert_eq!(tmp_stores.len(), 1);
    assert_eq!(tmp_stores[0], store);

    let tmp_store = client.get_store(store_id).unwrap();
    assert_eq!(tmp_store.get_id(), store.get_id());

    let region_key = region.get_start_key();
    let tmp_region = client.get_region(region_key).unwrap();
    assert_eq!(tmp_region.get_id(), region.get_id());

    let region_info = client.get_region_info(region_key).unwrap();
    assert_eq!(region_info.region, region);
    assert_eq!(region_info.leader, None);

    let tmp_region = client.get_region_by_id(region_id).wait().unwrap().unwrap();
    assert_eq!(tmp_region.get_id(), region.get_id());

    let mut prev_id = 0;
    for _ in 0..100 {
        let client = new_client(eps.clone(), None);
        let alloc_id = client.alloc_id().unwrap();
        assert!(alloc_id > prev_id);
        prev_id = alloc_id;
    }

    let poller = Builder::new()
        .pool_size(1)
        .name_prefix(thd_name!("poller"))
        .create();
    let (tx, rx) = mpsc::channel();
    let f = client.handle_region_heartbeat_response(1, move |resp| {
        let _ = tx.send(resp);
    });
    poller.spawn(f).forget();
    poller
        .spawn(client.region_heartbeat(region.clone(), peer.clone(), RegionStat::default()))
        .forget();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();

    let region_info = client.get_region_info(region_key).unwrap();
    assert_eq!(region_info.region, region);
    assert_eq!(region_info.leader.unwrap(), peer);

    client
        .store_heartbeat(pdpb::StoreStats::new())
        .wait()
        .unwrap();
    client.ask_split(metapb::Region::new()).wait().unwrap();
    client
        .report_split(metapb::Region::new(), metapb::Region::new())
        .wait()
        .unwrap();

    let region_info = client.get_region_info(region_key).unwrap();
    client.scatter_region(region_info).unwrap();
}

#[test]
fn test_reboot() {
    let eps_count = 1;
    let server = MockServer::with_case(eps_count, Arc::new(AlreadyBootstrapped));
    let eps = server.bind_addrs();
    let client = new_client(eps, None);

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
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(Split::new()));
    let env = Arc::new(
        EnvBuilder::new()
            .cq_count(1)
            .name_prefix(thd_name!("test_pd"))
            .build(),
    );
    let eps = server.bind_addrs();

    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    assert!(validate_endpoints(env, &new_config(eps), &mgr).is_err());
}

fn test_retry<F: Fn(&RpcClient)>(func: F) {
    let eps_count = 1;
    // Retry mocker returns `Err(_)` for most request, here two thirds are `Err(_)`.
    let retry = Arc::new(Retry::new(3));
    let server = MockServer::with_case(eps_count, retry);
    let eps = server.bind_addrs();

    let client = new_client(eps, None);

    for _ in 0..3 {
        func(&client);
    }
}

#[test]
fn test_retry_async() {
    let async = |client: &RpcClient| {
        let region = client.get_region_by_id(1);
        region.wait().unwrap();
    };
    test_retry(async);
}

#[test]
fn test_retry_sync() {
    let sync = |client: &RpcClient| {
        client.get_store(1).unwrap();
    };
    test_retry(sync)
}

fn restart_leader(mgr: SecurityManager) {
    let mgr = Arc::new(mgr);
    // Service has only one GetMembersResponse, so the leader never changes.
    let mut server =
        MockServer::<Service>::with_configuration(&mgr, vec![("127.0.0.1".to_owned(), 0); 3], None);
    let eps = server.bind_addrs();

    let client = new_client(eps.clone(), Some(Arc::clone(&mgr)));
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
    client
        .bootstrap_cluster(store.clone(), region.clone())
        .unwrap();

    let region = client
        .get_region_by_id(region.get_id())
        .wait()
        .unwrap()
        .unwrap();

    // Stop servers and restart them again.
    server.stop();
    server.start(&mgr, eps);

    // RECONNECT_INTERVAL_SEC is 1s.
    thread::sleep(Duration::from_secs(1));

    let region = client.get_region_by_id(region.get_id()).wait().unwrap();
    assert_eq!(region.unwrap().get_id(), region_id);
}

#[test]
fn test_restart_leader_insecure() {
    let mgr = SecurityManager::new(&SecurityConfig::default()).unwrap();
    restart_leader(mgr)
}

#[test]
fn test_restart_leader_secure() {
    let security_cfg = util::new_security_cfg();
    let mgr = SecurityManager::new(&security_cfg).unwrap();
    restart_leader(mgr)
}

#[test]
fn test_change_leader_async() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(LeaderChange::new()));
    let eps = server.bind_addrs();

    let counter = Arc::new(AtomicUsize::new(0));
    let client = new_client(eps, None);
    let counter1 = Arc::clone(&counter);
    client.handle_reconnect(move || {
        counter1.fetch_add(1, Ordering::SeqCst);
    });
    let leader = client.get_leader();

    for _ in 0..5 {
        let region = client.get_region_by_id(1);
        region.wait().ok();

        let new = client.get_leader();
        if new != leader {
            assert_eq!(1, counter.load(Ordering::SeqCst));
            return;
        }
        thread::sleep(LeaderChange::get_leader_interval());
    }

    panic!("failed, leader should changed");
}

#[test]
fn test_region_heartbeat_on_leader_change() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(LeaderChange::new()));
    let eps = server.bind_addrs();

    let client = new_client(eps, None);
    let poller = Builder::new()
        .pool_size(1)
        .name_prefix(thd_name!("poller"))
        .create();
    let (tx, rx) = mpsc::channel();
    let f = client.handle_region_heartbeat_response(1, move |resp| {
        tx.send(resp).unwrap();
    });
    poller.spawn(f).forget();
    let region = metapb::Region::new();
    let peer = metapb::Peer::new();
    let stat = RegionStat::default();
    poller
        .spawn(client.region_heartbeat(region.clone(), peer.clone(), stat.clone()))
        .forget();
    rx.recv_timeout(LeaderChange::get_leader_interval())
        .unwrap();

    let heartbeat_on_leader_change = |count| {
        let mut leader = client.get_leader();
        for _ in 0..count {
            loop {
                let _ = client.get_region_by_id(1).wait();
                let new = client.get_leader();
                if leader != new {
                    leader = new;
                    info!("leader changed!");
                    break;
                }
                thread::sleep(LeaderChange::get_leader_interval());
            }
        }
        poller
            .spawn(client.region_heartbeat(region.clone(), peer.clone(), stat.clone()))
            .forget();
        rx.recv_timeout(LeaderChange::get_leader_interval())
            .unwrap();
    };

    // Change PD leader once then heartbeat PD.
    heartbeat_on_leader_change(1);

    // Change PD leader twice without update the heartbeat sender, then heartbeat PD.
    heartbeat_on_leader_change(2);
}
