// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use error_code::ErrorCodeExt;
use futures::{executor::block_on, StreamExt};
use grpcio::{EnvBuilder, Error as GrpcError, RpcStatus, RpcStatusCode};
use kvproto::{metapb, pdpb};
use pd_client::{Error as PdError, Feature, PdClientV2, PdConnector, RpcClientV2};
use security::{SecurityConfig, SecurityManager};
use test_pd::{mocker::*, util::*, Server as MockServer};
use tikv_util::{config::ReadableDuration, mpsc::future::WakePolicy, thd_name};
use tokio::runtime::{Builder, Runtime};
use txn_types::TimeStamp;

fn setup_runtime() -> Runtime {
    Builder::new_multi_thread()
        .thread_name(thd_name!("poller"))
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

fn must_get_tso(client: &mut RpcClientV2, count: u32) -> TimeStamp {
    let (tx, mut responses) = client.create_tso_stream(WakePolicy::Immediately).unwrap();
    let mut req = pdpb::TsoRequest::default();
    req.mut_header().cluster_id = client.fetch_cluster_id().unwrap();
    req.count = count;
    tx.send(req).unwrap();
    let resp = block_on(responses.next()).unwrap().unwrap();
    let ts = resp.timestamp.unwrap();
    let physical = ts.physical as u64;
    let logical = ts.logical as u64;
    TimeStamp::compose(physical, logical)
}

#[test]
fn test_retry_rpc_client() {
    let eps_count = 1;
    let mut server = MockServer::new(eps_count);
    let eps = server.bind_addrs();
    let m_eps = eps.clone();
    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let m_mgr = mgr.clone();
    server.stop();
    let child = thread::spawn(move || {
        let cfg = new_config(m_eps);
        RpcClientV2::new(&cfg, None, m_mgr).unwrap();
    });
    thread::sleep(Duration::from_millis(500));
    server.start(&mgr, eps);
    child.join().unwrap();
}

#[test]
fn test_rpc_client() {
    let rt = setup_runtime();
    let _g = rt.enter();
    let eps_count = 1;
    let server = MockServer::new(eps_count);
    let eps = server.bind_addrs();

    let mut client = new_client_v2(eps.clone(), None);
    assert_ne!(client.fetch_cluster_id().unwrap(), 0);

    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::default();
    store.set_id(store_id);
    debug!("bootstrap store {:?}", store);

    let peer_id = client.alloc_id().unwrap();
    let mut peer = metapb::Peer::default();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::default();
    region.set_id(region_id);
    region.mut_peers().push(peer.clone());
    debug!("bootstrap region {:?}", region);

    client
        .bootstrap_cluster(store.clone(), region.clone())
        .unwrap();
    assert_eq!(client.is_cluster_bootstrapped().unwrap(), true);

    let tmp_stores = client.get_all_stores(false).unwrap();
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

    let tmp_region = block_on(client.get_region_by_id(region_id))
        .unwrap()
        .unwrap();
    assert_eq!(tmp_region.get_id(), region.get_id());

    let ts = must_get_tso(&mut client, 1);
    assert_ne!(ts, TimeStamp::zero());

    let ts100 = must_get_tso(&mut client, 100);
    assert_eq!(ts.logical() + 100, ts100.logical());

    let mut prev_id = 0;
    for _ in 0..10 {
        let mut client = new_client_v2(eps.clone(), None);
        let alloc_id = client.alloc_id().unwrap();
        assert!(alloc_id > prev_id);
        prev_id = alloc_id;
    }

    let (tx, mut responses) = client
        .create_region_heartbeat_stream(WakePolicy::Immediately)
        .unwrap();
    let mut req = pdpb::RegionHeartbeatRequest::default();
    req.set_region(region.clone());
    req.set_leader(peer.clone());
    tx.send(req).unwrap();
    block_on(tokio::time::timeout(
        Duration::from_secs(3),
        responses.next(),
    ))
    .unwrap();

    let region_info = client.get_region_info(region_key).unwrap();
    assert_eq!(region_info.region, region);
    assert_eq!(region_info.leader.unwrap(), peer);

    block_on(client.store_heartbeat(
        pdpb::StoreStats::default(),
        None, // store_report
        None,
    ))
    .unwrap();
    block_on(client.ask_batch_split(metapb::Region::default(), 1)).unwrap();
    block_on(client.report_batch_split(vec![metapb::Region::default(), metapb::Region::default()]))
        .unwrap();

    let region_info = client.get_region_info(region_key).unwrap();
    client.scatter_region(region_info).unwrap();
}

#[test]
fn test_connect_follower() {
    let connect_leader_fp = "connect_leader";
    let server = MockServer::new(2);
    let eps = server.bind_addrs();
    let mut cfg = new_config(eps);

    // test switch
    cfg.enable_forwarding = false;
    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let mut client1 = RpcClientV2::new(&cfg, None, mgr).unwrap();
    fail::cfg(connect_leader_fp, "return").unwrap();
    client1.alloc_id().unwrap_err();

    cfg.enable_forwarding = true;
    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let mut client = RpcClientV2::new(&cfg, None, mgr).unwrap();
    let leader_addr = client.get_leader().get_client_urls()[0].clone();
    let res = format!("{}", client.alloc_id().unwrap_err());
    let err = format!(
        "{}",
        PdError::Grpc(GrpcError::RpcFailure(RpcStatus::with_message(
            RpcStatusCode::UNAVAILABLE,
            leader_addr,
        )))
    );
    assert_eq!(res, err);

    fail::remove(connect_leader_fp);
}

#[test]
fn test_get_tombstone_stores() {
    let eps_count = 1;
    let server = MockServer::new(eps_count);
    let eps = server.bind_addrs();
    let mut client = new_client_v2(eps, None);

    let mut all_stores = vec![];
    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::default();
    store.set_id(store_id);
    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::default();
    region.set_id(region_id);
    client.bootstrap_cluster(store.clone(), region).unwrap();

    all_stores.push(store);
    assert_eq!(client.is_cluster_bootstrapped().unwrap(), true);
    let s = client.get_all_stores(false).unwrap();
    assert_eq!(s, all_stores);

    // Add tombstone store.
    let mut store99 = metapb::Store::default();
    store99.set_id(99);
    store99.set_state(metapb::StoreState::Tombstone);
    server.default_handler().add_store(store99.clone());

    // do not include tombstone.
    let s = client.get_all_stores(true).unwrap();
    assert_eq!(s, all_stores);

    all_stores.push(store99.clone());
    all_stores.sort_by_key(|a| a.get_id());
    // include tombstone, there should be 2 stores.
    let mut s = client.get_all_stores(false).unwrap();
    s.sort_by_key(|a| a.get_id());
    assert_eq!(s, all_stores);

    // Add another tombstone store.
    let mut store199 = store99;
    store199.set_id(199);
    server.default_handler().add_store(store199.clone());

    all_stores.push(store199);
    all_stores.sort_by_key(|a| a.get_id());
    let mut s = client.get_all_stores(false).unwrap();
    s.sort_by_key(|a| a.get_id());
    assert_eq!(s, all_stores);

    client.get_store(store_id).unwrap();
    client.get_store(99).unwrap_err();
    client.get_store(199).unwrap_err();
}

#[test]
fn test_get_tombstone_store() {
    let eps_count = 1;
    let server = MockServer::new(eps_count);
    let eps = server.bind_addrs();
    let mut client = new_client_v2(eps, None);

    let mut all_stores = vec![];
    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::default();
    store.set_id(store_id);
    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::default();
    region.set_id(region_id);
    client.bootstrap_cluster(store.clone(), region).unwrap();

    all_stores.push(store);
    assert_eq!(client.is_cluster_bootstrapped().unwrap(), true);
    let s = client.get_all_stores(false).unwrap();
    assert_eq!(s, all_stores);

    // Add tombstone store.
    let mut store99 = metapb::Store::default();
    store99.set_id(99);
    store99.set_state(metapb::StoreState::Tombstone);
    server.default_handler().add_store(store99.clone());

    let r = client.get_store(99);
    assert_eq!(r.unwrap_err().error_code(), error_code::pd::STORE_TOMBSTONE);
}

#[test]
fn test_reboot() {
    let eps_count = 1;
    let server = MockServer::with_case(eps_count, Arc::new(AlreadyBootstrapped));
    let eps = server.bind_addrs();
    let mut client = new_client_v2(eps, None);

    assert!(!client.is_cluster_bootstrapped().unwrap());

    match client.bootstrap_cluster(metapb::Store::default(), metapb::Region::default()) {
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
            .name_prefix(thd_name!("test-pd"))
            .build(),
    );
    let eps = server.bind_addrs();

    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let connector = PdConnector::new(env, mgr);
    assert!(block_on(connector.validate_endpoints(&new_config(eps), true)).is_err());
}

#[test]
fn test_validate_endpoints_retry() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(Split::new()));
    let env = Arc::new(
        EnvBuilder::new()
            .cq_count(1)
            .name_prefix(thd_name!("test-pd"))
            .build(),
    );
    let mut eps = server.bind_addrs();
    let mock_port = 65535;
    eps.insert(0, ("127.0.0.1".to_string(), mock_port));
    eps.pop();
    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let connector = PdConnector::new(env, mgr);
    assert!(block_on(connector.validate_endpoints(&new_config(eps), true)).is_err());
}

#[test]
fn test_incompatible_version() {
    let incompatible = Arc::new(Incompatible);
    let server = MockServer::with_case(1, incompatible);
    let eps = server.bind_addrs();

    let mut client = new_client_v2(eps, None);

    let resp = block_on(client.ask_batch_split(metapb::Region::default(), 2));
    assert_eq!(
        resp.unwrap_err().to_string(),
        PdError::Incompatible.to_string()
    );
}

fn restart_leader(mgr: SecurityManager) {
    let mgr = Arc::new(mgr);
    // Service has only one GetMembersResponse, so the leader never changes.
    let mut server =
        MockServer::<Service>::with_configuration(&mgr, vec![("127.0.0.1".to_owned(), 0); 3], None);
    let eps = server.bind_addrs();

    let mut client = new_client_v2(eps.clone(), Some(Arc::clone(&mgr)));
    // Put a region.
    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::default();
    store.set_id(store_id);

    let peer_id = client.alloc_id().unwrap();
    let mut peer = metapb::Peer::default();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::default();
    region.set_id(region_id);
    region.mut_peers().push(peer);
    client.bootstrap_cluster(store, region.clone()).unwrap();

    let region = block_on(client.get_region_by_id(region.get_id()))
        .unwrap()
        .unwrap();

    // Stop servers and restart them again.
    server.stop();
    server.start(&mgr, eps);

    // The default retry interval is 300ms so sleeps 400ms here.
    thread::sleep(Duration::from_millis(400));

    let region = block_on(client.get_region_by_id(region.get_id())).unwrap();
    assert_eq!(region.unwrap().get_id(), region_id);
}

#[test]
fn test_restart_leader_insecure() {
    let mgr = SecurityManager::new(&SecurityConfig::default()).unwrap();
    restart_leader(mgr)
}

#[test]
fn test_restart_leader_secure() {
    let security_cfg = test_util::new_security_cfg(None);
    let mgr = SecurityManager::new(&security_cfg).unwrap();
    restart_leader(mgr)
}

#[test]
fn test_change_leader_async() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(LeaderChange::new()));
    let eps = server.bind_addrs();

    let mut client = new_client_v2(eps, None);
    let mut reconnect_recv = client.subscribe_reconnect();
    let leader = client.get_leader();

    for _ in 0..5 {
        let region = block_on(client.get_region_by_id(1));
        region.ok();

        let new = client.get_leader();
        if new != leader {
            assert!(matches!(
                reconnect_recv.try_recv(),
                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
            ));
            return;
        }
        thread::sleep(LeaderChange::get_leader_interval());
    }

    panic!("failed, leader should changed");
}

#[test]
fn test_pd_client_ok_when_cluster_not_ready() {
    let pd_client_cluster_id_zero = "cluster_id_is_not_ready";
    let server = MockServer::with_case(3, Arc::new(AlreadyBootstrapped));
    let eps = server.bind_addrs();

    let mut client = new_client_v2(eps, None);
    fail::cfg(pd_client_cluster_id_zero, "return()").unwrap();
    // wait 100ms to let client load member.
    thread::sleep(Duration::from_millis(101));
    assert_eq!(client.reconnect().is_err(), true);
    fail::remove(pd_client_cluster_id_zero);
}

#[test]
fn test_pd_client_heartbeat_send_failed() {
    let rt = setup_runtime();
    let _g = rt.enter();
    let pd_client_send_fail_fp = "region_heartbeat_send_failed";
    fail::cfg(pd_client_send_fail_fp, "return()").unwrap();
    let server = MockServer::with_case(1, Arc::new(AlreadyBootstrapped));
    let eps = server.bind_addrs();

    let mut client = new_client_v2(eps, None);

    let (tx, mut responses) = client
        .create_region_heartbeat_stream(WakePolicy::Immediately)
        .unwrap();

    let mut heartbeat_send_fail = |ok| {
        let mut region = metapb::Region::default();
        region.set_id(1);
        let mut req = pdpb::RegionHeartbeatRequest::default();
        req.set_region(region);
        tx.send(req).unwrap();

        let rsp = block_on(tokio::time::timeout(
            Duration::from_millis(100),
            responses.next(),
        ));
        if ok {
            assert!(rsp.is_ok());
            assert_eq!(rsp.unwrap().unwrap().unwrap().get_region_id(), 1);
        } else {
            rsp.unwrap_err();
        }

        let region = block_on(client.get_region_by_id(1));
        if ok {
            assert!(region.is_ok());
            let r = region.unwrap();
            assert!(r.is_some());
            assert_eq!(1, r.unwrap().get_id());
        } else {
            region.unwrap_err();
        }
    };
    // send fail if network is block.
    heartbeat_send_fail(false);
    fail::remove(pd_client_send_fail_fp);
    // send success after network recovered.
    heartbeat_send_fail(true);
}

#[test]
fn test_region_heartbeat_on_leader_change() {
    let rt = setup_runtime();
    let _g = rt.enter();
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(LeaderChange::new()));
    let eps = server.bind_addrs();

    let mut client = new_client_v2(eps, None);

    let (tx, mut responses) = client
        .create_region_heartbeat_stream(WakePolicy::Immediately)
        .unwrap();

    tx.send(pdpb::RegionHeartbeatRequest::default()).unwrap();
    block_on(tokio::time::timeout(
        LeaderChange::get_leader_interval(),
        responses.next(),
    ))
    .unwrap()
    .unwrap()
    .unwrap();

    let mut heartbeat_on_leader_change = |count| {
        let mut leader = client.get_leader();
        for _ in 0..count {
            loop {
                let _ = block_on(client.get_region_by_id(1));
                let new = client.get_leader();
                if leader != new {
                    leader = new;
                    info!("leader changed!");
                    break;
                }
                thread::sleep(LeaderChange::get_leader_interval());
            }
        }
        tx.send(pdpb::RegionHeartbeatRequest::default()).unwrap();
        block_on(tokio::time::timeout(
            LeaderChange::get_leader_interval(),
            responses.next(),
        ))
        .unwrap()
        .unwrap()
        .unwrap();
    };

    // Change PD leader once then heartbeat PD.
    heartbeat_on_leader_change(1);

    // Change PD leader twice without update the heartbeat sender, then heartbeat
    // PD.
    heartbeat_on_leader_change(2);
}

#[test]
fn test_periodical_update() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(LeaderChange::new()));
    let eps = server.bind_addrs();

    let mut client = new_client_v2_with_update_interval(eps, None, ReadableDuration::secs(3));
    let mut reconnect_recv = client.subscribe_reconnect();
    let leader = client.get_leader();

    for _ in 0..5 {
        let new = client.get_leader();
        if new != leader {
            assert!(matches!(
                reconnect_recv.try_recv(),
                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
            ));
            return;
        }
        thread::sleep(LeaderChange::get_leader_interval());
    }

    panic!("failed, leader should changed");
}

#[test]
fn test_cluster_version() {
    let server = MockServer::<Service>::new(3);
    let eps = server.bind_addrs();

    let feature_a = Feature::require(0, 0, 1);
    let feature_b = Feature::require(5, 0, 0);
    let feature_c = Feature::require(5, 0, 1);

    let mut client = new_client_v2(eps, None);
    let feature_gate = client.feature_gate().clone();
    assert!(!feature_gate.can_enable(feature_a));

    let mut client_clone = client.clone();
    let mut emit_heartbeat = || {
        let req = pdpb::StoreStats::default();
        block_on(client_clone.store_heartbeat(req, /* store_report= */ None, None)).unwrap();
    };

    let set_cluster_version = |version: &str| {
        let h = server.default_handler();
        h.set_cluster_version(version.to_owned());
    };

    // Empty version string will be treated as invalid.
    emit_heartbeat();
    assert!(!feature_gate.can_enable(feature_a));

    // Explicitly invalid version string.
    set_cluster_version("invalid-version");
    emit_heartbeat();
    assert!(!feature_gate.can_enable(feature_a));

    // Correct version string.
    set_cluster_version("5.0.0");
    emit_heartbeat();
    assert!(feature_gate.can_enable(feature_a));
    assert!(feature_gate.can_enable(feature_b));
    assert!(!feature_gate.can_enable(feature_c));

    // Version can't go backwards.
    set_cluster_version("4.99");
    emit_heartbeat();
    assert!(feature_gate.can_enable(feature_b));
    assert!(!feature_gate.can_enable(feature_c));

    // After reconnect the version should be still accessible.
    // The default retry interval is 300ms so sleeps 400ms here.
    thread::sleep(Duration::from_millis(400));
    client.reconnect().unwrap();
    assert!(feature_gate.can_enable(feature_b));
    assert!(!feature_gate.can_enable(feature_c));

    // Version can go forwards.
    set_cluster_version("5.0.1");
    emit_heartbeat();
    assert!(feature_gate.can_enable(feature_c));
}
