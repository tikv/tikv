// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

use grpcio::EnvBuilder;
use kvproto::metapb;
use kvproto::pdpb;
use tokio::time::delay_for;

use pd_client::{validate_endpoints, Error as PdError, PdClient, RegionStat, RpcClient};
use raftstore::store;
use security::{SecurityConfig, SecurityManager};
use semver::Version;
use tikv_util::config::ReadableDuration;
use txn_types::TimeStamp;

use test_pd::{mocker::*, util::*, Server as MockServer};

#[tokio::test(threaded_scheduler)]
async fn test_retry_rpc_client() {
    let eps_count = 1;
    let mut server = MockServer::new(eps_count);
    let eps = server.bind_addrs();
    let m_eps = eps.clone();
    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    let m_mgr = mgr.clone();
    server.stop();
    let child = tokio::spawn(async move {
        let cfg = new_config(m_eps);
        assert!(RpcClient::new(&cfg, None, m_mgr).is_ok());
    });
    delay_for(Duration::from_millis(500)).await;
    server.start(&mgr, eps);
    assert!(child.await.is_ok());
}

#[tokio::test(threaded_scheduler)]
async fn test_rpc_client() {
    let eps_count = 1;
    let server = MockServer::new(eps_count);
    let eps = server.bind_addrs();

    let client = new_client(eps.clone(), None);
    assert_ne!(client.get_cluster_id().unwrap(), 0);

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

    assert!(client
        .bootstrap_cluster(store.clone(), region.clone())
        .is_ok());
    assert_eq!(client.is_cluster_bootstrapped().unwrap(), true);

    let tmp_stores = client.get_all_stores(false).unwrap();
    assert_eq!(tmp_stores.len(), 1);
    assert_eq!(tmp_stores[0], store);

    let tmp_store = client.get_store(store_id).await.unwrap();
    assert_eq!(tmp_store.get_id(), store.get_id());

    let region_key = region.get_start_key();
    let tmp_region = client.get_region(region_key).unwrap();
    assert_eq!(tmp_region.get_id(), region.get_id());

    let region_info = client.get_region_info(region_key).unwrap();
    assert_eq!(region_info.region, region);
    assert_eq!(region_info.leader, None);

    let tmp_region = client.get_region_by_id(region_id).await.unwrap().unwrap();
    assert_eq!(tmp_region.get_id(), region.get_id());

    let ts = client.get_tso().await.unwrap();
    assert_ne!(ts, TimeStamp::zero());

    let mut prev_id = 0;
    for _ in 0..100 {
        let client = new_client(eps.clone(), None);
        let alloc_id = client.alloc_id().unwrap();
        assert!(alloc_id > prev_id);
        prev_id = alloc_id;
    }

    let (tx, rx) = mpsc::channel();
    tokio::spawn(client.handle_region_heartbeat_response(1, move |resp| {
        let _ = tx.send(resp);
    }));
    tokio::spawn(client.region_heartbeat(
        store::RAFT_INIT_LOG_TERM,
        region.clone(),
        peer.clone(),
        RegionStat::default(),
        None,
    ));
    assert!(rx.recv_timeout(Duration::from_secs(3)).is_ok());

    let region_info = client.get_region_info(region_key).unwrap();
    assert_eq!(region_info.region, region);
    assert_eq!(region_info.leader.unwrap(), peer);

    assert!(client
        .store_heartbeat(pdpb::StoreStats::default())
        .await
        .is_ok());
    assert!(client
        .ask_batch_split(metapb::Region::default(), 1)
        .await
        .is_ok());
    assert!(client
        .report_batch_split(vec![metapb::Region::default(), metapb::Region::default()])
        .await
        .is_ok());

    let region_info = client.get_region_info(region_key).unwrap();
    assert!(client.scatter_region(region_info).is_ok());
}

#[tokio::test]
async fn test_get_tombstone_stores() {
    let eps_count = 1;
    let server = MockServer::new(eps_count);
    let eps = server.bind_addrs();
    let client = new_client(eps, None);

    let mut all_stores = vec![];
    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::default();
    store.set_id(store_id);
    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::default();
    region.set_id(region_id);
    assert!(client.bootstrap_cluster(store.clone(), region).is_ok());

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
    all_stores.sort_by(|a, b| a.get_id().cmp(&b.get_id()));
    // include tombstone, there should be 2 stores.
    let mut s = client.get_all_stores(false).unwrap();
    s.sort_by(|a, b| a.get_id().cmp(&b.get_id()));
    assert_eq!(s, all_stores);

    // Add another tombstone store.
    let mut store199 = store99;
    store199.set_id(199);
    server.default_handler().add_store(store199.clone());

    all_stores.push(store199);
    all_stores.sort_by(|a, b| a.get_id().cmp(&b.get_id()));
    let mut s = client.get_all_stores(false).unwrap();
    s.sort_by(|a, b| a.get_id().cmp(&b.get_id()));
    assert_eq!(s, all_stores);

    assert!(client.get_store(store_id).await.is_ok());
    assert!(client.get_store(99).await.is_err());
    assert!(client.get_store(199).await.is_err());
}

#[test]
fn test_reboot() {
    let eps_count = 1;
    let server = MockServer::with_case(eps_count, Arc::new(AlreadyBootstrapped));
    let eps = server.bind_addrs();
    let client = new_client(eps, None);

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
    assert!(validate_endpoints(env, &new_config(eps), mgr).is_err());
}

#[tokio::test]
async fn test_retry() {
    let eps_count = 1;
    // Retry mocker returns `Err(_)` for most request, here two thirds are `Err(_)`.
    let retry = Arc::new(Retry::new(3));
    let server = MockServer::with_case(eps_count, retry);
    let eps = server.bind_addrs();

    let client = new_client(eps, None);

    for _ in 0..3 {
        assert!(client.get_region_by_id(1).await.is_ok());
    }
}

#[tokio::test]
async fn test_not_retry() {
    let eps_count = 1;
    // NotRetry mocker returns Ok() with error header first, and next returns Ok() without any error header.
    let not_retry = Arc::new(NotRetry::new());
    let server = MockServer::with_case(eps_count, not_retry);
    let eps = server.bind_addrs();

    let client = new_client(eps, None);

    assert!(client.get_region_by_id(1).await.is_err());
}

#[tokio::test]
async fn test_incompatible_version() {
    let incompatible = Arc::new(Incompatible);
    let server = MockServer::with_case(1, incompatible);
    let eps = server.bind_addrs();

    let client = new_client(eps, None);

    let resp = client.ask_batch_split(metapb::Region::default(), 2).await;
    assert_eq!(
        resp.unwrap_err().to_string(),
        PdError::Incompatible.to_string()
    );
}

async fn restart_leader(mgr: SecurityManager) {
    let mgr = Arc::new(mgr);
    // Service has only one GetMembersResponse, so the leader never changes.
    let mut server =
        MockServer::<Service>::with_configuration(&mgr, vec![("127.0.0.1".to_owned(), 0); 3], None);
    let eps = server.bind_addrs();

    let client = new_client(eps.clone(), Some(Arc::clone(&mgr)));
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
    assert!(client.bootstrap_cluster(store, region.clone()).is_ok());

    let region = client
        .get_region_by_id(region.get_id())
        .await
        .unwrap()
        .unwrap();

    // Stop servers and restart them again.
    server.stop();
    server.start(&mgr, eps);

    // RECONNECT_INTERVAL_SEC is 1s.
    delay_for(Duration::from_secs(1)).await;

    let region = client.get_region_by_id(region.get_id()).await.unwrap();
    assert_eq!(region.unwrap().get_id(), region_id);
}

#[tokio::test]
async fn test_restart_leader_insecure() {
    let mgr = SecurityManager::new(&SecurityConfig::default()).unwrap();
    restart_leader(mgr).await;
}

#[tokio::test]
async fn test_restart_leader_secure() {
    let security_cfg = test_util::new_security_cfg(None);
    let mgr = SecurityManager::new(&security_cfg).unwrap();
    restart_leader(mgr).await;
}

#[tokio::test]
async fn test_change_leader_async() {
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
        let region = client.get_region_by_id(1).await;
        region.ok();

        let new = client.get_leader();
        if new != leader {
            assert!(counter.load(Ordering::SeqCst) >= 1);
            return;
        }
        delay_for(LeaderChange::get_leader_interval()).await;
    }

    panic!("failed, leader should changed");
}

#[tokio::test(threaded_scheduler)]
async fn test_region_heartbeat_on_leader_change() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(LeaderChange::new()));
    let eps = server.bind_addrs();

    let client = new_client(eps, None);
    let (tx, rx) = mpsc::channel();
    tokio::spawn(client.handle_region_heartbeat_response(1, move |resp| {
        assert!(tx.send(resp).is_ok());
    }));
    let region = metapb::Region::default();
    let peer = metapb::Peer::default();
    let stat = RegionStat::default();
    tokio::spawn(client.region_heartbeat(
        store::RAFT_INIT_LOG_TERM,
        region.clone(),
        peer.clone(),
        stat.clone(),
        None,
    ));
    assert!(rx.recv_timeout(LeaderChange::get_leader_interval()).is_ok());

    for count in &[1, 2] {
        let mut leader = client.get_leader();
        for _ in 0..*count {
            loop {
                let _ = client.get_region_by_id(1).await;
                let new = client.get_leader();
                if leader != new {
                    leader = new;
                    info!("leader changed!");
                    break;
                }
                delay_for(LeaderChange::get_leader_interval()).await;
            }
        }
        tokio::spawn(client.region_heartbeat(
            store::RAFT_INIT_LOG_TERM,
            region.clone(),
            peer.clone(),
            stat.clone(),
            None,
        ));
        assert!(rx.recv_timeout(LeaderChange::get_leader_interval()).is_ok());
    }
}

#[tokio::test]
async fn test_periodical_update() {
    let eps_count = 3;
    let server = MockServer::with_case(eps_count, Arc::new(LeaderChange::new()));
    let eps = server.bind_addrs();

    let counter = Arc::new(AtomicUsize::new(0));
    let client = new_client_with_update_interval(eps, None, ReadableDuration::secs(3));
    let counter1 = Arc::clone(&counter);
    client.handle_reconnect(move || {
        counter1.fetch_add(1, Ordering::SeqCst);
    });
    let leader = client.get_leader();

    for _ in 0..5 {
        let new = client.get_leader();
        if new != leader {
            assert!(counter.load(Ordering::SeqCst) >= 1);
            return;
        }
        delay_for(LeaderChange::get_leader_interval()).await;
    }

    panic!("failed, leader should changed");
}

#[tokio::test]
async fn test_cluster_version() {
    let server = MockServer::<Service>::new(3);
    let eps = server.bind_addrs();

    let client = new_client(eps, None);
    let cluster_version = client.cluster_version();
    assert!(cluster_version.get().is_none());

    let emit_heartbeat = || async {
        let req = pdpb::StoreStats::default();
        client.store_heartbeat(req).await.unwrap();
    };

    let set_cluster_version = |version: &str| {
        let h = server.default_handler();
        h.set_cluster_version(version.to_owned());
    };

    // Empty version string will be treated as invalid.
    emit_heartbeat().await;
    assert!(cluster_version.get().is_none());

    // Explicitly invalid version string.
    set_cluster_version("invalid-version");
    emit_heartbeat().await;
    assert!(cluster_version.get().is_none());

    let v_500 = Version::parse("5.0.0").unwrap();
    let v_501 = Version::parse("5.0.1").unwrap();

    // Correct version string.
    set_cluster_version("5.0.0");
    emit_heartbeat().await;
    assert_eq!(cluster_version.get().unwrap(), v_500);

    // Version can't go backwards.
    set_cluster_version("4.99");
    emit_heartbeat().await;
    assert_eq!(cluster_version.get().unwrap(), v_500);

    // After reconnect the version should be still accessable.
    client.reconnect().unwrap();
    assert_eq!(cluster_version.get().unwrap(), v_500);

    // Version can go forwards.
    set_cluster_version("5.0.1");
    emit_heartbeat().await;
    assert_eq!(cluster_version.get().unwrap(), v_501);
}
