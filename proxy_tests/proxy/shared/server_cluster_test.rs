// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error, net::SocketAddr, sync::Arc};

use hyper::{body, Client, StatusCode, Uri};
use proxy_server::status_server::StatusServer;
use security::SecurityConfig;
use tikv::config::ConfigController;

use crate::utils::v1_server::*;

async fn check_impl(authority: SocketAddr, _region_id: u64) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let uri = Uri::builder()
        .scheme("http")
        .authority(authority.to_string().as_str())
        .path_and_query("/debug/pprof/profile?seconds=15")
        .build()?;
    let resp = client.get(uri).await?;
    let (parts, raw_body) = resp.into_parts();
    let body = body::to_bytes(raw_body).await?;
    assert_eq!(
        StatusCode::OK,
        parts.status,
        "{}",
        String::from_utf8(body.to_vec())?
    );
    assert_eq!("image/svg+xml", parts.headers["content-type"].to_str()?);
    {
        let xmldata = body.as_ref();
        let xmlstr = std::str::from_utf8(xmldata);
        assert_eq!(xmlstr.is_ok(), true);
        let res = String::from(xmlstr.unwrap()).find("raftstore");
        assert_eq!(res.is_some(), true);
    }
    Ok(())
}

async fn check(authority: SocketAddr, region_id: u64) {
    if let Err(e) = check_impl(authority, region_id).await {
        panic!("error in check_impl {:?}", e);
    }
}

#[test]
fn test_pprof() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    let region = cluster.get_region(b"");
    let region_id = region.get_id();
    let peer = region.get_peers().get(0);
    assert!(peer.is_some());
    let store_id = peer.unwrap().get_store_id();
    let id = 1;

    let mut status_server = None;
    iter_ffi_helpers(
        &cluster,
        Some(vec![id]),
        &mut |_, ffiset: &mut FFIHelperSet| {
            let router = cluster.sim.rl().get_router(store_id);
            assert!(router.is_some());
            status_server = Some(
                StatusServer::new(
                    engine_store_ffi::ffi::gen_engine_store_server_helper(
                        ffiset.engine_store_server_helper_ptr,
                    ),
                    1,
                    ConfigController::default(),
                    Arc::new(SecurityConfig::default()),
                    router.unwrap(),
                    std::env::temp_dir(),
                )
                .unwrap(),
            );
        },
    );
    let addr = format!("127.0.0.1:{}", test_util::alloc_port());
    let mut status_server = status_server.unwrap();
    status_server.start(addr).unwrap();
    let check_task = check(status_server.listening_addr(), region_id);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let handle = rt.spawn(check_task);
    for i in 0..350 {
        let k = format!("k{}", i);
        cluster.must_put(k.as_bytes(), b"v");
        if handle.is_finished() {
            break;
        }
    }
    block_on(handle).unwrap();
    status_server.stop();
}

#[test]
fn test_safe_ts_basic() {
    let mut suite = TestSuite::new(1);
    let physical_time = 646454654654;
    suite
        .cluster
        .cluster_ext
        .set_expected_safe_ts(physical_time, physical_time);
    suite.must_check_leader(1, TimeStamp::new(physical_time), 1, 1);
    assert_eq!(suite.cluster.cluster_ext.test_data.checked_time, 1);

    suite.stop();
}

const INVALID_TIMESTAMP: u64 = u64::MAX;

#[test]
fn test_safe_ts_updates() {
    let mut suite = TestSuite::new(1);

    suite.cluster.cluster_ext.test_data.reset();

    let states = collect_all_states(&suite.cluster.cluster_ext, 1);
    let applied_index = states
        .get(&1)
        .unwrap()
        .in_memory_apply_state
        .get_applied_index();

    let physical_time = 646454654654;
    suite.must_check_leader(1, TimeStamp::new(physical_time), applied_index + 1, 1);

    assert_ne!(
        suite.cluster.cluster_ext.test_data.updated_self_safe_ts,
        physical_time
    );

    suite.cluster.must_put(b"k1", b"v1");

    let eng_ids = suite
        .cluster
        .engines
        .iter()
        .map(|e| e.0.to_owned())
        .collect::<Vec<_>>();

    check_key(
        &*(suite.cluster),
        b"k1",
        b"v1",
        Some(true),
        None,
        Some(vec![eng_ids[0]]),
    );

    assert_eq!(
        suite.cluster.cluster_ext.test_data.updated_self_safe_ts,
        physical_time
    );
    assert_eq!(
        suite.cluster.cluster_ext.test_data.updated_leader_safe_ts,
        INVALID_TIMESTAMP
    );

    let physical_time2 = 666454654654;
    suite.must_check_leader(1, TimeStamp::new(physical_time2), applied_index + 2, 1);

    assert_eq!(
        suite.cluster.cluster_ext.test_data.updated_self_safe_ts,
        physical_time
    );
    assert_eq!(
        suite.cluster.cluster_ext.test_data.updated_leader_safe_ts,
        physical_time2
    );

    suite.cluster.must_put(b"k2", b"v1");

    check_key(
        &*(suite.cluster),
        b"k2",
        b"v1",
        Some(true),
        None,
        Some(vec![eng_ids[0]]),
    );

    assert_eq!(
        suite.cluster.cluster_ext.test_data.updated_self_safe_ts,
        physical_time2
    );
    assert_eq!(
        suite.cluster.cluster_ext.test_data.updated_leader_safe_ts,
        INVALID_TIMESTAMP
    );
    suite.stop();
}

#[test]
fn test_raft_message_observer() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");

    fail::cfg("tiflash_force_reject_raft_append_message", "return").unwrap();
    fail::cfg("tiflash_force_reject_raft_snapshot_message", "return").unwrap();

    cluster.pd_client.add_peer(r1, new_peer(2, 2));

    std::thread::sleep(std::time::Duration::from_millis(500));

    check_key(
        &cluster,
        b"k1",
        b"v",
        Some(false),
        Some(false),
        Some(vec![2]),
    );

    fail::remove("tiflash_force_reject_raft_append_message");
    fail::remove("tiflash_force_reject_raft_snapshot_message");

    cluster.pd_client.must_have_peer(r1, new_peer(2, 2));
    cluster.pd_client.must_add_peer(r1, new_peer(3, 3));

    check_key(
        &cluster,
        b"k1",
        b"v1",
        Some(true),
        Some(true),
        Some(vec![2, 3]),
    );

    fail::cfg("tiflash_force_reject_raft_append_message", "return").unwrap();

    let _ = cluster.async_put(b"k2", b"v2").unwrap();

    std::thread::sleep(std::time::Duration::from_millis(500));

    check_key(
        &cluster,
        b"k3",
        b"v3",
        Some(false),
        Some(false),
        Some(vec![2, 3]),
    );

    fail::remove("tiflash_force_reject_raft_append_message");

    cluster.must_put(b"k3", b"v3");
    check_key(
        &cluster,
        b"k3",
        b"v3",
        Some(true),
        Some(true),
        Some(vec![1, 2, 3]),
    );
    cluster.shutdown();
}
