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

    suite.stop();
}
