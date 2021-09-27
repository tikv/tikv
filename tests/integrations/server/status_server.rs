// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use hyper::{body, Client, StatusCode, Uri};
use raftstore::engine_store_ffi::EngineStoreServerHelper;
use security::SecurityConfig;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use test_raftstore::{new_server_cluster, Simulator};
use tikv::config::ConfigController;
use tikv::server::status_server::{region_meta::RegionMeta, StatusServer};
use tikv_util::HandyRwLock;

async fn check(authority: SocketAddr, region_id: u64) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let uri = Uri::builder()
        .scheme("http")
        .authority(authority.to_string().as_str())
        .path_and_query(format!("/region/{}", region_id).as_str())
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
    assert_eq!("application/json", parts.headers["content-type"].to_str()?);
    serde_json::from_slice::<RegionMeta>(body.as_ref())?;
    Ok(())
}

#[test]
fn test_region_meta_endpoint() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    let region = cluster.get_region(b"");
    let region_id = region.get_id();
    let peer = region.get_peers().get(0);
    assert!(peer.is_some());
    let store_id = peer.unwrap().get_store_id();
    let router = cluster.sim.rl().get_router(store_id);
    assert!(router.is_some());

    let mut status_server = unsafe {
        let helperset = &*cluster
            .global_engine_helper_set
            .as_ref()
            .unwrap()
            .engine_store_server_helper;
        let helperptr = helperset as *const EngineStoreServerHelper;
        StatusServer::new(
            &*helperptr,
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            router.unwrap(),
        )
        .unwrap()
    };
    let addr = format!("127.0.0.1:{}", test_util::alloc_port());
    assert!(status_server.start(addr.clone(), addr).is_ok());
    let check_task = check(status_server.listening_addr(), region_id);
    let rt = tokio::runtime::Runtime::new().unwrap();
    if let Err(err) = rt.block_on(check_task) {
        panic!("{}", err);
    }
    status_server.stop();
}
