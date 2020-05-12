// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use futures::{Future, Stream};
use futures03::compat::{Compat, Compat01As03};
use hyper::rt;
use hyper::{Client, StatusCode, Uri};
use std::error::Error;
use std::net::SocketAddr;
use test_raftstore::{new_server_cluster, Simulator};
use tikv::config::ConfigController;
use tikv::server::status_server::{region_meta::RegionMeta, StatusServer};
use tikv_util::security::SecurityConfig;
use tikv_util::HandyRwLock;

async fn check(authority: SocketAddr, region_id: u64) -> Result<(), Box<dyn Error>> {
    let client = Client::new();
    let uri = Uri::builder()
        .scheme("http")
        .authority(authority.to_string().as_str())
        .path_and_query(format!("/region/{}", region_id).as_str())
        .build()?;
    let resp = Compat01As03::new(client.get(uri)).await?;
    let (parts, raw_body) = resp.into_parts();
    let body = Compat01As03::new(raw_body.concat2()).await?;
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
    let mut status_server =
        StatusServer::new(1, None, ConfigController::default(), router.unwrap());
    assert!(status_server
        .start("127.0.0.1:0".to_string(), &SecurityConfig::default())
        .is_ok());
    let check_task = Box::pin(check(status_server.listening_addr(), region_id));
    rt::run(Compat::new(check_task).map_err(|err| panic!("{}", err)));
    status_server.stop();
}
