// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::{
    debugpb::{
        FlashbackToVersionRequest, FlashbackToVersionResponse, GetAllRegionsInStoreRequest,
        RegionInfoRequest,
    },
    debugpb_grpc::DebugClient,
};
use test_raftstore::{must_kv_read_equal, write_and_read_key};

#[test]
fn test_flashback_to_version() {
    let (mut _cluster, kv_client, debug_client, ctx) =
        test_raftstore::must_new_cluster_kv_client_and_debug_client();
    let mut ts = 0;
    for i in 0..2000 {
        let v = format!("value@{}", i).into_bytes();
        let k = format!("key@{}", i % 1000).into_bytes();
        write_and_read_key(&kv_client, &ctx, &mut ts, k.clone(), v.clone());
    }

    let req = GetAllRegionsInStoreRequest::default();
    let regions = debug_client.get_all_regions_in_store(&req).unwrap().regions;
    let flashback_version = 5;
    // prepare flashback.
    let res = flashback_to_version(&debug_client, regions.clone(), flashback_version, ts + 1, 0);
    assert_eq!(res.is_ok(), true);
    // finish flashback.
    let res = flashback_to_version(&debug_client, regions, flashback_version, ts + 1, ts + 2);
    assert_eq!(res.is_ok(), true);

    ts += 2;
    must_kv_read_equal(&kv_client, ctx, b"key@1".to_vec(), b"value@1".to_vec(), ts);
}

#[test]
fn test_flashback_to_version_without_prepare() {
    let (mut _cluster, kv_client, debug_client, ctx) =
        test_raftstore::must_new_cluster_kv_client_and_debug_client();
    let mut ts = 0;
    for i in 0..2000 {
        let v = format!("value@{}", i).into_bytes();
        let k = format!("key@{}", i % 1000).into_bytes();
        write_and_read_key(&kv_client, &ctx, &mut ts, k.clone(), v.clone());
    }

    let req = GetAllRegionsInStoreRequest::default();
    let regions = debug_client.get_all_regions_in_store(&req).unwrap().regions;
    // finish flashback.
    match flashback_to_version(&debug_client, regions, 0, 1, 2).unwrap_err() {
        grpcio::Error::RpcFailure(status) => {
            assert_eq!(status.code(), grpcio::RpcStatusCode::UNKNOWN);
            assert_eq!(status.message(), "not in flashback state");
        }
        _ => panic!("expect not in flashback state"),
    }
}

#[test]
fn test_flashback_to_version_with_mismatch_ts() {
    let (mut _cluster, kv_client, debug_client, ctx) =
        test_raftstore::must_new_cluster_kv_client_and_debug_client();
    let mut ts = 0;
    for i in 0..2000 {
        let v = format!("value@{}", i).into_bytes();
        let k = format!("key@{}", i % 1000).into_bytes();
        write_and_read_key(&kv_client, &ctx, &mut ts, k.clone(), v.clone());
    }

    let req = GetAllRegionsInStoreRequest::default();
    let regions = debug_client.get_all_regions_in_store(&req).unwrap().regions;
    let flashback_version = 5;
    // prepare flashback.
    let res = flashback_to_version(&debug_client, regions.clone(), flashback_version, ts + 1, 0);
    assert_eq!(res.is_ok(), true);

    let res = flashback_to_version(
        &debug_client,
        regions.clone(),
        flashback_version,
        ts + 1,
        ts + 3,
    );
    assert_eq!(res.is_ok(), true);

    // use mismatch ts.
    match flashback_to_version(&debug_client, regions, flashback_version, ts + 2, ts + 3)
        .unwrap_err()
    {
        grpcio::Error::RpcFailure(status) => {
            assert_eq!(status.code(), grpcio::RpcStatusCode::UNKNOWN);
            assert_eq!(status.message(), "not in flashback state");
        }
        _ => panic!("expect not in flashback state"),
    }
}

fn flashback_to_version(
    client: &DebugClient,
    regions: Vec<u64>,
    version: u64,
    start_ts: u64,
    commit_ts: u64,
) -> grpcio::Result<FlashbackToVersionResponse> {
    for region_id in regions {
        let mut req = RegionInfoRequest::default();
        req.set_region_id(region_id);
        let r = client
            .region_info(&req)
            .unwrap()
            .region_local_state
            .unwrap()
            .region
            .take()
            .unwrap();
        let mut req = FlashbackToVersionRequest::default();
        req.set_version(version);
        req.set_region_id(region_id);
        req.set_start_key(r.get_start_key().to_vec());
        req.set_end_key(r.get_end_key().to_vec());
        req.set_start_ts(start_ts);
        req.set_commit_ts(commit_ts);
        client.flashback_to_version(&req)?;
    }
    Ok(FlashbackToVersionResponse::default())
}
