// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use collections::{HashMap, HashSet};
use engine_rocks::{raw::Range, util::get_cf_handle};
use engine_traits::{CachedTablet, MiscExt, CF_WRITE};
use keys::{data_key, DATA_MAX_KEY};
use kvproto::debugpb::{
    Db, DebugClient, FlashbackToVersionRequest, FlashbackToVersionResponse,
    GetAllRegionsInStoreRequest, RegionInfoRequest,
};
use test_raftstore::{must_kv_read_equal, write_and_read_key};
use tikv::{
    config::ConfigController,
    server::{debug::Debugger, debug2::DebuggerImplV2},
    storage::mvcc::{TimeStamp, Write, WriteType},
};
use txn_types::Key;

fn gen_mvcc_put_kv(
    k: &[u8],
    v: &[u8],
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
) -> (Vec<u8>, Vec<u8>) {
    let k = Key::from_encoded(data_key(k));
    let k = k.append_ts(commit_ts);
    let w = Write::new(WriteType::Put, start_ts, Some(v.to_vec()));
    (k.as_encoded().clone(), w.as_ref().to_bytes())
}

fn gen_delete_k(k: &[u8], commit_ts: TimeStamp) -> Vec<u8> {
    let k = Key::from_encoded(data_key(k));
    let k = k.append_ts(commit_ts);
    k.as_encoded().clone()
}

#[test]
fn test_compact() {
    let (split_key, _) = gen_mvcc_put_kv(b"k10", b"", 1.into(), 2.into());
    let (split_key2, _) = gen_mvcc_put_kv(b"k20", b"", 1.into(), 2.into());
    let regions = [
        (1, b"".to_vec(), split_key.clone()),
        (1000, split_key.clone(), split_key2.clone()),
        (1002, split_key2.clone(), b"".to_vec()),
    ];

    let check_compact = |from: Vec<u8>, to: Vec<u8>, regions_compacted: HashSet<u64>| {
        let count = 1;
        let mut cluster = test_raftstore_v2::new_node_cluster(0, count);
        cluster.cfg.raft_store.right_derive_when_split = false;
        cluster.run();

        let region = cluster.get_region(b"");
        cluster.must_split(&region, &split_key);
        let region = cluster.get_region(&split_key);
        cluster.must_split(&region, &split_key2);

        for i in 0..30 {
            let (k, v) = (format!("k{:02}", i), format!("value{}", i));
            let (k, v) = gen_mvcc_put_kv(k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
            cluster.must_put_cf(CF_WRITE, &k, &v);
        }
        for (registry, _) in &cluster.engines {
            registry.for_each_opened_tablet(|_, db: &mut CachedTablet<_>| {
                if let Some(db) = db.latest() {
                    db.flush_cf(CF_WRITE, true).unwrap();
                }
                true
            })
        }

        for i in 0..30 {
            let k = format!("k{:02}", i);
            let k = gen_delete_k(k.as_bytes(), 2.into());
            cluster.must_delete_cf(CF_WRITE, &k);
        }
        for (registry, _) in &cluster.engines {
            registry.for_each_opened_tablet(|_, db: &mut CachedTablet<_>| {
                if let Some(db) = db.latest() {
                    db.flush_cf(CF_WRITE, true).unwrap();
                }
                true
            })
        }

        let mut tablet_size_before_compact = HashMap::default();
        for (registry, _) in &cluster.engines {
            registry.for_each_opened_tablet(|region_id, db: &mut CachedTablet<_>| {
                if let Some(db) = db.latest() {
                    let cf_handle = get_cf_handle(db.as_inner(), CF_WRITE).unwrap();
                    let approximate_size = db
                        .as_inner()
                        .get_approximate_sizes_cf(cf_handle, &[Range::new(b"", DATA_MAX_KEY)])[0];
                    tablet_size_before_compact.insert(region_id, approximate_size);
                }
                true
            })
        }

        let debugger = DebuggerImplV2::new(
            cluster.engines[0].0.clone(),
            cluster.raft_engines.get(&1).unwrap().clone(),
            ConfigController::default(),
        );

        debugger
            .compact(Db::Kv, CF_WRITE, &from, &to, 1, Some("skip").into())
            .unwrap();

        let mut tablet_size_after_compact = HashMap::default();
        for (registry, _) in &cluster.engines {
            registry.for_each_opened_tablet(|region_id, db: &mut CachedTablet<_>| {
                if let Some(db) = db.latest() {
                    let cf_handle = get_cf_handle(db.as_inner(), CF_WRITE).unwrap();
                    let approximate_size = db
                        .as_inner()
                        .get_approximate_sizes_cf(cf_handle, &[Range::new(b"", DATA_MAX_KEY)])[0];
                    tablet_size_after_compact.insert(region_id, approximate_size);
                }
                true
            })
        }
        for (id, &size) in &tablet_size_after_compact {
            if regions_compacted.contains(id) {
                assert!(size == 0);
                continue;
            }

            assert_eq!(tablet_size_before_compact[id], size);
        }
    };

    // compact the middle region
    let region = regions[1].clone();
    let mut regions_compacted = HashSet::default();
    regions_compacted.insert(region.0);
    let from = keys::data_key(&region.1);
    let to = keys::data_end_key(&region.2);
    check_compact(from, to, regions_compacted);

    // compact first two regions
    let region1 = regions[0].clone();
    let region2 = regions[1].clone();
    let mut regions_compacted = HashSet::default();
    regions_compacted.insert(region1.0);
    regions_compacted.insert(region2.0);
    let from = keys::data_key(&region1.1);
    let to = keys::data_end_key(&region2.2);
    check_compact(from, to, regions_compacted);

    // compact all regions by specifying specific keys
    let region1 = regions[0].clone();
    let region2 = regions[2].clone();
    let mut regions_compacted = HashSet::default();
    let _ = regions
        .iter()
        .map(|(id, ..)| regions_compacted.insert(*id))
        .collect::<Vec<_>>();
    let from = keys::data_key(&region1.1);
    let to = keys::data_end_key(&region2.2);
    check_compact(from, to, regions_compacted.clone());

    // compact all regions
    check_compact(b"".to_vec(), b"".to_vec(), regions_compacted.clone());
    check_compact(b"z".to_vec(), b"z".to_vec(), regions_compacted.clone());
    check_compact(b"z".to_vec(), b"{".to_vec(), regions_compacted);
}

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
    println!("regions: {:?}", regions);
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
