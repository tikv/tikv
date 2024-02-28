// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use kvproto::import_sstpb::*;
use test_raftstore::sleep_ms;
use uuid::Uuid;

use super::util::*;

fn new_write_params() -> (Vec<Vec<u8>>, Vec<Vec<u8>>, (u8, u8)) {
    let mut keys = vec![];
    let mut values = vec![];
    let sst_range = (0u8, 10u8);
    for i in sst_range.0..sst_range.1 {
        keys.push(vec![i]);
        values.push(vec![i]);
    }
    (keys, values, sst_range)
}

#[test]
fn test_lease_expire() {
    let (_cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client();

    let mut meta = new_sst_meta(0, 0);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    let (keys, values, sst_range) = new_write_params();
    must_acquire_sst_lease(&import, &meta, Duration::MAX);
    let resp = send_write_sst(&import, &meta, keys.clone(), values.clone(), 1).unwrap();
    assert_eq!(resp.metas.len(), 1);
    // A successful ingest expires its lease.
    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(resp.metas[0].clone());
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error());
    check_ingested_txn_kvs(&tikv, &ctx, sst_range, 2);

    // Must fail with lease expired.
    let resp = send_write_sst(&import, &meta, keys.clone(), values.clone(), 1).unwrap();
    assert!(
        resp.get_error().get_message().contains("lease has expired"),
        "{:?}",
        resp
    );

    // Release release expires lease immediately.
    meta.set_uuid(Uuid::new_v4().as_bytes().into());
    must_acquire_sst_lease(&import, &meta, Duration::MAX);
    let resp = send_write_sst(&import, &meta, keys.clone(), values.clone(), 1).unwrap();
    assert!(!resp.has_error(), "{:?}", resp);
    must_release_sst_lease(&import, &meta);
    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(resp.metas[0].clone());
    let resp = import.ingest(&ingest).unwrap();
    assert!(
        resp.get_error().get_message().contains("lease has expired"),
        "{:?}",
        resp
    );
}

#[test]
fn test_lease_renew() {
    let (_cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client();

    let mut meta = new_sst_meta(0, 0);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    let (keys, values, _) = new_write_params();
    must_acquire_sst_lease(&import, &meta, Duration::from_millis(10));
    sleep_ms(200);

    // Must fail with lease expired.
    let resp = send_write_sst(&import, &meta, keys.clone(), values.clone(), 1).unwrap();
    assert!(
        resp.get_error().get_message().contains("lease has expired"),
        "{:?}",
        resp
    );

    must_acquire_sst_lease(&import, &meta, Duration::MAX);
    let resp = send_write_sst(&import, &meta, keys, values, 1).unwrap();
    assert!(!resp.has_error(), "{:?}", resp);
    assert_eq!(resp.metas.len(), 1, "{:?}", resp);
}

#[test]
fn test_lease_concurrent_requests() {}

// #[test]
// fn test_lease_hold() {}

#[test]
fn test_lease_invalid_uuid() {
    let (_cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client();

    let mut meta = new_sst_meta(0, 0);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    // Set an invalid uuid, an normal uuid should be 16 bytes.
    meta.set_uuid(vec![7, 7, 7]);
    let resp = send_acquire_lease(&import, &meta, Duration::MAX).unwrap();
    assert_eq!(resp.get_acquired().len(), 0, "{:?}", resp);

    let (keys, values, _) = new_write_params();
    let resp = send_write_sst(&import, &meta, keys.clone(), values.clone(), 1).unwrap();
    assert!(
        resp.get_error().get_message().contains("invalid lease"),
        "{:?}",
        resp
    );
}
