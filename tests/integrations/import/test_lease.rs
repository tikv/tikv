// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use kvproto::import_sstpb::*;
use uuid::Uuid;

use super::util::*;

#[test]
fn test_lease_expire() {
    let (_cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client();

    let mut meta = new_sst_meta(0, 0);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    let mut keys = vec![];
    let mut values = vec![];
    let sst_range = (0, 10);
    for i in sst_range.0..sst_range.1 {
        keys.push(vec![i]);
        values.push(vec![i]);
    }

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
fn test_lease_renew() {}

#[test]
fn test_lease_concurrent_requests() {}

// #[test]
// fn test_lease_hold() {}
