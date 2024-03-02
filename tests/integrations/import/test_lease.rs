// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use futures::{executor::block_on, SinkExt};
use grpcio::WriteFlags;
use kvproto::import_sstpb::*;
use test_raftstore::sleep_ms;
use test_sst_importer::*;
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

fn new_acquire(region_id: u64) -> AcquireLease {
    let meta = new_sst_meta(0, 0);
    let mut acquire = AcquireLease::default();
    acquire.mut_lease().mut_region().set_id(region_id);
    acquire.mut_lease().set_uuid(meta.get_uuid().into());
    acquire.set_ttl(1);
    acquire
}

fn new_release(mut acquire: AcquireLease) -> ReleaseLease {
    let mut release = ReleaseLease::default();
    release.set_lease(acquire.take_lease());
    release
}

fn new_write_request(v: u8, commit_ts: u64) -> WriteRequest {
    let mut pairs = vec![];
    let mut pair = Pair::default();
    pair.set_key(vec![v]);
    pair.set_value(vec![v]);
    pairs.push(pair);
    let mut batch = WriteBatch::default();
    batch.set_commit_ts(commit_ts);
    batch.set_pairs(pairs.into());
    let mut req = WriteRequest::default();
    req.set_batch(batch);
    req
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
fn test_lease_concurrent_requests() {
    let (_cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client();

    // Acquire two leases for the same region.
    let mut meta1 = new_sst_meta(0, 0);
    meta1.set_region_id(ctx.get_region_id());
    meta1.set_region_epoch(ctx.get_region_epoch().clone());
    must_acquire_sst_lease(&import, &meta1, Duration::MAX);

    let mut meta2 = new_sst_meta(0, 0);
    meta2.set_region_id(ctx.get_region_id());
    meta2.set_region_epoch(ctx.get_region_epoch().clone());
    must_acquire_sst_lease(&import, &meta2, Duration::MAX);

    // Release meta2 lease in advance.
    sleep_ms(100);
    must_release_sst_lease(&import, &meta2);

    // Write meta2 must fail with lease expire.
    let (keys, values, _) = new_write_params();
    let resp = send_write_sst(&import, &meta2, keys.clone(), values.clone(), 1).unwrap();
    assert_eq!(resp.metas.len(), 0);
    assert!(
        resp.get_error().get_message().contains("lease has expired"),
        "{:?}",
        resp
    );

    // Write meta1 must success.
    let resp = send_write_sst(&import, &meta1, keys, values, 1).unwrap();
    assert_eq!(resp.metas.len(), 1);
}

// A long-running request sent within a lease must can be served even if
// the lease is expired.
#[test]
fn test_lease_expire_before_request_finish() {
    let (_cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client();

    let mut meta1 = new_sst_meta(0, 0);
    meta1.set_region_id(ctx.get_region_id());
    meta1.set_region_epoch(ctx.get_region_epoch().clone());
    must_acquire_sst_lease(&import, &meta1, Duration::MAX);

    let commit_ts = 1;
    let (mut tx, rx) = import.write().unwrap();
    block_on(async {
        let mut r1 = WriteRequest::default();
        r1.set_meta(meta1.clone());
        tx.send((r1, WriteFlags::default())).await.unwrap();

        let req = new_write_request(1, commit_ts);
        tx.send((req, WriteFlags::default())).await.unwrap();
    });

    // Expire the lease.
    must_release_sst_lease(&import, &meta1);
    // New write must fail.
    let (keys, values, _) = new_write_params();
    let resp = send_write_sst(&import, &meta1, keys, values, 1).unwrap();
    assert!(
        resp.get_error().get_message().contains("lease has expired"),
        "{:?}",
        resp
    );

    // The existing write can continue.
    block_on(async {
        let req = new_write_request(2, commit_ts);
        tx.send((req, WriteFlags::default())).await.unwrap();
        tx.close().await.unwrap();

        let resp = rx.await.unwrap();
        assert_eq!(resp.metas.len(), 1);
    });
}

#[test]
fn test_lease_invalid_uuid() {
    let (_cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client();

    let mut meta = new_sst_meta(0, 0);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    // Set an invalid uuid, an normal uuid should be 16 bytes.
    meta.set_uuid(vec![7, 7, 7]);
    let resp = send_acquire_sst_lease(&import, &meta, Duration::MAX).unwrap();
    assert_eq!(resp.get_acquired().len(), 0, "{:?}", resp);

    let (keys, values, _) = new_write_params();
    let resp = send_write_sst(&import, &meta, keys.clone(), values.clone(), 1).unwrap();
    assert!(
        resp.get_error().get_message().contains("invalid lease"),
        "{:?}",
        resp
    );
}

#[test]
fn test_lease_batch_acquire_release() {
    let (_cluster, _ctx, _tikv, import) = new_cluster_and_tikv_import_client();

    let acquire1 = new_acquire(1);
    let acquire2 = new_acquire(2);
    let mut acquire3 = new_acquire(3);
    // Set an invalid uuid, an normal uuid should be 16 bytes.
    acquire3.mut_lease().set_uuid(vec![7, 7, 7]);

    // Acquire three leases in the same request.
    let mut req = LeaseRequest::default();
    req.mut_acquire().push(acquire1.clone());
    req.mut_acquire().push(acquire2.clone());
    req.mut_acquire().push(acquire3.clone());
    let resp = import.lease(&req).unwrap();
    assert_eq!(resp.get_acquired().len(), 2, "{:?}", resp);
    assert_eq!(&resp.get_acquired()[0], acquire1.get_lease(), "{:?}", resp);
    assert_eq!(&resp.get_acquired()[1], acquire2.get_lease(), "{:?}", resp);

    // Release three leases in the same request.
    let mut req = LeaseRequest::default();
    req.mut_release().push(new_release(acquire1.clone()));
    req.mut_release().push(new_release(acquire2.clone()));
    req.mut_release().push(new_release(acquire3));
    let resp = import.lease(&req).unwrap();
    assert_eq!(resp.get_released().len(), 2, "{:?}", resp);
    assert_eq!(&resp.get_released()[0], acquire1.get_lease(), "{:?}", resp);
    assert_eq!(&resp.get_released()[1], acquire2.get_lease(), "{:?}", resp);
}
