// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use futures::executor::block_on;
use pd_client::PdClient;
use tempfile::Builder;
use test_raftstore::sleep_ms;
use test_sst_importer::*;

use super::testsuite::*;

#[test]
fn test_lease_block_resolved_ts() {
    let mut suite = TestSuite::new(1);
    let region = suite.cluster.get_region(&[]);

    let initial_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();

    // The resolved-ts must advance.
    suite.must_get_rts_ge(region.id, initial_ts);

    let temp_dir = Builder::new().tempdir().unwrap();
    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(region.id);
    meta.set_region_epoch(region.get_region_epoch().clone());
    // The resolved-ts won't be updated from acquiring sst lease till ingest sst.
    let import = suite.get_import_client(region.id);
    must_acquire_sst_lease(import, &meta, Duration::MAX);
    // The resolved-ts must be less than the latest ts.
    let sst_commit_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();
    let blocked_rts = suite.region_resolved_ts(region.id).unwrap();
    assert!(
        sst_commit_ts > blocked_rts,
        "{:?}",
        (sst_commit_ts, blocked_rts)
    );

    let import = suite.get_import_client(region.id);
    let resp = send_upload_sst(import, &meta, &data).unwrap();
    assert!(!resp.has_error(), "{:?}", resp);
    // The resolved-ts must be blocked.
    suite.must_get_rts(region.id, blocked_rts);

    let ctx = suite.get_context(region.id);
    let import = suite.get_import_client(region.id);
    must_ingest_sst(import, ctx, meta);

    // The resolved-ts must advance after ingest sst.
    suite.must_get_rts_ge(region.id, blocked_rts);

    suite.stop();
}

#[test]
fn test_lease_release_unblock_resolved_ts() {
    let mut suite = TestSuite::new(1);
    let region = suite.cluster.get_region(&[]);

    let initial_ts = block_on(suite.cluster.pd_client.get_tso()).unwrap();

    // The resolved-ts must advance.
    suite.must_get_rts_ge(region.id, initial_ts);

    let temp_dir = Builder::new().tempdir().unwrap();
    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, _) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(region.id);
    meta.set_region_epoch(region.get_region_epoch().clone());
    // The resolved-ts won't be updated from acquiring sst lease till ingest sst.
    let import = suite.get_import_client(region.id);
    must_acquire_sst_lease(import, &meta, Duration::MAX);
    let blocked_rts1 = suite.region_resolved_ts(region.id).unwrap();
    sleep_ms(100);
    // The resolved-ts must be blocked within lease.
    suite.must_get_rts(region.id, blocked_rts1);
    // Until we explicitly release the lease.
    let import = suite.get_import_client(region.id);
    must_release_sst_lease(import, &meta);
    suite.must_get_rts_ge(region.id, blocked_rts1);

    // Block resolved ts until lease expires.
    let import = suite.get_import_client(region.id);
    must_acquire_sst_lease(import, &meta, Duration::from_millis(100));
    let blocked_rts2 = suite.region_resolved_ts(region.id).unwrap();
    sleep_ms(200);
    suite.must_get_rts_ge(region.id, blocked_rts2);

    suite.stop();
}
