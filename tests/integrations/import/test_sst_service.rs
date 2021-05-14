// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use tempfile::Builder;

use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::*;
use kvproto::tikvpb::*;

use super::util::*;
use pd_client::PdClient;

use test_sst_importer::*;
use tikv::config::TiKvConfig;

macro_rules! assert_to_string_contains {
    ($e:expr, $substr:expr) => {{
        let msg = $e.to_string();
        assert!(
            msg.contains($substr),
            "msg: {}; expr: {}",
            msg,
            stringify!($e)
        );
    }};
}

#[test]
fn test_upload_sst() {
    let (_cluster, ctx, _, import) = new_cluster_and_tikv_import_client();

    let data = vec![1; 1024];
    let crc32 = calc_data_crc32(&data);
    let length = data.len() as u64;

    // Mismatch crc32
    let meta = new_sst_meta(0, length);
    assert_to_string_contains!(send_upload_sst(&import, &meta, &data).unwrap_err(), "crc32");

    let mut meta = new_sst_meta(crc32, length);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());
    send_upload_sst(&import, &meta, &data).unwrap();

    // Can't upload the same uuid file again.
    assert_to_string_contains!(
        send_upload_sst(&import, &meta, &data).unwrap_err(),
        "FileExists"
    );
}

fn run_test_write_sst(ctx: Context, tikv: TikvClient, import: ImportSstClient) {
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
    let resp = send_write_sst(&import, &meta, keys, values, 1).unwrap();

    for m in resp.metas.into_iter() {
        let mut ingest = IngestRequest::default();
        ingest.set_context(ctx.clone());
        ingest.set_sst(m.clone());
        let resp = import.ingest(&ingest).unwrap();
        assert!(!resp.has_error());
    }
    check_ingested_txn_kvs(&tikv, &ctx, sst_range, 2);
}

#[test]
fn test_write_sst() {
    let (_cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client();

    run_test_write_sst(ctx, tikv, import);
}

#[test]
fn test_write_and_ingest_with_tde() {
    let (_tmp_dir, _cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client_tde();
    run_test_write_sst(ctx, tikv, import);
}

#[test]
fn test_ingest_sst() {
    let mut cfg = TiKvConfig::default();
    cfg.server.grpc_concurrency = 1;
    let (_cluster, ctx, _tikv, import) = open_cluster_and_tikv_import_client(Some(cfg));

    let temp_dir = Builder::new().prefix("test_ingest_sst").tempdir().unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);

    // No region id and epoch.
    send_upload_sst(&import, &meta, &data).unwrap();

    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta.clone());
    let resp = import.ingest(&ingest).unwrap();
    assert!(resp.has_error());

    // Set region id and epoch.
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());
    send_upload_sst(&import, &meta, &data).unwrap();
    // Can't upload the same file again.
    assert_to_string_contains!(
        send_upload_sst(&import, &meta, &data).unwrap_err(),
        "FileExists"
    );

    ingest.set_sst(meta);
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error(), "{:?}", resp.get_error());
}

#[test]
fn test_upload_and_ingest_with_tde() {
    let (_tmp_dir, _cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client_tde();

    let temp_dir = Builder::new().prefix("test_ingest_sst").tempdir().unwrap();
    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);

    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());
    send_upload_sst(&import, &meta, &data).unwrap();

    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta);
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error(), "{:?}", resp.get_error());

    check_ingested_kvs(&tikv, &ctx, sst_range);
}

#[test]
fn test_ingest_sst_without_crc32() {
    let (_cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client();

    let temp_dir = Builder::new()
        .prefix("test_ingest_sst_without_crc32")
        .tempdir()
        .unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    // Set crc32 == 0 and length != 0 still ingest success
    send_upload_sst(&import, &meta, &data).unwrap();
    meta.set_crc32(0);

    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta);
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error(), "{:?}", resp.get_error());

    // Check ingested kvs
    check_ingested_kvs(&tikv, &ctx, sst_range);
}

#[test]
fn test_download_sst() {
    let (_cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client();
    let temp_dir = Builder::new()
        .prefix("test_download_sst")
        .tempdir()
        .unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, _) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    // Checks that downloading a non-existing storage returns error.
    let mut download = DownloadRequest::default();
    download.set_sst(meta.clone());
    download.set_storage_backend(external_storage::make_local_backend(temp_dir.path()));
    download.set_name("missing.sst".to_owned());

    let result = import.download(&download).unwrap();
    assert!(
        result.has_error(),
        "unexpected download reply: {:?}",
        result
    );

    // Checks that downloading an empty SST returns OK (but cannot be ingested)
    download.set_name("test.sst".to_owned());
    download.mut_sst().mut_range().set_start(vec![sst_range.1]);
    download
        .mut_sst()
        .mut_range()
        .set_end(vec![sst_range.1 + 1]);
    let result = import.download(&download).unwrap();
    assert!(result.get_is_empty());

    // Now perform a proper download.
    download.mut_sst().mut_range().set_start(Vec::new());
    download.mut_sst().mut_range().set_end(Vec::new());
    let result = import.download(&download).unwrap();
    assert!(!result.get_is_empty());
    assert_eq!(result.get_range().get_start(), &[sst_range.0]);
    assert_eq!(result.get_range().get_end(), &[sst_range.1 - 1]);

    // Do an ingest and verify the result is correct.

    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta);
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error());

    check_ingested_kvs(&tikv, &ctx, sst_range);
}

#[test]
fn test_cleanup_sst() {
    let (mut cluster, ctx, _, import) = new_cluster_and_tikv_import_client();

    let temp_dir = Builder::new().prefix("test_cleanup_sst").tempdir().unwrap();

    let sst_path = temp_dir.path().join("test_split.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    send_upload_sst(&import, &meta, &data).unwrap();

    // Can not upload the same file when it exists.
    assert_to_string_contains!(
        send_upload_sst(&import, &meta, &data).unwrap_err(),
        "FileExists"
    );

    // The uploaded SST should be deleted if the region split.
    let region = cluster.get_region(&[]);
    cluster.must_split(&region, &[100]);

    check_sst_deleted(&import, &meta, &data);

    let left = cluster.get_region(&[]);
    let right = cluster.get_region(&[100]);

    let sst_path = temp_dir.path().join("test_merge.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(left.get_id());
    meta.set_region_epoch(left.get_region_epoch().clone());

    send_upload_sst(&import, &meta, &data).unwrap();

    // The uploaded SST should be deleted if the region merged.
    cluster.pd_client.must_merge(left.get_id(), right.get_id());
    let res = block_on(cluster.pd_client.get_region_by_id(left.get_id()));
    assert_eq!(res.unwrap(), None);

    check_sst_deleted(&import, &meta, &data);
}

#[test]
fn test_ingest_sst_region_not_found() {
    let (_cluster, mut ctx_not_found, _, import) = new_cluster_and_tikv_import_client();

    let temp_dir = Builder::new()
        .prefix("test_ingest_sst_errors")
        .tempdir()
        .unwrap();

    ctx_not_found.set_region_id(1 << 31); // A large region id that must no exists.
    let sst_path = temp_dir.path().join("test_split.sst");
    let sst_range = (0, 100);
    let (mut meta, _data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(ctx_not_found.get_region_id());
    meta.set_region_epoch(ctx_not_found.get_region_epoch().clone());

    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx_not_found);
    ingest.set_sst(meta);
    let resp = import.ingest(&ingest).unwrap();
    assert!(resp.get_error().has_region_not_found());
}
