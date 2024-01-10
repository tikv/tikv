// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use futures::{executor::block_on, stream::StreamExt};
use kvproto::{import_sstpb::*, kvrpcpb::Context, tikvpb::*};
use pd_client::PdClient;
use tempfile::Builder;
use test_sst_importer::*;
use tikv::config::TikvConfig;
use tikv_util::config::ReadableSize;

use super::util::*;

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
    let mut cfg = TikvConfig::default();
    let cleanup_interval = Duration::from_millis(10);
    cfg.raft_store.split_region_check_tick_interval.0 = cleanup_interval;
    cfg.raft_store.pd_heartbeat_tick_interval.0 = cleanup_interval;
    cfg.raft_store.report_region_buckets_tick_interval.0 = cleanup_interval;
    cfg.coprocessor.enable_region_bucket = Some(true);
    cfg.coprocessor.region_bucket_size = ReadableSize(200);
    cfg.raft_store.region_split_check_diff = Some(ReadableSize(200));
    cfg.server.addr = "127.0.0.1:0".to_owned();
    cfg.server.grpc_concurrency = 1;
    let (cluster, ctx, _tikv, import) = open_cluster_and_tikv_import_client(Some(cfg));

    let temp_dir = Builder::new().prefix("test_ingest_sst").tempdir().unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 255);
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

    for _ in 0..10 {
        let region_keys = cluster
            .pd_client
            .get_region_approximate_keys(ctx.get_region_id())
            .unwrap_or_default();
        if region_keys != 255 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            continue;
        }

        let buckets = cluster
            .pd_client
            .get_buckets(ctx.get_region_id())
            .unwrap_or_default();
        if buckets.meta.keys.len() <= 2 {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        return;
    }
    panic!("region keys is not 255 or buckets keys len less than 2")
}

fn switch_mode(import: &ImportSstClient, range: Range, mode: SwitchMode) {
    let mut switch_req = SwitchModeRequest::default();
    switch_req.set_mode(mode);
    switch_req.set_ranges(vec![range].into());
    let _ = import.switch_mode(&switch_req).unwrap();
}

#[test]
fn test_switch_mode_v2() {
    let mut cfg = TikvConfig::default();
    cfg.server.grpc_concurrency = 1;
    cfg.rocksdb.writecf.disable_auto_compactions = true;
    cfg.raft_store.right_derive_when_split = true;
    // cfg.rocksdb.writecf.level0_slowdown_writes_trigger = Some(2);
    let (mut cluster, mut ctx, _tikv, import) = open_cluster_and_tikv_import_client_v2(Some(cfg));

    let region = cluster.get_region(b"");
    cluster.must_split(&region, &[50]);
    let region = cluster.get_region(&[50]);
    ctx.set_region_epoch(region.get_region_epoch().clone());

    let mut key_range = Range::default();
    key_range.set_start([50].to_vec());
    switch_mode(&import, key_range.clone(), SwitchMode::Import);

    let temp_dir = Builder::new().prefix("test_ingest_sst").tempdir().unwrap();

    let upload_and_ingest =
        |sst_range, import: &ImportSstClient, path_name, ctx: &Context| -> IngestResponse {
            let sst_path = temp_dir.path().join(path_name);
            let (mut meta, data) = gen_sst_file(sst_path, sst_range);
            meta.set_cf_name("write".to_string());
            // Set region id and epoch.
            meta.set_region_id(ctx.get_region_id());
            meta.set_region_epoch(ctx.get_region_epoch().clone());
            send_upload_sst(import, &meta, &data).unwrap();
            let mut ingest = IngestRequest::default();
            ingest.set_context(ctx.clone());
            ingest.set_sst(meta);
            import.ingest(&ingest).unwrap()
        };

    // The first one will be ingested at the bottom level. And as the following ssts
    // are overlapped with the previous one, they will all be ingested at level 0.
    for i in 0..10 {
        let resp = upload_and_ingest((50, 100), &import, format!("test{}.sst", i), &ctx);
        assert!(!resp.has_error());
    }

    // For this region, it is not in the key range, so it is normal mode.
    let region = cluster.get_region(&[20]);
    let mut ctx2 = ctx.clone();
    ctx2.set_region_id(region.get_id());
    ctx2.set_region_epoch(region.get_region_epoch().clone());
    ctx2.set_peer(region.get_peers()[0].clone());
    for i in 0..6 {
        let resp = upload_and_ingest((0, 49), &import, format!("test-{}.sst", i), &ctx2);
        if i < 5 {
            assert!(!resp.has_error());
        } else {
            assert!(resp.get_error().has_server_is_busy());
        }
    }
    // Propose another switch mode request to let this region to ingest.
    let mut key_range2 = Range::default();
    key_range2.set_end([50].to_vec());
    switch_mode(&import, key_range2.clone(), SwitchMode::Import);
    let resp = upload_and_ingest((0, 49), &import, "test-6.sst".to_string(), &ctx2);
    assert!(!resp.has_error());
    // switching back to normal should make further ingest be rejected
    switch_mode(&import, key_range2, SwitchMode::Normal);
    let resp = upload_and_ingest((0, 49), &import, "test-7.sst".to_string(), &ctx2);
    assert!(resp.get_error().has_server_is_busy());

    // switch back to normal, so region 1 also starts to reject
    switch_mode(&import, key_range, SwitchMode::Normal);
    let resp = upload_and_ingest((50, 100), &import, "test10".to_string(), &ctx);
    assert!(resp.get_error().has_server_is_busy());
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
    download.set_storage_backend(external_storage_export::make_local_backend(temp_dir.path()));
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
fn test_cleanup_sst_v2() {
    let (mut cluster, ctx, _, import) = open_cluster_and_tikv_import_client_v2(None);

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

    // upload an SST of an unexisted region
    let sst_path = temp_dir.path().join("test_non_exist.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(9999);
    send_upload_sst(&import, &meta, &data).unwrap();
    // This should be cleanuped
    check_sst_deleted(&import, &meta, &data);

    let mut key_range = Range::default();
    key_range.set_start([50].to_vec());
    key_range.set_start([70].to_vec());
    // switch to import so that the overlapped sst will not be cleanuped
    switch_mode(&import, key_range.clone(), SwitchMode::Import);
    let sst_path = temp_dir.path().join("test_non_exist1.sst");
    let sst_range = (60, 80);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(9999);
    send_upload_sst(&import, &meta, &data).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert_to_string_contains!(
        send_upload_sst(&import, &meta, &data).unwrap_err(),
        "FileExists"
    );

    // switch back to normal mode
    switch_mode(&import, key_range, SwitchMode::Normal);
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

#[test]
fn test_ingest_multiple_sst() {
    let (_cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client();

    let temp_dir = Builder::new()
        .prefix("test_ingest_multiple_sst")
        .tempdir()
        .unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_range1 = (0, 100);
    let (mut meta1, data1) = gen_sst_file(sst_path, sst_range1);
    meta1.set_region_id(ctx.get_region_id());
    meta1.set_region_epoch(ctx.get_region_epoch().clone());

    let sst_path2 = temp_dir.path().join("write-test.sst");
    let sst_range2 = (100, 200);
    let (mut meta2, data2) = gen_sst_file(sst_path2, sst_range2);
    meta2.set_region_id(ctx.get_region_id());
    meta2.set_region_epoch(ctx.get_region_epoch().clone());
    meta2.set_cf_name("write".to_owned());

    send_upload_sst(&import, &meta1, &data1).unwrap();
    send_upload_sst(&import, &meta2, &data2).unwrap();

    let mut ingest = MultiIngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.mut_ssts().push(meta1);
    ingest.mut_ssts().push(meta2);
    let resp = import.multi_ingest(&ingest).unwrap();
    assert!(!resp.has_error(), "{:?}", resp.get_error());

    // Check ingested kvs
    check_ingested_kvs(&tikv, &ctx, sst_range1);
    check_ingested_kvs_cf(&tikv, &ctx, "write", sst_range2);
}

#[test]
fn test_duplicate_and_close() {
    let (_cluster, ctx, _, import) = new_cluster_and_tikv_import_client();
    let mut req = SwitchModeRequest::default();
    req.set_mode(SwitchMode::Import);
    import.switch_mode(&req).unwrap();

    let data_count: u64 = 4096;
    for commit_ts in 0..4 {
        let mut meta = new_sst_meta(0, 0);
        meta.set_region_id(ctx.get_region_id());
        meta.set_region_epoch(ctx.get_region_epoch().clone());

        let mut keys = vec![];
        let mut values = vec![];
        for i in 1000..data_count {
            let key = i.to_string();
            keys.push(key.as_bytes().to_vec());
            values.push(key.as_bytes().to_vec());
        }
        let resp = send_write_sst(&import, &meta, keys, values, commit_ts).unwrap();
        for m in resp.metas.into_iter() {
            let mut ingest = IngestRequest::default();
            ingest.set_context(ctx.clone());
            ingest.set_sst(m.clone());
            let resp = import.ingest(&ingest).unwrap();
            assert!(!resp.has_error());
        }
    }

    let mut duplicate = DuplicateDetectRequest::default();
    duplicate.set_context(ctx);
    duplicate.set_start_key((0_u64).to_string().as_bytes().to_vec());
    let mut stream = import.duplicate_detect(&duplicate).unwrap();
    let ret = block_on(async move {
        let mut ret: Vec<KvPair> = vec![];
        while let Some(resp) = stream.next().await {
            match resp {
                Ok(mut resp) => {
                    if resp.has_key_error() || resp.has_region_error() {
                        break;
                    }
                    let pairs = resp.take_pairs();
                    ret.append(&mut pairs.into());
                }
                Err(e) => {
                    println!("receive error: {:?}", e);
                    break;
                }
            }
        }
        ret
    });
    assert_eq!(ret.len(), (data_count - 1000) as usize * 4);
    req.set_mode(SwitchMode::Normal);
    import.switch_mode(&req).unwrap();
}

#[test]
fn test_suspend_import() {
    let (_cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client();
    let sst_range = (0, 10);
    let write = |sst_range: (u8, u8)| {
        let mut meta = new_sst_meta(0, 0);
        meta.set_region_id(ctx.get_region_id());
        meta.set_region_epoch(ctx.get_region_epoch().clone());

        let mut keys = vec![];
        let mut values = vec![];
        for i in sst_range.0..sst_range.1 {
            keys.push(vec![i]);
            values.push(vec![i]);
        }
        send_write_sst(&import, &meta, keys, values, 1)
    };
    let ingest = |sst_meta: &SstMeta| {
        let mut ingest = IngestRequest::default();
        ingest.set_context(ctx.clone());
        ingest.set_sst(sst_meta.clone());
        import.ingest(&ingest)
    };
    let multi_ingest = |sst_metas: &[SstMeta]| {
        let mut multi_ingest = MultiIngestRequest::default();
        multi_ingest.set_context(ctx.clone());
        multi_ingest.set_ssts(sst_metas.to_vec().into());
        import.multi_ingest(&multi_ingest)
    };
    let suspendctl = |for_time| {
        let mut req = SuspendImportRpcRequest::default();
        req.set_caller("test_suspend_import".to_owned());
        if for_time == 0 {
            req.set_should_suspend_imports(false);
        } else {
            req.set_should_suspend_imports(true);
            req.set_duration_in_secs(for_time);
        }
        req
    };

    let write_res = write(sst_range).unwrap();
    assert_eq!(write_res.metas.len(), 1);
    let sst = write_res.metas[0].clone();

    assert!(
        !import
            .suspend_import_rpc(&suspendctl(6000))
            .unwrap()
            .already_suspended
    );
    let write_res = write(sst_range);
    write_res.unwrap();
    let ingest_res = ingest(&sst).unwrap();
    assert!(
        ingest_res.get_error().has_server_is_busy(),
        "{:?}",
        ingest_res
    );
    let multi_ingest_res = multi_ingest(&[sst.clone()]).unwrap();
    assert!(
        multi_ingest_res.get_error().has_server_is_busy(),
        "{:?}",
        multi_ingest_res
    );

    assert!(
        import
            .suspend_import_rpc(&suspendctl(0))
            .unwrap()
            .already_suspended
    );

    let ingest_res = ingest(&sst);
    assert!(ingest_res.is_ok(), "{:?} => {:?}", sst, ingest_res);

    check_ingested_txn_kvs(&tikv, &ctx, sst_range, 2);

    // test timeout.
    assert!(
        !import
            .suspend_import_rpc(&suspendctl(1))
            .unwrap()
            .already_suspended
    );
    let sst_range = (10, 20);
    let write_res = write(sst_range);
    let sst = write_res.unwrap().metas;
    let res = multi_ingest(&sst);
    assert!(
        res.as_ref().unwrap().get_error().has_server_is_busy(),
        "{:?}",
        res
    );
    std::thread::sleep(Duration::from_secs(1));
    multi_ingest(&sst).unwrap();

    // check an insane value should be rejected.
    import
        .suspend_import_rpc(&suspendctl(u64::MAX - 42))
        .unwrap_err();
    let sst_range = (20, 30);
    let ssts = write(sst_range).unwrap();
    multi_ingest(ssts.get_metas()).unwrap();
}
