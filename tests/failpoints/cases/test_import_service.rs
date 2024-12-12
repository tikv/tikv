// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc::channel, Arc, Mutex},
    time::Duration,
};

use file_system::calc_crc32;
use futures::{
    executor::block_on,
    stream::{self, StreamExt},
    SinkExt,
};
use grpcio::{ChannelBuilder, Environment, Result, WriteFlags};
use kvproto::{disk_usage::DiskUsage, import_sstpb::*, tikvpb_grpc::TikvClient};
use tempfile::{Builder, TempDir};
use test_raftstore::{must_raw_put, Simulator};
use test_sst_importer::*;
use tikv::config::TikvConfig;
use tikv_util::{config::ReadableSize, sys::disk, HandyRwLock};

#[allow(dead_code)]
#[path = "../../integrations/import/util.rs"]
mod util;
use self::util::{
    check_ingested_kvs, new_cluster_and_tikv_import_client, new_cluster_and_tikv_import_client_tde,
    open_cluster_and_tikv_import_client_v2, send_upload_sst,
};
use crate::cases::test_import_service::util::{new_sst_meta, send_write_sst};

// Opening sst writer involves IO operation, it may block threads for a while.
// Test if download sst works when opening sst writer is blocked.
#[test]
fn test_download_sst_blocking_sst_writer() {
    let (_cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client();
    let temp_dir = Builder::new()
        .prefix("test_download_sst_blocking_sst_writer")
        .tempdir()
        .unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, _) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    // Sleep 20s, make sure it is large than grpc_keepalive_timeout (3s).
    let sst_writer_open_fp = "on_open_sst_writer";
    fail::cfg(sst_writer_open_fp, "sleep(20000)").unwrap();

    // Now perform a proper download.
    let mut download = DownloadRequest::default();
    download.set_sst(meta.clone());
    download.set_storage_backend(external_storage_export::make_local_backend(temp_dir.path()));
    download.set_name("test.sst".to_owned());
    download.mut_sst().mut_range().set_start(vec![sst_range.1]);
    download
        .mut_sst()
        .mut_range()
        .set_end(vec![sst_range.1 + 1]);
    download.mut_sst().mut_range().set_start(Vec::new());
    download.mut_sst().mut_range().set_end(Vec::new());
    let result = import.download(&download).unwrap();
    assert!(!result.get_is_empty());
    assert_eq!(result.get_range().get_start(), &[sst_range.0]);
    assert_eq!(result.get_range().get_end(), &[sst_range.1 - 1]);

    fail::remove(sst_writer_open_fp);

    // Do an ingest and verify the result is correct.
    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta);
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error());

    check_ingested_kvs(&tikv, &ctx, sst_range);
}

fn upload_sst(import: &ImportSstClient, meta: &SstMeta, data: &[u8]) -> Result<UploadResponse> {
    let mut r1 = UploadRequest::default();
    r1.set_meta(meta.clone());
    let mut r2 = UploadRequest::default();
    r2.set_data(data.to_vec());
    let reqs: Vec<_> = vec![r1, r2]
        .into_iter()
        .map(|r| Result::Ok((r, WriteFlags::default())))
        .collect();
    let (mut tx, rx) = import.upload().unwrap();
    let mut stream = stream::iter(reqs);
    block_on(async move {
        tx.send_all(&mut stream).await?;
        tx.close().await?;
        rx.await
    })
}

#[test]
fn test_download_to_full_disk() {
    let (_cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client();
    let temp_dir = Builder::new()
        .prefix("test_download_sst_blocking_sst_writer")
        .tempdir()
        .unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, _) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    // Now perform a proper download.
    let mut download = DownloadRequest::default();
    download.set_sst(meta.clone());
    download.set_storage_backend(external_storage_export::make_local_backend(temp_dir.path()));
    download.set_name("test.sst".to_owned());
    download.mut_sst().mut_range().set_start(vec![sst_range.1]);
    download
        .mut_sst()
        .mut_range()
        .set_end(vec![sst_range.1 + 1]);
    download.mut_sst().mut_range().set_start(Vec::new());
    download.mut_sst().mut_range().set_end(Vec::new());
    disk::set_disk_status(DiskUsage::AlmostFull);
    let result = import.download(&download).unwrap();
    assert!(!result.get_is_empty());
    assert!(result.has_error());
    assert_eq!(
        result.get_error().get_message(),
        "TiKV disk space is not enough."
    );
    disk::set_disk_status(DiskUsage::Normal);
}

#[test]
fn test_ingest_reentrant() {
    let (cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client();

    let temp_dir = Builder::new()
        .prefix("test_ingest_reentrant")
        .tempdir()
        .unwrap();

    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());
    upload_sst(&import, &meta, &data).unwrap();

    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx);
    ingest.set_sst(meta.clone());

    // Don't delete ingested sst file or we cannot find sst file in next ingest.
    fail::cfg("dont_delete_ingested_sst", "1*return").unwrap();

    let node_id = *cluster.sim.rl().get_node_ids().iter().next().unwrap();
    // Use sst save path to track the sst file checksum.
    let save_path = cluster
        .sim
        .rl()
        .importers
        .get(&node_id)
        .unwrap()
        .get_path(&meta);

    let checksum1 = calc_crc32(save_path.clone()).unwrap();
    // Do ingest and it will ingest successs.
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error());

    let checksum2 = calc_crc32(save_path).unwrap();
    // TODO: Remove this once write_global_seqno is deprecated.
    // Checksums are the same since the global seqno in the SST file no longer gets
    // updated with the default setting, which is write_global_seqno=false.
    assert_eq!(checksum1, checksum2);
    // Do ingest again and it can be reentrant
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error());
}

#[test]
fn test_ingest_key_manager_delete_file_failed() {
    // test with tde
    let (_tmp_key_dir, cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client_tde();

    let temp_dir = Builder::new()
        .prefix("test_download_sst_blocking_sst_writer")
        .tempdir()
        .unwrap();
    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());

    upload_sst(&import, &meta, &data).unwrap();

    let deregister_fp = "key_manager_fails_before_delete_file";
    // the first delete is in check before ingest, the second is in ingest cleanup
    // set the ingest clean up failed to trigger remove file but not remove key
    // condition
    fail::cfg(deregister_fp, "1*off->1*return->off").unwrap();

    // Do an ingest and verify the result is correct. Though the ingest succeeded,
    // the clone file is still in the key manager
    // TODO: how to check the key manager contains the clone key
    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta.clone());
    let resp = import.ingest(&ingest).unwrap();

    assert!(!resp.has_error());

    fail::remove(deregister_fp);

    let node_id = *cluster.sim.rl().get_node_ids().iter().next().unwrap();
    let save_path = cluster
        .sim
        .rl()
        .importers
        .get(&node_id)
        .unwrap()
        .get_path(&meta);
    // wait up to 5 seconds to make sure raw uploaded file is deleted by the async
    // clean up task.
    for _ in 0..50 {
        if !save_path.as_path().exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(!save_path.as_path().exists());

    // Do upload and ingest again, though key manager contains this file, the ingest
    // action should success.
    upload_sst(&import, &meta, &data).unwrap();
    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx);
    ingest.set_sst(meta);
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error());
}

#[test]
fn test_ingest_file_twice_and_conflict() {
    // test with tde
    let (_tmp_key_dir, _cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client_tde();

    let temp_dir = Builder::new()
        .prefix("test_ingest_file_twice_and_conflict")
        .tempdir()
        .unwrap();
    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());
    upload_sst(&import, &meta, &data).unwrap();
    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx);
    ingest.set_sst(meta);

    let latch_fp = "import::sst_service::ingest";
    let (tx1, rx1) = channel();
    let (tx2, rx2) = channel();
    let tx1 = Arc::new(Mutex::new(tx1));
    let rx2 = Arc::new(Mutex::new(rx2));
    fail::cfg_callback(latch_fp, move || {
        tx1.lock().unwrap().send(()).unwrap();
        rx2.lock().unwrap().recv().unwrap();
    })
    .unwrap();
    let resp_recv = import.ingest_async(&ingest).unwrap();

    // Make sure the before request has acquired lock.
    rx1.recv().unwrap();

    let resp = import.ingest(&ingest).unwrap();
    assert!(resp.has_error());
    assert_eq!("ingest file conflict", resp.get_error().get_message());
    tx2.send(()).unwrap();
    let resp = block_on(resp_recv).unwrap();
    assert!(!resp.has_error());

    fail::remove(latch_fp);
    let resp = import.ingest(&ingest).unwrap();
    assert!(resp.has_error());
    assert_eq!(
        "The file which would be ingested doest not exist.",
        resp.get_error().get_message()
    );
}

#[test]
fn test_delete_sst_v2_after_epoch_stale() {
    let mut config = TikvConfig::default();
    config.server.addr = "127.0.0.1:0".to_owned();
    let cleanup_interval = Duration::from_millis(10);
    config.raft_store.cleanup_import_sst_interval.0 = cleanup_interval;
    config.server.grpc_concurrency = 1;

    let (mut cluster, ctx, _tikv, import) = open_cluster_and_tikv_import_client_v2(Some(config));
    let temp_dir = Builder::new().prefix("test_ingest_sst").tempdir().unwrap();
    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    // disable data flushed
    fail::cfg("on_flush_completed", "return()").unwrap();
    send_upload_sst(&import, &meta, &data).unwrap();
    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta.clone());
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());
    send_upload_sst(&import, &meta, &data).unwrap();
    ingest.set_sst(meta.clone());

    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error(), "{:?}", resp.get_error());
    let (tx, rx) = channel::<()>();
    let tx = Arc::new(Mutex::new(tx));
    fail::cfg_callback("on_cleanup_import_sst_schedule", move || {
        tx.lock().unwrap().send(()).unwrap();
    })
    .unwrap();
    rx.recv_timeout(std::time::Duration::from_millis(100))
        .unwrap();
    assert_eq!(1, sst_file_count(&cluster.paths));

    // test restart cluster
    cluster.stop_node(1);
    cluster.start().unwrap();
    let count = sst_file_count(&cluster.paths);
    assert_eq!(1, count);

    // delete sts if the region epoch is stale.
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let (tx, rx) = channel::<()>();
    let tx = Arc::new(Mutex::new(tx));
    fail::cfg_callback("on_cleanup_import_sst_schedule", move || {
        tx.lock().unwrap().send(()).unwrap();
    })
    .unwrap();
    let region = cluster.get_region(b"zk10");
    pd_client.must_split_region(
        region,
        kvproto::pdpb::CheckPolicy::Usekey,
        vec![b"zk10".to_vec()],
    );

    rx.recv_timeout(std::time::Duration::from_millis(100))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert_eq!(0, sst_file_count(&cluster.paths));

    // test restart cluster
    cluster.stop_node(1);
    cluster.start().unwrap();
    let count = sst_file_count(&cluster.paths);
    assert_eq!(0, count);
    fail::remove("on_flush_completed");
}

#[test]
fn test_delete_sst_after_applied_sst() {
    // disable data flushed
    fail::cfg("on_flush_completed", "return()").unwrap();
    let (mut cluster, ctx, _tikv, import) = open_cluster_and_tikv_import_client_v2(None);
    let temp_dir = Builder::new().prefix("test_ingest_sst").tempdir().unwrap();
    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 100);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    // No region id and epoch.
    send_upload_sst(&import, &meta, &data).unwrap();
    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta.clone());
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());
    send_upload_sst(&import, &meta, &data).unwrap();
    ingest.set_sst(meta.clone());
    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error(), "{:?}", resp.get_error());

    // restart node
    cluster.stop_node(1);
    cluster.start().unwrap();
    let count = sst_file_count(&cluster.paths);
    assert_eq!(1, count);

    // flush manual
    fail::remove("on_flush_completed");
    let (tx, rx) = channel::<()>();
    let tx = Arc::new(Mutex::new(tx));
    fail::cfg_callback("on_flush_completed", move || {
        tx.lock().unwrap().send(()).unwrap();
    })
    .unwrap();
    for i in 0..count {
        cluster.must_put(format!("k-{}", i).as_bytes(), b"v");
    }
    cluster.flush_data();
    rx.recv_timeout(std::time::Duration::from_millis(100))
        .unwrap();
    fail::remove("on_flush_completed");
    std::thread::sleep(std::time::Duration::from_millis(100));
    let count = sst_file_count(&cluster.paths);
    assert_eq!(0, count);

    cluster.stop_node(1);
    cluster.start().unwrap();
}

#[test]
fn test_split_buckets_after_ingest_sst_v2() {
    let mut config = TikvConfig::default();
    config.server.addr = "127.0.0.1:0".to_owned();
    let cleanup_interval = Duration::from_millis(10);
    config.raft_store.split_region_check_tick_interval.0 = cleanup_interval;
    config.raft_store.pd_heartbeat_tick_interval.0 = cleanup_interval;
    config.raft_store.report_region_buckets_tick_interval.0 = cleanup_interval;
    config.coprocessor.enable_region_bucket = Some(true);
    config.coprocessor.region_bucket_size = ReadableSize(200);
    config.raft_store.region_split_check_diff = Some(ReadableSize(200));
    config.server.grpc_concurrency = 1;

    let (cluster, ctx, _tikv, import) = open_cluster_and_tikv_import_client_v2(Some(config));
    let temp_dir = Builder::new().prefix("test_ingest_sst").tempdir().unwrap();
    let sst_path = temp_dir.path().join("test.sst");
    let sst_range = (0, 255);
    let (mut meta, data) = gen_sst_file(sst_path, sst_range);
    send_upload_sst(&import, &meta, &data).unwrap();
    let mut ingest = IngestRequest::default();
    ingest.set_context(ctx.clone());
    ingest.set_sst(meta.clone());
    meta.set_region_id(ctx.get_region_id());
    meta.set_region_epoch(ctx.get_region_epoch().clone());
    send_upload_sst(&import, &meta, &data).unwrap();
    ingest.set_sst(meta.clone());

    let resp = import.ingest(&ingest).unwrap();
    assert!(!resp.has_error(), "{:?}", resp.get_error());

    let (tx, rx) = channel::<()>();
    let tx = Arc::new(Mutex::new(tx));
    fail::cfg_callback("on_update_region_keys", move || {
        tx.lock().unwrap().send(()).unwrap();
    })
    .unwrap();
    rx.recv_timeout(std::time::Duration::from_millis(100))
        .unwrap();

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

fn sst_file_count(paths: &Vec<TempDir>) -> u64 {
    let mut count = 0;
    for path in paths {
        let sst_dir = path.path().join("import-sst");
        for entry in std::fs::read_dir(sst_dir).unwrap() {
            let entry = entry.unwrap();
            if entry
                .path()
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap()
                .contains("0_0_0")
            {
                continue;
            }
            if entry.file_type().unwrap().is_file() {
                count += 1;
            }
        }
    }
    count
}

#[test]
fn test_flushed_applied_index_after_ingset() {
    // disable data flushed
    fail::cfg("on_flush_completed", "return()").unwrap();
    // disable data flushed
    let (mut cluster, ctx, _tikv, import) = open_cluster_and_tikv_import_client_v2(None);
    let temp_dir = Builder::new().prefix("test_ingest_sst").tempdir().unwrap();
    let sst_path = temp_dir.path().join("test.sst");

    // Create clients.
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(Arc::clone(&env)).connect(&cluster.sim.rl().get_addr(1));
    let client = TikvClient::new(channel);

    for i in 0..5 {
        let sst_range = (i * 20, (i + 1) * 20);
        let (mut meta, data) = gen_sst_file(sst_path.clone(), sst_range);
        // No region id and epoch.
        send_upload_sst(&import, &meta, &data).unwrap();
        let mut ingest = IngestRequest::default();
        ingest.set_context(ctx.clone());
        ingest.set_sst(meta.clone());
        meta.set_region_id(ctx.get_region_id());
        meta.set_region_epoch(ctx.get_region_epoch().clone());
        send_upload_sst(&import, &meta, &data).unwrap();
        ingest.set_sst(meta.clone());
        let resp = import.ingest(&ingest).unwrap();
        assert!(!resp.has_error(), "{:?}", resp.get_error());
    }

    // only 1 sst left because there is no more event to trigger a raft ready flush.
    let count = sst_file_count(&cluster.paths);
    assert_eq!(1, count);

    for i in 5..8 {
        let sst_range = (i * 20, (i + 1) * 20);
        let (mut meta, data) = gen_sst_file(sst_path.clone(), sst_range);
        // No region id and epoch.
        send_upload_sst(&import, &meta, &data).unwrap();
        let mut ingest = IngestRequest::default();
        ingest.set_context(ctx.clone());
        ingest.set_sst(meta.clone());
        meta.set_region_id(ctx.get_region_id());
        meta.set_region_epoch(ctx.get_region_epoch().clone());
        send_upload_sst(&import, &meta, &data).unwrap();
        ingest.set_sst(meta.clone());
        let resp = import.ingest(&ingest).unwrap();
        assert!(!resp.has_error(), "{:?}", resp.get_error());
    }

    // ingest more sst files, unflushed index still be 1.
    let count = sst_file_count(&cluster.paths);
    assert_eq!(1, count);

    // file a write to trigger ready flush, even if the write is not flushed.
    must_raw_put(&client, ctx, b"key1".to_vec(), b"value1".to_vec());
    let count = sst_file_count(&cluster.paths);
    assert_eq!(0, count);

    // restart node, should not tirgger any ingest
    fail::cfg("on_apply_ingest", "panic").unwrap();
    cluster.stop_node(1);
    cluster.start().unwrap();
    let count = sst_file_count(&cluster.paths);
    assert_eq!(0, count);

    fail::remove("on_apply_ingest");
    fail::remove("on_flush_completed");
}

#[test]
fn test_duplicate_detect_with_client_stop() {
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

    // failed to get snapshot. and stream is normal, it will get response with err.
    fail::cfg("failed_to_async_snapshot", "return()").unwrap();
    let mut stream = import.duplicate_detect(&duplicate).unwrap();
    let resp = block_on(async move {
        let resp: DuplicateDetectResponse = stream.next().await.unwrap().unwrap();
        resp
    });
    assert_eq!(
        resp.get_region_error().get_message(),
        "faild to get snapshot"
    );

    // failed to get snapshot, and stream stops.
    // A stopeed remote don't cause panic in server.
    let stream = import.duplicate_detect(&duplicate).unwrap();
    drop(stream);

    // drop stream after received part of response.
    // A stopped remote must not cause panic at server.
    fail::remove("failed_to_async_snapshot");
    let mut stream = import.duplicate_detect(&duplicate).unwrap();
    let ret: Vec<KvPair> = block_on(async move {
        let mut resp: DuplicateDetectResponse = stream.next().await.unwrap().unwrap();
        let pairs = resp.take_pairs();
        // drop stream, Do not cause panic at server.
        drop(stream);
        pairs.into()
    });

    assert_eq!(ret.len(), 4096);

    // call duplicate_detect() successfully.
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
