// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use engine_traits::{CfName, CF_DEFAULT, CF_WRITE};
use external_storage_export::{create_storage, make_local_backend};
use file_system::calc_crc32_bytes;
use futures::{executor::block_on, AsyncReadExt, StreamExt};
use kvproto::{
    brpb::{BackupClient, BackupRequest, BackupResponse},
    import_sstpb::{
        DownloadRequest, ImportSstClient, MultiIngestRequest, SstMeta, SwitchMode,
        SwitchModeRequest,
    },
};
use rand::Rng;
use tempfile::Builder;
use test_cloud_server::ServerCluster;
use tikv::config::TiKvConfig;
use tikv_util::config::{ReadableDuration, ReadableSize};
use txn_types::TimeStamp;

use crate::alloc_node_id;

fn assert_same_file_name(s1: String, s2: String) {
    let tokens1: Vec<&str> = s1.split('_').collect();
    let tokens2: Vec<&str> = s2.split('_').collect();
    assert_eq!(tokens1.len(), tokens2.len());
    // 2_1_1_e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855_1609407693105_write.sst
    // 2_1_1_e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855_1609407693199_write.sst
    // should be equal
    for i in 0..tokens1.len() {
        if i != 4 {
            assert_eq!(tokens1[i], tokens2[i]);
        }
    }
}

fn assert_same_files(mut files1: Vec<kvproto::brpb::File>, mut files2: Vec<kvproto::brpb::File>) {
    assert_eq!(files1.len(), files2.len());
    // Sort here by start key in case of unordered response (by pipelined write + scan)
    // `sort_by_key` couldn't be used here -- rustc would complain that `file.start_key.as_slice()`
    //       may not live long enough. (Is that a bug of rustc?)
    files1.sort_by(|f1, f2| f1.start_key.cmp(&f2.start_key));
    files2.sort_by(|f1, f2| f1.start_key.cmp(&f2.start_key));

    // After https://github.com/tikv/tikv/pull/8707 merged.
    // the backup file name will based on local timestamp.
    // so the two backup's file name may not be same, we should skip this check.
    for i in 0..files1.len() {
        let mut f1 = files1[i].clone();
        let mut f2 = files2[i].clone();
        assert_same_file_name(f1.name, f2.name);
        f1.name = "".to_string();
        f2.name = "".to_string();
        // the cipher_iv is different because iv is generated randomly
        assert_ne!(f1.cipher_iv, f2.cipher_iv);
        f1.cipher_iv = "".to_string().into_bytes();
        f2.cipher_iv = "".to_string().into_bytes();
        assert_eq!(f1, f2);
    }
}

#[test]
fn test_backup_and_import() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let update_conf = |_, conf: &mut TiKvConfig| {
        conf.backup.sst_max_size = ReadableSize::kb(64);
        conf.raft_store.raft_base_tick_interval = ReadableDuration::millis(100);
        conf.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(50);
        conf.raft_store.local_file_gc_timeout = ReadableDuration::millis(500);
        conf.raft_store.local_file_gc_tick_interval = ReadableDuration::millis(200);
    };
    let mut cluster1 = ServerCluster::new(vec![node_id], update_conf);
    // Backup file should be empty.
    let tmp = Builder::new().tempdir().unwrap();
    let backup_ts = cluster1.get_ts();
    let storage_path = make_unique_dir(tmp.path());
    let resps0 = backup(
        &cluster1,
        vec![],    // start
        vec![255], // end
        0.into(),  // begin_ts
        backup_ts,
        &make_unique_dir(tmp.path()),
    );
    assert!(resps0[0].get_files().is_empty(), "{:?}", resps0);

    // 3 version for each key.
    let key_count = 3000;
    must_kv_put(&cluster1, key_count, 3);

    // Push down backup request.
    let backup_ts = cluster1.get_ts();
    let resps1 = backup(
        &cluster1,
        vec![],
        vec![255],
        0.into(),
        backup_ts,
        &storage_path,
    );
    // Only leader can handle backup.
    assert!(!resps1.is_empty());
    assert!(!resps1[0].get_files().is_empty());
    cluster1.stop();

    // Use importer to restore backup files.
    let node2_id = alloc_node_id();
    let mut cluster2 = ServerCluster::new(vec![node2_id], update_conf);
    let context = cluster2.new_rpc_context(b"");
    let channel = cluster2.get_client_channel(context.get_peer().get_store_id());
    let import_sst_client = ImportSstClient::new(channel);
    let mut switch_mode_req = SwitchModeRequest::default();
    switch_mode_req.set_mode(SwitchMode::Import);
    import_sst_client.switch_mode(&switch_mode_req).unwrap();
    let backend = make_local_backend(&storage_path);
    let storage = create_storage(&backend, Default::default()).unwrap();
    let mut metas = vec![];
    for resp in &resps1 {
        let mut sst_meta = SstMeta::default();
        sst_meta.region_id = context.get_region_id();
        sst_meta.set_region_epoch(context.get_region_epoch().clone());
        sst_meta.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
        for f in resp.get_files() {
            let mut reader = storage.read(&f.name);
            let mut content = vec![];
            block_on(reader.read_to_end(&mut content)).unwrap();
            let mut m = sst_meta.clone();
            m.crc32 = calc_crc32_bytes(&content);
            m.length = content.len() as _;
            m.cf_name = name_to_cf(&f.name).to_owned();
            m.mut_range().set_start(f.get_start_key().to_vec());
            m.mut_range().set_end(f.get_end_key().to_vec());
            let name = f.get_name().to_string();
            metas.push((m, name));
        }
    }
    for store_id in cluster2.get_stores() {
        let channel = cluster2.get_client_channel(store_id);
        let download_client = ImportSstClient::new(channel);
        for (m, name) in &metas {
            let mut download_req = DownloadRequest::new();
            download_req.set_storage_backend(backend.clone());
            download_req.set_name(name.clone());
            download_req.set_sst(m.clone());
            download_client.download(&download_req).unwrap();
        }
    }
    // Make ingest command.
    let mut ingest = MultiIngestRequest::new();
    ingest.set_context(context);
    for (m, _) in &metas {
        ingest.mut_ssts().push(m.clone());
    }
    let resp = import_sst_client.multi_ingest(&ingest).unwrap();
    assert!(!resp.has_error(), "{:?}", resp);

    // Backup file should have same contents.
    let resps2 = backup(
        &cluster2,
        vec![],
        vec![255],
        0.into(),
        backup_ts,
        &make_unique_dir(tmp.path()),
    );
    let mut files1 = vec![];
    for resp in resps1 {
        files1.extend_from_slice(resp.get_files());
    }
    let mut files2 = vec![];
    for resp in resps2 {
        files2.extend_from_slice(resp.get_files());
    }
    assert_same_files(files1, files2);
    let mut switch_mode_req = SwitchModeRequest::default();
    switch_mode_req.set_mode(SwitchMode::Normal);
    import_sst_client.switch_mode(&switch_mode_req).unwrap();
    std::thread::sleep(Duration::from_secs(2));
    let importer = cluster2.get_sst_importer(node2_id);
    let ssts = importer.list_ssts().unwrap();
    assert!(ssts.is_empty());
    cluster2.stop();
}

pub fn must_kv_put(cluster: &ServerCluster, key_count: usize, versions: usize) {
    let mut batch = Vec::with_capacity(1024);
    let mut keys = Vec::with_capacity(1024);
    // Write 50 times to include more different ts.
    let batch_size = cmp::min(cmp::max(key_count / 50, 1), 1024);
    for _ in 0..versions {
        let mut j = 0;
        while j < key_count {
            let start_ts = cluster.get_ts();
            let limit = cmp::min(key_count, j + batch_size);
            batch.clear();
            keys.clear();
            for i in j..limit {
                let (k, v) = (format!("key_{}", i), format!("value_{}", i));
                keys.push(k.clone().into_bytes());
                let mutation = test_cloud_server::put_mut(&k, &v.repeat(50));
                batch.push(mutation);
            }
            cluster.kv_prewrite(batch.split_off(0), keys[0].clone(), start_ts);
            // Commit
            let commit_ts = cluster.get_ts();
            cluster.kv_commit(keys.split_off(0), start_ts, commit_ts);
            j = limit;
        }
    }
}

pub fn backup(
    cluster: &ServerCluster,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    begin_ts: TimeStamp,
    backup_ts: TimeStamp,
    path: &Path,
) -> Vec<BackupResponse> {
    let mut req = BackupRequest::default();
    req.set_start_key(start_key);
    req.set_end_key(end_key);
    req.set_cf(CF_WRITE.to_string());
    req.start_version = begin_ts.into_inner();
    req.end_version = backup_ts.into_inner();
    req.set_storage_backend(make_local_backend(path));
    req.set_is_raw_kv(false);
    let stores = cluster.get_stores();
    let mut resps = vec![];
    for store_id in stores {
        let channel = cluster.get_client_channel(store_id);
        let client = BackupClient::new(channel);
        let mut stream = client.backup(&req).unwrap();
        loop {
            let (result, s) = block_on(stream.into_future());
            stream = s;
            if let Some(res) = result {
                resps.push(res.unwrap());
                continue;
            }
            break;
        }
    }
    resps
}

// Extract CF name from sst name.
pub fn name_to_cf(name: &str) -> CfName {
    if name.contains(CF_DEFAULT) {
        CF_DEFAULT
    } else if name.contains(CF_WRITE) {
        CF_WRITE
    } else {
        unreachable!()
    }
}

pub fn make_unique_dir(path: &Path) -> PathBuf {
    let uid: u64 = rand::thread_rng().gen();
    let tmp_suffix = format!("{:016x}", uid);
    let unique = path.join(tmp_suffix);
    fs::create_dir_all(&unique).unwrap();
    unique
}
