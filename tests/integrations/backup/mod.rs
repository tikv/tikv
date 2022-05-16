// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs::File, time::Duration};

use engine_traits::{CF_DEFAULT, CF_WRITE};
use external_storage_export::{create_storage, make_local_backend};
use file_system::calc_crc32_bytes;
use futures::{executor::block_on, AsyncReadExt, StreamExt};
use kvproto::{
    import_sstpb::*,
    kvrpcpb::*,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request},
};
use tempfile::Builder;
use test_backup::*;
use tikv::coprocessor::checksum_crc64_xor;
use tikv_util::HandyRwLock;
use txn_types::TimeStamp;

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
    let mut suite = TestSuite::new(3, 144 * 1024 * 1024, ApiVersion::V1);
    // 3 version for each key.
    let key_count = 60;
    suite.must_kv_put(key_count, 3);

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let backup_ts = suite.alloc_ts();
    let storage_path = make_unique_dir(tmp.path());
    let rx = suite.backup(
        vec![],   // start
        vec![],   // end
        0.into(), // begin_ts
        backup_ts,
        &storage_path,
    );
    let resps1 = block_on(rx.collect::<Vec<_>>());
    // Only leader can handle backup.
    assert_eq!(resps1.len(), 1);
    let files1 = resps1[0].files.clone();
    // Short value is piggybacked in write cf, so we get 1 sst at least.
    assert!(!resps1[0].get_files().is_empty());

    // Delete all data, there should be no backup files.
    suite.cluster.must_delete_range_cf(CF_DEFAULT, b"", b"");
    suite.cluster.must_delete_range_cf(CF_WRITE, b"", b"");
    // Backup file should have same contents.
    let rx = suite.backup(
        vec![],   // start
        vec![],   // end
        0.into(), // begin_ts
        backup_ts,
        &make_unique_dir(tmp.path()),
    );
    let resps2 = block_on(rx.collect::<Vec<_>>());
    assert!(resps2[0].get_files().is_empty(), "{:?}", resps2);

    // Use importer to restore backup files.
    let backend = make_local_backend(&storage_path);
    let storage = create_storage(&backend, Default::default()).unwrap();
    let region = suite.cluster.get_region(b"");
    let mut sst_meta = SstMeta::default();
    sst_meta.region_id = region.get_id();
    sst_meta.set_region_epoch(region.get_region_epoch().clone());
    sst_meta.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    let mut metas = vec![];
    for f in files1.clone().into_iter() {
        let mut reader = storage.read(&f.name);
        let mut content = vec![];
        block_on(reader.read_to_end(&mut content)).unwrap();
        let mut m = sst_meta.clone();
        m.crc32 = calc_crc32_bytes(&content);
        m.length = content.len() as _;
        m.cf_name = name_to_cf(&f.name).to_owned();
        metas.push((m, content));
    }

    for (m, c) in &metas {
        for importer in suite.cluster.sim.rl().importers.values() {
            let mut f = importer.create(m).unwrap();
            f.append(c).unwrap();
            f.finish().unwrap();
        }

        // Make ingest command.
        let mut ingest = Request::default();
        ingest.set_cmd_type(CmdType::IngestSst);
        ingest.mut_ingest_sst().set_sst(m.clone());
        let mut header = RaftRequestHeader::default();
        let leader = suite.context.get_peer().clone();
        header.set_peer(leader);
        header.set_region_id(suite.context.get_region_id());
        header.set_region_epoch(suite.context.get_region_epoch().clone());
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.mut_requests().push(ingest);
        let resp = suite
            .cluster
            .call_command_on_leader(cmd, Duration::from_secs(5))
            .unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
    }

    // Backup file should have same contents.
    let rx = suite.backup(
        vec![],   // start
        vec![],   // end
        0.into(), // begin_ts
        backup_ts,
        &make_unique_dir(tmp.path()),
    );
    let resps3 = block_on(rx.collect::<Vec<_>>());
    assert_same_files(files1.into_vec(), resps3[0].files.clone().into_vec());

    suite.stop();
}

#[test]
fn test_backup_huge_range_and_import() {
    let mut suite = TestSuite::new(3, 100, ApiVersion::V1);
    // 3 version for each key.
    // make sure we will have two batch files
    let key_count = 1024 * 3 / 2;
    suite.must_kv_put(key_count, 3);

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let backup_ts = suite.alloc_ts();
    let storage_path = make_unique_dir(tmp.path());
    let rx = suite.backup(
        vec![],   // start
        vec![],   // end
        0.into(), // begin_ts
        backup_ts,
        &storage_path,
    );
    let resps1 = block_on(rx.collect::<Vec<_>>());
    // Only leader can handle backup.
    // ... But the response may be split into two parts (when meeting huge region).
    assert_eq!(resps1.len(), 2, "{:?}", resps1);
    let mut files1 = resps1
        .iter()
        .flat_map(|x| x.files.iter())
        .cloned()
        .collect::<Vec<_>>();
    // Short value is piggybacked in write cf, so we get 1 sst at least.
    assert!(!resps1[0].get_files().is_empty());

    // Sort the files for avoiding race conditions. (would this happen?)
    if files1[0].start_key > files1[1].start_key {
        files1.swap(0, 1);
    }
    assert_eq!(resps1[0].start_key, b"".to_vec());
    assert_eq!(resps1[0].end_key, resps1[1].start_key);
    assert_eq!(resps1[1].end_key, b"".to_vec());

    assert_eq!(files1.len(), 2);
    assert_ne!(files1[0].start_key, files1[0].end_key);
    assert_ne!(files1[1].start_key, files1[1].end_key);
    assert_eq!(files1[0].end_key, files1[1].start_key);

    // Use importer to restore backup files.
    let backend = make_local_backend(&storage_path);
    let storage = create_storage(&backend, Default::default()).unwrap();
    let region = suite.cluster.get_region(b"");
    let mut sst_meta = SstMeta::default();
    sst_meta.region_id = region.get_id();
    sst_meta.set_region_epoch(region.get_region_epoch().clone());
    let mut metas = vec![];
    for f in files1.clone().into_iter() {
        let mut reader = storage.read(&f.name);
        let mut content = vec![];
        block_on(reader.read_to_end(&mut content)).unwrap();
        let mut m = sst_meta.clone();
        m.crc32 = calc_crc32_bytes(&content);
        m.length = content.len() as _;
        // set different uuid for each file
        m.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
        m.cf_name = name_to_cf(&f.name).to_owned();
        metas.push((m, content));
    }

    for (m, c) in &metas {
        for importer in suite.cluster.sim.rl().importers.values() {
            let mut f = importer.create(m).unwrap();
            f.append(c).unwrap();
            f.finish().unwrap();
        }

        // Make ingest command.
        let mut ingest = Request::default();
        ingest.set_cmd_type(CmdType::IngestSst);
        ingest.mut_ingest_sst().set_sst(m.clone());
        let mut header = RaftRequestHeader::default();
        let leader = suite.context.get_peer().clone();
        header.set_peer(leader);
        header.set_region_id(suite.context.get_region_id());
        header.set_region_epoch(suite.context.get_region_epoch().clone());
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.mut_requests().push(ingest);
        let resp = suite
            .cluster
            .call_command_on_leader(cmd, Duration::from_secs(5))
            .unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
    }

    // Backup file should have same contents.
    let rx = suite.backup(
        vec![],   // start
        vec![],   // end
        0.into(), // begin_ts
        backup_ts,
        &make_unique_dir(tmp.path()),
    );
    let resps3 = block_on(rx.collect::<Vec<_>>());
    assert_same_files(
        files1,
        resps3
            .iter()
            .flat_map(|x| x.files.iter())
            .cloned()
            .collect(),
    );

    suite.stop();
}

fn test_backup_rawkv_cross_version_impl(cur_api_ver: ApiVersion, dst_api_ver: ApiVersion) {
    let suite = TestSuite::new(3, 144 * 1024 * 1024, cur_api_ver);
    let key_count = 60;

    let cf = match cur_api_ver {
        ApiVersion::V2 => String::from(""),
        _ => String::from(CF_DEFAULT),
    };
    for i in 0..key_count {
        let (k, v) = suite.gen_raw_kv(i);
        suite.must_raw_put(k.clone().into_bytes(), v.clone().into_bytes(), cf.clone());
    }

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let storage_path = make_unique_dir(tmp.path());
    let rx = suite.backup_raw(
        vec![b'r', b'a'], // start
        vec![b'r', b'z'], // end
        cf,
        &storage_path,
        dst_api_ver,
    );
    let resps1 = block_on(rx.collect::<Vec<_>>());
    // Only leader can handle backup.
    assert_eq!(resps1.len(), 1);
    let files1 = resps1[0].files.clone();
    assert!(!resps1[0].get_files().is_empty());

    let mut target_suite = TestSuite::new(3, 144 * 1024 * 1024, dst_api_ver);
    // Use importer to restore backup files.
    let backend = make_local_backend(&storage_path);
    let storage = create_storage(&backend, Default::default()).unwrap();
    let region = target_suite.cluster.get_region(b"");
    let mut sst_meta = SstMeta::default();
    sst_meta.region_id = region.get_id();
    sst_meta.set_region_epoch(region.get_region_epoch().clone());
    sst_meta.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    sst_meta.set_api_version(dst_api_ver);
    let mut metas = vec![];
    for f in files1.clone().into_iter() {
        let mut reader = storage.read(&f.name);
        let mut content = vec![];
        block_on(reader.read_to_end(&mut content)).unwrap();
        let mut m = sst_meta.clone();
        m.crc32 = calc_crc32_bytes(&content);
        m.length = content.len() as _;
        m.cf_name = name_to_cf(&f.name).to_owned();
        metas.push((m, content));
    }

    for (m, c) in &metas {
        for importer in target_suite.cluster.sim.rl().importers.values() {
            let mut f = importer.create(m).unwrap();
            f.append(c).unwrap();
            f.finish().unwrap();
        }

        // Make ingest command.
        let mut ingest = Request::default();
        ingest.set_cmd_type(CmdType::IngestSst);
        ingest.mut_ingest_sst().set_sst(m.clone());
        let mut header = RaftRequestHeader::default();
        let leader = target_suite.context.get_peer().clone();
        header.set_peer(leader);
        header.set_region_id(target_suite.context.get_region_id());
        header.set_region_epoch(target_suite.context.get_region_epoch().clone());
        let mut cmd = RaftCmdRequest::default();
        cmd.set_header(header);
        cmd.mut_requests().push(ingest);
        let resp = target_suite
            .cluster
            .call_command_on_leader(cmd, Duration::from_secs(5))
            .unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
    }

    let cf = match dst_api_ver {
        ApiVersion::V2 => String::from(""),
        _ => String::from(CF_DEFAULT),
    };
    for i in 0..key_count {
        let (k, v) = target_suite.gen_raw_kv(i);
        let key = {
            let mut key = k.into_bytes();
            if cur_api_ver != ApiVersion::V2 && dst_api_ver == ApiVersion::V2 {
                key.insert(0, b'r')
            }
            key
        };
        let ret_val = target_suite.must_raw_get(key, cf.clone());
        assert_eq!(v.clone().into_bytes(), ret_val);
    }

    // Backup file should have same contents.
    // Set non-empty range to check if it's incorrectly encoded.
    let rx = target_suite.backup_raw(
        vec![b'r', b'a'], // start
        vec![b'r', b'z'], // end
        cf,
        &make_unique_dir(tmp.path()),
        dst_api_ver,
    );
    let resps3 = block_on(rx.collect::<Vec<_>>());
    let files3 = resps3[0].files.clone();

    // After https://github.com/tikv/tikv/pull/8707 merged.
    // the backup file name will based on local timestamp.
    // so the two backup's file name may not be same, we should skip this check.
    assert_eq!(files1.len(), 1);
    assert_eq!(files3.len(), 1);
    assert_eq!(files1[0].total_bytes, files3[0].total_bytes);
    assert_eq!(files1[0].total_kvs, files3[0].total_kvs);
    suite.stop();
    target_suite.stop();
}

#[test]
fn test_backup_rawkv_convert() {
    let raw_test_cases = vec![
        (ApiVersion::V1, ApiVersion::V1),
        (ApiVersion::V1ttl, ApiVersion::V1ttl),
        (ApiVersion::V2, ApiVersion::V2),
        (ApiVersion::V1, ApiVersion::V2),
        (ApiVersion::V1ttl, ApiVersion::V2),
    ];
    for (cur_api_ver, dst_api_ver) in raw_test_cases {
        test_backup_rawkv_cross_version_impl(cur_api_ver, dst_api_ver);
    }
}

fn test_backup_raw_meta_impl(cur_api_version: ApiVersion, dst_api_version: ApiVersion) {
    let suite = TestSuite::new(3, 144 * 1024 * 1024, cur_api_version);
    let key_count: u64 = 60;
    let cf = match cur_api_version {
        ApiVersion::V1 | ApiVersion::V1ttl => String::from(CF_DEFAULT),
        ApiVersion::V2 => String::from(""),
    };
    let digest = crc64fast::Digest::new();
    let mut admin_checksum: u64 = 0;
    let mut admin_total_kvs: u64 = 0;
    let mut admin_total_bytes: u64 = 0;

    for i in 0..key_count {
        let (k, v) = suite.gen_raw_kv(i);
        admin_total_kvs += 1;
        admin_total_bytes += (k.len() + v.len()) as u64;
        admin_checksum =
            checksum_crc64_xor(admin_checksum, digest.clone(), k.as_bytes(), v.as_bytes());
        suite.must_raw_put(k.clone().into_bytes(), v.clone().into_bytes(), cf.clone());
    }

    let (store_checksum, store_kvs, store_bytes) =
        suite.storage_raw_checksum("ra".to_owned(), "rz".to_owned());

    assert_eq!(admin_checksum, store_checksum);
    assert_eq!(admin_total_kvs, store_kvs);
    assert_eq!(admin_total_bytes, store_bytes);

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let storage_path = make_unique_dir(tmp.path());
    let rx = suite.backup_raw(
        "ra".to_owned().into_bytes(), // start
        "rz".to_owned().into_bytes(), // end
        cf,
        &storage_path,
        dst_api_version,
    );
    let resps1 = block_on(rx.collect::<Vec<_>>());
    // Only leader can handle backup.
    assert_eq!(resps1.len(), 1);
    let files: Vec<_> = resps1[0].files.clone().into_iter().collect();
    // Short value is piggybacked in write cf, so we get 1 sst at least.
    assert!(!files.is_empty());
    let mut checksum = 0;
    let mut total_kvs = 0;
    let mut total_bytes = 0;
    for f in files {
        checksum ^= f.get_crc64xor();
        total_kvs += f.get_total_kvs();
        total_bytes += f.get_total_bytes();
    }
    assert_eq!(total_kvs, key_count);
    assert_eq!(total_kvs, admin_total_kvs);
    assert_eq!(total_bytes, admin_total_bytes);
    assert_eq!(checksum, admin_checksum);
    // assert_eq!(total_size, 1619); // the number changed when kv size change, should not be an test points.
    // please update this number (must be > 0) when the test failed

    suite.stop();
}

#[test]
fn test_backup_raw_meta() {
    let raw_meta_test_cases = vec![
        (ApiVersion::V1, ApiVersion::V1),
        (ApiVersion::V1ttl, ApiVersion::V1ttl),
        (ApiVersion::V2, ApiVersion::V2),
    ];
    for (cur_api_ver, dst_api_ver) in raw_meta_test_cases {
        test_backup_raw_meta_impl(cur_api_ver, dst_api_ver);
    }
}

#[test]
fn test_invalid_external_storage() {
    let mut suite = TestSuite::new(1, 144 * 1024 * 1024, ApiVersion::V1);
    // Put some data.
    suite.must_kv_put(3, 1);

    // Set backup directory read-only. TiKV fails to backup.
    let tmp = Builder::new().tempdir().unwrap();
    let f = File::open(&tmp.path()).unwrap();
    let mut perms = f.metadata().unwrap().permissions();
    perms.set_readonly(true);
    f.set_permissions(perms.clone()).unwrap();

    let backup_ts = suite.alloc_ts();
    let storage_path = tmp.path();
    let rx = suite.backup(
        vec![],   // start
        vec![],   // end
        0.into(), // begin_ts
        backup_ts,
        storage_path,
    );

    // Wait util the backup request is handled.
    let resps = block_on(rx.collect::<Vec<_>>());
    assert!(resps[0].has_error());

    perms.set_readonly(false);
    f.set_permissions(perms).unwrap();

    suite.stop();
}

#[test]
fn calculated_commit_ts_after_commit() {
    fn test_impl(
        commit_fn: impl FnOnce(&mut TestSuite, /* txn_start_ts */ TimeStamp) -> TimeStamp,
    ) {
        let mut suite = TestSuite::new(1, 144 * 1024 * 1024, ApiVersion::V1);
        // Put some data.
        suite.must_kv_put(3, 1);

        // Begin a txn before backup
        let txn_start_ts = suite.alloc_ts();

        // Trigger backup request.
        let tmp = Builder::new().tempdir().unwrap();
        let backup_ts = suite.alloc_ts();
        let storage_path = make_unique_dir(tmp.path());
        let rx = suite.backup(
            vec![],   // start
            vec![],   // end
            0.into(), // begin_ts
            backup_ts,
            &storage_path,
        );
        let _ = block_on(rx.collect::<Vec<_>>());

        let commit_ts = commit_fn(&mut suite, txn_start_ts);
        assert!(commit_ts > backup_ts);

        suite.stop();
    }

    // Async commit
    test_impl(|suite, start_ts| {
        let (k, v) = (b"my_key", b"my_value");
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.key = k.to_vec();
        mutation.value = v.to_vec();

        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(suite.context.clone());
        prewrite_req.mut_mutations().push(mutation);
        prewrite_req.set_primary_lock(k.to_vec());
        prewrite_req.set_start_version(start_ts.into_inner());
        prewrite_req.set_lock_ttl(2000);
        prewrite_req.set_use_async_commit(true);
        let prewrite_resp = suite.tikv_cli.kv_prewrite(&prewrite_req).unwrap();
        let min_commit_ts: TimeStamp = prewrite_resp.get_min_commit_ts().into();
        assert!(!min_commit_ts.is_zero());
        suite.must_kv_commit(vec![k.to_vec()], start_ts, min_commit_ts);
        min_commit_ts
    });

    // 1PC
    test_impl(|suite, start_ts| {
        let (k, v) = (b"my_key", b"my_value");
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.key = k.to_vec();
        mutation.value = v.to_vec();

        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(suite.context.clone());
        prewrite_req.mut_mutations().push(mutation);
        prewrite_req.set_primary_lock(k.to_vec());
        prewrite_req.set_start_version(start_ts.into_inner());
        prewrite_req.set_lock_ttl(2000);
        prewrite_req.set_try_one_pc(true);
        let prewrite_resp = suite.tikv_cli.kv_prewrite(&prewrite_req).unwrap();
        let commit_ts: TimeStamp = prewrite_resp.get_one_pc_commit_ts().into();
        assert!(!commit_ts.is_zero());
        commit_ts
    });
}
