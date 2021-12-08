// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs::File, time::Duration};

use engine_traits::{CF_DEFAULT, CF_WRITE};
use external_storage_export::{create_storage, make_local_backend};
use file_system::calc_crc32_bytes;
use futures::{executor::block_on, AsyncReadExt, StreamExt};
use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::*;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request};
use tempfile::Builder;
use test_backup::*;
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

fn assert_same_files(files1: Vec<kvproto::brpb::File>, files2: Vec<kvproto::brpb::File>) {
    assert_eq!(files1.len(), files2.len());

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
    let mut suite = TestSuite::new(3, 144 * 1024 * 1024);
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
    let mut suite = TestSuite::new(3, 100);
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
    assert_eq!(resps1.len(), 1);
    let files1 = resps1[0].files.clone();
    // Short value is piggybacked in write cf, so we get 1 sst at least.
    assert!(!resps1[0].get_files().is_empty());
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
    assert_same_files(files1.into_vec(), resps3[0].files.clone().into_vec());

    suite.stop();
}

#[test]
fn test_backup_meta() {
    let mut suite = TestSuite::new(3, 144 * 1024 * 1024);
    // 3 version for each key.
    let key_count = 60;
    suite.must_kv_put(key_count, 3);

    let backup_ts = suite.alloc_ts();
    // key are order by lexicographical order, 'a'-'z' will cover all
    let (admin_checksum, admin_total_kvs, admin_total_bytes) =
        suite.admin_checksum(backup_ts, "a".to_owned(), "z".to_owned());

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
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
    assert_eq!(total_kvs, key_count as u64);
    assert_eq!(total_kvs, admin_total_kvs);
    assert_eq!(total_bytes, admin_total_bytes);
    assert_eq!(checksum, admin_checksum);

    suite.stop();
}

#[test]
fn test_backup_rawkv() {
    let mut suite = TestSuite::new(3, 144 * 1024 * 1024);
    let key_count = 60;

    let cf = String::from(CF_DEFAULT);
    for i in 0..key_count {
        let (k, v) = suite.gen_raw_kv(i);
        suite.must_raw_put(k.clone().into_bytes(), v.clone().into_bytes(), cf.clone());
    }

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let storage_path = make_unique_dir(tmp.path());
    let rx = suite.backup_raw(
        vec![b'a'], // start
        vec![b'z'], // end
        cf.clone(),
        &storage_path,
    );
    let resps1 = block_on(rx.collect::<Vec<_>>());
    // Only leader can handle backup.
    assert_eq!(resps1.len(), 1);
    let files1 = resps1[0].files.clone();
    assert!(!resps1[0].get_files().is_empty());

    // Delete all data, there should be no backup files.
    suite.cluster.must_delete_range_cf(CF_DEFAULT, b"", b"");
    // Backup file should have same contents.
    let rx = suite.backup_raw(
        vec![], // start
        vec![], // end
        cf.clone(),
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
    // Set non-empty range to check if it's incorrectly encoded.
    let rx = suite.backup_raw(
        vec![b'a'], // start
        vec![b'z'], // end
        cf,
        &make_unique_dir(tmp.path()),
    );
    let resps3 = block_on(rx.collect::<Vec<_>>());
    let files3 = resps3[0].files.clone();

    // After https://github.com/tikv/tikv/pull/8707 merged.
    // the backup file name will based on local timestamp.
    // so the two backup's file name may not be same, we should skip this check.
    assert_eq!(files1.len(), 1);
    assert_eq!(files3.len(), 1);
    assert_eq!(files1[0].sha256, files3[0].sha256);
    assert_eq!(files1[0].total_bytes, files3[0].total_bytes);
    assert_eq!(files1[0].total_kvs, files3[0].total_kvs);
    assert_eq!(files1[0].size, files3[0].size);
    suite.stop();
}

#[test]
fn test_backup_raw_meta() {
    let suite = TestSuite::new(3, 144 * 1024 * 1024);
    let key_count: u64 = 60;
    let cf = String::from(CF_DEFAULT);

    for i in 0..key_count {
        let (k, v) = suite.gen_raw_kv(i);
        suite.must_raw_put(k.clone().into_bytes(), v.clone().into_bytes(), cf.clone());
    }
    // Keys are order by lexicographical order, 'a'-'z' will cover all.
    let (admin_checksum, admin_total_kvs, admin_total_bytes) =
        suite.raw_kv_checksum("a".to_owned(), "z".to_owned(), CF_DEFAULT);

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let storage_path = make_unique_dir(tmp.path());
    let rx = suite.backup_raw(
        vec![], // start
        vec![], // end
        cf,
        &storage_path,
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
    let mut total_size = 0;
    for f in files {
        checksum ^= f.get_crc64xor();
        total_kvs += f.get_total_kvs();
        total_bytes += f.get_total_bytes();
        total_size += f.get_size();
    }
    assert_eq!(total_kvs, key_count + 1);
    assert_eq!(total_kvs, admin_total_kvs);
    assert_eq!(total_bytes, admin_total_bytes);
    assert_eq!(checksum, admin_checksum);
    assert_eq!(total_size, 1611);
    // please update this number (must be > 0) when the test failed

    suite.stop();
}

#[test]
fn test_invalid_external_storage() {
    let mut suite = TestSuite::new(1, 144 * 1024 * 1024);
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
        &storage_path,
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
        let mut suite = TestSuite::new(1, 144 * 1024 * 1024);
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
