// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::*;
use std::thread;
use std::time::Duration;

use futures::channel::mpsc as future_mpsc;
use futures::StreamExt;
use futures_executor::block_on;
use futures_util::io::AsyncReadExt;
use grpcio::{ChannelBuilder, Environment};

use backup::Task;
use engine::*;
use engine_traits::IterOptions;
use engine_traits::{CfName, CF_DEFAULT, CF_WRITE};
use external_storage::*;
use kvproto::backup::*;
use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::*;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request};
use kvproto::tikvpb::TikvClient;
use rand::Rng;
use tempfile::Builder;
use test_raftstore::*;
use tidb_query::storage::scanner::{RangesScanner, RangesScannerOptions};
use tidb_query::storage::{IntervalRange, Range};
use tikv::config::BackupConfig;
use tikv::coprocessor::checksum_crc64_xor;
use tikv::coprocessor::dag::TiKVStorage;
use tikv::storage::kv::Engine;
use tikv::storage::SnapshotStore;
use tikv_util::collections::HashMap;
use tikv_util::config::ReadableSize;
use tikv_util::file::calc_crc32_bytes;
use tikv_util::time::Instant;
use tikv_util::worker::Worker;
use tikv_util::HandyRwLock;
use txn_types::TimeStamp;

struct TestSuite {
    cluster: Cluster<ServerCluster>,
    endpoints: HashMap<u64, Worker<Task>>,
    tikv_cli: TikvClient,
    context: Context,
    ts: TimeStamp,

    _env: Arc<Environment>,
}

// Retry if encounter error
macro_rules! retry_req {
    ($call_req: expr, $check_resp: expr, $resp:ident, $retry:literal, $timeout:literal) => {
        let start = Instant::now();
        let timeout = Duration::from_millis($timeout);
        let mut tried_times = 0;
        while tried_times < $retry || start.saturating_elapsed() < timeout {
            if $check_resp {
                break;
            } else {
                thread::sleep(Duration::from_millis(200));
                tried_times += 1;
                $resp = $call_req;
                continue;
            }
        }
    };
}

impl TestSuite {
    fn new(count: usize, sst_max_size: u64) -> TestSuite {
        super::init();
        let mut cluster = new_server_cluster(1, count);
        // Increase the Raft tick interval to make this test case running reliably.
        configure_for_lease_read(&mut cluster, Some(100), None);
        cluster.run();

        let mut endpoints = HashMap::default();
        for (id, engines) in &cluster.engines {
            // Create and run backup endpoints.
            let sim = cluster.sim.rl();
            let backup_endpoint = backup::Endpoint::new(
                *id,
                sim.storages[&id].clone(),
                sim.region_info_accessors[&id].clone(),
                engines.kv.clone(),
                BackupConfig {
                    num_threads: 4,
                    batch_size: 8,
                    sst_max_size: ReadableSize(sst_max_size),
                },
            );
            let mut worker = Worker::new(format!("backup-{}", id));
            worker.start(backup_endpoint).unwrap();
            endpoints.insert(*id, worker);
        }

        // Make sure there is a leader.
        cluster.must_put(b"foo", b"foo");
        let region_id = 1;
        let leader = cluster.leader_of_region(region_id).unwrap();
        let leader_addr = cluster.sim.rl().get_addr(leader.get_store_id()).to_owned();

        let epoch = cluster.get_region_epoch(region_id);
        let mut context = Context::default();
        context.set_region_id(region_id);
        context.set_peer(leader);
        context.set_region_epoch(epoch);

        let env = Arc::new(Environment::new(1));
        let channel = ChannelBuilder::new(env.clone()).connect(&leader_addr);
        let tikv_cli = TikvClient::new(channel);

        TestSuite {
            cluster,
            endpoints,
            tikv_cli,
            context,
            ts: TimeStamp::zero(),
            _env: env,
        }
    }

    fn alloc_ts(&mut self) -> TimeStamp {
        *self.ts.incr()
    }

    fn stop(mut self) {
        for (_, mut worker) in self.endpoints {
            worker.stop().unwrap();
        }
        self.cluster.shutdown();
    }

    fn must_raw_put(&self, k: Vec<u8>, v: Vec<u8>, cf: String) {
        let mut request = RawPutRequest::default();
        request.set_context(self.context.clone());
        request.set_key(k);
        request.set_value(v);
        request.set_cf(cf);
        let mut response = self.tikv_cli.raw_put(&request).unwrap();
        retry_req!(
            self.tikv_cli.raw_put(&request).unwrap(),
            !response.has_region_error() && response.error.is_empty(),
            response,
            10,   // retry 10 times
            1000  // 1s timeout
        );
        assert!(
            !response.has_region_error(),
            "{:?}",
            response.get_region_error(),
        );
        assert!(response.error.is_empty(), "{:?}", response.get_error());
    }

    fn must_kv_prewrite(&self, muts: Vec<Mutation>, pk: Vec<u8>, ts: TimeStamp) {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.context.clone());
        prewrite_req.set_mutations(muts.into_iter().collect());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts.into_inner();
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        let mut prewrite_resp = self.tikv_cli.kv_prewrite(&prewrite_req).unwrap();
        retry_req!(
            self.tikv_cli.kv_prewrite(&prewrite_req).unwrap(),
            !prewrite_resp.has_region_error() && prewrite_resp.errors.is_empty(),
            prewrite_resp,
            10,   // retry 10 times
            3000  // 3s timeout
        );
        assert!(
            !prewrite_resp.has_region_error(),
            "{:?}",
            prewrite_resp.get_region_error()
        );
        assert!(
            prewrite_resp.errors.is_empty(),
            "{:?}",
            prewrite_resp.get_errors()
        );
    }

    fn must_kv_commit(&self, keys: Vec<Vec<u8>>, start_ts: TimeStamp, commit_ts: TimeStamp) {
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(self.context.clone());
        commit_req.start_version = start_ts.into_inner();
        commit_req.set_keys(keys.into_iter().collect());
        commit_req.commit_version = commit_ts.into_inner();
        let mut commit_resp = self.tikv_cli.kv_commit(&commit_req).unwrap();
        retry_req!(
            self.tikv_cli.kv_commit(&commit_req).unwrap(),
            !commit_resp.has_region_error() && !commit_resp.has_error(),
            commit_resp,
            10,   // retry 10 times
            3000  // 3s timeout
        );
        assert!(
            !commit_resp.has_region_error(),
            "{:?}",
            commit_resp.get_region_error()
        );
        assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    }

    fn must_kv_put(&mut self, key_count: usize, versions: usize) {
        for _ in 0..versions {
            for i in 0..key_count {
                let (k, v) = (format!("key_{}", i), format!("value_{}", i));
                // Prewrite
                let start_ts = self.alloc_ts();
                let mut mutation = Mutation::default();
                mutation.set_op(Op::Put);
                mutation.key = k.clone().into_bytes();
                mutation.value = v.clone().into_bytes();
                self.must_kv_prewrite(vec![mutation], k.clone().into_bytes(), start_ts);
                // Commit
                let commit_ts = self.alloc_ts();
                self.must_kv_commit(vec![k.clone().into_bytes()], start_ts, commit_ts);
            }
        }
    }

    fn backup(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        begin_ts: TimeStamp,
        backup_ts: TimeStamp,
        path: &Path,
    ) -> future_mpsc::UnboundedReceiver<BackupResponse> {
        let mut req = BackupRequest::default();
        req.set_start_key(start_key);
        req.set_end_key(end_key);
        req.start_version = begin_ts.into_inner();
        req.end_version = backup_ts.into_inner();
        req.set_storage_backend(make_local_backend(path));
        req.set_is_raw_kv(false);
        let (tx, rx) = future_mpsc::unbounded();
        for end in self.endpoints.values() {
            let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
            end.schedule(task).unwrap();
        }
        rx
    }

    fn backup_raw(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        cf: String,
        path: &Path,
    ) -> future_mpsc::UnboundedReceiver<BackupResponse> {
        let mut req = BackupRequest::default();
        req.set_start_key(start_key);
        req.set_end_key(end_key);
        req.set_storage_backend(make_local_backend(path));
        req.set_is_raw_kv(true);
        req.set_cf(cf);
        let (tx, rx) = future_mpsc::unbounded();
        for end in self.endpoints.values() {
            let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
            end.schedule(task).unwrap();
        }
        rx
    }

    fn admin_checksum(&self, backup_ts: TimeStamp, start: String, end: String) -> (u64, u64, u64) {
        let mut checksum = 0;
        let mut total_kvs = 0;
        let mut total_bytes = 0;
        let sim = self.cluster.sim.rl();
        let engine = sim.storages[&self.context.get_peer().get_store_id()].clone();
        let snapshot = engine.snapshot(&self.context.clone()).unwrap();
        let snap_store = SnapshotStore::new(
            snapshot,
            backup_ts,
            IsolationLevel::Si,
            false,
            Default::default(),
            false,
        );
        let mut scanner = RangesScanner::new(RangesScannerOptions {
            storage: TiKVStorage::new(snap_store, false),
            ranges: vec![Range::Interval(IntervalRange::from((start, end)))],
            scan_backward_in_range: false,
            is_key_only: false,
            is_scanned_range_aware: false,
        });
        let digest = crc64fast::Digest::new();
        while let Some((k, v)) = scanner.next().unwrap() {
            checksum = checksum_crc64_xor(checksum, digest.clone(), &k, &v);
            total_kvs += 1;
            total_bytes += (k.len() + v.len()) as u64;
        }
        (checksum, total_kvs, total_bytes)
    }

    fn gen_raw_kv(&self, key_idx: u64) -> (String, String) {
        (format!("key_{}", key_idx), format!("value_{}", key_idx))
    }

    fn raw_kv_checksum(&self, start: String, end: String, cf: CfName) -> (u64, u64, u64) {
        let start = start.into_bytes();
        let end = end.into_bytes();

        let mut total_kvs = 0;
        let mut total_bytes = 0;

        let sim = self.cluster.sim.rl();
        let engine = sim.storages[&self.context.get_peer().get_store_id()].clone();
        let snapshot = engine.snapshot(&self.context.clone()).unwrap();
        let mut iter_opt = IterOptions::default();
        if !end.is_empty() {
            iter_opt.set_upper_bound(&end, DATA_KEY_PREFIX_LEN);
        }
        let mut iter = snapshot.iter_cf(cf, iter_opt).unwrap();

        if !iter.seek(&start).unwrap() {
            return (0, 0, 0);
        }
        while iter.valid().unwrap() {
            total_kvs += 1;
            total_bytes += (iter.key().len() + iter.value().len()) as u64;
            iter.next().unwrap();
        }
        (0, total_kvs, total_bytes)
    }
}

// Extrat CF name from sst name.
fn name_to_cf(name: &str) -> CfName {
    if name.contains(CF_DEFAULT) {
        CF_DEFAULT
    } else if name.contains(CF_WRITE) {
        CF_WRITE
    } else {
        unreachable!()
    }
}

fn make_unique_dir(path: &Path) -> PathBuf {
    let uid: u64 = rand::thread_rng().gen();
    let tmp_suffix = format!("{:016x}", uid);
    let unique = path.join(tmp_suffix);
    fs::create_dir_all(&unique).unwrap();
    unique
}

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

fn assert_same_files(files1: Vec<kvproto::backup::File>, files2: Vec<kvproto::backup::File>) {
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
    let storage = create_storage(&backend).unwrap();
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
        assert!(!resp.get_header().has_error(), resp);
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
    let storage = create_storage(&backend).unwrap();
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
        assert!(!resp.get_header().has_error(), resp);
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
    let storage = create_storage(&backend).unwrap();
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
        assert!(!resp.get_header().has_error(), resp);
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
