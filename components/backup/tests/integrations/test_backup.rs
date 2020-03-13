// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::*;
use std::thread;
use std::time::{Duration, Instant};

use futures::sync::mpsc as future_mpsc;
use futures::{Future, Stream};
use futures_executor::block_on;
use futures_util::io::AsyncReadExt;
use grpcio::{ChannelBuilder, Environment};

use backup::Task;
use engine::CF_DEFAULT;
use engine::*;
use external_storage::*;
use kvproto::backup::*;
use kvproto::coprocessor::KeyRange;
use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::*;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request};
use kvproto::tikvpb_grpc::TikvClient;
use tempfile::Builder;
use test_raftstore::*;
use tikv::coprocessor::checksum_crc64_xor;
use tikv::coprocessor::dag::{ScanOn, Scanner};
use tikv::storage::kv::Engine;
use tikv::storage::SnapshotStore;
use tikv_util::collections::HashMap;
use tikv_util::file::calc_crc32_bytes;
use tikv_util::worker::Worker;
use tikv_util::HandyRwLock;

struct TestSuite {
    cluster: Cluster<ServerCluster>,
    endpoints: HashMap<u64, Worker<Task>>,
    tikv_cli: TikvClient,
    context: Context,
    ts: u64,

    _env: Arc<Environment>,
}

// Retry if encounter error
macro_rules! retry_req {
    ($call_req: expr, $check_resp: expr, $resp:ident, $retry:literal, $timeout:literal) => {
        let start = Instant::now();
        let timeout = Duration::from_millis($timeout);
        let mut tried_times = 0;
        while tried_times < $retry || start.elapsed() < timeout {
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
    fn new(count: usize) -> TestSuite {
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
            ts: 0,
            _env: env,
        }
    }

    fn alloc_ts(&mut self) -> u64 {
        self.ts += 1;
        self.ts
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
            !response.has_region_error() && !response.error.is_empty(),
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

    fn must_kv_prewrite(&self, muts: Vec<Mutation>, pk: Vec<u8>, ts: u64) {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.context.clone());
        prewrite_req.set_mutations(muts.into_iter().collect());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts;
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

    fn must_kv_commit(&self, keys: Vec<Vec<u8>>, start_ts: u64, commit_ts: u64) {
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(self.context.clone());
        commit_req.start_version = start_ts;
        commit_req.set_keys(keys.into_iter().collect());
        commit_req.commit_version = commit_ts;
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

    fn backup(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        begin_ts: u64,
        backup_ts: u64,
        path: &Path,
    ) -> future_mpsc::UnboundedReceiver<BackupResponse> {
        let mut req = BackupRequest::new();
        req.set_start_key(start_key);
        req.set_end_key(end_key);
        req.start_version = begin_ts;
        req.end_version = backup_ts;
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

    fn admin_checksum(&self, backup_ts: u64, start: String, end: String) -> (u64, u64, u64) {
        let mut checksum = 0;
        let mut total_kvs = 0;
        let mut total_bytes = 0;
        let sim = self.cluster.sim.rl();
        let engine = sim.storages[&self.context.get_peer().get_store_id()].clone();
        let snapshot = engine.snapshot(&self.context.clone()).unwrap();
        let snap_store = SnapshotStore::new(snapshot, backup_ts, IsolationLevel::SI, false);
        let mut range = KeyRange::default();
        range.set_start(start.into_bytes());
        range.set_end(end.into_bytes());
        let mut scanner = Scanner::new(&snap_store, ScanOn::Table, false, false, range).unwrap();
        let digest = crc64fast::Digest::new();
        while let Some((k, v)) = scanner.next_row().unwrap() {
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
        let mut iter_opt = IterOption::default();
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
fn name_to_cf(name: &str) -> engine::CfName {
    if name.contains(CF_DEFAULT) {
        CF_DEFAULT
    } else if name.contains(CF_WRITE) {
        CF_WRITE
    } else {
        unreachable!()
    }
}

#[test]
fn test_backup_and_import() {
    let mut suite = TestSuite::new(3);

    // 3 version for each key.
    for _ in 0..3 {
        // 60 keys.
        for i in 0..60 {
            let (k, v) = (format!("key_{}", i), format!("value_{}", i));
            // Prewrite
            let start_ts = suite.alloc_ts();
            let mut mutation = Mutation::default();
            mutation.op = Op::Put;
            mutation.key = k.clone().into_bytes();
            mutation.value = v.clone().into_bytes();
            suite.must_kv_prewrite(vec![mutation], k.clone().into_bytes(), start_ts);
            // Commit
            let commit_ts = suite.alloc_ts();
            suite.must_kv_commit(vec![k.clone().into_bytes()], start_ts, commit_ts);
        }
    }

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let backup_ts = suite.alloc_ts();
    let storage_path = tmp.path().join(format!("{}", backup_ts));
    let rx = suite.backup(
        vec![], // start
        vec![], // end
        0,      // begin_ts
        backup_ts,
        &storage_path,
    );
    let resps1 = rx.collect().wait().unwrap();
    // Only leader can handle backup.
    assert_eq!(resps1.len(), 1);
    let files1 = resps1[0].files.clone();
    // Short value is piggybacked in write cf, so we get 1 sst at least.
    assert!(!resps1[0].get_files().is_empty());

    // Delete all data, there should be no backup files.
    suite.cluster.must_delete_range_cf(CF_DEFAULT, b"", b"");
    suite.cluster.must_delete_range_cf(CF_WRITE, b"", b"");
    // Backup file should have same contents.
    // backup ts + 1 avoid file already exist.
    let rx = suite.backup(
        vec![], // start
        vec![], // end
        0,      // begin_ts
        backup_ts,
        &tmp.path().join(format!("{}", backup_ts + 1)),
    );
    let resps2 = rx.collect().wait().unwrap();
    assert!(resps2[0].get_files().is_empty(), "{:?}", resps2);

    // Use importer to restore backup files.
    let backend = make_local_backend(&storage_path);
    let storage = create_storage(&backend).unwrap();
    let region = suite.cluster.get_region(b"");
    let mut sst_meta = SSTMeta::new();
    sst_meta.region_id = region.get_id();
    sst_meta.set_region_epoch(region.get_region_epoch().clone());
    sst_meta.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    let mut metas = vec![];
    for f in files1.clone().into_iter() {
        let mut reader = storage.read(&f.name).unwrap();
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
        ingest.set_cmd_type(CmdType::IngestSST);
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
    // backup ts + 2 avoid file already exist.
    let rx = suite.backup(
        vec![], // start
        vec![], // end
        0,      // begin_ts
        backup_ts,
        &tmp.path().join(format!("{}", backup_ts + 2)),
    );
    let resps3 = rx.collect().wait().unwrap();
    assert_eq!(files1, resps3[0].files);

    suite.stop();
}

#[test]
fn test_backup_meta() {
    let mut suite = TestSuite::new(3);
    let key_count = 60;

    // 3 version for each key.
    for _ in 0..3 {
        for i in 0..key_count {
            let (k, v) = (format!("key_{}", i), format!("value_{}", i));
            // Prewrite
            let start_ts = suite.alloc_ts();
            let mut mutation = Mutation::default();
            mutation.op = Op::Put;
            mutation.key = k.clone().into_bytes();
            mutation.value = v.clone().into_bytes();
            suite.must_kv_prewrite(vec![mutation], k.clone().into_bytes(), start_ts);
            // Commit
            let commit_ts = suite.alloc_ts();
            suite.must_kv_commit(vec![k.clone().into_bytes()], start_ts, commit_ts);
        }
    }
    let backup_ts = suite.alloc_ts();
    // key are order by lexicographical order, 'a'-'z' will cover all
    let (admin_checksum, admin_total_kvs, admin_total_bytes) =
        suite.admin_checksum(backup_ts, "a".to_owned(), "z".to_owned());

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let storage_path = tmp.path().join(format!("{}", backup_ts));
    let rx = suite.backup(
        vec![], // start
        vec![], // end
        0,      // begin_ts
        backup_ts,
        &storage_path,
    );
    let resps1 = rx.collect().wait().unwrap();
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
    assert_eq!(total_kvs, key_count);
    assert_eq!(total_kvs, admin_total_kvs);
    assert_eq!(total_bytes, admin_total_bytes);
    assert_eq!(checksum, admin_checksum);
    assert_eq!(total_size, 2205);
    // please update this number (must be > 0) when the test failed

    suite.stop();
}

#[test]
fn test_backup_rawkv() {
    let mut suite = TestSuite::new(3);
    let key_count = 60;

    let cf = String::from(CF_DEFAULT);
    for i in 0..key_count {
        let (k, v) = suite.gen_raw_kv(i);
        suite.must_raw_put(k.clone().into_bytes(), v.clone().into_bytes(), cf.clone());
    }

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let backup_ts = suite.alloc_ts();
    let storage_path = tmp.path().join(format!("{}", backup_ts));
    let rx = suite.backup_raw(
        vec![b'a'], // start
        vec![b'z'], // end
        cf.clone(),
        &storage_path,
    );
    let resps1 = rx.collect().wait().unwrap();
    // Only leader can handle backup.
    assert_eq!(resps1.len(), 1);
    let files1 = resps1[0].files.clone();
    assert!(!resps1[0].get_files().is_empty());

    // Delete all data, there should be no backup files.
    suite.cluster.must_delete_range_cf(CF_DEFAULT, b"", b"");
    // Backup file should have same contents.
    // backup ts + 1 avoid file already exist.
    let rx = suite.backup_raw(
        vec![], // start
        vec![], // end
        cf.clone(),
        &tmp.path().join(format!("{}", backup_ts + 1)),
    );
    let resps2 = rx.collect().wait().unwrap();
    assert!(resps2[0].get_files().is_empty(), "{:?}", resps2);

    // Use importer to restore backup files.
    let backend = make_local_backend(&storage_path);
    let storage = create_storage(&backend).unwrap();
    let region = suite.cluster.get_region(b"");
    let mut sst_meta = SSTMeta::default();
    sst_meta.region_id = region.get_id();
    sst_meta.set_region_epoch(region.get_region_epoch().clone());
    sst_meta.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    let mut metas = vec![];
    for f in files1.clone().into_iter() {
        let mut reader = storage.read(&f.name).unwrap();
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
        ingest.set_cmd_type(CmdType::IngestSST);
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
    // backup ts + 2 avoid file already exist.
    // Set non-empty range to check if it's incorrectly encoded.
    let rx = suite.backup_raw(
        vec![b'a'], // start
        vec![b'z'], // end
        cf,
        &tmp.path().join(format!("{}", backup_ts + 2)),
    );
    let resps3 = rx.collect().wait().unwrap();
    assert_eq!(files1, resps3[0].files);

    suite.stop();
}

#[test]
fn test_backup_raw_meta() {
    let mut suite = TestSuite::new(3);
    let key_count: u64 = 60;
    let cf = String::from(CF_DEFAULT);

    for i in 0..key_count {
        let (k, v) = suite.gen_raw_kv(i);
        suite.must_raw_put(k.clone().into_bytes(), v.clone().into_bytes(), cf.clone());
    }
    let backup_ts = suite.alloc_ts();
    // Keys are order by lexicographical order, 'a'-'z' will cover all.
    let (admin_checksum, admin_total_kvs, admin_total_bytes) =
        suite.raw_kv_checksum("a".to_owned(), "z".to_owned(), CF_DEFAULT);

    // Push down backup request.
    let tmp = Builder::new().tempdir().unwrap();
    let storage_path = tmp.path().join(format!("{}", backup_ts));
    let rx = suite.backup_raw(
        vec![], // start
        vec![], // end
        cf,
        &storage_path,
    );
    let resps1 = rx.collect().wait().unwrap();
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
    assert_eq!(total_kvs, key_count + 1);
    assert_eq!(total_kvs, admin_total_kvs);
    assert_eq!(total_bytes, admin_total_bytes);
    assert_eq!(checksum, admin_checksum);

    suite.stop();
}
