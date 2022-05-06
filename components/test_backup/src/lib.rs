// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp, fs,
    path::{Path, PathBuf},
    sync::*,
    thread,
    time::Duration,
};

use api_version::{dispatch_api_version, KvFormat, RawValue};
use backup::Task;
use collections::HashMap;
use engine_traits::{CfName, IterOptions, CF_DEFAULT, CF_WRITE, DATA_KEY_PREFIX_LEN};
use external_storage_export::make_local_backend;
use futures::channel::mpsc as future_mpsc;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{brpb::*, kvrpcpb::*, tikvpb::TikvClient};
use rand::Rng;
use test_raftstore::*;
use tidb_query_common::storage::{
    scanner::{RangesScanner, RangesScannerOptions},
    IntervalRange, Range,
};
use tikv::{
    config::BackupConfig,
    coprocessor::{checksum_crc64_xor, dag::TiKvStorage},
    storage::{
        kv::{Engine, SnapContext},
        SnapshotStore,
    },
};
use tikv_util::{
    config::ReadableSize,
    time::Instant,
    worker::{LazyWorker, Worker},
    HandyRwLock,
};
use txn_types::TimeStamp;

pub struct TestSuite {
    pub cluster: Cluster<ServerCluster>,
    pub endpoints: HashMap<u64, LazyWorker<Task>>,
    pub tikv_cli: TikvClient,
    pub context: Context,
    pub ts: TimeStamp,
    pub bg_worker: Worker,
    pub api_version: ApiVersion,

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
    pub fn new(count: usize, sst_max_size: u64, api_version: ApiVersion) -> TestSuite {
        let mut cluster = new_server_cluster_with_api_ver(1, count, api_version);
        // Increase the Raft tick interval to make this test case running reliably.
        configure_for_lease_read(&mut cluster, Some(100), None);
        cluster.run();

        let mut endpoints = HashMap::default();
        let bg_worker = Worker::new("backup-test");
        for (id, engines) in &cluster.engines {
            // Create and run backup endpoints.
            let sim = cluster.sim.rl();
            let backup_endpoint = backup::Endpoint::new(
                *id,
                sim.storages[id].clone(),
                sim.region_info_accessors[id].clone(),
                engines.kv.as_inner().clone(),
                BackupConfig {
                    num_threads: 4,
                    batch_size: 8,
                    sst_max_size: ReadableSize(sst_max_size),
                    ..Default::default()
                },
                sim.get_concurrency_manager(*id),
                api_version,
            );
            let mut worker = bg_worker.lazy_build(format!("backup-{}", id));
            worker.start(backup_endpoint);
            endpoints.insert(*id, worker);
        }

        // Make sure there is a leader.
        let tmp_value = String::from("foo").into_bytes();
        let value = dispatch_api_version!(api_version, {
            let raw_value = RawValue {
                user_value: tmp_value,
                expire_ts: None,
                is_delete: false,
            };
            API::encode_raw_value_owned(raw_value)
        });
        cluster.must_put(b"foo", &value); // make raw apiv1ttl/apiv2 encode happy.
        let region_id = 1;
        let leader = cluster.leader_of_region(region_id).unwrap();
        let leader_addr = cluster.sim.rl().get_addr(leader.get_store_id());

        let epoch = cluster.get_region_epoch(region_id);
        let mut context = Context::default();
        context.set_region_id(region_id);
        context.set_peer(leader);
        context.set_region_epoch(epoch);
        context.set_api_version(api_version);

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
            bg_worker,
            api_version,
        }
    }

    pub fn alloc_ts(&mut self) -> TimeStamp {
        *self.ts.incr()
    }

    pub fn stop(mut self) {
        for (_, mut worker) in self.endpoints {
            worker.stop();
        }
        self.bg_worker.stop();
        self.cluster.shutdown();
    }

    pub fn must_raw_put(&self, k: Vec<u8>, v: Vec<u8>, cf: String) {
        let mut request = RawPutRequest::default();
        let mut context = self.context.clone();
        if context.api_version == ApiVersion::V1ttl {
            context.api_version = ApiVersion::V1;
        }
        request.set_context(context);
        request.set_key(k);
        request.set_value(v);
        request.set_cf(cf);
        let ttl = match self.api_version {
            ApiVersion::V1 => 0,
            _ => u64::MAX,
        };
        request.set_ttl(ttl);
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

    pub fn must_raw_get(&self, k: Vec<u8>, cf: String) -> Vec<u8> {
        let mut request = RawGetRequest::default();
        let mut context = self.context.clone();
        if context.api_version == ApiVersion::V1ttl {
            context.api_version = ApiVersion::V1;
        }
        request.set_context(context);
        request.set_key(k);
        request.set_cf(cf);
        let mut response = self.tikv_cli.raw_get(&request).unwrap();
        retry_req!(
            self.tikv_cli.raw_get(&request).unwrap(),
            !response.has_region_error() && response.error.is_empty(),
            response,
            10,   // retry 10 times
            1000  // 1s timeout
        );
        assert!(response.error.is_empty(), "{:?}", response.get_error());
        response.take_value()
    }

    pub fn must_kv_prewrite(&self, muts: Vec<Mutation>, pk: Vec<u8>, ts: TimeStamp) {
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

    pub fn must_kv_commit(&self, keys: Vec<Vec<u8>>, start_ts: TimeStamp, commit_ts: TimeStamp) {
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

    pub fn must_kv_put(&mut self, key_count: usize, versions: usize) {
        let mut batch = Vec::with_capacity(1024);
        let mut keys = Vec::with_capacity(1024);
        // Write 50 times to include more different ts.
        let batch_size = cmp::min(cmp::max(key_count / 50, 1), 1024);
        for _ in 0..versions {
            let mut j = 0;
            while j < key_count {
                let start_ts = self.alloc_ts();
                let limit = cmp::min(key_count, j + batch_size);
                batch.clear();
                keys.clear();
                for i in j..limit {
                    let (k, v) = (format!("key_{}", i), format!("value_{}", i));
                    keys.push(k.clone().into_bytes());
                    // Prewrite
                    let mut mutation = Mutation::default();
                    mutation.set_op(Op::Put);
                    mutation.key = k.clone().into_bytes();
                    mutation.value = v.clone().into_bytes();
                    batch.push(mutation);
                }
                self.must_kv_prewrite(batch.split_off(0), keys[0].clone(), start_ts);
                // Commit
                let commit_ts = self.alloc_ts();
                self.must_kv_commit(keys.split_off(0), start_ts, commit_ts);
                j = limit;
            }
        }
    }

    pub fn backup(
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
            end.scheduler().schedule(task).unwrap();
        }
        rx
    }

    pub fn backup_raw(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        cf: String,
        path: &Path,
        dst_api_ver: ApiVersion,
    ) -> future_mpsc::UnboundedReceiver<BackupResponse> {
        let mut req = BackupRequest::default();
        req.set_start_key(start_key);
        req.set_end_key(end_key);
        req.set_storage_backend(make_local_backend(path));
        req.set_is_raw_kv(true);
        req.set_cf(cf);
        req.set_dst_api_version(dst_api_ver);
        let (tx, rx) = future_mpsc::unbounded();
        for end in self.endpoints.values() {
            let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
            end.scheduler().schedule(task).unwrap();
        }
        rx
    }

    pub fn admin_checksum(
        &self,
        backup_ts: TimeStamp,
        start: String,
        end: String,
    ) -> (u64, u64, u64) {
        let mut checksum = 0;
        let mut total_kvs = 0;
        let mut total_bytes = 0;
        let sim = self.cluster.sim.rl();
        let engine = sim.storages[&self.context.get_peer().get_store_id()].clone();
        let snap_ctx = SnapContext {
            pb_ctx: &self.context,
            ..Default::default()
        };
        let snapshot = engine.snapshot(snap_ctx).unwrap();
        let snap_store = SnapshotStore::new(
            snapshot,
            backup_ts,
            IsolationLevel::Si,
            false,
            Default::default(),
            Default::default(),
            false,
        );
        let mut scanner = RangesScanner::new(RangesScannerOptions {
            storage: TiKvStorage::new(snap_store, false),
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

    pub fn gen_raw_kv(&self, key_idx: u64) -> (String, String) {
        (format!("rkey_{}", key_idx), format!("value_{}", key_idx))
    }

    pub fn raw_kv_checksum(&self, start: String, end: String, cf: CfName) -> (u64, u64, u64) {
        let start = start.into_bytes();
        let end = end.into_bytes();

        let mut total_kvs = 0;
        let mut total_bytes = 0;

        let sim = self.cluster.sim.rl();
        let engine = sim.storages[&self.context.get_peer().get_store_id()].clone();
        let snap_ctx = SnapContext {
            pb_ctx: &self.context,
            ..Default::default()
        };
        let snapshot = engine.snapshot(snap_ctx).unwrap();
        let mut iter_opt = IterOptions::default();
        if !end.is_empty() {
            iter_opt.set_upper_bound(&end, DATA_KEY_PREFIX_LEN);
        }
        let mut iter = snapshot.iter_cf(cf, iter_opt).unwrap();

        if !iter.seek(&start).unwrap() {
            return (0, 0, 0);
        }
        let digest = crc64fast::Digest::new();
        let mut checksum: u64 = 0;
        while iter.valid().unwrap() {
            let key = iter.key();
            let value = iter.value();
            total_kvs += 1;
            total_bytes += (key.len() + value.len()) as u64;
            checksum = checksum_crc64_xor(checksum, digest.clone(), key, value);
            iter.next().unwrap();
        }
        (checksum, total_kvs, total_bytes)
    }

    pub fn storage_raw_checksum(&self, start: String, end: String) -> (u64, u64, u64) {
        let mut req = RawChecksumRequest::default();
        let mut context = self.context.clone();
        if context.api_version == ApiVersion::V1ttl {
            context.api_version = ApiVersion::V1;
        }
        req.set_context(context);
        let mut range = KeyRange::default();
        range.set_start_key(start.into_bytes());
        range.set_end_key(end.into_bytes());
        req.set_ranges(protobuf::RepeatedField::from_vec(vec![range]));
        let response = self.tikv_cli.raw_checksum(&req).unwrap();
        (response.checksum, response.total_kvs, response.total_bytes)
    }
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
