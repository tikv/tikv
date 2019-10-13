// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::*;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use futures::sync::mpsc as future_mpsc;
use futures::{Future, Stream};
use grpcio::{ChannelBuilder, Environment};

use backup::Task;
use engine::CF_DEFAULT;
use engine::*;
use external_storage::*;
use kvproto::backup::*;
use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::*;
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftRequestHeader, Request};
use kvproto::tikvpb_grpc::TikvClient;
use tempfile::Builder;
use test_raftstore::*;
use tikv_util::collections::HashMap;
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
        while tried_times < $retry && start.elapsed() < timeout {
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
            5,    // retry 5 times
            5000  // 100ms timeout
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
            5,    // retry 5 times
            5000  // 100ms timeout
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
        backup_ts: u64,
        path: String,
    ) -> future_mpsc::UnboundedReceiver<BackupResponse> {
        let mut req = BackupRequest::new();
        req.set_start_key(start_key);
        req.set_end_key(end_key);
        req.start_version = backup_ts;
        req.end_version = backup_ts;
        req.set_path(path);
        let (tx, rx) = future_mpsc::unbounded();
        for end in self.endpoints.values() {
            let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
            end.schedule(task).unwrap();
        }
        rx
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
    let storage_path = format!(
        "local://{}",
        tmp.path().join(format!("{}", backup_ts)).display()
    );
    let rx = suite.backup(
        vec![], // start
        vec![], // end
        backup_ts,
        storage_path.clone(),
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
        backup_ts,
        format!(
            "local://{}",
            tmp.path().join(format!("{}", backup_ts + 1)).display()
        ),
    );
    let resps2 = rx.collect().wait().unwrap();
    assert!(resps2[0].get_files().is_empty(), "{:?}", resps2);

    // Use importer to restore backup files.
    let storage = create_storage(&storage_path).unwrap();
    let region = suite.cluster.get_region(b"");
    let mut sst_meta = SstMeta::new();
    sst_meta.region_id = region.get_id();
    sst_meta.set_region_epoch(region.get_region_epoch().clone());
    sst_meta.set_uuid(uuid::Uuid::new_v4().as_bytes().to_vec());
    let mut metas = vec![];
    for f in files1.clone().into_iter() {
        let mut reader = storage.read(&f.name).unwrap();
        let mut content = vec![];
        reader.read_to_end(&mut content).unwrap();
        let mut m = sst_meta.clone();
        m.crc32 = f.crc32;
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
    // backup ts + 2 avoid file already exist.
    let rx = suite.backup(
        vec![], // start
        vec![], // end
        backup_ts,
        format!(
            "local://{}",
            tmp.path().join(format!("{}", backup_ts + 2)).display()
        ),
    );
    let resps3 = rx.collect().wait().unwrap();
    assert_eq!(files1, resps3[0].files);

    suite.stop();
}
