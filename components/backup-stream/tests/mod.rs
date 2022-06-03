// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg(test)]

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::Duration,
};

use backup_stream::{
    metadata::{store::SlashEtcStore, MetadataClient, StreamTask},
    observer::BackupStreamObserver,
    router::Router,
    Endpoint, Task,
};
use futures::{executor::block_on, Future};
use grpcio::ChannelBuilder;
use kvproto::{
    brpb::{Local, StorageBackend},
    kvrpcpb::*,
    tikvpb::*,
};
use pd_client::PdClient;
use tempdir::TempDir;
use test_raftstore::{new_server_cluster, Cluster, ServerCluster};
use test_util::retry;
use tikv::config::BackupStreamConfig;
use tikv_util::{
    codec::{
        number::NumberEncoder,
        stream_event::{EventIterator, Iterator},
    },
    info,
    worker::LazyWorker,
    HandyRwLock,
};
use txn_types::{Key, TimeStamp, WriteRef};
use walkdir::WalkDir;

fn mutation(k: Vec<u8>, v: Vec<u8>) -> Mutation {
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.key = k;
    mutation.value = v;
    mutation
}

fn make_table_key(table_id: i64, key: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut table_key = b"t".to_vec();
    // make it comparable to uint.
    table_key
        .encode_u64(table_id as u64 ^ 0x8000_0000_0000_0000)
        .unwrap();
    Write::write_all(&mut table_key, key).unwrap();
    table_key
}

fn make_record_key(table_id: i64, handle: u64) -> Vec<u8> {
    let mut record = make_table_key(table_id, b"_r");
    record.encode_u64(handle ^ 0x8000_0000_0000_0000).unwrap();
    record
}

fn make_split_key_at_record(table_id: i64, handle: u64) -> Vec<u8> {
    let mut record = make_record_key(table_id, handle);
    // push an extra byte for don't put the key in the boundary of the region.
    // (Or the mock cluster may find wrong region for putting)
    record.push(255u8);
    let key = Key::from_raw(&record);
    key.into_encoded()
}

fn make_encoded_record_key(table_id: i64, handle: u64, ts: u64) -> Vec<u8> {
    let key = Key::from_raw(&make_record_key(table_id, handle));
    key.append_ts(TimeStamp::new(ts)).into_encoded()
}

pub struct Suite {
    endpoints: HashMap<u64, LazyWorker<Task>>,
    meta_store: SlashEtcStore,
    cluster: Cluster<ServerCluster>,
    tikv_cli: HashMap<u64, TikvClient>,
    obs: HashMap<u64, BackupStreamObserver>,
    env: Arc<grpcio::Environment>,

    temp_files: TempDir,
    flushed_files: TempDir,
    case_name: String,
}

impl Suite {
    pub fn simple_task(&self, name: &str) -> StreamTask {
        let mut task = StreamTask::default();
        task.info.set_name(name.to_owned());
        task.info.set_start_ts(0);
        task.info.set_end_ts(1000);
        let mut storage = StorageBackend::new();
        let mut local = Local::new();
        local.path = self.flushed_files.path().display().to_string();
        storage.set_local(local);
        task.info.set_storage(storage);
        task.info.set_table_filter(vec!["*.*".to_owned()].into());
        task
    }

    fn start_br_stream_on(&mut self, id: u64) -> LazyWorker<Task> {
        let cluster = &mut self.cluster;
        let worker = LazyWorker::new(format!("br-{}", id));
        let mut s = cluster.sim.wl();

        let ob = BackupStreamObserver::new(worker.scheduler());
        let ob2 = ob.clone();
        s.coprocessor_hooks
            .entry(id)
            .or_default()
            .push(Box::new(move |host| {
                ob.register_to(host);
            }));
        self.obs.insert(id, ob2);
        worker
    }

    fn start_endpoint(&mut self, id: u64) {
        let cluster = &mut self.cluster;
        let worker = self.endpoints.get_mut(&id).unwrap();
        let sim = cluster.sim.wl();
        let raft_router = sim.get_server_router(id);
        let cm = sim.get_concurrency_manager(id);
        let regions = sim.region_info_accessors.get(&id).unwrap().clone();
        let mut cfg = BackupStreamConfig::default();
        cfg.enable = true;
        cfg.temp_path = format!("/{}/{}", self.temp_files.path().display(), id);
        let ob = self.obs.get(&id).unwrap().clone();
        let endpoint = Endpoint::new(
            id,
            self.meta_store.clone(),
            cfg,
            worker.scheduler(),
            ob,
            regions,
            raft_router,
            cluster.pd_client.clone(),
            cm,
        );
        worker.start(endpoint);
    }

    pub fn new(case: &str, n: usize) -> Self {
        let cluster = new_server_cluster(42, n);
        let mut suite = Self {
            endpoints: Default::default(),
            meta_store: Default::default(),
            obs: Default::default(),
            tikv_cli: Default::default(),
            env: Arc::new(grpcio::Environment::new(1)),
            cluster,

            temp_files: TempDir::new("temp").unwrap(),
            flushed_files: TempDir::new("flush").unwrap(),
            case_name: case.to_owned(),
        };
        for id in 1..=(n as u64) {
            let worker = suite.start_br_stream_on(id);
            suite.endpoints.insert(id, worker);
        }
        suite.cluster.run();
        for id in 1..=(n as u64) {
            suite.start_endpoint(id);
        }
        // TODO: The current mock metastore (slash_etc) doesn't supports multi-version.
        //       We must wait until the endpoints get ready to watching the metastore, or some modifies may be lost.
        //       Either make Endpoint::with_client wait until watch did start or make slash_etc support multi-version,
        //       then we can get rid of this sleep.
        std::thread::sleep(Duration::from_secs(1));
        suite
    }

    fn get_meta_cli(&self) -> MetadataClient<SlashEtcStore> {
        MetadataClient::new(self.meta_store.clone(), 0)
    }

    fn must_split(&mut self, key: &[u8]) {
        let region = self.cluster.get_region(key);
        self.cluster.must_split(&region, key);
    }

    fn must_register_task(&self, for_table: i64, name: &str) {
        let cli = self.get_meta_cli();
        block_on(cli.insert_task_with_range(
            &self.simple_task(name),
            &[(
                &make_table_key(for_table, b""),
                &make_table_key(for_table + 1, b""),
            )],
        ))
        .unwrap();
        let name = name.to_owned();
        self.wait_with(move |r| block_on(r.get_task_info(&name)).is_ok())
    }

    async fn write_records(&mut self, from: usize, n: usize, for_table: i64) -> HashSet<Vec<u8>> {
        let mut inserted = HashSet::default();
        for ts in (from..(from + n)).map(|x| x * 2) {
            let ts = ts as u64;
            let key = make_record_key(for_table, ts);
            let muts = vec![mutation(key.clone(), b"hello, world".to_vec())];
            let enc_key = Key::from_raw(&key).into_encoded();
            let region = self.cluster.get_region_id(&enc_key);
            let start_ts = self.cluster.pd_client.get_tso().await.unwrap();
            self.must_kv_prewrite(region, muts, key.clone(), start_ts);
            let commit_ts = self.cluster.pd_client.get_tso().await.unwrap();
            self.must_kv_commit(region, vec![key.clone()], start_ts, commit_ts);
            inserted.insert(make_encoded_record_key(
                for_table,
                ts,
                commit_ts.into_inner(),
            ));
        }
        inserted
    }

    fn just_commit_a_key(&mut self, key: Vec<u8>, start_ts: TimeStamp, commit_ts: TimeStamp) {
        let enc_key = Key::from_raw(&key).into_encoded();
        let region = self.cluster.get_region_id(&enc_key);
        self.must_kv_commit(region, vec![key], start_ts, commit_ts)
    }

    fn just_async_commit_prewrite(&mut self, ts: u64, for_table: i64) -> TimeStamp {
        let key = make_record_key(for_table, ts);
        let muts = vec![mutation(key.clone(), b"hello, world".to_vec())];
        let enc_key = Key::from_raw(&key).into_encoded();
        let region = self.cluster.get_region_id(&enc_key);
        let ts = self.must_kv_async_commit_prewrite(region, muts, key, TimeStamp::new(ts));
        info!("async prewrite success!"; "min_commit_ts" => %ts);
        ts
    }

    fn force_flush_files(&self, task: &str) {
        self.run(|| Task::ForceFlush(task.to_owned()))
    }

    fn run(&self, mut t: impl FnMut() -> Task) {
        for worker in self.endpoints.values() {
            worker.scheduler().schedule(t()).unwrap();
        }
    }

    fn check_for_write_records<'a>(
        &self,
        path: &Path,
        key_set: impl std::iter::Iterator<Item = &'a [u8]>,
    ) {
        let mut remain_keys = key_set.collect::<HashSet<_>>();
        let n = remain_keys.len();
        let mut extra_key = 0;
        let mut extra_len = 0;
        for entry in WalkDir::new(path) {
            let entry = entry.unwrap();
            println!("checking: {:?}", entry);
            if entry.file_type().is_file()
                && entry
                    .file_name()
                    .to_str()
                    .map_or(false, |s| s.ends_with(".log"))
            {
                let content = std::fs::read(entry.path()).unwrap();
                let mut iter = EventIterator::new(content);
                loop {
                    if !iter.valid() {
                        break;
                    }
                    iter.next().unwrap();
                    if !remain_keys.remove(iter.key()) {
                        extra_key += 1;
                        extra_len += iter.key().len() + iter.value().len();
                    }

                    let value = iter.value();
                    let wf = WriteRef::parse(value).unwrap();
                    assert_eq!(wf.short_value, Some(b"hello, world" as &[u8]));
                }
            }
        }

        if extra_key != 0 {
            println!(
                "check_for_write_records of “{}”: extra {} keys ({:.02}% of recorded keys), extra {} bytes.",
                self.case_name,
                extra_key,
                (extra_key as f64) / (n as f64) * 100.0,
                extra_len
            )
        }
        if !remain_keys.is_empty() {
            panic!(
                "not all keys are recorded: it remains {:?} (total = {})",
                remain_keys
                    .iter()
                    .take(3)
                    .map(|v| hex::encode(v))
                    .collect::<Vec<_>>(),
                remain_keys.len()
            );
        }
    }
}

// Copy & Paste from cdc::tests::TestSuite, maybe make it a mixin?
impl Suite {
    pub fn must_kv_prewrite(
        &mut self,
        region_id: u64,
        muts: Vec<Mutation>,
        pk: Vec<u8>,
        ts: TimeStamp,
    ) {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.get_context(region_id));
        prewrite_req.set_mutations(muts.into());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts.into_inner();
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        let prewrite_resp = self
            .get_tikv_client(region_id)
            .kv_prewrite(&prewrite_req)
            .unwrap();
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

    pub fn must_kv_async_commit_prewrite(
        &mut self,
        region_id: u64,
        muts: Vec<Mutation>,
        pk: Vec<u8>,
        ts: TimeStamp,
    ) -> TimeStamp {
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(self.get_context(region_id));
        prewrite_req.use_async_commit = true;
        prewrite_req.secondaries = muts
            .iter()
            .filter_map(|m| {
                if m.op != Op::Put && m.op != Op::Del && m.op != Op::Insert {
                    None
                } else {
                    Some(m.key.clone())
                }
            })
            .collect();
        prewrite_req.set_mutations(muts.into());
        prewrite_req.primary_lock = pk;
        prewrite_req.start_version = ts.into_inner();
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        let prewrite_resp = self
            .get_tikv_client(region_id)
            .kv_prewrite(&prewrite_req)
            .unwrap();
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
        assert_ne!(prewrite_resp.min_commit_ts, 0);
        TimeStamp::new(prewrite_resp.min_commit_ts)
    }

    pub fn must_kv_commit(
        &mut self,
        region_id: u64,
        keys: Vec<Vec<u8>>,
        start_ts: TimeStamp,
        commit_ts: TimeStamp,
    ) {
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(self.get_context(region_id));
        commit_req.start_version = start_ts.into_inner();
        commit_req.set_keys(keys.into_iter().collect());
        commit_req.commit_version = commit_ts.into_inner();
        let commit_resp = self
            .get_tikv_client(region_id)
            .kv_commit(&commit_req)
            .unwrap();
        assert!(
            !commit_resp.has_region_error(),
            "{:?}",
            commit_resp.get_region_error()
        );
        assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    }

    pub fn get_context(&mut self, region_id: u64) -> Context {
        let epoch = self.cluster.get_region_epoch(region_id);
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let mut context = Context::default();
        context.set_region_id(region_id);
        context.set_peer(leader);
        context.set_region_epoch(epoch);
        context
    }

    pub fn get_tikv_client(&mut self, region_id: u64) -> &TikvClient {
        let leader = self.cluster.leader_of_region(region_id).unwrap();
        let store_id = leader.get_store_id();
        let addr = self.cluster.sim.rl().get_addr(store_id);
        let env = self.env.clone();
        self.tikv_cli
            .entry(leader.get_store_id())
            .or_insert_with(|| {
                let channel = ChannelBuilder::new(env).connect(&addr);
                TikvClient::new(channel)
            })
    }

    pub fn sync(&self) {
        self.wait_with(|_| true)
    }

    pub fn wait_with(&self, cond: impl FnMut(&Router) -> bool + Send + 'static + Clone) {
        self.endpoints
            .iter()
            .map({
                move |(_, wkr)| {
                    let (tx, rx) = std::sync::mpsc::channel();
                    wkr.scheduler()
                        .schedule(Task::Sync(
                            Box::new(move || tx.send(()).unwrap()),
                            Box::new(cond.clone()),
                        ))
                        .unwrap();
                    rx
                }
            })
            .for_each(|rx| rx.recv().unwrap())
    }

    pub fn wait_for_flush(&self) {
        use std::ffi::OsString;
        for _ in 0..100 {
            if !walkdir::WalkDir::new(&self.temp_files)
                .into_iter()
                .any(|x| x.unwrap().path().extension() == Some(&OsString::from("log")))
            {
                return;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        let v = walkdir::WalkDir::new(&self.temp_files)
            .into_iter()
            .collect::<Vec<_>>();
        if !v.is_empty() {
            panic!("the temp isn't empty after the deadline ({:?})", v)
        }
    }

    pub fn must_shuffle_leader(&mut self, region_id: u64) {
        let region = retry!(run_async_test(
            self.cluster.pd_client.get_region_by_id(region_id)
        ))
        .unwrap()
        .unwrap();
        let leader = self.cluster.leader_of_region(region_id);
        for peer in region.get_peers() {
            if leader.as_ref().map(|p| p.id != peer.id).unwrap_or(true) {
                self.cluster.transfer_leader(region_id, peer.clone());
                self.cluster.reset_leader_of_region(region_id);
                return;
            }
        }
        panic!("must_shuffle_leader: region has no peer")
    }
}

fn run_async_test<T>(test: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(test)
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use backup_stream::{errors::Error, metadata::MetadataClient, Task};
    use tikv_util::{box_err, defer, info, HandyRwLock};
    use txn_types::TimeStamp;

    use crate::{make_record_key, make_split_key_at_record, run_async_test};

    #[test]
    fn basic() {
        let mut suite = super::Suite::new("basic", 4);

        run_async_test(async {
            // write data before the task starting, for testing incremental scanning.
            let round1 = suite.write_records(0, 128, 1).await;
            suite.must_register_task(1, "test_basic");
            suite.sync();
            let round2 = suite.write_records(256, 128, 1).await;
            suite.force_flush_files("test_basic");
            suite.wait_for_flush();
            suite.check_for_write_records(
                suite.flushed_files.path(),
                round1.union(&round2).map(Vec::as_slice),
            );
        });
        suite.cluster.shutdown();
    }

    #[test]
    fn with_split() {
        let mut suite = super::Suite::new("with_split", 4);
        run_async_test(async {
            let round1 = suite.write_records(0, 128, 1).await;
            suite.must_split(&make_split_key_at_record(1, 42));
            suite.must_register_task(1, "test_with_split");
            let round2 = suite.write_records(256, 128, 1).await;
            suite.force_flush_files("test_with_split");
            suite.wait_for_flush();
            suite.check_for_write_records(
                suite.flushed_files.path(),
                round1.union(&round2).map(Vec::as_slice),
            );
        });
        suite.cluster.shutdown();
    }

    #[test]
    /// This case tests whether the backup can continue when the leader failes.
    fn leader_down() {
        let mut suite = super::Suite::new("leader_down", 4);
        suite.must_register_task(1, "test_leader_down");
        suite.sync();
        let round1 = run_async_test(suite.write_records(0, 128, 1));
        let leader = suite.cluster.leader_of_region(1).unwrap().get_store_id();
        suite.cluster.stop_node(leader);
        let round2 = run_async_test(suite.write_records(256, 128, 1));
        suite.force_flush_files("test_leader_down");
        suite.wait_for_flush();
        suite.check_for_write_records(
            suite.flushed_files.path(),
            round1.union(&round2).map(Vec::as_slice),
        );
        suite.cluster.shutdown();
    }

    #[test]
    /// This case tests whehter the checkpoint ts (next backup ts) can be advanced correctly
    /// when async commit is enabled.
    fn async_commit() {
        let mut suite = super::Suite::new("async_commit", 3);
        run_async_test(async {
            suite.must_register_task(1, "test_async_commit");
            suite.sync();
            suite.write_records(0, 128, 1).await;
            let ts = suite.just_async_commit_prewrite(256, 1);
            suite.write_records(258, 128, 1).await;
            suite.force_flush_files("test_async_commit");
            std::thread::sleep(Duration::from_secs(4));
            let cli = MetadataClient::new(suite.meta_store.clone(), 1);
            assert_eq!(
                cli.global_progress_of_task("test_async_commit")
                    .await
                    .unwrap(),
                256
            );
            suite.just_commit_a_key(make_record_key(1, 256), TimeStamp::new(256), ts);
            suite.force_flush_files("test_async_commit");
            suite.wait_for_flush();
            let cp = cli
                .global_progress_of_task("test_async_commit")
                .await
                .unwrap();
            assert!(cp > 256, "it is {:?}", cp);
        });
        suite.cluster.shutdown();
    }

    #[test]
    fn fatal_error() {
        let mut suite = super::Suite::new("fatal_error", 3);
        suite.must_register_task(1, "test_fatal_error");
        suite.sync();
        run_async_test(suite.write_records(0, 1, 1));
        suite.force_flush_files("test_fatal_error");
        suite.wait_for_flush();
        let (victim, endpoint) = suite.endpoints.iter().next().unwrap();
        endpoint
            .scheduler()
            .schedule(Task::FatalError(
                "test_fatal_error".to_owned(),
                Box::new(Error::Other(box_err!("everything is alright"))),
            ))
            .unwrap();
        let meta_cli = suite.get_meta_cli();
        suite.sync();
        let err = run_async_test(meta_cli.get_last_error("test_fatal_error", *victim))
            .unwrap()
            .unwrap();
        info!("err"; "err" => ?err);
        assert_eq!(err.error_code, error_code::backup_stream::OTHER.code);
        assert!(err.error_message.contains("everything is alright"));
        assert_eq!(err.store_id, *victim);
        let paused = run_async_test(meta_cli.check_task_paused("test_fatal_error")).unwrap();
        assert!(paused);
        let safepoints = suite.cluster.pd_client.gc_safepoints.rl();
        let checkpoint = run_async_test(
            suite
                .get_meta_cli()
                .global_progress_of_task("test_fatal_error"),
        )
        .unwrap();

        assert!(
            safepoints.iter().any(|sp| {
                sp.serivce.contains(&format!("{}", victim))
                    && sp.ttl >= Duration::from_secs(60 * 60 * 24)
                    && sp.safepoint.into_inner() == checkpoint
            }),
            "{:?}",
            safepoints
        );
    }

    #[test]
    fn inflight_messages() {
        // We should remove the failpoints when paniked or we may get stucked.
        defer! {{
            fail::remove("delay_on_start_observe");
            fail::remove("delay_on_flush");
        }}
        let mut suite = super::Suite::new("inflight_message", 3);
        suite.must_register_task(1, "inflight_message");
        run_async_test(suite.write_records(0, 128, 1));
        fail::cfg("delay_on_flush", "pause").unwrap();
        suite.force_flush_files("inflight_message");
        fail::cfg("delay_on_start_observe", "pause").unwrap();
        suite.must_shuffle_leader(1);
        // Handling the `StartObserve` message and doing flush are executed asynchronously.
        // Make a delay of unblocking flush thread for make sure we have handled the `StartObserve`.
        std::thread::sleep(Duration::from_secs(1));
        fail::cfg("delay_on_flush", "off").unwrap();
        suite.wait_for_flush();
        let checkpoint = run_async_test(
            suite
                .get_meta_cli()
                .global_progress_of_task("inflight_message"),
        );
        fail::cfg("delay_on_start_observe", "off").unwrap();
        // The checkpoint should not advance if there are inflight messages.
        assert_eq!(checkpoint.unwrap(), 0);
        run_async_test(suite.write_records(256, 128, 1));
        suite.force_flush_files("inflight_message");
        suite.wait_for_flush();
        let checkpoint = run_async_test(
            suite
                .get_meta_cli()
                .global_progress_of_task("inflight_message"),
        )
        .unwrap();
        // The checkpoint should be advanced as expection when the inflight message has been consumed.
        assert!(checkpoint > 512, "checkpoint = {}", checkpoint);
    }
}
