// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_compression::futures::write::ZstdDecoder;
use backup_stream::{
    errors::Result,
    metadata::{
        keys::{KeyValue, MetaKey},
        store::{MetaStore, SlashEtcStore},
        MetadataClient, StreamTask,
    },
    observer::BackupStreamObserver,
    router::{Router, TaskSelector},
    utils, Endpoint, GetCheckpointResult, RegionCheckpointOperation, RegionSet, Service, Task,
};
use futures::{executor::block_on, AsyncWriteExt, Future, Stream, StreamExt};
use grpcio::{ChannelBuilder, Server, ServerBuilder};
use kvproto::{
    brpb::{CompressionType, Local, Metadata, StorageBackend},
    kvrpcpb::*,
    logbackuppb::{SubscribeFlushEventRequest, SubscribeFlushEventResponse},
    logbackuppb_grpc::{create_log_backup, LogBackupClient},
    tikvpb::*,
};
use pd_client::PdClient;
use raftstore::RegionInfoAccessor;
use tempdir::TempDir;
use test_pd_client::TestPdClient;
use test_raftstore::{new_server_cluster, Cluster, ServerCluster};
use test_util::retry;
use tikv::config::{BackupStreamConfig, ResolvedTsConfig};
use tikv_util::{
    codec::{
        number::NumberEncoder,
        stream_event::{EventIterator, Iterator},
    },
    debug, info,
    worker::LazyWorker,
    HandyRwLock,
};
use txn_types::{Key, TimeStamp, WriteRef};
use walkdir::WalkDir;

#[derive(Debug)]
pub struct FileSegments {
    path: PathBuf,
    segments: Vec<(usize, usize)>,
}

#[derive(Default, Debug)]
pub struct LogFiles {
    default_cf: Vec<FileSegments>,
    write_cf: Vec<FileSegments>,
}

pub type TestEndpoint = Endpoint<
    ErrorStore<SlashEtcStore>,
    RegionInfoAccessor,
    engine_test::kv::KvTestEngine,
    TestPdClient,
>;

pub fn mutation(k: Vec<u8>, v: Vec<u8>) -> Mutation {
    mutation_op(k, v, Op::Put)
}

pub fn mutation_op(k: Vec<u8>, v: Vec<u8>, op: Op) -> Mutation {
    let mut mutation = Mutation::default();
    mutation.set_op(op);
    mutation.key = k;
    mutation.value = v;
    mutation
}

pub fn make_table_key(table_id: i64, key: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut table_key = b"t".to_vec();
    // make it comparable to uint.
    table_key
        .encode_u64(table_id as u64 ^ 0x8000_0000_0000_0000)
        .unwrap();
    Write::write_all(&mut table_key, key).unwrap();
    table_key
}

pub fn make_record_key(table_id: i64, handle: u64) -> Vec<u8> {
    let mut record = make_table_key(table_id, b"_r");
    record.encode_u64(handle ^ 0x8000_0000_0000_0000).unwrap();
    record
}

pub fn make_split_key_at_record(table_id: i64, handle: u64) -> Vec<u8> {
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

#[derive(Clone)]
pub struct ErrorStore<S> {
    inner: S,

    error_provider: Arc<dyn Fn(&str) -> Result<()> + Send + Sync>,
}

pub struct SuiteBuilder {
    name: String,
    nodes: usize,
    metastore_error: Box<dyn Fn(&str) -> Result<()> + Send + Sync>,
    cfg: Box<dyn FnOnce(&mut BackupStreamConfig)>,
}

impl SuiteBuilder {
    pub fn new_named(s: &str) -> Self {
        Self {
            name: s.to_owned(),
            nodes: 4,
            metastore_error: Box::new(|_| Ok(())),
            cfg: Box::new(|cfg| {
                cfg.enable = true;
            }),
        }
    }

    pub fn nodes(mut self, n: usize) -> Self {
        self.nodes = n;
        self
    }

    pub fn inject_meta_store_error<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) -> Result<()> + Send + Sync + 'static,
    {
        self.metastore_error = Box::new(f);
        self
    }

    pub fn cfg(mut self, f: impl FnOnce(&mut BackupStreamConfig) + 'static) -> Self {
        let old_f = self.cfg;
        self.cfg = Box::new(move |cfg| {
            old_f(cfg);
            f(cfg);
        });
        self
    }

    pub fn build(self) -> Suite {
        let Self {
            name: case,
            nodes: n,
            metastore_error,
            cfg: cfg_f,
        } = self;

        info!("start test"; "case" => %case, "nodes" => %n);
        let cluster = new_server_cluster(42, n);
        let mut suite = Suite {
            endpoints: Default::default(),
            meta_store: ErrorStore {
                inner: Default::default(),

                error_provider: Arc::from(metastore_error),
            },
            obs: Default::default(),
            tikv_cli: Default::default(),
            log_backup_cli: Default::default(),
            servers: Default::default(),
            env: Arc::new(grpcio::Environment::new(1)),
            cluster,

            temp_files: TempDir::new("temp").unwrap(),
            flushed_files: TempDir::new("flush").unwrap(),
            case_name: case,
        };
        for id in 1..=(n as u64) {
            let worker = suite.start_br_stream_on(id);
            suite.endpoints.insert(id, worker);
        }
        suite.cluster.run();
        let mut cfg = BackupStreamConfig::default();
        cfg_f(&mut cfg);
        for id in 1..=(n as u64) {
            suite.start_endpoint(id, cfg.clone());
            let cli = suite.start_log_backup_client_on(id);
            suite.log_backup_cli.insert(id, cli);
        }
        // We must wait until the endpoints get ready to watching the metastore, or some
        // modifies may be lost. Either make Endpoint::with_client wait until watch did
        // start or make slash_etc support multi-version, then we can get rid of this
        // sleep.
        std::thread::sleep(Duration::from_secs(1));
        suite
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> MetaStore for ErrorStore<S> {
    type Snap = S::Snap;

    async fn snapshot(&self) -> backup_stream::errors::Result<Self::Snap> {
        (self.error_provider)("snapshot")?;
        self.inner.snapshot().await
    }

    async fn watch(
        &self,
        keys: backup_stream::metadata::store::Keys,
        start_rev: i64,
    ) -> backup_stream::errors::Result<backup_stream::metadata::store::KvChangeSubscription> {
        (self.error_provider)("watch")?;
        self.inner.watch(keys, start_rev).await
    }

    async fn txn(
        &self,
        txn: backup_stream::metadata::store::Transaction,
    ) -> backup_stream::errors::Result<()> {
        (self.error_provider)("txn")?;
        self.inner.txn(txn).await
    }

    async fn txn_cond(
        &self,
        txn: backup_stream::metadata::store::CondTransaction,
    ) -> backup_stream::errors::Result<()> {
        (self.error_provider)("txn_cond")?;
        self.inner.txn_cond(txn).await
    }
}

pub struct Suite {
    pub endpoints: HashMap<u64, LazyWorker<Task>>,
    pub meta_store: ErrorStore<SlashEtcStore>,
    pub cluster: Cluster<ServerCluster>,
    tikv_cli: HashMap<u64, TikvClient>,
    log_backup_cli: HashMap<u64, LogBackupClient>,
    obs: HashMap<u64, BackupStreamObserver>,
    env: Arc<grpcio::Environment>,
    // The place to make services live as long as suite.
    servers: Vec<Server>,

    temp_files: TempDir,
    pub flushed_files: TempDir,
    case_name: String,
}

impl Suite {
    pub const PROMISED_SHORT_VALUE: &'static [u8] = b"hello, world";
    pub const PROMISED_LONG_VALUE: &'static [u8] = &[0xbb; 4096];

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
        task.info.set_compression_type(CompressionType::Zstd);
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

    /// create a subscription stream. this has simply asserted no error, because
    /// in theory observing flushing should not emit error. change that if
    /// needed.
    pub fn flush_stream(
        &self,
        panic_while_fail: bool,
    ) -> impl Stream<Item = (u64, SubscribeFlushEventResponse)> {
        let streams = self
            .log_backup_cli
            .iter()
            .map(|(id, cli)| {
                let stream = cli
                    .subscribe_flush_event(&{
                        let mut r = SubscribeFlushEventRequest::default();
                        r.set_client_id(format!("test-{}", id));
                        r
                    })
                    .unwrap_or_else(|err| panic!("failed to subscribe on {} because {}", id, err));
                let id = *id;
                stream.filter_map(move |x| {
                    futures::future::ready(match x {
                        Ok(x) => Some((id, x)),
                        Err(err) => {
                            if panic_while_fail {
                                panic!("failed to rec from {} because {}", id, err)
                            } else {
                                println!("[WARN] failed to rec from {} because {}", id, err);
                                None
                            }
                        }
                    })
                })
            })
            .collect::<Vec<_>>();

        futures::stream::select_all(streams)
    }

    fn start_log_backup_client_on(&mut self, id: u64) -> LogBackupClient {
        let endpoint = self
            .endpoints
            .get(&id)
            .expect("must register endpoint first");

        let serv = Service::new(endpoint.scheduler());
        let builder =
            ServerBuilder::new(self.env.clone()).register_service(create_log_backup(serv));
        let mut server = builder.bind("127.0.0.1", 0).build().unwrap();
        server.start();
        let (_, port) = server.bind_addrs().next().unwrap();
        let addr = format!("127.0.0.1:{}", port);
        let channel = ChannelBuilder::new(self.env.clone()).connect(&addr);
        let client = LogBackupClient::new(channel);
        self.servers.push(server);
        client
    }

    fn start_endpoint(&mut self, id: u64, mut cfg: BackupStreamConfig) {
        let cluster = &mut self.cluster;
        let worker = self.endpoints.get_mut(&id).unwrap();
        let sim = cluster.sim.wl();
        let raft_router = sim.get_server_router(id);
        let cm = sim.get_concurrency_manager(id);
        let regions = sim.region_info_accessors.get(&id).unwrap().clone();
        let ob = self.obs.get(&id).unwrap().clone();
        cfg.enable = true;
        cfg.temp_path = format!("/{}/{}", self.temp_files.path().display(), id);
        let endpoint: TestEndpoint = Endpoint::new(
            id,
            self.meta_store.clone(),
            cfg,
            ResolvedTsConfig::default(),
            worker.scheduler(),
            ob,
            regions,
            raft_router,
            cluster.pd_client.clone(),
            cm,
            Arc::clone(&self.env),
            cluster.store_metas[&id]
                .lock()
                .unwrap()
                .region_read_progress
                .clone(),
            Arc::clone(&sim.security_mgr),
        );
        worker.start(endpoint);
    }

    pub fn get_meta_cli(&self) -> MetadataClient<ErrorStore<SlashEtcStore>> {
        MetadataClient::new(self.meta_store.clone(), 0)
    }

    #[allow(dead_code)]
    pub fn dump_slash_etc(&self) {
        self.meta_store.inner.blocking_lock().dump();
    }

    pub fn must_split(&mut self, key: &[u8]) {
        let region = self.cluster.get_region(key);
        self.cluster.must_split(&region, key);
    }

    pub fn must_register_task(&self, for_table: i64, name: &str) {
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
        self.wait_with_router(move |r| block_on(r.get_task_info(&name)).is_ok())
    }

    /// This function tries to calculate the global checkpoint from the flush
    /// status of nodes.
    ///
    /// NOTE: this won't check the region consistency for now, the checkpoint
    /// may be weaker than expected.
    pub fn global_checkpoint(&self) -> u64 {
        let (tx, rx) = std::sync::mpsc::channel();
        self.run(|| {
            let tx = tx.clone();
            Task::RegionCheckpointsOp(RegionCheckpointOperation::Get(
                RegionSet::Universal,
                Box::new(move |rs| rs.into_iter().for_each(|x| tx.send(x).unwrap())),
            ))
        });
        drop(tx);

        rx.into_iter()
            .map(|r| match r {
                GetCheckpointResult::Ok { checkpoint, region } => {
                    info!("getting checkpoint"; "checkpoint" => %checkpoint, utils::slog_region(&region));
                    checkpoint.into_inner()
                }
                GetCheckpointResult::NotFound { .. }
                | GetCheckpointResult::EpochNotMatch { .. } => {
                    unreachable!()
                }
            })
            .min()
            .unwrap_or(0)
    }

    pub async fn advance_global_checkpoint(&self, task: &str) -> Result<()> {
        let cp = self.global_checkpoint();
        self.meta_store
            .set(KeyValue(
                MetaKey::central_global_checkpoint_of(task),
                cp.to_be_bytes().to_vec(),
            ))
            .await
    }

    pub async fn write_records(
        &mut self,
        from: usize,
        n: usize,
        for_table: i64,
    ) -> HashSet<Vec<u8>> {
        let mut inserted = HashSet::default();
        for ts in (from..(from + n)).map(|x| x * 2) {
            let ts = ts as u64;
            let key = make_record_key(for_table, ts);
            let value = if ts % 4 == 0 {
                Self::PROMISED_SHORT_VALUE.to_vec()
            } else {
                Self::PROMISED_LONG_VALUE.to_vec()
            };
            let muts = vec![mutation(key.clone(), value)];
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

    pub fn commit_keys(&mut self, keys: Vec<Vec<u8>>, start_ts: TimeStamp, commit_ts: TimeStamp) {
        let mut region_keys = HashMap::<u64, Vec<Vec<u8>>>::new();
        for k in keys {
            let enc_key = Key::from_raw(&k).into_encoded();
            let region = self.cluster.get_region_id(&enc_key);
            region_keys.entry(region).or_default().push(k);
        }

        for (region, keys) in region_keys {
            self.must_kv_commit(region, keys, start_ts, commit_ts);
        }
    }

    pub fn just_commit_a_key(&mut self, key: Vec<u8>, start_ts: TimeStamp, commit_ts: TimeStamp) {
        let enc_key = Key::from_raw(&key).into_encoded();
        let region = self.cluster.get_region_id(&enc_key);
        self.must_kv_commit(region, vec![key], start_ts, commit_ts)
    }

    pub fn just_async_commit_prewrite(&mut self, ts: u64, for_table: i64) -> TimeStamp {
        let key = make_record_key(for_table, ts);
        let muts = vec![mutation(key.clone(), b"hello, world".to_vec())];
        let enc_key = Key::from_raw(&key).into_encoded();
        let region = self.cluster.get_region_id(&enc_key);
        let ts = self.must_kv_async_commit_prewrite(region, muts, key, TimeStamp::new(ts));
        info!("async prewrite success!"; "min_commit_ts" => %ts);
        ts
    }

    pub fn force_flush_files(&self, task: &str) {
        self.run(|| Task::ForceFlush(task.to_owned()));
        self.sync();
    }

    pub fn run(&self, mut t: impl FnMut() -> Task) {
        for worker in self.endpoints.values() {
            worker.scheduler().schedule(t()).unwrap();
        }
    }

    pub fn get_files_to_check(&self, path: &Path) -> std::io::Result<LogFiles> {
        let mut res = LogFiles::default();
        for entry in WalkDir::new(path.join("v1/backupmeta")) {
            let entry = entry?;
            if entry.file_name().to_str().unwrap().ends_with(".meta") {
                let content = std::fs::read(entry.path())?;
                let meta = protobuf::parse_from_bytes::<Metadata>(&content)?;
                for fg in meta.get_file_groups() {
                    let mut default_segs = vec![];
                    let mut write_segs = vec![];
                    for file in fg.get_data_files_info() {
                        let v = if file.cf == "default" || file.cf.is_empty() {
                            Some(&mut default_segs)
                        } else if file.cf == "write" {
                            Some(&mut write_segs)
                        } else {
                            None
                        };
                        v.into_iter().for_each(|v| {
                            v.push((
                                file.get_range_offset() as usize,
                                (file.get_range_offset() + file.get_range_length()) as usize,
                            ))
                        });
                    }
                    let p = path.join(fg.get_path());
                    if !default_segs.is_empty() {
                        res.default_cf.push(FileSegments {
                            path: p.clone(),
                            segments: default_segs,
                        })
                    }
                    if !write_segs.is_empty() {
                        res.write_cf.push(FileSegments {
                            path: p,
                            segments: write_segs,
                        })
                    }
                }
            }
        }
        Ok(res)
    }

    #[track_caller]
    pub fn check_for_write_records<'a>(
        &self,
        path: &Path,
        key_set: impl std::iter::Iterator<Item = &'a [u8]>,
    ) {
        let mut remain_keys = key_set.collect::<HashSet<_>>();
        let n = remain_keys.len();
        let mut extra_key = 0;
        let mut extra_len = 0;
        let files = self.get_files_to_check(path).unwrap_or_default();
        let mut default_keys = HashSet::new();
        let content_of = |buf: &[u8], range: (usize, usize)| {
            let mut decoder = ZstdDecoder::new(Vec::new());
            let pbuf: &[u8] = &buf[range.0..range.1];
            run_async_test(async {
                decoder.write_all(pbuf).await.unwrap();
                decoder.flush().await.unwrap();
                decoder.close().await.unwrap();
            });
            decoder.into_inner()
        };
        for entry in files.write_cf {
            debug!("checking write: {:?}", entry);

            let buf = std::fs::read(&entry.path).unwrap();
            for &file_info in entry.segments.iter() {
                let data = content_of(&buf, file_info);
                let mut iter = EventIterator::new(&data);
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
                    if wf.short_value.is_none() {
                        let mut key = Key::from_encoded_slice(iter.key()).truncate_ts().unwrap();
                        key.append_ts_inplace(wf.start_ts);

                        default_keys.insert(key.into_encoded());
                    } else {
                        assert_eq!(wf.short_value, Some(Self::PROMISED_SHORT_VALUE));
                    }
                }
            }
        }

        for entry in files.default_cf {
            debug!("checking default: {:?}", entry);

            let buf = std::fs::read(&entry.path).unwrap();
            for &file_info in entry.segments.iter() {
                let data = content_of(&buf, file_info);
                let mut iter = EventIterator::new(&data);
                loop {
                    if !iter.valid() {
                        break;
                    }
                    iter.next().unwrap();
                    if !default_keys.remove(iter.key()) {
                        extra_key += 1;
                        extra_len += iter.key().len() + iter.value().len();
                    }

                    let value = iter.value();
                    assert_eq!(value, Self::PROMISED_LONG_VALUE);
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
        assert_empty(&remain_keys, "not all keys are recorded");
        assert_empty(&default_keys, "some keys don't have default entry");
    }
}

#[track_caller]
fn assert_empty(v: &HashSet<impl AsRef<[u8]>>, msg: impl Display) {
    if !v.is_empty() {
        panic!(
            "{msg}: it remains {:?}... (total = {})",
            v.iter().take(3).map(|v| hex::encode(v)).collect::<Vec<_>>(),
            v.len()
        );
    }
}

// Copy & Paste from cdc::tests::TestSuite, maybe make it a mixin?
impl Suite {
    pub fn tso(&self) -> TimeStamp {
        run_async_test(self.cluster.pd_client.get_tso()).unwrap()
    }

    pub fn must_kv_pessimistic_lock(
        &mut self,
        region_id: u64,
        keys: Vec<Vec<u8>>,
        ts: TimeStamp,
        pk: Vec<u8>,
    ) {
        let mut lock_req = PessimisticLockRequest::new();
        lock_req.set_context(self.get_context(region_id));
        let mut mutations = vec![];
        for key in keys {
            mutations.push(mutation_op(key, vec![], Op::PessimisticLock));
        }
        lock_req.set_mutations(mutations.into());
        lock_req.primary_lock = pk;
        lock_req.start_version = ts.into_inner();
        lock_req.lock_ttl = ts.into_inner() + 1;
        let resp = self
            .get_tikv_client(region_id)
            .kv_pessimistic_lock(&lock_req)
            .unwrap();

        assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
        assert!(resp.errors.is_empty(), "{:?}", resp.get_errors());
    }

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
        self.wait_with_router(|_| true)
    }

    pub fn wait_with(&self, cond: impl FnMut(&mut TestEndpoint) -> bool + Send + 'static + Clone) {
        self.endpoints
            .iter()
            .map({
                move |(_, wkr)| {
                    let (tx, rx) = std::sync::mpsc::channel();
                    let mut cond = cond.clone();
                    wkr.scheduler()
                        .schedule(Task::Sync(
                            Box::new(move || tx.send(()).unwrap()),
                            Box::new(move |this| {
                                let ep = this
                                    .downcast_mut::<TestEndpoint>()
                                    .expect("`Sync` with wrong type");
                                cond(ep)
                            }),
                        ))
                        .unwrap();
                    rx
                }
            })
            .for_each(|rx| rx.recv().unwrap())
    }

    pub fn wait_with_router(&self, mut cond: impl FnMut(&Router) -> bool + Send + 'static + Clone) {
        self.wait_with(move |ep: &mut TestEndpoint| cond(&ep.range_router))
    }

    pub fn wait_for_flush(&self) {
        self.wait_with_router(move |r| {
            let task_names = block_on(r.select_task(TaskSelector::All.reference()));
            for task_name in task_names {
                let tsk = block_on(r.get_task_info(&task_name));
                if tsk.unwrap().is_flushing() {
                    return false;
                }
            }
            true
        });
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
                self.cluster.must_transfer_leader(region_id, peer.clone());
                self.cluster.reset_leader_of_region(region_id);
                return;
            }
        }
        panic!("must_shuffle_leader: region has no peer")
    }
}

pub fn run_async_test<T>(test: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(test)
}
