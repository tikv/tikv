// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::Deref,
    sync::{atomic::AtomicU64, Arc},
    thread,
    time::Duration,
    vec,
};

use bytes::Buf;
use file_system::IORateLimiter;
use kvenginepb as pb;
use tikv_util::mpsc;

use crate::{dfs::InMemFS, *};

macro_rules! unwrap_or_return {
    ( $e:expr, $m:expr ) => {
        match $e {
            Ok(x) => x,
            Err(y) => {
                error!("{:?} {:?}", y, $m);
                return;
            }
        }
    };
}

// FIXME(youjiali1995): it has data race and may not be suilable for the current kvengine.
#[ignore]
#[test]
fn test_engine() {
    init_logger();
    let (listener_tx, listener_rx) = mpsc::unbounded();
    let tester = EngineTester::new();
    let meta_change_listener = Box::new(TestMetaChangeListener {
        sender: listener_tx,
    });
    let rate_limiter = Arc::new(IORateLimiter::new_for_test());
    let engine = Engine::open(
        tester.fs.clone(),
        tester.opts.clone(),
        tester.clone(),
        tester.clone(),
        tester.core.clone(),
        meta_change_listener,
        rate_limiter,
    )
    .unwrap();
    {
        let shard = engine.get_shard(1).unwrap();
        store_bool(&shard.active, true);
    }
    let (applier_tx, applier_rx) = mpsc::unbounded();
    let (meta_tx, meta_rx) = mpsc::unbounded();
    let meta_listener = MetaListener::new(listener_rx, applier_tx.clone());
    thread::spawn(move || {
        meta_listener.run();
    });
    let applier = Applier::new(engine.clone(), applier_rx, meta_tx);
    thread::spawn(move || {
        applier.run();
    });
    let meta_applier = MetaApplier::new(engine.clone(), meta_rx);
    thread::spawn(move || {
        meta_applier.run();
    });
    let mut keys = vec![];
    for i in &[1000, 3000, 6000, 9000] {
        keys.push(i_to_key(*i));
    }
    let mut splitter = Splitter::new(keys.clone(), applier_tx.clone());
    let handle = thread::spawn(move || {
        splitter.run();
    });
    let (begin, end) = (0, 10000);
    load_data(begin, end, applier_tx);
    handle.join().unwrap();
    check_get(begin, end, &engine);
    check_iterater(begin, end, &engine);
}

#[derive(Clone)]
struct TestMetaChangeListener {
    sender: mpsc::Sender<pb::ChangeSet>,
}

impl MetaChangeListener for TestMetaChangeListener {
    fn on_change_set(&self, cs: pb::ChangeSet) {
        println!("on meta change listener");
        info!("on meta change listener");
        self.sender.send(cs).unwrap();
    }
}

#[derive(Clone)]
struct EngineTester {
    core: Arc<EngineTesterCore>,
}

impl Deref for EngineTester {
    type Target = EngineTesterCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl EngineTester {
    fn new() -> Self {
        let initial_cs = new_initial_cs();
        let initial_meta = ShardMeta::new(&initial_cs);
        let metas = dashmap::DashMap::new();
        metas.insert(1, Arc::new(initial_meta));
        Self {
            core: Arc::new(EngineTesterCore {
                metas,
                fs: Arc::new(InMemFS::new()),
                opts: Arc::new(new_test_options()),
                id: AtomicU64::new(0),
            }),
        }
    }
}

struct EngineTesterCore {
    metas: dashmap::DashMap<u64, Arc<ShardMeta>>,
    fs: Arc<dfs::InMemFS>,
    opts: Arc<Options>,
    id: AtomicU64,
}

impl MetaIterator for EngineTester {
    fn iterate<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(kvenginepb::ChangeSet),
    {
        for meta in &self.metas {
            f(meta.value().to_change_set())
        }
        Ok(())
    }
}

impl RecoverHandler for EngineTester {
    fn recover(&self, _engine: &Engine, _shard: &Arc<Shard>, _info: &ShardMeta) -> Result<()> {
        Ok(())
    }
}

impl IDAllocator for EngineTesterCore {
    fn alloc_id(&self, count: usize) -> Vec<u64> {
        let start_id = self
            .id
            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let end_id = start_id + count as u64;
        let mut ids = Vec::with_capacity(count);
        for id in start_id..end_id {
            ids.push(id);
        }
        ids
    }
}

struct MetaListener {
    meta_rx: mpsc::Receiver<pb::ChangeSet>,
    applier_tx: mpsc::Sender<ApplyTask>,
}

impl MetaListener {
    fn new(meta_rx: mpsc::Receiver<pb::ChangeSet>, applier_tx: mpsc::Sender<ApplyTask>) -> Self {
        Self {
            meta_rx,
            applier_tx,
        }
    }

    fn run(&self) {
        loop {
            let cs = unwrap_or_return!(self.meta_rx.recv(), "meta_listener_a");
            let (tx, rx) = mpsc::bounded(1);
            let task = ApplyTask::new_cs(cs, tx);
            self.applier_tx.send(task).unwrap();
            let res = unwrap_or_return!(rx.recv(), "meta_listener_b");
            unwrap_or_return!(res, "meta_listener_c");
        }
    }
}

struct Applier {
    engine: Engine,
    task_rx: mpsc::Receiver<ApplyTask>,
    meta_tx: mpsc::Sender<pb::ChangeSet>,
}

impl Applier {
    fn new(
        engine: Engine,
        task_rx: mpsc::Receiver<ApplyTask>,
        meta_tx: mpsc::Sender<pb::ChangeSet>,
    ) -> Self {
        Self {
            engine,
            task_rx,
            meta_tx,
        }
    }

    fn run(&self) {
        let mut seq = 2;
        loop {
            let mut task = unwrap_or_return!(self.task_rx.recv(), "apply recv task");
            seq += 1;
            if let Some(wb) = task.wb.as_mut() {
                wb.set_sequence(seq);
                self.engine.write(wb);
            }
            if let Some(mut cs) = task.cs.take() {
                cs.set_sequence(seq);
                if cs.has_split() {
                    let mut ids = vec![];
                    for new_shard in cs.get_split().get_new_shards() {
                        ids.push(new_shard.shard_id);
                    }
                    unwrap_or_return!(self.engine.split(cs, 1), "apply split");
                    for id in ids {
                        let shard = self.engine.get_shard(id).unwrap();
                        shard.set_active(true);
                    }
                    info!("applier executed split");
                } else {
                    unwrap_or_return!(self.meta_tx.send(cs), "apply else");
                    info!("applier sent cs to meta applier");
                }
            }
            task.result_tx.send(Ok(())).unwrap();
        }
    }
}

struct ApplyTask {
    wb: Option<WriteBatch>,
    cs: Option<pb::ChangeSet>,
    result_tx: mpsc::Sender<Result<()>>,
}

impl ApplyTask {
    fn new_cs(cs: pb::ChangeSet, result_tx: mpsc::Sender<Result<()>>) -> Self {
        Self {
            wb: None,
            cs: Some(cs),
            result_tx,
        }
    }

    fn new_wb(wb: WriteBatch, result_tx: mpsc::Sender<Result<()>>) -> Self {
        Self {
            wb: Some(wb),
            cs: None,
            result_tx,
        }
    }
}

struct MetaApplier {
    engine: Engine,
    meta_rx: mpsc::Receiver<pb::ChangeSet>,
}

impl MetaApplier {
    fn new(engine: Engine, meta_rx: mpsc::Receiver<pb::ChangeSet>) -> Self {
        Self { engine, meta_rx }
    }

    fn run(&self) {
        loop {
            let cs = unwrap_or_return!(self.meta_rx.recv(), "meta_applier recv");
            unwrap_or_return!(
                self.engine.apply_change_set(ChangeSet::new(cs)),
                "meta_applier cs"
            );
        }
    }
}

struct Splitter {
    apply_sender: mpsc::Sender<ApplyTask>,
    keys: Vec<Vec<u8>>,
    shard_ver: u64,
    new_id: u64,
}

impl Splitter {
    fn new(keys: Vec<Vec<u8>>, apply_sender: mpsc::Sender<ApplyTask>) -> Self {
        Self {
            keys,
            apply_sender,
            shard_ver: 1,
            new_id: 1,
        }
    }

    fn run(&mut self) {
        let keys = self.keys.clone();
        for key in keys {
            thread::sleep(Duration::from_millis(200));
            self.new_id += 1;
            self.split(key.clone(), vec![self.new_id, 1]);
        }
    }

    fn send_task(&mut self, cs: pb::ChangeSet) {
        let (tx, rx) = mpsc::bounded(1);
        let task = ApplyTask {
            cs: Some(cs),
            wb: None,
            result_tx: tx,
        };
        self.apply_sender.send(task).unwrap();
        let res = unwrap_or_return!(rx.recv(), "splitter recv");
        res.unwrap();
    }

    fn split(&mut self, key: Vec<u8>, new_ids: Vec<u64>) {
        let mut cs = pb::ChangeSet::new();
        cs.set_shard_id(1);
        cs.set_shard_ver(self.shard_ver);
        let mut finish_split = pb::Split::new();
        finish_split.set_keys(protobuf::RepeatedField::from_vec(vec![key.clone()]));
        let mut new_shards = Vec::new();
        for new_id in &new_ids {
            let mut new_shard = pb::Properties::new();
            new_shard.set_shard_id(*new_id);
            new_shards.push(new_shard);
        }
        finish_split.set_new_shards(protobuf::RepeatedField::from_vec(new_shards));
        cs.set_split(finish_split);
        self.send_task(cs);
        info!(
            "splitter sent split task to applier, ids {:?} key {}",
            new_ids,
            String::from_utf8_lossy(key.as_slice())
        );
        self.shard_ver += 1;
    }
}

fn new_initial_cs() -> pb::ChangeSet {
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(1);
    cs.set_shard_ver(1);
    cs.set_sequence(1);
    let mut snap = pb::Snapshot::new();
    snap.set_base_version(1);
    snap.set_end(GLOBAL_SHARD_END_KEY.to_vec());
    let props = snap.mut_properties();
    props.shard_id = 1;
    cs.set_snapshot(snap);
    cs
}

fn new_test_options() -> Options {
    let mut opts = Options::default();
    opts.table_builder_options.block_size = 4 << 15;
    opts.dynamic_mem_table_size = false;
    opts.base_size = 4 << 15;
    opts.num_compactors = 1;
    opts
}

fn i_to_key(i: i32) -> Vec<u8> {
    format!("key{:06}", i).into_bytes()
}

fn load_data(begin: usize, end: usize, tx: mpsc::Sender<ApplyTask>) {
    let mut wb = WriteBatch::new(1);
    for i in begin..end {
        let key = format!("key{:06}", i);
        for cf in 0..3 {
            let val = key.repeat(cf + 2);
            let version = if cf == 1 { 0 } else { 1 };
            wb.put(cf, key.as_bytes(), val.as_bytes(), 0, &[], version);
        }
        if i % 100 == 99 {
            info!("load data {}:{}", i - 99, i);
            write_data(wb, &tx);
            wb = WriteBatch::new(1);
            thread::sleep(Duration::from_millis(10));
        }
    }
    if wb.num_entries() > 0 {
        write_data(wb, &tx);
    }
}

fn write_data(wb: WriteBatch, applier_tx: &mpsc::Sender<ApplyTask>) {
    let (result_tx, result_rx) = mpsc::bounded(1);
    let task = ApplyTask::new_wb(wb, result_tx);
    if let Err(err) = applier_tx.send(task) {
        panic!("{:?}", err);
    }
    result_rx.recv().unwrap().unwrap();
}

fn check_get(begin: usize, end: usize, en: &Engine) {
    for i in begin..end {
        let key = format!("key{:06}", i);
        let shard = get_shard_for_key(key.as_bytes(), en);
        let snap = SnapAccess::new(&shard);
        for cf in 0..3 {
            let version = if cf == 1 { 0 } else { 2 };
            let item = snap.get(cf, key.as_bytes(), version);
            if item.is_valid() {
                assert_eq!(item.get_value(), key.repeat(cf + 2).as_bytes());
            } else {
                let shard_stats = shard.get_stats();
                panic!(
                    "failed to get key {}, shard {}:{}, stats {:?}",
                    key, shard.id, shard.ver, shard_stats,
                );
            }
        }
    }
}

fn check_iterater(begin: usize, end: usize, en: &Engine) {
    thread::sleep(Duration::from_secs(1));
    for cf in 0..3 {
        let mut i = begin;
        let ids = vec![2, 3, 4, 5, 1];
        for id in ids {
            let shard = en.get_shard(id).unwrap();
            let snap = SnapAccess::new(&shard);
            let mut iter = snap.new_iterator(cf, false, false, None);
            iter.seek(shard.start.chunk());
            while iter.valid() {
                if iter.key.chunk() >= shard.end.chunk() {
                    break;
                }
                let key = format!("key{:06}", i);
                assert_eq!(iter.key(), key.as_bytes());
                let item = iter.item();
                assert_eq!(item.get_value(), key.repeat(cf + 2).as_bytes());
                i += 1;
                iter.next();
            }
        }
        assert_eq!(i, end);
    }
}

fn get_shard_for_key(key: &[u8], en: &Engine) -> Arc<Shard> {
    for id in 1_u64..=5 {
        if let Some(shard) = en.get_shard(id) {
            if shard.overlap_key(key) {
                return shard;
            }
        }
    }
    en.get_shard(1).unwrap()
}

pub(crate) fn init_logger() {
    use slog::Drain;
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build();
    let drain = std::sync::Mutex::new(drain).fuse();
    let logger = slog::Logger::root(drain, o!());
    slog_global::set_global(logger);
}
