use crate::{dfs::InMemFS, *};
use crossbeam::channel;
use crossbeam_epoch as epoch;
use futures::executor;
use kvenginepb as pb;
use std::{
    ops::Deref,
    sync::{atomic::AtomicU64, Arc},
    thread,
    time::Duration,
};

#[test]
fn test_engine() {
    init_logger();
    let (tx, rx) = channel::bounded(256);
    let tester = EngineTester::new();

    let engine = Engine::open(
        tester.fs.clone(),
        tester.opts.clone(),
        tester.clone(),
        tester.clone(),
        tester.core.clone(),
        tx,
    )
    .unwrap();
    {
        let g = &epoch::pin();
        let shard = engine.get_shard(1, g).unwrap();
        store_bool(&shard.active, true);
    }
    let meta_applier = TestMetaApplier {
        meta_rx: rx,
        engine: engine.clone(),
    };
    thread::spawn(move || {
        meta_applier.run();
    });

    let (begin, end) = (0, 10000);
    let cf = 0;
    let val_repeat = 10;
    let mut seq = 1;
    load_data(&engine, begin, end, 0, 10, &mut seq);
    let g = &epoch::pin();
    let shard = engine.get_shard(1, g).unwrap();
    store_bool(&shard.active, true);
    thread::sleep(Duration::from_millis(100));
    let snap = shard.new_snap_access(engine.opts.cfs, g);
    check_get(&snap, begin, end, cf, val_repeat);
    check_iterater(&snap, begin, end, cf, val_repeat);
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
        let initial_meta = ShardMeta::new(initial_cs);
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
    fn recover(&self, engine: &Engine, shard: &Shard, info: &ShardMeta) -> Result<()> {
        return Ok(());
    }
}

impl IDAllocator for EngineTesterCore {
    fn alloc_id(&self, count: usize) -> std::result::Result<Vec<u64>, String> {
        let start_id = self
            .id
            .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let end_id = start_id + count as u64;
        let mut ids = Vec::with_capacity(count);
        for id in start_id..end_id {
            ids.push(id);
        }
        Ok(ids)
    }
}

struct TestMetaApplier {
    engine: Engine,
    meta_rx: channel::Receiver<pb::ChangeSet>,
}

impl TestMetaApplier {
    fn run(&self) {
        let mut seq = 2;
        loop {
            if let Ok(cs) = self.meta_rx.recv() {
                let mut cs = cs;
                cs.set_sequence(seq);
                seq += 1;
                if let Err(e) = self.engine.apply_change_set(cs) {
                    error!("{:?}", e);
                }
            }
        }
    }
}

fn new_initial_cs() -> pb::ChangeSet {
    let mut cs = pb::ChangeSet::new();
    cs.set_shard_id(1);
    cs.set_shard_ver(1);
    cs.set_sequence(1);
    let mut snap = pb::Snapshot::new();
    snap.set_base_ts(1);
    snap.set_end(GLOBAL_SHARD_END_KEY.to_vec());
    let props = snap.mut_properties();
    props.shard_id = 1;
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

fn load_data(en: &Engine, begin: usize, end: usize, cf: usize, val_repeat: usize, seq: &mut u64) {
    let mut wb = WriteBatch::new(1, en.opts.cfs.clone());
    for i in begin..end {
        let key = format!("key{:06}", i);
        let val = key.repeat(val_repeat);
        wb.put(cf, key.as_bytes(), val.as_bytes(), 0, &[], 1);
        if i % 100 == 99 {
            *seq = *seq + 1;
            wb.set_sequence(*seq);
            en.write(&mut wb);
            wb.reset();
        }
    }
    if wb.num_entries() > 0 {
        en.write(&mut wb);
    }
}

fn check_get(snap: &SnapAccess, begin: usize, end: usize, cf: usize, val_repeat: usize) {
    for i in begin..end {
        let key = format!("key{:06}", i);
        let item = snap.get(cf, key.as_bytes(), 2).unwrap();
        assert_eq!(item.get_value(), key.repeat(val_repeat).as_bytes());
    }
}

fn check_iterater(snap: &SnapAccess, begin: usize, end: usize, cf: usize, val_repeat: usize) {
    let mut iter = snap.new_iterator(cf, false, false);
    let mut i = begin;
    iter.rewind();
    while iter.valid() {
        let key = format!("key{:06}", i);
        assert_eq!(iter.key(), key.as_bytes());
        let item = iter.item();
        assert_eq!(item.get_value(), key.repeat(val_repeat).as_bytes());
        i += 1;
        iter.next();
    }
    assert_eq!(i, end);
}

fn init_logger() {
    use slog::Drain;
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build();
    let drain = std::sync::Mutex::new(drain).fuse();
    let logger = slog::Logger::root(drain, o!());
    slog_global::set_global(logger);
}
