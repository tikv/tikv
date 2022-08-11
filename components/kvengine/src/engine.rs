// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display, Formatter},
    iter::{FromIterator, Iterator},
    ops::Deref,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::{mapref::entry::Entry, DashMap};
use file_system::IORateLimiter;
use fslock;
use moka::sync::SegmentedCache;
use slog_global::info;
use tikv_util::mpsc;

use crate::{
    apply::ChangeSet,
    meta::ShardMeta,
    table::{
        memtable::CFTable,
        sstable::{BlockCacheKey, MAGIC_NUMBER, ZSTD_COMPRESSION},
    },
    *,
};

#[derive(Clone)]
pub struct Engine {
    pub core: Arc<EngineCore>,
    pub(crate) meta_change_listener: Box<dyn MetaChangeListener>,
}

impl Deref for Engine {
    type Target = EngineCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Debug for Engine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO: complete debug info.
        let cnt = self.shards.len();
        let str = format!("num_shards: {}", cnt);
        f.write_str(&str)
    }
}

impl Engine {
    pub fn open(
        fs: Arc<dyn dfs::DFS>,
        opts: Arc<Options>,
        meta_iter: impl MetaIterator,
        recoverer: impl RecoverHandler + 'static,
        id_allocator: Arc<dyn IDAllocator>,
        meta_change_listener: Box<dyn MetaChangeListener>,
        rate_limiter: Arc<IORateLimiter>,
    ) -> Result<Engine> {
        info!("open KVEngine");
        if !opts.local_dir.exists() {
            std::fs::create_dir_all(&opts.local_dir).unwrap();
        }
        if !opts.local_dir.is_dir() {
            panic!("path {:?} is not dir", &opts.local_dir);
        }
        let lock_path = opts.local_dir.join("LOCK");
        let mut x = fslock::LockFile::open(&lock_path)?;
        x.lock()?;
        let mut max_capacity =
            opts.max_block_cache_size as usize / opts.table_builder_options.block_size;
        if max_capacity < 512 {
            max_capacity = 512
        }
        let cache: SegmentedCache<BlockCacheKey, Bytes> =
            SegmentedCache::new(max_capacity as u64, 256);
        let (flush_tx, flush_rx) = mpsc::unbounded();
        let (compact_tx, compact_rx) = mpsc::unbounded();
        let (free_tx, free_rx) = mpsc::unbounded();
        let core = EngineCore {
            engine_id: AtomicU64::new(meta_iter.engine_id()),
            shards: DashMap::new(),
            opts: opts.clone(),
            flush_tx,
            compact_tx,
            fs: fs.clone(),
            cache,
            comp_client: CompactionClient::new(fs.clone(), opts.remote_compactor_addr.clone()),
            id_allocator,
            managed_safe_ts: AtomicU64::new(0),
            tmp_file_id: AtomicU64::new(0),
            rate_limiter,
            free_tx,
        };
        let en = Engine {
            core: Arc::new(core),
            meta_change_listener,
        };
        let metas = en.read_meta(meta_iter)?;
        info!("engine load {} shards", metas.len());
        en.load_shards(metas, recoverer)?;
        let flush_en = en.clone();
        thread::Builder::new()
            .name("flush".to_string())
            .spawn(move || {
                flush_en.run_flush_worker(flush_rx);
            })
            .unwrap();
        let compact_en = en.clone();
        thread::Builder::new()
            .name("compaction".to_string())
            .spawn(move || {
                compact_en.run_compaction(compact_rx);
            })
            .unwrap();
        thread::Builder::new()
            .name("free_mem".to_string())
            .spawn(move || {
                free_mem(free_rx);
            })
            .unwrap();
        Ok(en)
    }

    fn load_shards(
        &self,
        metas: HashMap<u64, ShardMeta>,
        recoverer: impl RecoverHandler + 'static,
    ) -> Result<()> {
        let mut parents = HashSet::new();
        for meta in metas.values() {
            if let Some(parent) = &meta.parent {
                if !parents.contains(&parent.id) {
                    parents.insert(parent.id);
                    info!("load parent of {}", meta.tag());
                    let parent_shard = self.load_shard(parent)?;
                    recoverer.recover(self, &parent_shard, parent)?;
                }
                if parent.id != meta.id {
                    self.load_with_parent_mem_tables(meta);
                }
            }
        }
        let concurrency = num_cpus::get();
        let (token_tx, token_rx) = tikv_util::mpsc::bounded(concurrency);
        for _ in 0..concurrency {
            token_tx.send(true).unwrap();
        }
        for meta in metas.values() {
            let meta = meta.clone();
            let engine = self.clone();
            let recoverer = recoverer.clone();
            let token_tx = token_tx.clone();
            token_rx.recv().unwrap();
            std::thread::spawn(move || {
                if let Some(parent) = &meta.parent {
                    if parent.id == meta.id {
                        engine.load_with_parent_mem_tables(&meta);
                    }
                }
                let shard = engine.load_shard(&meta).unwrap();
                recoverer.recover(&engine, &shard, &meta).unwrap();
                token_tx.send(true).unwrap();
            });
        }
        for _ in 0..concurrency {
            token_rx.recv().unwrap();
        }
        Ok(())
    }

    fn load_with_parent_mem_tables(&self, meta: &ShardMeta) {
        let parent_id = meta.parent.as_ref().unwrap().id;
        let parent_data = self.shards.get(&parent_id).unwrap().get_data();
        let shard = self.load_shard(&meta).unwrap();
        let shard_data = shard.get_data();
        let new_data = ShardData::new(
            shard_data.start.clone(),
            shard_data.end.clone(),
            shard_data.del_prefixes.clone(),
            shard.split_mem_tables(&parent_data.mem_tbls),
            shard_data.l0_tbls.clone(),
            shard_data.cfs.clone(),
        );
        shard.set_data(new_data);
    }

    pub fn set_engine_id(&self, engine_id: u64) {
        self.engine_id.store(engine_id, Ordering::Release);
    }

    pub fn get_engine_id(&self) -> u64 {
        self.engine_id.load(Ordering::Acquire)
    }
}

pub struct EngineCore {
    pub(crate) engine_id: AtomicU64,
    pub(crate) shards: DashMap<u64, Arc<Shard>>,
    pub opts: Arc<Options>,
    pub(crate) flush_tx: mpsc::Sender<FlushMsg>,
    pub(crate) compact_tx: mpsc::Sender<CompactMsg>,
    pub(crate) fs: Arc<dyn dfs::DFS>,
    pub(crate) cache: SegmentedCache<BlockCacheKey, Bytes>,
    pub(crate) comp_client: CompactionClient,
    pub(crate) id_allocator: Arc<dyn IDAllocator>,
    pub(crate) managed_safe_ts: AtomicU64,
    pub(crate) tmp_file_id: AtomicU64,
    pub(crate) rate_limiter: Arc<IORateLimiter>,
    pub(crate) free_tx: mpsc::Sender<CFTable>,
}

impl EngineCore {
    fn read_meta(&self, meta_iter: impl MetaIterator) -> Result<HashMap<u64, ShardMeta>> {
        let mut metas = HashMap::new();
        let engine_id = meta_iter.engine_id();
        meta_iter.iterate(|cs| {
            let meta = ShardMeta::new(engine_id, &cs);
            metas.insert(meta.id, meta);
        })?;
        Ok(metas)
    }

    fn load_shard(&self, meta: &ShardMeta) -> Result<Arc<Shard>> {
        if let Some(shard) = self.get_shard(meta.id) {
            if shard.ver == meta.ver {
                return Ok(shard);
            }
        }
        info!("load shard {}", meta.tag());
        let change_set = self.prepare_change_set(meta.to_change_set(), false)?;
        self.ingest(change_set, false)?;
        let shard = self.get_shard(meta.id);
        Ok(shard.unwrap())
    }

    pub fn ingest(&self, cs: ChangeSet, active: bool) -> Result<()> {
        let engine_id = self.engine_id.load(Ordering::Acquire);
        let shard = Shard::new_for_ingest(engine_id, &cs, self.opts.clone());
        shard.set_active(active);
        let (l0s, scfs) = self.create_snapshot_tables(cs.get_snapshot(), &cs);
        let data = ShardData::new(
            shard.start.clone(),
            shard.end.clone(),
            shard.get_data().del_prefixes.clone(),
            vec![CFTable::new()],
            l0s,
            scfs,
        );
        shard.set_data(data);
        store_bool(&shard.initial_flushed, true);
        self.refresh_shard_states(&shard);
        match self.shards.entry(shard.id) {
            Entry::Occupied(entry) => {
                let old = entry.get();
                let mut old_mem_tbls = old.get_data().mem_tbls.clone();
                let old_total_seq = old.get_write_sequence() + old.get_meta_sequence();
                let new_total_seq = shard.get_write_sequence() + shard.get_meta_sequence();
                if new_total_seq > old_total_seq {
                    entry.replace_entry(Arc::new(shard));
                    for mem_tbl in old_mem_tbls.drain(..) {
                        self.free_tx.send(mem_tbl).unwrap();
                    }
                } else {
                    info!("ingest found shard already exists with higher sequence, skip insert");
                    return Ok(());
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(shard));
            }
        }
        Ok(())
    }

    pub fn get_snap_access(&self, id: u64) -> Option<SnapAccess> {
        if let Some(ptr) = self.shards.get(&id) {
            return Some(ptr.new_snap_access());
        }
        None
    }

    pub fn get_shard(&self, id: u64) -> Option<Arc<Shard>> {
        if let Some(ptr) = self.shards.get(&id) {
            return Some(ptr.value().clone());
        }
        None
    }

    pub fn get_shard_with_ver(&self, shard_id: u64, shard_ver: u64) -> Result<Arc<Shard>> {
        let shard = self.get_shard(shard_id).ok_or(Error::ShardNotFound)?;
        if shard.ver != shard_ver {
            warn!(
                "shard {} version not match, request {}",
                shard.tag(),
                shard_ver,
            );
            return Err(Error::ShardNotMatch);
        }
        Ok(shard)
    }

    pub fn remove_shard(&self, shard_id: u64) -> bool {
        let x = self.shards.remove(&shard_id);
        if let Some((_, _ptr)) = x {
            return true;
        }
        false
    }

    pub fn size(&self) -> u64 {
        self.shards
            .iter()
            .map(|x| x.value().estimated_size.load(Ordering::Relaxed) + 1)
            .reduce(|x, y| x + y)
            .unwrap_or(0)
    }

    pub(crate) fn trigger_flush(&self, shard: &Shard) {
        if !shard.is_active() {
            return;
        }
        if !shard.get_initial_flushed() {
            self.trigger_initial_flush(shard);
            return;
        }
        let data = shard.get_data();
        let mut mem_tbls = data.mem_tbls.clone();
        while let Some(mem_tbl) = mem_tbls.pop() {
            // writable mem-table's version is 0.
            if mem_tbl.get_version() != 0 {
                self.flush_tx
                    .send(FlushMsg::Task(FlushTask::new_normal(shard, mem_tbl)))
                    .unwrap();
            }
        }
    }

    fn trigger_initial_flush(&self, shard: &Shard) {
        let guard = shard.parent_snap.read().unwrap();
        let parent_snap = guard.as_ref().unwrap().clone();
        let mut mem_tbls = vec![];
        let data = shard.get_data();
        // A newly split shard's meta_sequence is an in-mem state until initial flush.
        let data_sequence = shard.get_meta_sequence();
        for mem_tbl in &data.mem_tbls.as_slice()[1..] {
            debug!(
                "trigger initial flush check mem table version {}, size {}, parent base {} parent write {}",
                mem_tbl.get_version(),
                mem_tbl.size(),
                parent_snap.base_version,
                parent_snap.data_sequence,
            );
            if mem_tbl.get_version() > parent_snap.base_version + parent_snap.data_sequence
                && mem_tbl.get_version() <= shard.base_version + data_sequence
                && mem_tbl.has_data_in_range(&shard.start, &shard.end)
            {
                mem_tbls.push(mem_tbl.clone());
            }
        }
        self.flush_tx
            .send(FlushMsg::Task(FlushTask::new_initial(
                shard,
                InitialFlush {
                    parent_snap,
                    mem_tbls,
                    base_version: shard.base_version,
                    data_sequence,
                },
            )))
            .unwrap();
    }

    pub fn build_ingest_files(
        &self,
        shard_id: u64,
        shard_ver: u64,
        mut iter: Box<dyn table::Iterator>,
        ingest_id: Vec<u8>,
        meta: ShardMeta,
    ) -> Result<kvenginepb::ChangeSet> {
        let shard = self.get_shard_with_ver(shard_id, shard_ver)?;
        let l0_version = shard.load_mem_table_version();
        let mut cs = new_change_set(shard_id, shard_ver);
        let ingest_files = cs.mut_ingest_files();
        ingest_files
            .mut_properties()
            .mut_keys()
            .push(INGEST_ID_KEY.to_string());
        ingest_files.mut_properties().mut_values().push(ingest_id);
        let opts = dfs::Options::new(shard_id, shard_ver);
        let (tx, rx) = tikv_util::mpsc::unbounded();
        let mut tbl_cnt = 0;
        let block_size = self.opts.table_builder_options.block_size;
        let max_table_size = self.opts.table_builder_options.max_table_size;
        let mut builder = table::sstable::Builder::new(0, block_size, ZSTD_COMPRESSION);
        let mut fids = vec![];
        iter.rewind();
        while iter.valid() {
            if fids.is_empty() {
                fids = self.id_allocator.alloc_id(10);
            }
            let id = fids.pop().unwrap();
            builder.reset(id);
            while iter.valid() {
                builder.add(iter.key(), iter.value());
                iter.next();
                if builder.estimated_size() > max_table_size || !iter.valid() {
                    info!("builder estimated_size {}", builder.estimated_size());
                    let mut buf = BytesMut::with_capacity(builder.estimated_size());
                    let res = builder.finish(0, &mut buf);
                    let level = meta.get_ingest_level(&res.smallest, &res.biggest);
                    if level == 0 {
                        let mut offsets = vec![buf.len() as u32; NUM_CFS];
                        offsets[0] = 0;
                        for offset in offsets {
                            buf.put_u32_le(offset);
                        }
                        buf.put_u64_le(l0_version);
                        buf.put_u32_le(NUM_CFS as u32);
                        buf.put_u32_le(MAGIC_NUMBER);
                        let mut l0_create = kvenginepb::L0Create::new();
                        l0_create.set_id(id);
                        l0_create.set_smallest(res.smallest);
                        l0_create.set_biggest(res.biggest);
                        ingest_files.mut_l0_creates().push(l0_create);
                    } else {
                        let mut tbl_create = kvenginepb::TableCreate::new();
                        tbl_create.set_id(id);
                        tbl_create.set_cf(0);
                        tbl_create.set_level(level);
                        tbl_create.set_smallest(res.smallest);
                        tbl_create.set_biggest(res.biggest);
                        ingest_files.mut_table_creates().push(tbl_create);
                    }
                    tbl_cnt += 1;
                    let fs = self.fs.clone();
                    let atx = tx.clone();
                    self.fs.get_runtime().spawn(async move {
                        if let Err(err) = fs.create(id, buf.freeze(), opts).await {
                            atx.send(Err(err)).unwrap();
                        } else {
                            atx.send(Ok(())).unwrap();
                        }
                    });
                    break;
                }
            }
        }
        for _ in 0..tbl_cnt {
            rx.recv().unwrap()?
        }
        Ok(cs)
    }

    // get_all_shard_id_vers collects all the id and vers of the engine.
    // To prevent the shard change during the iteration, we iterate twice and make sure there is
    // no change during the iteration.
    // Use this method first, then get each shard by id to reduce lock contention.
    pub fn get_all_shard_id_vers(&self) -> Vec<IDVer> {
        loop {
            let id_vers = self.collect_shard_id_vers();
            let id_vers_set = HashSet::<_>::from_iter(id_vers.iter());
            let recheck = self.collect_shard_id_vers();
            if recheck.len() == id_vers_set.len()
                && recheck.iter().all(|id_ver| id_vers_set.contains(id_ver))
            {
                return id_vers;
            }
        }
    }

    fn collect_shard_id_vers(&self) -> Vec<IDVer> {
        self.shards
            .iter()
            .map(|x| IDVer::new(x.id, x.ver))
            .collect()
    }

    // meta_committed should be called when a change set is committed in the raft group.
    pub fn meta_committed(&self, cs: &kvenginepb::ChangeSet, rejected: bool) {
        if cs.has_flush() || cs.has_initial_flush() {
            let table_version = change_set_table_version(cs);
            let id_ver = IDVer::new(cs.shard_id, cs.shard_ver);
            self.flush_tx
                .send(FlushMsg::Committed((id_ver, table_version)))
                .unwrap();
        }
        if rejected && (cs.has_compaction() || cs.has_destroy_range()) {
            // Notify the compaction runner otherwise the shard can't be compacted any more.
            self.compact_tx
                .send(CompactMsg::Applied(IDVer::new(cs.shard_id, cs.shard_ver)))
                .unwrap();
        }
    }

    pub fn set_shard_active(&self, shard_id: u64, active: bool) {
        if let Some(shard) = self.get_shard(shard_id) {
            info!("shard {} set active {}", shard.tag(), active);
            shard.set_active(active);
            if active {
                self.refresh_shard_states(&shard);
                self.trigger_flush(&shard);
            } else {
                store_bool(&shard.compacting, false);
                self.flush_tx.send(FlushMsg::Clear(shard_id)).unwrap();
                self.compact_tx
                    .send(CompactMsg::Clear(IDVer::new(shard.id, shard.ver)))
                    .unwrap();
            }
        }
    }

    pub(crate) fn refresh_shard_states(&self, shard: &Shard) {
        shard.refresh_states();
        if shard.ready_to_compact() {
            self.compact_tx
                .send(CompactMsg::Compact(IDVer::new(shard.id, shard.ver)))
                .unwrap();
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct ShardTag {
    pub engine_id: u64,
    pub id_ver: IDVer,
}

impl ShardTag {
    pub fn new(engine_id: u64, id_ver: IDVer) -> Self {
        Self { engine_id, id_ver }
    }

    pub fn from_comp_req(req: &CompactionRequest) -> Self {
        Self {
            engine_id: req.engine_id,
            id_ver: IDVer::new(req.shard_id, req.shard_ver),
        }
    }
}

impl Display for ShardTag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.engine_id, self.id_ver.id, self.id_ver.ver
        )
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct IDVer {
    pub id: u64,
    pub ver: u64,
}

impl IDVer {
    pub fn new(id: u64, ver: u64) -> Self {
        Self { id, ver }
    }

    pub fn from_change_set(cs: &kvenginepb::ChangeSet) -> Self {
        Self::new(cs.shard_id, cs.shard_ver)
    }
}

pub fn new_filename(file_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.sst", file_id))
}

pub fn new_tmp_filename(file_id: u64, tmp_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.{}.tmp", file_id, tmp_id))
}

fn free_mem(free_rx: mpsc::Receiver<CFTable>) {
    loop {
        let mut tables = vec![];
        let tbl = free_rx.recv().unwrap();
        tables.push(tbl);
        let cnt = free_rx.len();
        for _ in 0..cnt {
            tables.push(free_rx.recv().unwrap());
        }
        thread::sleep(Duration::from_secs(5));
        drop(tables);
    }
}
