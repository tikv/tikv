// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use file_system::IORateLimiter;
use fslock;
use moka::sync::SegmentedCache;
use slog_global::info;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::iter::Iterator;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tikv_util::mpsc;

use crate::apply::ChangeSet;
use crate::meta::ShardMeta;
use crate::table::memtable::CFTable;
use crate::table::sstable::BlockCacheKey;
use crate::*;

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
        let (flush_tx, flush_rx) = mpsc::bounded(opts.num_mem_tables);
        let (free_tx, free_rx) = mpsc::unbounded();
        let core = EngineCore {
            shards: DashMap::new(),
            opts: opts.clone(),
            flush_tx,
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
                compact_en.run_compaction();
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
                    let parent_shard = self.load_shard(parent)?;
                    recoverer.recover(self, &parent_shard, parent)?;
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
}

pub struct EngineCore {
    pub(crate) shards: DashMap<u64, Arc<Shard>>,
    pub opts: Arc<Options>,
    pub(crate) flush_tx: mpsc::Sender<FlushTask>,
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
        meta_iter.iterate(|cs| {
            let meta = ShardMeta::new(&cs);
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
        info!("load shard {}:{}", meta.id, meta.ver);
        let change_set = self.prepare_change_set(meta.to_change_set(), false)?;
        self.ingest(change_set, false)?;
        let shard = self.get_shard(meta.id);
        Ok(shard.unwrap())
    }

    pub fn ingest(&self, cs: ChangeSet, active: bool) -> Result<()> {
        let shard = Shard::new_for_ingest(&cs, self.opts.clone());
        shard.set_active(active);
        let (l0s, scfs) = self.create_snapshot_tables(cs.get_snapshot(), &cs);
        let data = ShardData::new(
            shard.start.clone(),
            shard.end.clone(),
            vec![CFTable::new()],
            l0s,
            scfs,
        );
        shard.set_data(data);
        store_bool(&shard.initial_flushed, true);
        shard.refresh_states();
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

    pub fn trigger_flush(&self, shard: &Arc<Shard>) {
        if !shard.get_initial_flushed() {
            self.trigger_initial_flush(shard);
            return;
        }
        let data = shard.get_data();
        if let Some(mem_tbl) = data.get_last_read_only_mem_table() {
            info!(
                "shard {}:{} trigger flush mem-table ts {}, size {}",
                shard.id,
                shard.ver,
                mem_tbl.get_version(),
                mem_tbl.size(),
            );
            self.flush_tx
                .send(FlushTask {
                    shard_id: shard.id,
                    shard_ver: shard.ver,
                    start: shard.start.to_vec(),
                    end: shard.end.to_vec(),
                    normal: Some(mem_tbl),
                    initial: None,
                })
                .unwrap();
        }
    }

    fn trigger_initial_flush(&self, shard: &Arc<Shard>) {
        let guard = shard.parent_snap.read().unwrap();
        let parent_snap = guard.as_ref().unwrap().clone();
        let mut mem_tbls = vec![];
        let data = shard.get_data();
        for mem_tbl in &data.mem_tbls.as_slice()[1..] {
            debug!(
                "trigger initial flush check mem table version {}, size {}, parent base {} parent write {}",
                mem_tbl.get_version(),
                mem_tbl.size(),
                parent_snap.base_version,
                parent_snap.data_sequence,
            );
            if mem_tbl.get_version() > parent_snap.base_version + parent_snap.data_sequence
                && mem_tbl.has_data_in_range(&shard.start, &shard.end)
            {
                mem_tbls.push(mem_tbl.clone());
            }
        }
        self.flush_tx
            .send(FlushTask {
                shard_id: shard.id,
                shard_ver: shard.ver,
                start: shard.start.to_vec(),
                end: shard.end.to_vec(),
                normal: None,
                initial: Some(InitialFlush {
                    parent_snap,
                    mem_tbls,
                    base_version: shard.base_version,
                    // A newly split shard's meta_sequence is an in-mem state until initial flush.
                    data_sequence: shard.get_meta_sequence(),
                }),
            })
            .unwrap();
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
