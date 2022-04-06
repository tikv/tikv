// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use dashmap::DashMap;
use file_system::{IOOp, IORateLimiter, IOType};
use fslock;
use moka::sync::SegmentedCache;
use slog_global::info;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::iter::Iterator;
use std::ops::Deref;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tikv_util::mpsc;

use crate::meta::ShardMeta;
use crate::table::memtable::CFTable;
use crate::table::sstable::{BlockCacheKey, L0Table, SSTable};
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
                    recoverer.recover(&self, &parent_shard, parent)?;
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
    pub(crate) free_tx: mpsc::Sender<Arc<Vec<CFTable>>>,
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
        let shard = Shard::new_for_loading(meta, self.opts.clone());
        let dfs_opts = dfs::Options::new(shard.id, shard.ver);
        let mut l0_tbls = Vec::new();
        let mut scf_builders = Vec::new();
        for cf in 0..NUM_CFS {
            let scf_builder = ShardCFBuilder::new(cf);
            scf_builders.push(scf_builder);
        }
        let mut ids = meta.all_files();
        ids.retain(|id| {
            let local_path = self.local_file_path(*id);
            !local_path.exists()
        });
        self.pre_load_files_by_ids(meta.id, meta.ver, ids)?;
        for (fid, fm) in &meta.files {
            let file = self.fs.open(*fid, dfs_opts)?;
            if fm.cf == -1 {
                let l0_tbl = L0Table::new(file, Some(self.cache.clone()))?;
                l0_tbls.push(l0_tbl);
                continue;
            }
            let tbl = SSTable::new(file, Some(self.cache.clone()))?;
            scf_builders[fm.cf as usize].add_table(tbl, fm.level as usize);
        }
        l0_tbls.sort_by(|a, b| b.version().cmp(&a.version()));
        *shard.l0_tbls.write().unwrap() = L0Tables::new(l0_tbls);
        for cf in (0..NUM_CFS).rev() {
            shard.set_cf(cf, scf_builders[cf].build());
        }
        info!("load shard {}:{}", shard.id, shard.ver);
        self.shards.insert(shard.id, Arc::new(shard));
        let shard = self.get_shard(meta.id);
        Ok(shard.unwrap())
    }

    pub fn get_snap_access(&self, id: u64) -> Option<Arc<SnapAccess>> {
        if let Some(ptr) = self.shards.get(&id) {
            return Some(Arc::new(SnapAccess::new(&ptr)));
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
        if let Some(mem_tbl) = shard.get_last_read_only_mem_table() {
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
                    normal: Some(mem_tbl.clone()),
                    initial: None,
                })
                .unwrap();
        }
    }

    fn trigger_initial_flush(&self, shard: &Arc<Shard>) {
        let guard = shard.parent_snap.read().unwrap();
        let parent_snap = guard.as_ref().unwrap().clone();
        let mut mem_tbls = vec![];
        for mem_tbl in &shard.get_mem_tbls().tbls.as_slice()[1..] {
            debug!(
                "trigger initial flush check mem table version {}, size {}, parent base {} parent write {}",
                mem_tbl.get_version(),
                mem_tbl.size(),
                parent_snap.base_version,
                parent_snap.data_sequence,
            );
            if mem_tbl.get_version() > parent_snap.base_version + parent_snap.data_sequence {
                if mem_tbl.has_data_in_range(&shard.start, &shard.end) {
                    mem_tbls.push(mem_tbl.clone());
                }
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

    pub(crate) fn pre_load_files_by_ids(
        &self,
        shard_id: u64,
        shard_ver: u64,
        ids: Vec<u64>,
    ) -> Result<()> {
        let length = ids.len();
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(length);
        let runtime = self.fs.get_runtime();
        for id in &ids {
            let fs = self.fs.clone();
            let opts = dfs::Options::new(shard_id, shard_ver);
            let tx = result_tx.clone();
            let id = *id;
            runtime.spawn(async move {
                let res = fs.read_file(id, opts).await;
                tx.send(res.map(|data| (id, data))).unwrap();
            });
        }
        let mut errors = vec![];
        for _ in 0..length {
            match result_rx.recv().unwrap() {
                Ok((id, data)) => {
                    if let Err(err) = self.write_local_file(id, data) {
                        error!("write local file failed {:?}", &err);
                        errors.push(err.into());
                    }
                }
                Err(err) => {
                    error!("prefetch failed {:?}", &err);
                    errors.push(err);
                }
            }
        }
        if errors.len() > 0 {
            return Err(errors.pop().unwrap().into());
        }
        Ok(())
    }

    fn write_local_file(&self, id: u64, data: Bytes) -> std::io::Result<()> {
        let local_file_name = self.local_file_path(id);
        let tmp_file_name = self.tmp_file_path(id);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DSYNC)
            .open(&tmp_file_name)?;
        let mut start_off = 0;
        let write_batch_size = 256 * 1024;
        while start_off < data.len() {
            self.rate_limiter
                .request(IOType::Compaction, IOOp::Write, write_batch_size);
            let end_off = std::cmp::min(start_off + write_batch_size, data.len());
            file.write(&data[start_off..end_off])?;
            start_off = end_off;
        }
        std::fs::rename(&tmp_file_name, &local_file_name)
    }

    pub(crate) fn local_file_path(&self, file_id: u64) -> PathBuf {
        self.opts.local_dir.join(new_filename(file_id))
    }

    fn tmp_file_path(&self, file_id: u64) -> PathBuf {
        let tmp_id = self
            .tmp_file_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.opts.local_dir.join(new_tmp_filename(file_id, tmp_id))
    }
}

pub fn new_filename(file_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.sst", file_id))
}

pub fn new_tmp_filename(file_id: u64, tmp_id: u64) -> PathBuf {
    PathBuf::from(format!("{:016x}.{}.tmp", file_id, tmp_id))
}

fn free_mem(free_rx: mpsc::Receiver<Arc<Vec<CFTable>>>) {
    loop {
        let mut tables = vec![];
        let x = free_rx.recv().unwrap();
        tables.push(x);
        let cnt = free_rx.len();
        for _ in 0..cnt {
            tables.push(free_rx.recv().unwrap());
        }
        thread::sleep(Duration::from_secs(5));
        drop(tables);
    }
}
