// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::Bytes;
use crossbeam_epoch as epoch;
use dashmap::DashMap;
use fslock;
use moka::sync::SegmentedCache;
use slog_global::info;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::iter::Iterator;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use tikv_util::mpsc;

use crate::meta::ShardMeta;
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
        recoverer: impl RecoverHandler,
        id_allocator: Arc<dyn IDAllocator>,
        meta_change_listener: Box<dyn MetaChangeListener>,
    ) -> Result<Engine> {
        info!("Open Engine");
        check_options(&opts)?;
        // TODO: add diretory lock to avoid multiple instance open the same dir.
        let lock_path = opts.dir.join("LOCK");
        let mut x = fslock::LockFile::open(&lock_path)?;
        x.lock()?;
        let mut max_capacity =
            opts.max_block_cache_size as usize / opts.table_builder_options.block_size;
        if max_capacity < 512 {
            max_capacity = 512
        }
        let cache: SegmentedCache<BlockCacheKey, Bytes> = SegmentedCache::new(max_capacity, 64);
        let (flush_tx, flush_rx) = mpsc::bounded(opts.num_mem_tables);
        let (flush_result_tx, flush_result_rx) = mpsc::bounded(opts.num_mem_tables);
        let core = EngineCore {
            shards: DashMap::new(),
            opts: opts.clone(),
            flush_tx,
            fs: fs.clone(),
            cache,
            comp_client: CompactionClient::new(fs.clone(), opts.remote_compaction_addr.clone()),
            id_allocator,
            managed_safe_ts: AtomicU64::new(0),
        };
        let en = Engine {
            core: Arc::new(core),
            meta_change_listener,
        };
        let metas = en.read_meta(meta_iter)?;
        en.load_shards(metas, recoverer)?;
        let flush_en = en.clone();
        thread::spawn(move || {
            flush_en.run_flush_worker(flush_rx, flush_result_tx);
        });
        let flush_result_en = en.clone();
        thread::spawn(move || {
            flush_result_en.run_flush_result(flush_result_rx);
        });
        let compact_en = en.clone();
        thread::spawn(move || {
            compact_en.run_compaction();
        });
        Ok(en)
    }

    fn load_shards(
        &self,
        metas: HashMap<u64, ShardMeta>,
        recoverer: impl RecoverHandler,
    ) -> Result<()> {
        let mut parents = HashSet::new();
        for (id, meta) in &metas {
            if let Some(parent) = &meta.parent {
                if !parents.contains(&parent.id) {
                    parents.insert(parent.id);
                    let parent_shard = self.load_shard(parent)?;
                    recoverer.recover(&self, &parent_shard, parent)?;
                }
            }
            let shard = self.load_shard(meta)?;
            recoverer.recover(&self, &shard, meta)?;
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
}

impl EngineCore {
    fn read_meta(&self, meta_iter: impl MetaIterator) -> Result<HashMap<u64, ShardMeta>> {
        let mut metas = HashMap::new();
        meta_iter.iterate(|cs| {
            let meta = ShardMeta::new(cs);
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
            let mut scf_builder = ShardCFBuilder::new(self.opts.cfs[cf].max_levels);
            scf_builders.push(scf_builder);
        }
        for (fid, fm) in &meta.files {
            let file = self.fs.open(*fid, dfs_opts)?;
            if fm.cf == -1 {
                let l0_tbl = L0Table::new(file, self.cache.clone())?;
                l0_tbls.push(l0_tbl);
                continue;
            }
            let tbl = SSTable::new(file, self.cache.clone())?;
            scf_builders[fm.cf as usize].add_table(tbl, fm.level as usize);
        }
        l0_tbls.sort_by(|a, b| b.commit_ts().cmp(&a.commit_ts()));
        shard.l0_tbls.store(
            epoch::Owned::new(Arc::new(L0Tables::new(l0_tbls))),
            Ordering::Release,
        );
        for cf in (0..NUM_CFS).rev() {
            shard.cfs[cf].store(
                epoch::Owned::new(Arc::new(scf_builders[cf].build())),
                Ordering::Release,
            );
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

    pub fn remove_shard(&self, shard_id: u64, remove_file: bool) -> bool {
        let g = epoch::pin();
        let x = self.shards.remove(&shard_id);
        if let Some((_, ptr)) = x {
            if remove_file {
                ptr.remove_file.store(true, Ordering::Relaxed);
            }
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
        let g = &epoch::pin();
        let mem_tbls = shard.get_mem_tbls(g);
        for i in (0..mem_tbls.tbls.len()).rev() {
            let mem_tbl = &mem_tbls.tbls[i];
            if mem_tbl.is_applying() {
                continue;
            }
            if i == 1 && shard.get_split_stage() == kvenginepb::SplitStage::PreSplit {
                mem_tbl.set_split_stage(kvenginepb::SplitStage::PreSplitFlushDone);
            }
            info!(
                "shard {}:{} trigger flush mem-table ts {}, size {}",
                shard.id,
                shard.ver,
                mem_tbl.get_version(),
                mem_tbl.size()
            );
            self.flush_tx
                .send(FlushTask {
                    shard_id: shard.id,
                    shard_ver: shard.ver,
                    split_stage: shard.get_split_stage(),
                    mem_tbl: mem_tbl.clone(),
                    next_mem_tbl_size: 0,
                })
                .unwrap();
        }
    }
}

fn check_options(opts: &Options) -> Result<()> {
    if !opts.dir.exists() {
        std::fs::create_dir_all(&opts.dir)?;
        return Ok(());
    }
    if !opts.dir.is_dir() {
        return Err(Error::ErrOpen("path is not dir".to_string()));
    }
    Ok(())
}
