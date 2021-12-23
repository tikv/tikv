// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes};
use dashmap::DashMap;
use protobuf::ProtobufEnum;
use std::iter::Iterator;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering::*, *},
        Arc,
    },
    time::Instant,
};

use crate::*;
use crate::{
    meta::ShardMeta,
    table::{
        self,
        memtable::{self, CFTable},
        search,
        sstable::L0Table,
        sstable::SSTable,
    },
};
use crossbeam_epoch as epoch;
use epoch::{Atomic, Guard, Owned, Shared};
use kvenginepb as pb;
use slog_global::*;

pub(crate) struct MemTables {
    pub tbls: Vec<memtable::CFTable>,
}

impl MemTables {
    pub(crate) fn new(tbls: Vec<memtable::CFTable>) -> Self {
        MemTables { tbls }
    }
}

pub(crate) struct SplitContext {
    pub(crate) split_keys: Vec<Bytes>,
    pub(crate) mem_tbls: Vec<memtable::CFTable>,
}

impl SplitContext {
    pub(crate) fn new(keys: &[Vec<u8>]) -> Self {
        let mut split_keys = Vec::with_capacity(keys.len());
        for key in keys {
            split_keys.push(Bytes::copy_from_slice(key.as_slice()));
        }
        let mut mem_tbls = Vec::with_capacity(keys.len() + 1);
        if keys.len() > 0 {
            for _ in 0..=keys.len() {
                mem_tbls.push(memtable::CFTable::new());
            }
        }
        Self {
            split_keys,
            mem_tbls,
        }
    }

    pub(crate) fn get_spliting_index(&self, key: &[u8]) -> usize {
        let mut i = 0;
        while i < self.split_keys.len() {
            if key < self.split_keys[i].chunk() {
                break;
            }
            i += 1;
        }
        i
    }

    pub(crate) fn get_spliting_table(&self, key: &[u8]) -> &memtable::CFTable {
        let idx = self.get_spliting_index(key);
        &self.mem_tbls[idx]
    }

    pub(crate) fn write(&self, cf: usize, entry: &memtable::WriteBatchEntry, buf: &[u8]) {
        let key = entry.key(buf);
        let idx = self.get_spliting_index(key);
        let mem_tbl = self.mem_tbls[idx].get_cf(cf);
        mem_tbl.put(buf, entry);
    }
}

pub(crate) struct L0Tables {
    pub tbls: Vec<L0Table>,
}

impl L0Tables {
    pub(crate) fn new(tbls: Vec<L0Table>) -> Self {
        Self { tbls }
    }

    pub(crate) fn total_size(&self) -> u64 {
        let mut size = 0;
        for tbl in &self.tbls {
            size += tbl.size()
        }
        size
    }
}

pub struct Shard {
    pub id: u64,
    pub ver: u64,
    pub start: Bytes,
    pub end: Bytes,
    pub(crate) cfs: [Atomic<Arc<ShardCF>>; NUM_CFS],
    pub(crate) opt: Arc<Options>,

    mem_tbls: Atomic<Arc<MemTables>>,
    pub(crate) l0_tbls: Atomic<Arc<L0Tables>>,

    split_stage: AtomicI32,
    pub(crate) split_ctx: Atomic<Arc<SplitContext>>,

    // If the shard is not active, flush mem table and do compaction will ignore this shard.
    pub(crate) active: AtomicBool,

    pub(crate) properties: Properties,
    pub(crate) compacting: AtomicBool,
    pub(crate) initial_flushed: AtomicBool,
    pub(crate) last_switch_time: AtomicU64,
    pub(crate) max_mem_table_size: AtomicU64,

    pub(crate) base_ts: u64,

    pub(crate) estimated_size: AtomicU64,
    pub(crate) meta_seq: AtomicU64,
    pub(crate) write_sequence: AtomicU64,

    pub(crate) ingest_pre_split_seq: u64,

    size_stats: Atomic<ShardSizeStats>,

    pub(crate) compact_lock: tokio::sync::Mutex<()>,
}

pub const MEM_TABLE_SIZE_KEY: &str = "_mem_table_size";

impl Shard {
    pub fn new(
        props: &pb::Properties,
        ver: u64,
        start: &[u8],
        end: &[u8],
        opt: Arc<Options>,
    ) -> Self {
        let base_size = opt.base_size;
        let shard = Self {
            id: props.shard_id,
            ver,
            start: Bytes::copy_from_slice(start),
            end: Bytes::copy_from_slice(end),
            cfs: Default::default(),
            opt: opt.clone(),
            mem_tbls: Atomic::new(Arc::new(MemTables::new(vec![CFTable::new()]))),
            l0_tbls: Atomic::new(Arc::new(L0Tables::new(Vec::new()))),
            split_stage: AtomicI32::new(kvenginepb::SplitStage::Initial.value()),
            split_ctx: Atomic::new(Arc::new(SplitContext::new(&[]))),
            active: Default::default(),
            properties: Properties::new().apply_pb(props),
            compacting: Default::default(),
            initial_flushed: Default::default(),
            last_switch_time: Default::default(),
            max_mem_table_size: AtomicU64::new(base_size / 4),
            base_ts: 1,
            estimated_size: Default::default(),
            meta_seq: Default::default(),
            write_sequence: Default::default(),
            ingest_pre_split_seq: Default::default(),
            size_stats: Default::default(),
            compact_lock: tokio::sync::Mutex::new(()),
        };
        if let Some(val) = get_shard_property(MEM_TABLE_SIZE_KEY, props) {
            shard.set_max_mem_table_size(LittleEndian::read_u64(val.as_slice()))
        }
        for cf in 0..NUM_CFS {
            let scf = Arc::new(ShardCF::new(opt.cfs[cf].max_levels));
            shard.cfs[cf].store(Owned::new(scf), Relaxed);
        }
        shard
    }

    pub fn new_for_loading(meta: &ShardMeta, opt: Arc<Options>) -> Self {
        let mut shard = Self::new(
            &meta.properties.to_pb(meta.id),
            meta.ver,
            meta.start.as_slice(),
            meta.end.as_slice(),
            opt,
        );
        if meta.pre_split.is_some()
            && meta.split_stage.value() >= pb::SplitStage::PreSplitFlushDone.value()
        {
            // We don't set split keys if the split stage has not reached enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE
            // because some the recover data should be executed before split key is set.
            shard.set_split_keys(meta.pre_split.as_ref().unwrap().get_keys());
        }
        shard.set_split_stage(meta.split_stage);
        store_bool(&shard.initial_flushed, true);
        shard.base_ts = meta.base_ts;
        shard.meta_seq.store(meta.seq, Release);
        shard.write_sequence.store(meta.seq, Release);
        shard
    }

    pub fn new_for_ingest(cs: pb::ChangeSet, opt: Arc<Options>) -> Self {
        let snap = cs.get_snapshot();
        let mut shard = Self::new(
            snap.get_properties(),
            cs.shard_ver,
            snap.start.as_slice(),
            snap.end.as_slice(),
            opt,
        );
        if snap.get_split_keys().len() > 0 {
            info!(
                "shard {}:{} set pre-split keys by ingest",
                cs.get_shard_id(),
                cs.get_shard_ver()
            );
            shard.set_split_keys(snap.get_split_keys());
            if cs.stage == pb::SplitStage::PreSplit {
                shard.ingest_pre_split_seq = cs.sequence;
                info!(
                    "shard {}:{} set pre-split seuqence {} by ingest",
                    shard.id, shard.ver, shard.ingest_pre_split_seq
                );
            }
        }
        shard.set_split_stage(cs.get_stage());
        store_bool(&shard.initial_flushed, true);
        shard.base_ts = snap.base_ts;
        shard.meta_seq.store(cs.sequence, Release);
        shard.write_sequence.store(cs.sequence, Release);
        info!(
            "ingest shard {}:{} max_table_size {}, mem_table_ts {}",
            cs.shard_id,
            cs.shard_ver,
            shard.get_max_mem_table_size(),
            shard.load_mem_table_ts()
        );
        shard
    }

    fn table_ids(&self) -> Vec<u64> {
        let g = &epoch::pin();
        let mut ids = Vec::new();
        let shared = self.l0_tbls.load(Relaxed, &g);
        let l0s = unsafe { shared.as_ref().unwrap() };
        for tbl in &l0s.tbls {
            ids.push(tbl.id());
        }
        let ids_ref = &mut ids;
        self.for_each_level(|_cf, l| {
            for tbl in l.tables.iter() {
                ids_ref.push(tbl.id());
            }
            false
        });
        ids
    }

    pub fn set_active(&self, active: bool) {
        self.active.store(active, Release);
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Acquire)
    }

    pub fn is_splitting(&self) -> bool {
        self.split_stage.load(Acquire) >= pb::SplitStage::PreSplit.value()
    }

    pub(crate) fn refresh_estimated_size(&self) {
        let mut stats = ShardSizeStats::default();
        let g = &epoch::pin();
        let l0s = self.get_l0_tbls(g);
        for l0 in &l0s.tbls {
            stats.l0 += l0.size();
        }
        self.for_each_level(|cf, l| {
            stats.cfs[cf] += l.total_size;
            false
        });
        store_u64(&self.estimated_size, stats.cf_size() + stats.l0);
        let owned = Owned::new(stats);
        self.size_stats.store(owned, Relaxed);
    }

    pub(crate) fn set_max_mem_table_size(&self, size: u64) {
        self.max_mem_table_size.store(size, Release);
    }

    pub(crate) fn get_max_mem_table_size(&self) -> u64 {
        self.max_mem_table_size.load(Acquire)
    }

    pub(crate) fn set_split_keys(&self, keys: &[Vec<u8>]) -> bool {
        if self.get_split_stage() == pb::SplitStage::Initial {
            self.split_ctx
                .store(Owned::new(Arc::new(SplitContext::new(keys))), Release);
            self.set_split_stage(pb::SplitStage::PreSplit);
            return true;
        }
        warn!(
            "shard {}:{} failed to set split key got stage {}",
            self.id,
            self.ver,
            self.get_split_stage().value()
        );
        false
    }

    pub(crate) fn for_each_level<F>(&self, mut f: F)
    where
        F: FnMut(usize /*cf*/, &LevelHandler) -> bool, /*stopped*/
    {
        let g = &epoch::pin();
        for cf in 0..NUM_CFS {
            let scf = self.get_cf(cf, g);
            for lh in &scf.levels {
                if f(cf, lh) {
                    return;
                }
            }
        }
    }

    pub fn get_suggest_split_keys(&self, target_size: u64) -> Vec<Bytes> {
        let mut keys = Vec::new();
        let estimated_size = load_u64(&self.estimated_size);
        if estimated_size < target_size {
            return keys;
        }
        let g = &epoch::pin();
        let l0s = self.get_l0_tbls(g);
        if l0s.tbls.len() > 0 && l0s.total_size() > (estimated_size * 3 / 10) {
            if let Some(tbl) = l0s.tbls[0].get_cf(0) {
                if let Some(split_key) = tbl.get_suggest_split_key() {
                    info!(
                        "shard {}:{} get table suggest split key {:x}, start {:x}, end {:x}",
                        self.id, self.ver, split_key, self.start, self.end
                    );
                    keys.push(split_key);
                    return keys;
                }
            }
        }
        let max_cf = self.get_cf(0, g);
        let mut max_level = &max_cf.levels[0];
        for i in 1..max_cf.levels.len() {
            let level = &max_cf.levels[i];
            if level.total_size > max_level.total_size {
                max_level = level;
            }
        }
        let level_target_size =
            ((target_size as f64) * (max_level.total_size as f64) / (estimated_size as f64)) as u64;
        let mut current_size = 0;
        for i in 0..max_level.tables.len() {
            let tbl = &max_level.tables[i];
            current_size += tbl.size();
            if i != 0 && current_size > level_target_size {
                keys.push(Bytes::copy_from_slice(tbl.smallest()));
                current_size = 0
            }
        }
        keys
    }

    pub fn overlap_range(&self, start_key: &[u8], end_key: &[u8]) -> bool {
        self.start < end_key && start_key < self.end
    }

    pub fn overlap_key(&self, key: &[u8]) -> bool {
        self.start <= key && key < self.end
    }

    pub fn get_split_stage(&self) -> pb::SplitStage {
        pb::SplitStage::from_i32(self.split_stage.load(Acquire)).unwrap()
    }

    pub(crate) fn set_split_stage(&self, stage: pb::SplitStage) {
        self.split_stage.store(stage.value(), Release);
    }

    pub(crate) fn get_split_ctx<'a>(&self, g: &'a epoch::Guard) -> &'a Arc<SplitContext> {
        load_resource(&self.split_ctx, g)
    }

    pub(crate) fn get_ref_split_keys<'a>(&self, g: &'a epoch::Guard) -> &'a Vec<Bytes> {
        let ctx = self.get_split_ctx(g);
        &ctx.split_keys
    }

    pub fn get_split_keys(&self) -> Vec<Bytes> {
        let g = epoch::pin();
        self.get_ref_split_keys(&g).clone()
    }

    pub(crate) fn get_mem_tbls<'a>(&self, g: &'a epoch::Guard) -> &'a Arc<MemTables> {
        load_resource(&self.mem_tbls, g)
    }

    pub(crate) fn get_l0_tbls<'a>(&self, g: &'a epoch::Guard) -> &'a Arc<L0Tables> {
        load_resource(&self.l0_tbls, g)
    }

    pub(crate) fn get_cf<'a>(&self, cf: usize, g: &'a epoch::Guard) -> &'a Arc<ShardCF> {
        load_resource(&self.cfs[cf], g)
    }

    pub(crate) fn set_cf(&self, cf: usize, scf: ShardCF) {
        self.cfs[cf].store(epoch::Owned::new(Arc::new(scf)), Release);
    }

    pub fn get_property(&self, key: &str) -> Option<Bytes> {
        self.properties.get(key)
    }

    pub fn set_property(&self, key: &str, val: &[u8]) {
        self.properties.set(key, val);
    }

    pub(crate) fn next_mem_table_size(&self, current_size: u64, last_switch_time: Instant) -> u64 {
        let dur = last_switch_time.elapsed();
        let time_in_ms = dur.as_millis() as u64 + 1;
        let bytes_per_sec = current_size * 1000 / time_in_ms;
        let next_mem_size = bytes_per_sec * self.opt.max_mem_table_size_factor as u64;
        Self::bounded_mem_size(next_mem_size)
    }

    pub(crate) fn bounded_mem_size(size: u64) -> u64 {
        const MAX_MEM_SIZE_UPPER_LIMIT: u64 = 128 * 1024 * 1024;
        const MAX_MEM_SIZE_LOWER_LIMIT: u64 = 4 * 1024 * 1024;
        let mut bounded = size;
        if bounded > MAX_MEM_SIZE_UPPER_LIMIT {
            bounded = MAX_MEM_SIZE_UPPER_LIMIT;
        }
        if bounded < MAX_MEM_SIZE_LOWER_LIMIT {
            bounded = MAX_MEM_SIZE_LOWER_LIMIT;
        }
        bounded
    }

    pub(crate) fn load_mem_table_ts(&self) -> u64 {
        self.base_ts + self.meta_seq.load(Acquire)
    }

    pub fn get_all_files(&self) -> Vec<u64> {
        let mut files = Vec::new();
        let g = &epoch::pin();
        let l0s = self.get_l0_tbls(g);
        for l0 in &l0s.tbls {
            files.push(l0.id());
        }
        self.for_each_level(|_cf, lh| {
            for tbl in lh.tables.iter() {
                files.push(tbl.id())
            }
            false
        });
        files.sort();
        files
    }

    pub fn get_writable_mem_table<'a>(&self, g: &'a Guard) -> &'a memtable::CFTable {
        let mem_tbls = self.get_mem_tbls(g);
        &mem_tbls.tbls[0]
    }

    pub fn atomic_add_mem_table<'a>(&self, g: &'a Guard, mem_tbl: memtable::CFTable) {
        info!("shard {}:{} atomic add new mem table", self.id, self.ver);
        loop {
            let mem_tbl = mem_tbl.clone();
            let shared = self.mem_tbls.load(Acquire, g);
            let old_mem_tbls = unsafe { &shared.deref().tbls };
            let mut tbl_vec = Vec::with_capacity(old_mem_tbls.len() + 1);
            tbl_vec.push(mem_tbl);
            for tbl in old_mem_tbls {
                tbl_vec.push(tbl.clone());
            }
            let new_mem_tbls = MemTables { tbls: tbl_vec };
            if cas_resource(&self.mem_tbls, g, shared, Arc::new(new_mem_tbls)) {
                break;
            }
        }
    }

    pub fn atomic_remove_mem_table<'a>(&self, g: &'a Guard) {
        info!("shard {}:{} atomic remove mem table", self.id, self.ver);
        loop {
            let shared = self.mem_tbls.load(Acquire, g);
            let old_mem_tbls = unsafe { &shared.deref().tbls };
            let old_len = old_mem_tbls.len();
            if old_len <= 1 {
                warn!("atomic remove mem table with old table len {}", old_len);
                return;
            }
            let new_len = old_len - 1;
            let mut tbl_vec = Vec::with_capacity(new_len);
            for i in 0..new_len {
                tbl_vec.push(old_mem_tbls[i].clone());
            }
            let new_mem_tbls = MemTables { tbls: tbl_vec };
            if cas_resource(&self.mem_tbls, g, shared, Arc::new(new_mem_tbls)) {
                break;
            }
        }
    }

    pub fn atomic_add_l0_table<'a>(&self, g: &'a Guard, l0_tbl: L0Table) {
        info!(
            "shard {}:{} atomic add l0 table, commit_ts {}",
            self.id,
            self.ver,
            l0_tbl.commit_ts()
        );
        loop {
            let l0_tbl = l0_tbl.clone();
            let shared = self.l0_tbls.load(Acquire, g);
            let old_l0_tbls = unsafe { &shared.deref().tbls };
            let mut tbl_vec = Vec::with_capacity(old_l0_tbls.len());
            tbl_vec.push(l0_tbl);
            for tbl in old_l0_tbls {
                tbl_vec.push(tbl.clone());
            }
            let new_l0_tabls = L0Tables { tbls: tbl_vec };
            if cas_resource(&self.l0_tbls, g, shared, Arc::new(new_l0_tabls)) {
                break;
            }
        }
    }

    pub fn atomic_remove_l0_tables<'a>(&self, g: &'a Guard, n: usize) {
        info!(
            "shard {}:{} atomic remove {} l0 tables",
            self.id, self.ver, n,
        );
        loop {
            let shared = self.l0_tbls.load(Acquire, g);
            let old_l0_tbls = unsafe { &shared.deref().tbls };
            let new_len = old_l0_tbls.len() - n;
            let new_l0_tbls = L0Tables {
                tbls: old_l0_tbls[..new_len].to_vec(),
            };
            if cas_resource(&self.l0_tbls, g, shared, Arc::new(new_l0_tbls)) {
                break;
            }
        }
    }

    pub fn get_write_sequence(&self) -> u64 {
        self.write_sequence.load(Ordering::Acquire)
    }

    pub fn get_meta_sequence(&self) -> u64 {
        self.meta_seq.load(Ordering::Acquire)
    }

    pub fn mark_mem_table_applying_flush(&self, commit_ts: u64) {
        let g = &epoch::pin();
        let shared = self.mem_tbls.load(Acquire, g);
        let mems = unsafe { &shared.deref().tbls };
        for mem in mems.iter().rev() {
            let mem_version = mem.get_version();
            if mem_version > commit_ts {
                return;
            }
            if mem_version == commit_ts {
                mem.set_applying();
                break;
            }
        }
    }

    pub fn get_estimated_size(&self) -> u64 {
        self.estimated_size.load(Ordering::Relaxed)
    }

    pub fn get_initial_flushed(&self) -> bool {
        self.initial_flushed.load(Ordering::Acquire)
    }
}

pub fn load_resource_with_shared<'a, T>(ptr: &Atomic<T>, g: &'a Guard) -> (Shared<'a, T>, &'a T) {
    let shared = ptr.load(Acquire, g);
    (shared, unsafe { shared.deref() })
}

pub fn load_resource<'a, T>(ptr: &Atomic<T>, g: &'a Guard) -> &'a T {
    let shared = ptr.load(Acquire, g);
    unsafe { shared.deref() }
}

pub fn cas_resource<'a, T>(ptr: &Atomic<T>, g: &'a Guard, old: epoch::Shared<T>, new: T) -> bool {
    ptr.compare_exchange(old, Owned::new(new), Release, Relaxed, g)
        .is_ok()
}

pub fn store_resource<'a, T>(ptr: &Atomic<T>, new: T) {
    ptr.store(Owned::new(new), Release);
}

pub fn store_u64(ptr: &AtomicU64, val: u64) {
    ptr.store(val, Release);
}

pub fn load_u64(ptr: &AtomicU64) -> u64 {
    ptr.load(Acquire)
}

pub fn store_bool(ptr: &AtomicBool, val: bool) {
    ptr.store(val, Release)
}

pub fn load_bool(ptr: &AtomicBool) -> bool {
    ptr.load(Acquire)
}

#[derive(Default)]
struct ShardSizeStats {
    pub l0: u64,
    pub cfs: [u64; NUM_CFS],
}

impl ShardSizeStats {
    pub fn cf_size(&self) -> u64 {
        let mut cf_total = 0;
        for cf in &self.cfs {
            cf_total += *cf;
        }
        cf_total
    }
}

pub(crate) struct ShardCFBuilder {
    levels: Vec<LevelHandlerBuilder>,
}

impl ShardCFBuilder {
    pub(crate) fn new(max_level: usize) -> Self {
        Self {
            levels: vec![LevelHandlerBuilder::new(); max_level],
        }
    }

    pub(crate) fn build(&mut self) -> ShardCF {
        let mut levels = Vec::with_capacity(self.levels.len());
        for i in 0..self.levels.len() {
            levels.push(self.levels[i].build(i + 1))
        }
        ShardCF { levels }
    }

    pub(crate) fn add_table(&mut self, tbl: SSTable, level: usize) {
        self.levels[level - 1].add_table(tbl)
    }
}

#[derive(Clone)]
struct LevelHandlerBuilder {
    tables: Option<Vec<SSTable>>,
}

impl LevelHandlerBuilder {
    fn new() -> Self {
        Self {
            tables: Some(vec![]),
        }
    }

    fn build(&mut self, level: usize) -> LevelHandler {
        let mut tables = self.tables.take().unwrap();
        tables.sort_by(|a, b| a.smallest().cmp(b.smallest()));
        let mut total_size = 0;
        for tbl in tables.iter() {
            total_size += tbl.size()
        }
        LevelHandler {
            tables,
            level,
            total_size,
        }
    }

    fn add_table(&mut self, tbl: SSTable) {
        if self.tables.is_none() {
            self.tables = Some(vec![])
        }
        self.tables.as_mut().unwrap().push(tbl)
    }
}

#[derive(Default, Clone)]
pub(crate) struct ShardCF {
    pub(crate) levels: Vec<LevelHandler>,
}

impl ShardCF {
    pub(crate) fn new(levels: usize) -> Self {
        let mut scf = Self::default();
        for j in 1..=levels {
            scf.levels.push(LevelHandler::new(j));
        }
        scf
    }

    pub(crate) fn set_has_overlapping(&self, cd: &mut CompactDef) {
        if cd.move_down() {
            return;
        }
        let kr = get_key_range(&cd.top);
        for lvl_idx in (cd.level + 1)..self.levels.len() {
            let lh = &self.levels[lvl_idx];
            let (left, right) = lh.overlapping_tables(&kr);
            if left < right {
                cd.has_overlap = true;
                return;
            }
        }
    }
}

#[derive(Default, Clone)]
pub struct LevelHandler {
    pub(crate) tables: Vec<SSTable>,
    pub(crate) level: usize,
    pub(crate) total_size: u64,
}

impl LevelHandler {
    pub fn new(level: usize) -> Self {
        Self {
            tables: Vec::new(),
            level,
            total_size: 0,
        }
    }

    fn overlapping_tables(&self, key_range: &KeyRange) -> (usize, usize) {
        get_tables_in_range(
            &self.tables,
            key_range.left.chunk(),
            key_range.right.chunk(),
        )
    }

    pub fn get(&self, key: &[u8], version: u64, key_hash: u64) -> table::Value {
        self.get_in_table(key, version, key_hash, self.get_table(key))
    }

    fn get_in_table(
        &self,
        key: &[u8],
        version: u64,
        key_hash: u64,
        tbl: Option<&SSTable>,
    ) -> table::Value {
        if tbl.is_none() {
            return table::Value::new();
        }
        tbl.unwrap().get(key, version, key_hash)
    }

    fn get_table(&self, key: &[u8]) -> Option<&SSTable> {
        let idx = search(self.tables.len(), |i| self.tables[i].biggest() >= key);
        if idx >= self.tables.len() {
            return None;
        }
        return Some(&self.tables[idx]);
    }
}

#[derive(Default, Clone)]
pub struct Properties {
    m: DashMap<String, Bytes>,
}

impl Properties {
    pub fn new() -> Self {
        Self {
            m: dashmap::DashMap::new(),
        }
    }

    pub fn set(&self, key: &str, val: &[u8]) {
        self.m.insert(key.to_string(), Bytes::copy_from_slice(val));
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let bin = self.m.get(key)?;
        Some(bin.value().clone())
    }

    pub fn to_pb(&self, shard_id: u64) -> kvenginepb::Properties {
        let mut props = kvenginepb::Properties::new();
        props.shard_id = shard_id;
        self.m.iter().for_each(|r| {
            props.keys.push(r.key().clone());
            props.values.push(r.value().to_vec());
        });
        props
    }

    pub fn apply_pb(self, props: &kvenginepb::Properties) -> Self {
        let keys = props.get_keys();
        let vals = props.get_values();
        for i in 0..keys.len() {
            let key = &keys[i];
            let val = &vals[i];
            self.set(key, val.as_slice());
        }
        self
    }
}

pub fn get_shard_property(key: &str, props: &kvenginepb::Properties) -> Option<Vec<u8>> {
    let keys = props.get_keys();
    for i in 0..keys.len() {
        if key == keys[i] {
            return Some(props.get_values()[i].clone());
        }
    }
    None
}

pub fn get_splitting_start_end<'a: 'b, 'b>(
    start: &'a [u8],
    end: &'a [u8],
    split_keys: &'b [Vec<u8>],
    i: usize,
) -> (&'b [u8], &'b [u8]) {
    let start_key: &'b [u8];
    let end_key: &'b [u8];
    if i != 0 {
        start_key = split_keys[i - 1].as_slice();
    } else {
        start_key = start as &'b [u8];
    }
    if i == split_keys.len() {
        end_key = end;
    } else {
        end_key = split_keys[i].as_slice();
    }
    (start_key, end_key)
}
