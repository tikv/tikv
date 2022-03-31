// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes};
use dashmap::DashMap;
use std::iter::Iterator;
use std::sync::RwLock;
use std::{
    mem,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering::*, *},
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
use kvenginepb as pb;
use slog_global::*;
use tikv_util::time::InstantExt;

#[derive(Clone)]
pub(crate) struct MemTables {
    pub tbls: Arc<Vec<memtable::CFTable>>,
}

impl MemTables {
    pub(crate) fn new(tbls: Vec<memtable::CFTable>) -> Self {
        MemTables {
            tbls: Arc::new(tbls),
        }
    }
}

#[derive(Clone)]
pub(crate) struct L0Tables {
    pub tbls: Arc<Vec<L0Table>>,
}

impl L0Tables {
    pub(crate) fn new(tbls: Vec<L0Table>) -> Self {
        Self {
            tbls: Arc::new(tbls),
        }
    }

    pub(crate) fn total_size(&self) -> u64 {
        let mut size = 0;
        for tbl in self.tbls.as_ref() {
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
    pub parent_id: u64,
    pub(crate) cfs: [RwLock<ShardCF>; NUM_CFS],
    pub(crate) opt: Arc<Options>,

    mem_tbls: RwLock<MemTables>,
    pub(crate) l0_tbls: RwLock<L0Tables>,

    // If the shard is not active, flush mem table and do compaction will ignore this shard.
    pub(crate) active: AtomicBool,

    pub(crate) properties: Properties,
    pub(crate) compacting: AtomicBool,
    pub(crate) initial_flushed: AtomicBool,
    pub(crate) last_switch_time: RwLock<Instant>,
    pub(crate) max_mem_table_size: AtomicU64,

    pub(crate) base_version: u64,

    pub(crate) estimated_size: AtomicU64,

    // meta_seq is the raft log index of the applied change set.
    // Because change set are applied in the worker thread, the value is usually smaller
    // than write_sequence.
    pub(crate) meta_seq: AtomicU64,

    // write_sequence is the raft log index of the applied write batch.
    pub(crate) write_sequence: AtomicU64,

    pub(crate) compaction_priority: RwLock<Option<CompactionPriority>>,

    pub(crate) parent_snap: RwLock<Option<pb::Snapshot>>,
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
            parent_id: 0,
            cfs: [
                RwLock::new(ShardCF::new(0)),
                RwLock::new(ShardCF::new(1)),
                RwLock::new(ShardCF::new(2)),
            ],
            opt: opt.clone(),
            mem_tbls: RwLock::new(MemTables::new(vec![CFTable::new()])),
            l0_tbls: RwLock::new(L0Tables::new(Vec::new())),
            active: Default::default(),
            properties: Properties::new().apply_pb(props),
            compacting: Default::default(),
            initial_flushed: Default::default(),
            last_switch_time: RwLock::new(Instant::now()),
            max_mem_table_size: AtomicU64::new(base_size / 4),
            base_version: Default::default(),
            estimated_size: Default::default(),
            meta_seq: Default::default(),
            write_sequence: Default::default(),
            compaction_priority: RwLock::new(None),
            parent_snap: RwLock::new(None),
        };
        if let Some(val) = get_shard_property(MEM_TABLE_SIZE_KEY, props) {
            shard.set_max_mem_table_size(LittleEndian::read_u64(val.as_slice()))
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
        store_bool(&shard.initial_flushed, true);
        shard.base_version = meta.base_version;
        shard.meta_seq.store(meta.seq, Release);
        shard.write_sequence.store(meta.data_sequence, Release);
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
        store_bool(&shard.initial_flushed, true);
        shard.base_version = snap.base_version;
        shard.meta_seq.store(cs.sequence, Release);
        shard.write_sequence.store(cs.sequence, Release);
        info!(
            "ingest shard {}:{} max_table_size {}, mem_table_version {}, change {:?}",
            cs.shard_id,
            cs.shard_ver,
            shard.get_max_mem_table_size(),
            shard.load_mem_table_version(),
            &cs,
        );
        shard
    }

    pub fn set_active(&self, active: bool) {
        self.active.store(active, Release);
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Acquire)
    }

    pub(crate) fn refresh_estimated_size(&self) {
        let mut size = self.get_l0_total_size();
        self.for_each_level(|_, l| {
            size += self.get_level_total_size(l);
            false
        });
        store_u64(&self.estimated_size, size);
    }

    pub(crate) fn set_max_mem_table_size(&self, size: u64) {
        self.max_mem_table_size.store(size, Release);
    }

    pub(crate) fn get_max_mem_table_size(&self) -> u64 {
        self.max_mem_table_size.load(Acquire)
    }

    pub(crate) fn for_each_level<F>(&self, mut f: F)
    where
        F: FnMut(usize /*cf*/, &LevelHandler) -> bool, /*stopped*/
    {
        for cf in 0..NUM_CFS {
            let scf = self.get_cf(cf);
            for lh in scf.levels.as_ref() {
                if f(cf, lh) {
                    return;
                }
            }
        }
    }

    pub fn get_suggest_split_keys(&self, target_size: u64) -> Vec<Bytes> {
        let estimated_size = load_u64(&self.estimated_size);
        if estimated_size < target_size {
            return vec![];
        }
        let mut keys = Vec::new();
        let max_cf = self.get_cf(0);
        let mut max_level = &max_cf.levels[0];
        let mut max_level_total_size = 0;
        for i in 1..max_cf.levels.len() {
            let level = &max_cf.levels[i];
            let level_total_size = self.get_level_total_size(level);
            if level_total_size > max_level_total_size {
                max_level = level;
                max_level_total_size = level_total_size;
            }
        }
        let level_target_size =
            ((target_size as f64) * (max_level_total_size as f64) / (estimated_size as f64)) as u64;
        let mut current_size = 0;
        for i in 0..max_level.tables.len() {
            let tbl = &max_level.tables[i];
            if self.cover_full_table(tbl.smallest(), tbl.biggest()) {
                current_size += tbl.size();
            } else {
                current_size += tbl.size() / 2;
            }
            if i != 0 && current_size > level_target_size {
                keys.push(Bytes::copy_from_slice(tbl.smallest()));
                current_size = 0
            }
        }
        keys
    }

    pub fn overlap_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        self.start <= biggest && smallest < self.end
    }

    pub fn cover_full_table(&self, smallest: &[u8], biggest: &[u8]) -> bool {
        self.start <= smallest && biggest < self.end
    }

    pub fn overlap_key(&self, key: &[u8]) -> bool {
        self.start <= key && key < self.end
    }

    pub(crate) fn get_mem_tbls(&self) -> MemTables {
        self.mem_tbls.read().unwrap().clone()
    }

    pub(crate) fn get_l0_tbls(&self) -> L0Tables {
        self.l0_tbls.read().unwrap().clone()
    }

    pub(crate) fn get_cf(&self, cf: usize) -> ShardCF {
        self.cfs[cf].read().unwrap().clone()
    }

    pub(crate) fn set_cf(&self, cf: usize, scf: ShardCF) {
        *self.cfs[cf].write().unwrap() = scf;
    }

    pub fn get_property(&self, key: &str) -> Option<Bytes> {
        self.properties.get(key)
    }

    pub fn set_property(&self, key: &str, val: &[u8]) {
        self.properties.set(key, val);
    }

    pub(crate) fn next_mem_table_size(&self, current_size: u64, last_switch_time: Instant) -> u64 {
        let dur = last_switch_time.saturating_elapsed();
        let time_in_ms = dur.as_millis() as u64 + 1;
        let bytes_per_sec = current_size * 1000 / time_in_ms;
        let next_mem_size = bytes_per_sec * self.opt.max_mem_table_size_factor as u64;
        Self::bounded_mem_size(next_mem_size)
    }

    pub(crate) fn bounded_mem_size(size: u64) -> u64 {
        const MAX_MEM_SIZE_UPPER_LIMIT: u64 = 128 * 1024 * 1024;
        const MAX_MEM_SIZE_LOWER_LIMIT: u64 = 2 * 1024 * 1024;
        let mut bounded = size;
        if bounded > MAX_MEM_SIZE_UPPER_LIMIT {
            bounded = MAX_MEM_SIZE_UPPER_LIMIT;
        }
        if bounded < MAX_MEM_SIZE_LOWER_LIMIT {
            bounded = MAX_MEM_SIZE_LOWER_LIMIT;
        }
        bounded
    }

    pub(crate) fn load_mem_table_version(&self) -> u64 {
        self.base_version + self.write_sequence.load(Acquire)
    }

    pub fn get_all_files(&self) -> Vec<u64> {
        let mut files = Vec::new();
        let l0s = self.get_l0_tbls();
        for l0 in l0s.tbls.as_ref() {
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

    pub fn get_l0_files(&self) -> Vec<u64> {
        let mut files = Vec::new();
        let l0s = self.get_l0_tbls();
        for l0 in l0s.tbls.as_ref() {
            files.push(l0.id());
        }
        files
    }

    pub fn get_writable_mem_table(&self) -> memtable::CFTable {
        let guard = self.mem_tbls.read().unwrap();
        guard.tbls.first().unwrap().clone()
    }

    pub fn get_last_read_only_mem_table(&self) -> Option<memtable::CFTable> {
        let guard = self.mem_tbls.read().unwrap();
        if guard.tbls.len() < 2 {
            return None;
        }
        Some(guard.tbls.last().unwrap().clone())
    }

    pub fn atomic_add_mem_table(&self, mem_tbl: memtable::CFTable) {
        info!("shard {}:{} atomic add new mem table", self.id, self.ver);
        let mem_tbls = self.get_mem_tbls();
        let mut tbl_vec = Vec::with_capacity(mem_tbls.tbls.len() + 1);
        tbl_vec.push(mem_tbl);
        for tbl in mem_tbls.tbls.as_ref() {
            tbl_vec.push(tbl.clone());
        }
        let new_tables = Arc::new(tbl_vec);
        let mut guard = self.mem_tbls.write().unwrap();
        guard.tbls = new_tables;
    }

    pub fn atomic_remove_mem_table(&self) -> Arc<Vec<CFTable>> {
        let mem_tbls = self.get_mem_tbls();
        let old_tables = mem_tbls.tbls.as_ref();
        let old_len = old_tables.len();
        if old_len <= 1 {
            warn!("atomic remove mem table with old table len {}", old_len);
            return Arc::new(vec![]);
        }
        info!(
            "shard {}:{} atomic remove mem table version {}",
            self.id,
            self.ver,
            old_tables.last().unwrap().get_version()
        );
        let new_len = old_len - 1;
        let mut tbl_vec = Vec::with_capacity(new_len);
        for tbl in old_tables {
            tbl_vec.push(tbl.clone());
            if tbl_vec.len() == new_len {
                break;
            }
        }
        let new_tables = Arc::new(tbl_vec);
        let mut guard = self.mem_tbls.write().unwrap();
        mem::replace(&mut guard.tbls, new_tables)
    }

    pub fn set_split_mem_tables(&self, old_mem_tbls: &[CFTable]) {
        let mut new_mem_tbls = Vec::with_capacity(old_mem_tbls.len());
        for old_mem_tbl in old_mem_tbls {
            if old_mem_tbl.is_empty()
                || old_mem_tbl.has_data_in_range(self.start.chunk(), self.end.chunk())
            {
                new_mem_tbls.push(old_mem_tbl.new_split());
            }
        }
        let mut guard = self.mem_tbls.write().unwrap();
        let new_mem_tbls = MemTables {
            tbls: Arc::new(new_mem_tbls),
        };
        *guard = new_mem_tbls;
    }

    pub fn atomic_add_l0_table(&self, l0_tbl: L0Table) {
        info!(
            "shard {}:{} atomic add l0 table, version {}",
            self.id,
            self.ver,
            l0_tbl.version()
        );
        let mut guard = self.l0_tbls.write().unwrap();
        let old_l0_tbls = guard.tbls.as_ref();
        let mut tbl_vec = Vec::with_capacity(old_l0_tbls.len());
        tbl_vec.push(l0_tbl);
        for tbl in old_l0_tbls {
            tbl_vec.push(tbl.clone());
        }
        *guard = L0Tables::new(tbl_vec);
    }

    pub fn atomic_remove_l0_tables(&self, n: usize) {
        info!(
            "shard {}:{} atomic remove {} l0 tables",
            self.id, self.ver, n,
        );
        let mut guard = self.l0_tbls.write().unwrap();
        let old_l0_tbls = guard.tbls.as_slice();
        let new_len = old_l0_tbls.len() - n;
        let mut new_tbls = Vec::with_capacity(new_len);
        for tbl in &old_l0_tbls[..new_len] {
            new_tbls.push(tbl.clone());
        }
        *guard = L0Tables::new(new_tbls);
    }

    pub fn get_write_sequence(&self) -> u64 {
        self.write_sequence.load(Ordering::Acquire)
    }

    pub fn get_meta_sequence(&self) -> u64 {
        self.meta_seq.load(Ordering::Acquire)
    }

    pub fn get_estimated_size(&self) -> u64 {
        self.estimated_size.load(Ordering::Relaxed)
    }

    pub fn get_initial_flushed(&self) -> bool {
        self.initial_flushed.load(Acquire)
    }

    pub(crate) fn refresh_compaction_priority(&self) {
        let mut max_pri = CompactionPriority::default();
        max_pri.shard_id = self.id;
        max_pri.shard_ver = self.ver;
        max_pri.cf = -1;
        let l0s = self.get_l0_tbls();
        if l0s.tbls.len() > 2 {
            let size_score = l0s.total_size() as f64 / self.opt.base_size as f64;
            let num_tbl_score = l0s.tbls.len() as f64 / 4.0;
            max_pri.score = size_score * 0.6 + num_tbl_score * 0.4;
        }
        for l0 in l0s.tbls.as_ref() {
            if !self.cover_full_table(l0.smallest(), l0.biggest()) {
                // set highest priority for newly split L0.
                max_pri.score += 2.0;
                let mut lock = self.compaction_priority.write().unwrap();
                *lock = Some(max_pri);
                return;
            }
        }
        for cf in 0..NUM_CFS {
            let scf = self.get_cf(cf);
            for lh in &scf.levels[..scf.levels.len() - 1] {
                let level_total_size = self.get_level_total_size(lh);
                let score = level_total_size as f64
                    / ((self.opt.base_size as f64) * 10f64.powf((lh.level - 1) as f64));
                if max_pri.score < score {
                    max_pri.score = score;
                    max_pri.level = lh.level;
                    max_pri.cf = cf as isize;
                }
            }
        }
        let mut lock = self.compaction_priority.write().unwrap();
        if max_pri.score <= 1.0 {
            *lock = None
        } else {
            *lock = Some(max_pri);
        }
    }

    pub(crate) fn get_compaction_priority(&self) -> Option<CompactionPriority> {
        self.compaction_priority.read().unwrap().clone()
    }

    pub(crate) fn get_level_total_size(&self, level: &LevelHandler) -> u64 {
        let mut total_size = 0;
        for tbl in &level.tables {
            if self.cover_full_table(tbl.smallest(), tbl.biggest()) {
                total_size += tbl.size();
            } else {
                total_size += tbl.size() / 2;
            }
        }
        total_size
    }

    pub(crate) fn get_l0_total_size(&self) -> u64 {
        let mut total_size = 0;
        let l0s = self.get_l0_tbls();
        for l0 in l0s.tbls.as_ref() {
            if self.cover_full_table(l0.smallest(), l0.biggest()) {
                total_size += l0.size();
            } else {
                total_size += l0.size() / 2;
            }
        }
        total_size
    }
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

pub(crate) struct ShardCFBuilder {
    levels: Vec<LevelHandlerBuilder>,
}

impl ShardCFBuilder {
    pub(crate) fn new(cf: usize) -> Self {
        Self {
            levels: vec![LevelHandlerBuilder::new(); CF_LEVELS[cf]],
        }
    }

    pub(crate) fn build(&mut self) -> ShardCF {
        let mut levels = Vec::with_capacity(self.levels.len());
        for i in 0..self.levels.len() {
            levels.push(self.levels[i].build(i + 1))
        }
        ShardCF {
            levels: Arc::new(levels),
        }
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
        LevelHandler { tables, level }
    }

    fn add_table(&mut self, tbl: SSTable) {
        if self.tables.is_none() {
            self.tables = Some(vec![])
        }
        self.tables.as_mut().unwrap().push(tbl)
    }
}

#[derive(Clone)]
pub(crate) struct ShardCF {
    pub(crate) levels: Arc<Vec<LevelHandler>>,
}

impl ShardCF {
    pub(crate) fn new(cf: usize) -> Self {
        let mut levels = vec![];
        for j in 1..=CF_LEVELS[cf] {
            levels.push(LevelHandler::new(j));
        }
        Self {
            levels: Arc::new(levels),
        }
    }

    pub(crate) fn new_with_levels(levels: Vec<LevelHandler>) -> Self {
        Self {
            levels: Arc::new(levels),
        }
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
}

impl LevelHandler {
    pub fn new(level: usize) -> Self {
        Self {
            tables: Vec::new(),
            level,
        }
    }

    pub(crate) fn overlapping_tables(&self, key_range: &KeyRange) -> (usize, usize) {
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
