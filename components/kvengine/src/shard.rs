use std::{borrow::Borrow, cell::RefCell, collections::HashMap, sync::{Arc, Mutex, atomic::{self, AtomicBool, AtomicI32, AtomicI64, AtomicU64, Ordering::*}, mpsc}, time::Instant};
use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes};
use dashmap::DashMap;
use pb::changeset;
use protobuf::ProtobufEnum;

use crate::{meta::ShardMeta, table::{self, memtable::{self, CFTable, Hint}, search, sstable::L0Table, sstable::SSTable}};
use crate::*;
use crossbeam_epoch as epoch;
use epoch::{Atomic, Owned, Guard};
use slog_global::*;
use kvenginepb as pb;

pub(crate) struct MemTables {
    pub tbls: Vec<memtable::CFTable>,
}

impl MemTables {
    pub(crate) fn new(tbls: Vec<memtable::CFTable>) -> Self {
        MemTables {
            tbls,
        }
    }

    pub(crate) fn get(&self, i: usize) -> &memtable::CFTable {
        &self.tbls[i]
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
        Self {
            tbls,
        }
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
    pub(crate) cfs: [Atomic<ShardCF>; NUM_CFS],
    opt: Arc<Options>,

    mem_tbls: Atomic<MemTables>,
    pub(crate) l0_tbls: Atomic<L0Tables>,

    split_stage: AtomicI32,
    pub(crate) split_ctx: Atomic<SplitContext>,

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

    pub(crate) ingest_pre_split_seq: u64,

    size_stats: Atomic<ShardSizeStats>,
    
    pub(crate) remove_file: AtomicBool,

    pub(crate) compact_lock: tokio::sync::Mutex<()>,
}


pub const MEM_TABLE_SIZE_KEY: &str = "_mem_table_size";

impl Shard {
    pub fn new(props: &pb::Properties, ver: u64, start: &[u8], end: &[u8], opt: Arc<Options>) -> Self {
        let base_size = opt.base_size;
        let shard = Self {
            id: props.shard_id,
            ver,
            start: Bytes::copy_from_slice(start),
            end: Bytes::copy_from_slice(end),
            cfs: Default::default(),
            opt: opt.clone(),
            mem_tbls: Atomic::new(MemTables::new(vec![CFTable::new()])),
            l0_tbls: Atomic::new(L0Tables::new(Vec::new())),
            split_stage: AtomicI32::new(kvenginepb::SplitStage::Initial.value()),
            split_ctx: Atomic::new(SplitContext::new(&[])),
            active: Default::default(),
            properties: Properties::new().apply_pb(props),
            compacting: Default::default(),
            initial_flushed: Default::default(),
            last_switch_time: Default::default(),
            max_mem_table_size: AtomicU64::new(base_size / 4),
            base_ts: 1,
            estimated_size: Default::default(),
            meta_seq: Default::default(),
            ingest_pre_split_seq: Default::default(),
            size_stats: Default::default(),
            remove_file: AtomicBool::new(false),
            compact_lock: tokio::sync::Mutex::new(()),
        };
        if let Some(val) = get_shard_property(MEM_TABLE_SIZE_KEY, props){
            shard.set_max_mem_table_size(LittleEndian::read_u64(val.as_slice()))
        }
        for cf in 0..NUM_CFS {
            let mut scf = ShardCF::new(opt.cfs[cf].max_levels);
            shard.cfs[cf].store(Owned::new(scf), Relaxed);
        }
        shard
    }

    pub fn new_for_loading(meta: &ShardMeta, opt: Arc<Options>) -> Self {
        let mut shard = Self::new(&meta.properties.to_pb(meta.id), meta.ver, meta.start.as_slice(), meta.end.as_slice(), opt);
        if meta.pre_split.is_some() && meta.split_stage.value() >= pb::SplitStage::PreSplitFlushDone.value() {
            // We don't set split keys if the split stage has not reached enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE
		    // because some the recover data should be executed before split key is set.
            shard.set_split_keys(meta.pre_split.as_ref().unwrap().get_keys());
        }
        shard.set_split_stage(meta.split_stage);
        store_bool(&shard.initial_flushed, true);
        shard.base_ts = meta.base_ts;
        shard.meta_seq.store(meta.seq, Relaxed);
        shard
    }

    pub fn new_for_ingest(cs: pb::ChangeSet, opt: Arc<Options>) -> Self {
        let snap = cs.get_snapshot();
        let mut shard = Self::new(snap.get_properties(), cs.shard_ver, snap.start.as_slice(), snap.end.as_slice(), opt);
        if snap.get_split_keys().len() > 0 {
            info!("shard {}:{} set pre-split keys by ingest", cs.get_shard_id(), cs.get_shard_ver());
            shard.set_split_keys(snap.get_split_keys());
            if cs.stage == pb::SplitStage::PreSplit {
                shard.ingest_pre_split_seq = cs.sequence;
                info!("shard {}:{} set pre-split seuqence {} by ingest", shard.id, shard.ver, shard.ingest_pre_split_seq);
            }
        }
        shard.set_split_stage(cs.get_stage());
        store_bool(&shard.initial_flushed, true);
        shard.base_ts = snap.base_ts;
        shard.meta_seq.store(cs.sequence, Relaxed);
        info!("ingest shard {}:{} max_table_size {}, mem_table_ts {}", cs.shard_id, cs.shard_ver, shard.get_max_mem_table_size(), shard.load_mem_table_ts());
        shard
    }

    fn table_ids(&self) -> Vec<u64> {
        let g = &epoch::pin();
        let mut ids = Vec::new();
        let shared = self.l0_tbls.load(Relaxed, &g);
        let l0s = unsafe{shared.as_ref().unwrap()};
        for tbl in &l0s.tbls {
            ids.push(tbl.id());
        }
        let ids_ref = &mut ids;
        self.for_each_level(|cf, l| {
            for tbl in &l.tables {
                ids_ref.push(tbl.id());
            }
            false
        });
        ids
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
            stats.cfs[cf] += l.total_size();
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
            self.split_ctx.store(Owned::new(SplitContext::new(keys)), Release);
            return true;
        }
        warn!("shard {}:{} failed to set split key got stage {}", self.id, self.ver, self.get_split_stage().value());
        false
    }

    pub(crate) fn for_each_level<F>(&self, mut f: F) where F: FnMut(usize/*cf*/, &LevelHandler) -> bool/*stopped*/ {
        let g = &epoch::pin();
        for cf in 0..NUM_CFS {
            let scf = self.get_cf(cf, g);
            for lh in &scf.levels {
                if f(cf, lh) {
                    return
                }
            }
        }
    }

    fn load_writable_mem_table<'a>(&self, g: &'a Guard) -> &'a memtable::CFTable {
        let tbls = self.get_mem_tbls(g);
        tbls.get(0)
    }

    pub(crate) fn get_suggest_split_keys(&self, target_size: u64) -> Vec<Bytes> {
        let mut keys = Vec::new();
        let estimated_size = load_u64(&self.estimated_size);
        if estimated_size < target_size {
            return keys
        }
        let g = &epoch::pin();
        let l0s = self.get_l0_tbls(g);
        if l0s.tbls.len() > 0 && l0s.total_size() > (estimated_size * 3 / 10) {
            if let Some(tbl) = l0s.tbls[0].get_cf(0) {
                if let Some(split_key) = tbl.get_suggest_split_key() {
                    info!("shard {}:{} get table suggest split key {:x}, start {:x}, end {:x}", self.id, self.ver, split_key, self.start, self.end);
                    keys.push(split_key);
                    return keys
                }
            }
        }
        let max_cf = self.get_cf(0, g);
        let mut max_level = &max_cf.levels[0];
        for i in 1..max_cf.levels.len() {
            let level = &max_cf.levels[i];
            if level.total_size() > max_level.total_size() {
                max_level = level;
            }
        }
        let level_target_size = ((target_size as f64) * (max_level.total_size() as f64) / (estimated_size as f64)) as u64;
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

    pub(crate) fn get_split_ctx<'a>(&self, g: &'a epoch::Guard) -> &'a SplitContext {
        load_resource(&self.split_ctx, g)
    }

    pub(crate) fn get_split_keys<'a>(&self, g: &'a epoch::Guard) -> &'a Vec<Bytes> {
        let ctx = self.get_split_ctx(g);
        &ctx.split_keys
    }

    pub(crate) fn get_mem_tbls<'a>(&self, g: &'a epoch::Guard) -> &'a MemTables {
        load_resource(&self.mem_tbls, g)
    }

    pub(crate) fn get_l0_tbls<'a>(&self, g: &'a epoch::Guard) -> &'a L0Tables {
        load_resource(&self.l0_tbls, g)
    }

    pub(crate) fn get_cf<'a>(&self, cf: usize, g: &'a epoch::Guard) -> &'a ShardCF {
        load_resource(&self.cfs[cf], g)
    }

    pub fn get_property(&self, key: &str) -> Option<Bytes> {
        self.properties.get(key)
    }

    pub fn set_property(&self, key: &str, val: &[u8]) {
        self.properties.set(key, val);
    }

    pub(crate) fn next_mem_table_size(&self, current_size: i64, last_switch_time: Instant) -> i64 {
        todo!()
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
        self.for_each_level(|cf, lh|{
            for tbl in &lh.tables {
                files.push(tbl.id())
            }
            false
        });
        files.sort();
        files
    }

    pub fn new_snap_access<'a>(&self, cfs: [CFConfig; NUM_CFS], g: &'a Guard) -> SnapAccess<'a> {
        let mut splitting = None;
        if self.is_splitting() {
            splitting = Some(self.get_split_ctx(g));
        }
        let mem_tbls = self.get_mem_tbls(g);
        let l0_tbls = self.get_l0_tbls(g);
        let mut scfs = Vec::with_capacity(NUM_CFS);
        for cf in 0..NUM_CFS {
            let scf = self.get_cf(cf, g);
            scfs.push(scf);
        }
        SnapAccess::new(cfs, splitting, mem_tbls, l0_tbls, scfs)
    }

    pub fn get_writable_mem_table<'a>(&self, g: &'a Guard) -> &'a memtable::CFTable {
        let mem_tbls = self.get_mem_tbls(g);
        &mem_tbls.tbls[0]
    }

    pub fn atomic_add_mem_table<'a>(&self, g: &'a Guard, mem_tbl: memtable::CFTable) {
        loop {
            let mem_tbl = mem_tbl.clone();
            let shared = self.mem_tbls.load(Acquire, g);
            let old_mem_tbls = unsafe {&shared.deref().tbls};
            let mut tbl_vec = Vec::with_capacity(old_mem_tbls.len() + 1);
            tbl_vec.push(mem_tbl);
            for tbl in old_mem_tbls {
                tbl_vec.push(tbl.clone());
            }
            let new_mem_tbls = MemTables {
                tbls: tbl_vec,
            };
            if cas_resource(&self.mem_tbls, g, shared, new_mem_tbls) {
                break;
            }
        }
    }

    pub fn atomic_remove_mem_table<'a>(&self, g: &'a Guard) {
        loop {
            let shared = self.mem_tbls.load(Acquire, g);
            let old_mem_tbls = unsafe {&shared.deref().tbls};
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
            let new_mem_tbls = MemTables {
                tbls: tbl_vec,
            };
            if cas_resource(&self.mem_tbls, g, shared, new_mem_tbls) {
                break;
            }
        }
    }


    pub fn atomic_add_l0_table<'a>(&self, g: &'a Guard, l0_tbl: L0Table) {
        loop {
            let l0_tbl = l0_tbl.clone();
            let shared = self.l0_tbls.load(Acquire, g);
            let old_l0_tbls = unsafe {&shared.deref().tbls};
            let mut tbl_vec = Vec::with_capacity(old_l0_tbls.len());
            tbl_vec.push(l0_tbl);
            for tbl in old_l0_tbls {
                tbl_vec.push(tbl.clone());
            }
            let new_l0_tabls = L0Tables {
                tbls: tbl_vec,
            };
            if cas_resource(&self.l0_tbls, g, shared, new_l0_tabls) {
                break;
            }
        }
    }
}

pub fn load_resource<'a, T>(ptr: &Atomic<T>, g: &'a Guard) -> &'a T {
    let shared = ptr.load(Acquire, g);
    unsafe {shared.deref()}
}

pub fn cas_resource<'a, T>(ptr: &Atomic<T>, g: &'a Guard, old: epoch::Shared<T>, new: T) -> bool {
    ptr.compare_exchange(old, Owned::new(new), AcqRel, AcqRel, g).is_ok()
}

pub fn store_u64(ptr: &AtomicU64, val: u64) {
    ptr.store(val, Release);
}

pub fn load_u64(ptr: &AtomicU64) -> u64 {
    ptr.load( Acquire)
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

#[derive(Default)]
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

    fn set_has_overlapping(&self, cf: CompactDef) {
        todo!()
    }
}

#[derive(Default)]
pub struct LevelHandler {
    pub(crate) tables: Vec<SSTable>,
    pub(crate) level: usize,
    pub(crate) total_size: AtomicU64,
}

impl LevelHandler {
    pub fn new(level: usize) -> Self {
        Self {
            tables: Vec::new(),
            level,
            total_size: AtomicU64::new(0),
        }
    }

    fn overlapping_tables(&self, key_range: KeyRange) -> (usize, usize) {
        get_tables_in_range(&self.tables, key_range.start, key_range.end)
    }

    pub fn get(&self, key: &[u8], version: u64, key_hash: u64) -> table::Value {
        self.get_in_table(key, version, key_hash, self.get_table(key))
    }

    fn get_in_table(&self, key: &[u8], version: u64, key_hash: u64, tbl: Option<&SSTable>) -> table::Value {
        if tbl.is_none() {
            return table::Value::new();
        }
        tbl.unwrap().get(key, version, key_hash)
    }

    fn get_table(&self, key: &[u8]) -> Option<&SSTable> {
        let idx = search(self.tables.len(), |i| self.tables[i].biggest() >= key);
        if idx >= self.tables.len() {
            return None
        }
        return Some(&self.tables[idx])
    }

    fn total_size(&self) -> u64 {
        let mut total = 0;
        for tbl in &self.tables {
            total += tbl.size();
        }
        total
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
            return Some(props.get_values()[i].clone())
        }
    }
    None
}

pub fn get_splitting_start_end<'a: 'b, 'b>(start: &'a [u8], end: &'a [u8], split_keys: &'b [Vec<u8>], i: usize) -> (&'b [u8], &'b [u8]) {
    let start_key: &'b [u8];
    let end_key: &'b [u8];
    if i != 0 {
        start_key = split_keys[i-1].as_slice();
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
