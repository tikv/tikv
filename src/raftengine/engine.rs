use std::cmp;
use std::fmt;
use std::io::BufRead;
use std::mem;
use std::sync::RwLock;
use std::time::Instant;
use std::u64;

use protobuf;
use protobuf::Message as PbMsg;
use raft::eraftpb::Entry;

use crate::util::collections::{HashMap, HashSet};

use super::config::Config;
use super::log_batch::{Command, LogBatch, LogItemType, OpType};
use super::memtable::MemTable;
use super::metrics::*;
use super::pipe_log::{PipeLog, FILE_MAGIC_HEADER, VERSION};
use super::Error;
use super::Result;

const SLOTS_COUNT: usize = 128;

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum RecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
}

impl From<i32> for RecoveryMode {
    fn from(i: i32) -> RecoveryMode {
        assert!(
            RecoveryMode::TolerateCorruptedTailRecords as i32 <= i
                && i <= RecoveryMode::AbsoluteConsistency as i32
        );
        unsafe { mem::transmute(i) }
    }
}

pub struct RaftEngine {
    cfg: Config,

    // Multiple slots
    // region_id -> MemTable.
    memtables: Vec<RwLock<HashMap<u64, MemTable>>>,

    // Persistent entries.
    pipe_log: RwLock<PipeLog>,
}

impl fmt::Debug for RaftEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RaftEngine dir: {}", self.cfg.dir)
    }
}

impl RaftEngine {
    pub fn new(cfg: Config) -> RaftEngine {
        let pip_log = PipeLog::open(
            &cfg.dir,
            cfg.bytes_per_sync.0 as usize,
            cfg.target_file_size.0 as usize,
        )
        .unwrap_or_else(|e| panic!("Open raft log failed, error: {:?}", e));
        let mut memtables = Vec::with_capacity(SLOTS_COUNT);
        for _ in 0..SLOTS_COUNT {
            memtables.push(RwLock::new(HashMap::default()));
        }
        let mut engine = RaftEngine {
            cfg,
            memtables,
            pipe_log: RwLock::new(pip_log),
        };
        let recovery_mode = RecoveryMode::from(engine.cfg.recovery_mode);
        engine
            .recover(recovery_mode)
            .unwrap_or_else(|e| panic!("Recover raft log failed, error: {:?}", e));
        engine
    }

    // recover from disk.
    fn recover(&mut self, recovery_mode: RecoveryMode) -> Result<()> {
        // Get first file number and last file number.
        let (first_file_num, active_file_num) = {
            let pipe_log = self.pipe_log.read().unwrap();
            (pipe_log.first_file_num(), pipe_log.active_file_num())
        };

        let start = Instant::now();

        // Iterate files one by one
        let mut current_read_file = first_file_num;
        loop {
            if current_read_file > active_file_num {
                break;
            }

            // Read a file
            let content = {
                let mut pipe_log = self.pipe_log.write().unwrap();
                pipe_log
                    .read_next_file()
                    .unwrap_or_else(|e| {
                        panic!(
                            "Read content of file {} failed, error {:?}",
                            current_read_file, e
                        )
                    })
                    .unwrap_or_else(|| panic!("Expect has content, but get None"))
            };

            // Verify file header
            let mut buf = content.as_slice();
            let start_ptr: *const u8 = buf.as_ptr();
            if buf.len() < FILE_MAGIC_HEADER.len() || !buf.starts_with(FILE_MAGIC_HEADER) {
                panic!("Raft log file {} is corrupted.", current_read_file);
            }
            buf.consume(FILE_MAGIC_HEADER.len() + VERSION.len());

            // Iterate all LogBatch in one file
            let mut offset = (FILE_MAGIC_HEADER.len() + VERSION.len()) as u64;
            loop {
                match unsafe { LogBatch::from_bytes(&mut buf, current_read_file, start_ptr) } {
                    Ok(Some(log_batch)) => {
                        self.apply_to_memtable(log_batch, current_read_file);
                        offset = unsafe { buf.as_ptr().offset_from(start_ptr) as u64 };
                    }
                    Ok(None) => {
                        info!("Recovered raft log file {}.", current_read_file);
                        break;
                    }
                    e @ Err(Error::TooShort) => {
                        if current_read_file == active_file_num {
                            match recovery_mode {
                                RecoveryMode::TolerateCorruptedTailRecords => {
                                    warn!(
                                    "Encounter err {:?}, incomplete batch in last log file {}, \
                                     offset {}, truncate it in TolerateCorruptedTailRecords \
                                     recovery mode.",
                                    e,
                                    current_read_file,
                                    offset
                                );
                                    let mut pipe_log = self.pipe_log.write().unwrap();
                                    pipe_log.truncate_active_log(offset).unwrap();
                                    break;
                                }
                                RecoveryMode::AbsoluteConsistency => {
                                    panic!(
                                    "Encounter err {:?}, incomplete batch in last log file {}, \
                                     offset {}, panic in AbsoluteConsistency recovery mode.",
                                    e,
                                    current_read_file,
                                    offset
                                );
                                }
                            }
                        } else {
                            panic!("Corruption occur in middle log file {}", current_read_file);
                        }
                    }
                    Err(e) => {
                        panic!(
                            "Failed when recover log file {}, error {:?}",
                            current_read_file, e
                        );
                    }
                }
            }

            // Only keep latest entries in cache, keep cache below limited size.
            if self.cfg.cache_size_limit.0 > 0
                && (current_read_file - first_file_num) * self.cfg.target_file_size.0
                    > self.cfg.cache_size_limit.0
            {
                let total_files_in_cache =
                    self.cfg.cache_size_limit.0 / self.cfg.target_file_size.0;
                if current_read_file > total_files_in_cache {
                    for memtables in &self.memtables {
                        let mut memtables = memtables.write().unwrap();
                        for memtable in memtables.values_mut() {
                            memtable.evict_old_from_cache(current_read_file - total_files_in_cache);
                        }
                    }
                }
            }

            current_read_file += 1;
        }

        info!("Recover raft log takes {:?}", start.elapsed());

        Ok(())
    }

    // Rewrite inactive region's entries and key/value pairs,
    // so the old files can be dropped ASAP.
    pub fn rewrite_inactive(&self) -> bool {
        let inactive_file_num = {
            let pipe_log = self.pipe_log.read().unwrap();
            pipe_log.files_before(self.cfg.cache_size_limit.0 as usize)
        };

        if inactive_file_num == 0 {
            return false;
        }

        let mut has_write = false;
        let mut memory_usage = 0;
        for slot in 0..SLOTS_COUNT {
            let mut memtables = self.memtables[slot].write().unwrap();
            for memtable in memtables.values_mut() {
                memory_usage += memtable.entries_size();

                let min_file_num = match memtable.min_file_num() {
                    Some(file_num) => file_num,
                    None => continue,
                };

                // Has no entry in inactive files, skip.
                if min_file_num >= inactive_file_num {
                    continue;
                }

                // Has entries in inactive files, at the same time the total entries is less
                // than `compact_threshold`, compaction will not be triggered, so we need rewrite
                // these entries, so the old files can be dropped ASAP.
                if memtable.entries_count() < self.cfg.compact_threshold {
                    REWRITE_COUNTER.inc();
                    REWRITE_ENTRIES_COUNT_HISTOGRAM.observe(memtable.entries_count() as f64);
                    has_write = true;

                    // Dump all entries
                    // Not all entries are in cache always, we may need read remains
                    // entries from file.
                    let mut ents = Vec::with_capacity(memtable.entries_count());
                    let mut ents_idx = Vec::with_capacity(memtable.entries_count());
                    memtable.fetch_all(&mut ents, &mut ents_idx);
                    let mut all_ents = Vec::with_capacity(memtable.entries_count());
                    for i in ents_idx {
                        let e = self
                            .read_entry_from_file(i.file_num, i.offset, i.len, i.index)
                            .unwrap_or_else(|e| {
                                panic!(
                                    "Read entry from file {} at offset {} failed \
                                     when rewriting, err {:?}",
                                    i.file_num, i.offset, e
                                )
                            });
                        all_ents.push(e);
                    }
                    all_ents.extend(ents.into_iter());
                    let log_batch = LogBatch::new();
                    log_batch.add_entries(memtable.region_id(), all_ents);

                    // Dump all key value pairs
                    let mut kvs = vec![];
                    memtable.fetch_all_kvs(&mut kvs);
                    for kv in &kvs {
                        log_batch.put(memtable.region_id(), &kv.0, &kv.1);
                    }

                    // Rewrite to new log file
                    let write_res = {
                        let mut pipe_log = self.pipe_log.write().unwrap();
                        pipe_log.append_log_batch(&log_batch, false)
                    };

                    // Apply to memtable
                    match write_res {
                        // Using slef.apply_to_memtable here will cause deadlock.
                        Ok(file_num) => {
                            for item in log_batch.items.borrow_mut().drain(..) {
                                match item.item_type {
                                    LogItemType::Entries => {
                                        let entries_to_add = item.entries.unwrap();
                                        assert_eq!(entries_to_add.region_id, memtable.region_id());
                                        memtable.append(
                                            entries_to_add.entries,
                                            entries_to_add.entries_index.into_inner(),
                                        );
                                    }
                                    LogItemType::CMD => {
                                        panic!("Memtable doesn't contain command item.");
                                    }
                                    LogItemType::KV => {
                                        let kv = item.kv.unwrap();
                                        assert_eq!(kv.region_id, memtable.region_id());
                                        match kv.op_type {
                                            OpType::Put => {
                                                memtable.put(kv.key, kv.value.unwrap(), file_num);
                                            }
                                            OpType::Del => {
                                                memtable.delete(kv.key.as_slice());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => panic!("Rewrite inactive region entries failed. error {:?}", e),
                    }
                }
            }
        }
        RAFTENGINE_MEMORY_USAGE_GAUGE.set(memory_usage as f64);

        has_write
    }

    pub fn regions_need_force_compact(&self) -> HashSet<u64> {
        // first_file_num: the oldest file number.
        // current_file_num: current file number.
        // inactive_file_num: files before this one should not in cache.
        // gc_file_num: entries in these files should compact by force.
        let (inactive_file_num, gc_file_num) = {
            let pipe_log = self.pipe_log.read().unwrap();
            (
                pipe_log.files_before(self.cfg.cache_size_limit.0 as usize),
                pipe_log.files_before(self.cfg.total_size_limit.0 as usize),
            )
        };

        let mut regions = HashSet::default();
        let region_entries_size_limit = self.cfg.region_size.0 * 2 / 3;
        for slot in 0..SLOTS_COUNT {
            let memtables = self.memtables[slot].read().unwrap();
            for memtable in memtables.values() {
                // Total size of entries for this region exceed limit.
                if memtable.entries_size() > region_entries_size_limit {
                    info!(
                        "region {}'s total raft log size {} exceed limit \
                         need force compaction",
                        memtable.region_id(),
                        memtable.entries_size(),
                    );
                    regions.insert(memtable.region_id());
                    continue;
                }

                if inactive_file_num == 0 || gc_file_num == 0 {
                    continue;
                }

                let min_file_num = match memtable.min_file_num() {
                    Some(file_num) => file_num,
                    None => continue,
                };

                // Has entries left behind too far, this happens when
                // some followers left behind for a long time.
                if min_file_num < gc_file_num {
                    info!(
                        "region {}'s some followers left behind too far, \
                         need force compaction",
                        memtable.region_id()
                    );
                    regions.insert(memtable.region_id());
                }
            }
        }
        NEED_COMPACT_REGIONS_HISTOGRAM.observe(regions.len() as f64);

        regions
    }

    pub fn evict_old_from_cache(&self) {
        let inactive_file_num = {
            let pipe_log = self.pipe_log.read().unwrap();
            pipe_log.files_before(self.cfg.cache_size_limit.0 as usize)
        };

        if inactive_file_num == 0 {
            return;
        }

        for slot in 0..SLOTS_COUNT {
            let mut memtables = self.memtables[slot].write().unwrap();
            for memtable in memtables.values_mut() {
                memtable.evict_old_from_cache(inactive_file_num);
            }
        }
    }

    pub fn purge_expired_files(&self) -> Result<()> {
        let mut min_file_num = u64::MAX;
        for memtables in &self.memtables {
            let memtables = memtables.read().unwrap();
            let file_num = memtables.values().fold(u64::MAX, |min, x| {
                cmp::min(min, x.min_file_num().map_or(u64::MAX, |num| num))
            });
            if file_num < min_file_num {
                min_file_num = file_num;
            }
        }

        let mut pipe_log = self.pipe_log.write().unwrap();
        pipe_log.purge_to(min_file_num)
    }

    pub fn compact_to(&self, region_id: u64, index: u64) -> u64 {
        let mut memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .write()
            .unwrap();
        if let Some(memtable) = memtables.get_mut(&region_id) {
            memtable.compact_to(index)
        } else {
            0
        }
    }

    pub fn write(&self, log_batch: LogBatch, sync: bool) -> Result<()> {
        let write_res = {
            let mut pipe_log = self.pipe_log.write().unwrap();
            pipe_log.append_log_batch(&log_batch, sync)
        };
        match write_res {
            Ok(file_num) => {
                self.post_append_to_file(log_batch, file_num);
                Ok(())
            }
            Err(e) => panic!("Append log batch to pipe log failed, error: {:?}", e),
        }
    }

    pub fn sync_data(&self) -> Result<()> {
        let mut pipe_log = self.pipe_log.write().unwrap();
        pipe_log.sync_data()
    }

    pub fn kv_count(&self, region_id: u64) -> usize {
        let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        if let Some(memtable) = memtables.get(&region_id) {
            return memtable.kvs_total_count();
        }
        0
    }

    pub fn put(&self, region_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let log_batch = LogBatch::new();
        log_batch.put(region_id, key, value);
        self.write(log_batch, false)
    }

    pub fn put_msg<M: protobuf::Message>(&self, region_id: u64, key: &[u8], m: &M) -> Result<()> {
        let log_batch = LogBatch::new();
        log_batch.put_msg(region_id, key, m)?;
        self.write(log_batch, false)
    }

    pub fn get(&self, region_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        if let Some(memtable) = memtables.get(&region_id) {
            Ok(memtable.get(key))
        } else {
            Ok(None)
        }
    }

    pub fn get_msg<M: protobuf::Message>(&self, region_id: u64, key: &[u8]) -> Result<Option<M>> {
        let value = self.get(region_id, key)?;

        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::new();
        m.merge_from_bytes(value.unwrap().as_slice())?;
        Ok(Some(m))
    }

    pub fn get_entry(&self, region_id: u64, log_idx: u64) -> Result<Option<Entry>> {
        // Fetch from cache
        let entry_idx = {
            let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
                .read()
                .unwrap();
            if let Some(memtable) = memtables.get(&region_id) {
                match memtable.get_entry(log_idx) {
                    Some((Some(entry), _)) => return Ok(Some(entry)),
                    Some((None, Some(idx))) => idx,
                    Some((None, None)) | None => return Ok(None),
                }
            } else {
                return Ok(None);
            }
        };

        // Read from file
        let entry = self
            .read_entry_from_file(
                entry_idx.file_num,
                entry_idx.offset,
                entry_idx.len,
                entry_idx.index,
            )
            .unwrap_or_else(|e| {
                panic!(
                    "Read entry from file for region {} index {} failed, err {:?}",
                    region_id, log_idx, e
                )
            });
        Ok(Some(entry))
    }

    fn read_entry_from_file(
        &self,
        file_num: u64,
        offset: u64,
        len: u64,
        expect_idx: u64,
    ) -> Result<Entry> {
        let content = {
            let pipe_log = self.pipe_log.read().unwrap();
            pipe_log.fread(file_num, offset, len).unwrap_or_else(|e| {
                panic!(
                    "Read from file {} in offset {} failed, err {:?}",
                    file_num, offset, e
                )
            })
        };
        let mut e = Entry::new();
        e.merge_from_bytes(content.as_slice())?;
        if e.get_index() != expect_idx {
            panic!(
                "Except entry's index is {}, but got {}",
                expect_idx,
                e.get_index()
            );
        }
        Ok(e)
    }

    pub fn fetch_all_entries_for_region(&self, region_id: u64) -> Result<Vec<Entry>> {
        let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        if let Some(memtable) = memtables.get(&region_id) {
            let mut entries = vec![];
            let mut entries_idx = vec![];
            memtable.fetch_all(&mut entries, &mut entries_idx);
            if !entries_idx.is_empty() {
                let mut vec = Vec::with_capacity(entries.len() + entries_idx.len());
                for idx in &entries_idx {
                    let e =
                        self.read_entry_from_file(idx.file_num, idx.offset, idx.len, idx.index)?;
                    vec.push(e);
                }
                vec.extend(entries.into_iter());
                Ok(vec)
            } else {
                Ok(entries)
            }
        } else {
            Ok(vec![])
        }
    }

    pub fn fetch_entries_to(
        &self,
        region_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec: &mut Vec<Entry>,
    ) -> Result<u64> {
        let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        if let Some(memtable) = memtables.get(&region_id) {
            let mut entries = Vec::with_capacity((end - begin) as usize);
            let mut entries_idx = Vec::with_capacity((end - begin) as usize);
            match memtable.fetch_entries_to(begin, end, max_size, &mut entries, &mut entries_idx) {
                Ok(num) => {
                    let count = num + entries_idx.len() as u64;
                    // Read remain entries from file if there are.
                    for idx in &entries_idx {
                        let e = self.read_entry_from_file(
                            idx.file_num,
                            idx.offset,
                            idx.len,
                            idx.index,
                        )?;
                        vec.push(e);
                    }
                    vec.extend(entries.into_iter());
                    return Ok(count);
                }
                Err(e) => {
                    error!("Fetch entries from memtable failed, err {:?}", e);
                    return Err(e);
                }
            }
        } else {
            Ok(0)
        }
    }

    // command interface
    pub fn clean_region(&self, region_id: u64) -> Result<()> {
        let log_batch = LogBatch::new();
        log_batch.clean_region(region_id);
        self.write(log_batch, true)
    }

    // only used in test
    pub fn is_empty(&self) -> bool {
        for memtables in &self.memtables {
            let memtables = memtables.read().unwrap();
            if !memtables.is_empty() {
                return false;
            }
        }
        true
    }

    // only used in test
    pub fn region_not_exist(&self, region_id: u64) -> bool {
        let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        memtables.get(&region_id).is_none()
    }

    fn post_append_to_file(&self, log_batch: LogBatch, file_num: u64) {
        // 0 means write nothing.
        if file_num == 0 {
            return;
        }
        self.apply_to_memtable(log_batch, file_num);
    }

    fn apply_to_memtable(&self, log_batch: LogBatch, file_num: u64) {
        for item in log_batch.items.borrow_mut().drain(..) {
            match item.item_type {
                LogItemType::Entries => {
                    let entries_to_add = item.entries.unwrap();
                    let region_id = entries_to_add.region_id;
                    let mut memtables = self.memtables[region_id as usize % SLOTS_COUNT]
                        .write()
                        .unwrap();
                    let memtable = memtables
                        .entry(region_id)
                        .or_insert_with(|| MemTable::new(region_id, self.cfg.region_size.0 / 2));
                    memtable.append(
                        entries_to_add.entries,
                        entries_to_add.entries_index.into_inner(),
                    );
                }
                LogItemType::CMD => {
                    let command = item.command.unwrap();
                    match command {
                        Command::Clean { region_id } => {
                            let mut memtables = self.memtables[region_id as usize % SLOTS_COUNT]
                                .write()
                                .unwrap();
                            memtables.remove(&region_id);
                        }
                    }
                }
                LogItemType::KV => {
                    let kv = item.kv.unwrap();
                    let mut memtables = self.memtables[kv.region_id as usize % SLOTS_COUNT]
                        .write()
                        .unwrap();
                    let memtable = memtables
                        .entry(kv.region_id)
                        .or_insert_with(|| MemTable::new(kv.region_id, self.cfg.region_size.0 / 2));
                    match kv.op_type {
                        OpType::Put => {
                            memtable.put(kv.key, kv.value.unwrap(), file_num);
                        }
                        OpType::Del => {
                            memtable.delete(kv.key.as_slice());
                        }
                    }
                }
            }
        }
    }
}
