
use std::u64;
use std::cmp;
use std::io::BufRead;
use std::sync::RwLock;
use std::mem;
use std::fmt;
use std::time::Instant;

use protobuf;
use kvproto::eraftpb::Entry;

use util::collections::{HashMap, HashSet};

use super::Result;
use super::Error;
use super::memtable::MemTable;
use super::pipe_log::{PipeLog, FILE_MAGIC_HEADER, VERSION};
use super::log_batch::{Command, LogBatch, LogItemType, OpType};
use super::config::Config;
use super::metrics::*;

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
            RecoveryMode::TolerateCorruptedTailRecords as i32 <= i &&
                i <= RecoveryMode::AbsoluteConsistency as i32
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RaftEngine dir: {}", self.cfg.dir)
    }
}

impl RaftEngine {
    pub fn new(cfg: Config) -> RaftEngine {
        let pip_log = PipeLog::open(
            &cfg.dir,
            cfg.bytes_per_sync.0 as usize,
            cfg.target_file_size.0 as usize,
        ).unwrap_or_else(|e| panic!("Open raft log failed, error: {:?}", e));
        let mut memtables = Vec::with_capacity(SLOTS_COUNT);
        for _ in 0..SLOTS_COUNT {
            memtables.push(RwLock::new(HashMap::default()));
        }
        let mut engine = RaftEngine {
            cfg: cfg,
            memtables: memtables,
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
        let (mut current_read_file, active_file_num) = {
            let pipe_log = self.pipe_log.read().unwrap();
            (pipe_log.first_file_num(), pipe_log.active_file_num())
        };

        let start = Instant::now();

        // Iterate files one by one
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
                            current_read_file,
                            e
                        )
                    })
                    .unwrap_or_else(|| panic!("Expect has content, but get None"))
            };

            // Verify file header
            let mut buf = content.as_slice();
            if buf.len() < FILE_MAGIC_HEADER.len() || !buf.starts_with(FILE_MAGIC_HEADER) {
                panic!("Raft log file {} is corrupted.", current_read_file);
            }
            buf.consume(FILE_MAGIC_HEADER.len() + VERSION.len());

            // Iterate all LogBatch in one file
            let mut offset = FILE_MAGIC_HEADER.len() as u64;
            loop {
                match LogBatch::from_bytes(&mut buf) {
                    Ok(Some((log_batch, advance))) => {
                        offset += advance as u64;
                        self.apply_to_memtable(log_batch, current_read_file);
                    }
                    Ok(None) => {
                        info!("Recovered raft log file {}.", current_read_file);
                        break;
                    }
                    e @ Err(Error::TooShort) => if current_read_file == active_file_num {
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
                    },
                    Err(e) => {
                        panic!(
                            "Failed when recover log file {}, error {:?}",
                            current_read_file,
                            e
                        );
                    }
                }
            }

            current_read_file += 1;
        }

        info!("Recover raft log takes {:?}", start.elapsed());

        Ok(())
    }

    // Rewrite inactive region's entries and key/value pairs, so the old log can be dropped.
    pub fn rewrite_inactive(&self) -> bool {
        let (first_file_num, inactive_file_num) = {
            let pipe_log = self.pipe_log.read().unwrap();
            (
                pipe_log.first_file_num(),
                pipe_log.files_should_evict(self.cfg.total_size_limit.0 as usize),
            )
        };

        if inactive_file_num == first_file_num {
            return false;
        }

        let mut has_write = false;
        for slot in 0..SLOTS_COUNT {
            let mut memtables = self.memtables[slot].write().unwrap();
            for memtable in memtables.values_mut() {
                let min_file_num = match memtable.min_file_num() {
                    Some(file_num) => file_num,
                    None => continue,
                };

                // Has no entry in inactive files, skip.
                if min_file_num >= inactive_file_num {
                    continue;
                }

                // There are two situations we need rewrite raft logs:
                // 1) Has entries in inactive files, at the same time the total entries is less
                // than `compact_threshold`, compaction will not be triggered, so we need rewrite
                // these entries, so the old files can be dropped.
                // 2) Has entries in inactive files, and the total size of entries is small,
                // rewriting is trivial, to make the old files can be dropped ASAP we rewrite
                // these entries.
                let count = memtable.entries_count();
                if count > 0 &&
                    (count < self.cfg.compact_threshold ||
                        (count < 2 * self.cfg.compact_threshold &&
                            memtable.entries_size() < self.cfg.rewrite_size_threshold.0 as usize))
                {
                    REWRITE_COUNTER.inc();
                    REWRITE_ENTRIES_COUNT_HISTOGRAM.observe(count as f64);
                    has_write = true;

                    // dump all entries
                    let mut ents = Vec::with_capacity(count);
                    memtable.fetch_all(&mut ents);
                    let mut log_batch = LogBatch::default();
                    log_batch.add_entries(memtable.region_id(), ents.clone());

                    // dump all key value pairs
                    let mut kvs = vec![];
                    memtable.fetch_all_kvs(&mut kvs);
                    for kv in &kvs {
                        log_batch.put(memtable.region_id(), &kv.0, &kv.1);
                    }

                    // rewrite to new log file
                    let mut pipe_log = self.pipe_log.write().unwrap();
                    match pipe_log.append_log_batch(&mut log_batch, false) {
                        // Using slef.apply_to_memtable here will cause deadlock.
                        Ok(file_num) => for item in log_batch.items.drain(..) {
                            match item.item_type {
                                LogItemType::Entries => {
                                    let entries_to_add = item.entries.unwrap();
                                    assert_eq!(entries_to_add.region_id, memtable.region_id());
                                    memtable.append(
                                        entries_to_add.entries,
                                        entries_to_add.entries_size,
                                        file_num,
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
                        },
                        Err(e) => panic!("Rewrite inactive region entries failed. error {:?}", e),
                    }
                }
            }
        }
        has_write
    }

    pub fn regions_need_compact(&self) -> HashSet<u64> {
        let (first_file_num, inactive_file_num) = {
            let pipe_log = self.pipe_log.read().unwrap();
            (
                pipe_log.first_file_num(),
                pipe_log.files_should_evict(self.cfg.total_size_limit.0 as usize),
            )
        };

        let mut regions = HashSet::default();
        if first_file_num == inactive_file_num {
            return regions;
        }

        for slot in 0..SLOTS_COUNT {
            let memtables = self.memtables[slot].read().unwrap();
            for memtable in memtables.values() {
                let min_file_num = match memtable.min_file_num() {
                    Some(file_num) => file_num,
                    None => continue,
                };

                // Has no entry in inactive files, skip.
                if min_file_num >= inactive_file_num {
                    continue;
                }

                // There are many entries in inactive files, this happens when some follower
                // left behind for a long time.
                if memtable.entries_count_before_file(inactive_file_num) >=
                    self.cfg.compact_threshold
                {
                    regions.insert(memtable.region_id());
                }
            }
        }
        NEED_COMPACT_REGIONS_HISTOGRAM.observe(regions.len() as f64);

        regions
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

    pub fn write(&self, mut log_batch: LogBatch, sync: bool) -> Result<()> {
        let write_res = {
            let mut pipe_log = self.pipe_log.write().unwrap();
            pipe_log.append_log_batch(&mut log_batch, sync)
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
        let mut log_batch = LogBatch::default();
        log_batch.put(region_id, key, value);
        self.write(log_batch, false)
    }

    pub fn put_msg<M: protobuf::Message>(&self, region_id: u64, key: &[u8], m: &M) -> Result<()> {
        let mut log_batch = LogBatch::default();
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

    pub fn get_msg<M>(&self, region_id: u64, key: &[u8]) -> Result<Option<M>>
    where
        M: protobuf::Message + protobuf::MessageStatic,
    {
        let value = self.get(region_id, key)?;

        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::new();
        m.merge_from_bytes(value.unwrap().as_slice())?;
        Ok(Some(m))
    }

    pub fn get_entry(&self, region_id: u64, log_idx: u64) -> Result<Option<Entry>> {
        let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        if let Some(memtable) = memtables.get(&region_id) {
            Ok(memtable.get_entry(log_idx))
        } else {
            Ok(None)
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
            memtable.fetch_entries_to(begin, end, max_size, vec)
        } else {
            Ok(0)
        }
    }

    // command interface
    pub fn clean_region(&self, region_id: u64) -> Result<()> {
        let mut log_batch = LogBatch::default();
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
        if file_num == 0 {
            return;
        }
        self.apply_to_memtable(log_batch, file_num);
    }

    fn apply_to_memtable(&self, mut log_batch: LogBatch, file_num: u64) {
        for item in log_batch.items.drain(..) {
            match item.item_type {
                LogItemType::Entries => {
                    let entries_to_add = item.entries.unwrap();
                    let region_id = entries_to_add.region_id;
                    let mut memtables = self.memtables[region_id as usize % SLOTS_COUNT]
                        .write()
                        .unwrap();
                    let memtable = memtables
                        .entry(region_id)
                        .or_insert_with(|| MemTable::new(region_id));
                    memtable.append(
                        entries_to_add.entries,
                        entries_to_add.entries_size,
                        file_num,
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
                        .or_insert_with(|| MemTable::new(kv.region_id));
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
