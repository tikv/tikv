
use std::u64;
use std::cmp;
use std::io::BufRead;
use std::sync::RwLock;

use util::collections::HashMap;

use super::Result;
use super::Error;
use super::mem_entries::MemEntries;
use super::pipe_log::{PipeLog, FILE_MAGIC_HEADER, VERSION};
use super::log_batch::{Command, LogBatch, LogItemType};

const DEFAULT_BYTES_PER_SYNC: usize = 32 * 1024;

const SLOTS_COUNT: usize = 32;

#[derive(Clone, Copy)]
enum RecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
}

struct MultiRaftEngine {
    // Raft log directory.
    pub dir: String,

    // Multiple slots
    // region_id -> MemEntries.
    pub mem_entries: Vec<RwLock<HashMap<u64, MemEntries>>>,

    // Rewrite inactive region's entries to new pipe log file,
    // so the old log file can be dropped.
    // Rewrite one slot for each time.
    next_rewrite_slot: usize,

    // Persistent entries.
    pub pipe_log: RwLock<PipeLog>,
}

impl MultiRaftEngine {
    pub fn new(dir: String, recovery_mode: RecoveryMode, bytes_per_sync: usize) -> MultiRaftEngine {
        let pip_log = PipeLog::open(&dir, bytes_per_sync)
            .unwrap_or_else(|e| panic!("Open raft log failed, error: {:?}", e));
        let mut mem_entries = Vec::with_capacity(SLOTS_COUNT);
        for _ in 0..SLOTS_COUNT {
            mem_entries.push(RwLock::new(HashMap::default()));
        }
        let mut engine = MultiRaftEngine {
            dir: dir,
            mem_entries: mem_entries,
            next_rewrite_slot: 0,
            pipe_log: RwLock::new(pip_log),
        };
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
                        self.apply_to_cache(log_batch, current_read_file);
                    }
                    Ok(None) => {
                        info!("Recovered raft log file {}.", current_read_file);
                        break;
                    }
                    e @ Err(Error::TooShort) => if current_read_file == active_file_num {
                        match recovery_mode {
                            RecoveryMode::TolerateCorruptedTailRecords => {
                                warn!(
                                    "Incomplete batch in last log file {}, offset {}, \
                                     truncate it in TolerateCorruptedTailRecords recovery mode.",
                                    current_read_file,
                                    offset
                                );
                                let mut pipe_log = self.pipe_log.write().unwrap();
                                pipe_log.truncate_active_log(offset).unwrap();
                                break;
                            }
                            RecoveryMode::AbsoluteConsistency => {
                                panic!(
                                    "Incomplete batch in last log file {}, offset {}, \
                                     panic in AbsoluteConsistency recovery mode.",
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

        Ok(())
    }

    // Rewrite inactive region's entries and key/value pairs, so the old log can be dropped.
    pub fn rewrite_inactive(&mut self) {
        let active_file_num = {
            let pipe_log = self.pipe_log.read().unwrap();
            pipe_log.active_file_num()
        };

        let mut mem_entries = self.mem_entries[self.next_rewrite_slot].write().unwrap();
        for entries in mem_entries.values_mut() {
            let max_file_num = match entries.max_file_num() {
                Some(file_num) => file_num,
                None => continue,
            };
            let count = entries.entry_queue.len();
            if count > 0 && count <= 50 && max_file_num + 8 < active_file_num {
                // Rewrite entries
                let mut ents = Vec::with_capacity(count);
                entries.fetch_all(&mut ents);
                let mut log_batch = LogBatch::default();
                log_batch.add_entries(entries.region_id, ents.clone());

                let mut kvs = vec![];
                entries.fetch_all_kvs(&mut kvs);
                for kv in &kvs {
                    log_batch.add_kv(entries.region_id, &kv.0, &kv.1);
                }
                {
                    let mut pipe_log = self.pipe_log.write().unwrap();
                    match pipe_log.append_log_batch(&log_batch, false) {
                        Ok(file_num) => if file_num > 0 {
                            entries.append(ents, file_num);
                            for kv in kvs.drain(..) {
                                entries.add_kv(kv.0, kv.1, file_num);
                            }
                        },
                        Err(e) => panic!("Rewrite inactive region entries failed. error {:?}", e),
                    }
                }
            }
        }

        self.next_rewrite_slot += 1;
        self.next_rewrite_slot %= SLOTS_COUNT;
    }

    pub fn purge_expired_file(&mut self) -> Result<()> {
        let mut min_file_num = u64::MAX;
        for mem_entries in &mut self.mem_entries {
            let mem_entries = mem_entries.read().unwrap();
            let file_num = mem_entries.values().fold(u64::MAX, |min, x| {
                cmp::min(min, x.min_file_num().map_or(u64::MAX, |num| num))
            });
            if file_num < min_file_num {
                min_file_num = file_num;
            }
        }

        let mut pipe_log = self.pipe_log.write().unwrap();
        pipe_log.purge_to(min_file_num)
    }

    pub fn compact_to(&mut self, region_id: u64, index: u64) {
        let mut mem_entries = self.mem_entries[region_id as usize % SLOTS_COUNT]
            .write()
            .unwrap();
        if let Some(cache) = mem_entries.get_mut(&region_id) {
            cache.compact_to(index);
        }
    }

    pub fn append_log_batch(&mut self, log_batch: LogBatch, sync: bool) -> Result<()> {
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

    fn post_append_to_file(&mut self, log_batch: LogBatch, file_num: u64) {
        if file_num == 0 {
            return;
        }
        self.apply_to_cache(log_batch, file_num);
    }

    fn apply_to_cache(&mut self, mut log_batch: LogBatch, file_num: u64) {
        for item in log_batch.items.drain(..) {
            match item.item_type {
                LogItemType::Entries => {
                    let entries_to_add = item.entries.unwrap();
                    let region_id = entries_to_add.region_id;
                    let mut mem_entries = self.mem_entries[region_id as usize % SLOTS_COUNT]
                        .write()
                        .unwrap();
                    let mem_queue = mem_entries
                        .entry(region_id)
                        .or_insert_with(|| MemEntries::new(region_id));
                    mem_queue.append(entries_to_add.entries, file_num);
                }
                LogItemType::CMD => {
                    let command = item.command.unwrap();
                    match command {
                        Command::Clean { region_id } => {
                            let mut mem_entries = self.mem_entries
                                [region_id as usize % SLOTS_COUNT]
                                .write()
                                .unwrap();
                            mem_entries.remove(&region_id);
                        }
                    }
                }
                LogItemType::KV => {
                    let kv = item.kv.unwrap();
                    let mut mem_entries = self.mem_entries[kv.region_id as usize % SLOTS_COUNT]
                        .write()
                        .unwrap();
                    let mut mem_queue = mem_entries
                        .entry(kv.region_id)
                        .or_insert_with(|| MemEntries::new(kv.region_id));
                    mem_queue.add_kv(kv.key, kv.value, file_num);
                }
            }
        }
    }
}
