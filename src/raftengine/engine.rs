
use std::u64;
use std::cmp;
use std::io::BufRead;

use util::collections::HashMap;

use super::Result;
use super::Error;
use super::mem_entries::MemEntries;
use super::pipe_log::{PipeLog, FILE_MAGIC_HEADER};
use super::log_batch::{Command, LogBatch, LogItemType};

#[derive(Clone, Copy)]
enum RecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
}

struct MultiRaftEngine {
    // Raft log directory.
    pub dir: String,

    // region_id -> MemEntries.
    pub mem_entries: HashMap<u64, MemEntries>,

    // Persistent entries.
    pub pipe_log: PipeLog,
}

impl MultiRaftEngine {
    pub fn new(dir: String, recovery_mode: RecoveryMode) -> MultiRaftEngine {
        let pip_log =
            PipeLog::open(&dir).unwrap_or_else(|e| panic!("Open raft log failed, error: {:?}", e));
        let mut engine = MultiRaftEngine {
            dir: dir,
            mem_entries: HashMap::default(),
            pipe_log: pip_log,
        };
        engine
            .recover(recovery_mode)
            .unwrap_or_else(|e| panic!("Recover raft log failed, error: {:?}", e));
        engine
    }

    // recover from disk.
    fn recover(&mut self, recovery_mode: RecoveryMode) -> Result<()> {
        let mut current_read_file = self.pipe_log.first_file_num();
        let active_file_num = self.pipe_log.active_file_num();
        loop {
            if current_read_file > active_file_num {
                break;
            }

            // Read a file
            let content = self.pipe_log
                .read_next_file()
                .unwrap_or_else(|e| {
                    panic!(
                        "Read content of file {} failed, error {:?}",
                        current_read_file,
                        e
                    )
                })
                .unwrap_or_else(|| panic!("Expect has content, but get None"));

            // Verify file header
            let mut buf = content.as_slice();
            if buf.len() < FILE_MAGIC_HEADER.len() || !buf.starts_with(FILE_MAGIC_HEADER) {
                panic!("Raft log file {} is corrupted.", current_read_file);
            }
            buf.consume(FILE_MAGIC_HEADER.len());

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
                                self.pipe_log.truncate_active_log(offset).unwrap();
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

    // Rewrite inactive region's entries, so the old log can be dropped.
    pub fn rewrite_inactive(&mut self) {
        let active_file_num = self.pipe_log.active_file_num();
        for entries in self.mem_entries.values_mut() {
            let max_file_num = match entries.max_file_num() {
                Some(file_num) => file_num,
                None => continue,
            };
            let count = entries.entry_queue.len();
            if count > 0 && count <= 50 && max_file_num + 8 < active_file_num {
                // Todo: zero copy
                let mut ents = Vec::with_capacity(count);
                entries.fetch_all(&mut ents);
                let mut log_batch = LogBatch::default();
                log_batch.add_entries(entries.region_id, ents.clone());
                match self.pipe_log.append_log_batch(&log_batch, false) {
                    Ok(file_num) => if file_num > 0 {
                        entries.append(ents, file_num);
                    },
                    Err(e) => panic!("Rewrite inactive region entries failed. error {:?}", e),
                }
            }
        }
    }

    pub fn purge_expired_file(&mut self) -> Result<()> {
        let min_file_num = self.mem_entries.values().fold(u64::MAX, |min, x| {
            cmp::min(min, x.min_file_num().map_or(u64::MAX, |num| num))
        });

        self.pipe_log.purge_to(min_file_num)
    }

    pub fn compact_to(&mut self, region_id: u64, index: u64) {
        if let Some(cache) = self.mem_entries.get_mut(&region_id) {
            cache.compact_to(index);
        }
    }

    pub fn append_log_batch(&mut self, log_batch: LogBatch, sync: bool) -> Result<()> {
        let write_res = self.pipe_log.append_log_batch(&log_batch, sync);
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
                    let mem_queue = self.mem_entries
                        .entry(entries_to_add.region_id)
                        .or_insert_with(|| MemEntries::new(entries_to_add.region_id));
                    mem_queue.append(entries_to_add.entries, file_num);
                }
                LogItemType::CMD => {
                    let command = item.command.unwrap();
                    match command {
                        Command::Clean { region_id } => {
                            self.mem_entries.remove(&region_id);
                        }
                    }
                }
            }
        }
    }
}
