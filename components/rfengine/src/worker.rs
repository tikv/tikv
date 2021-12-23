// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    io::Write,
    path::Path,
    path::PathBuf,
    sync::mpsc::Receiver,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use slog_global::*;

use crate::*;

pub(crate) struct RotateTask {
    pub(crate) epoch_id: u32,
    pub(crate) states: BTreeMap<StateKey, Bytes>,
}

#[derive(PartialEq, PartialOrd, Eq, Ord, Clone)]
pub(crate) struct StateKey {
    pub(crate) region_id: u64,
    pub(crate) key: Bytes,
}

impl StateKey {
    pub(crate) fn new(region_id: u64, key: &[u8]) -> Self {
        Self {
            region_id,
            key: Bytes::copy_from_slice(key),
        }
    }
}

pub(crate) struct Worker {
    dir: PathBuf,
    epoches: Vec<Epoch>,
    truncated_idx: HashMap<u64, u64>,
    buf: BytesMut,
    task_rx: Receiver<Task>,
}

impl Worker {
    pub(crate) fn new(dir: PathBuf, epoches: Vec<Epoch>, task_rx: Receiver<Task>) -> Self {
        Self {
            dir,
            epoches,
            truncated_idx: HashMap::new(),
            buf: BytesMut::new(),
            task_rx,
        }
    }

    pub(crate) fn run(&mut self) {
        loop {
            let res = self.task_rx.recv();
            if res.is_err() {
                return;
            }
            let task = res.unwrap();
            match task {
                Task::Rotate { epoch_id, states } => {
                    self.handle_rotate_task(epoch_id, &states);
                }
                Task::Truncate { region_id, index } => {
                    self.handle_truncate_task(region_id, index);
                }
                Task::Close => return,
            }
        }
    }

    fn handle_rotate_task(&mut self, epoch_id: u32, states: &BTreeMap<StateKey, Bytes>) {
        let mut epoch = Epoch::new(epoch_id);
        epoch.has_wal_file = true;
        if let Err(err) = self.write_states(epoch_id, states) {
            error!("failed to write states {:?}", err);
        } else {
            epoch.has_state_file = true;
        }
        self.epoches.push(epoch);
        if self.epoches.len() >= 2 {
            let idx = self.epoches.len() - 2;
            let compact_id = self.epoches[idx].id;
            if let Err(err) = self.compact(idx) {
                error!("failed to compact epoch {} {:?}", compact_id, err);
            }
        }
    }

    fn write_states(&mut self, epoch_id: u32, states: &BTreeMap<StateKey, Bytes>) -> Result<()> {
        self.buf.truncate(0);
        for (k, v) in states {
            self.buf.put_u64_le(k.region_id);
            self.buf.put_u16_le(k.key.len() as u16);
            self.buf.extend_from_slice(k.key.chunk());
            self.buf.put_u32_le(v.len() as u32);
            self.buf.extend_from_slice(v.chunk());
        }
        let checksum = crc32c::crc32c(self.buf.chunk());
        self.buf.put_u32_le(checksum);
        let filename = states_file_name(&self.dir, epoch_id);
        let mut file = fs::File::create(filename)?;
        file.write_all(self.buf.chunk())?;
        file.sync_all()?;
        if epoch_id == 1 {
            return Ok(());
        }
        let old_epoch_state_file = states_file_name(&self.dir, epoch_id - 1);
        fs::remove_file(old_epoch_state_file)?;
        Ok(())
    }

    fn compact(&mut self, epoch_idx: usize) -> Result<()> {
        let epoch_id = self.epoches[epoch_idx].id;
        let mut entries_map = HashMap::new();
        let mut it = WALIterator::new(self.dir.clone(), epoch_id);
        it.iterate(|tp, entry| {
            if tp != TYPE_RAFT_LOG {
                return;
            }
            let op = parse_log(entry);
            if let Some(truncated_idx) = self.truncated_idx.get(&op.region_id) {
                if *truncated_idx > op.index {
                    return;
                }
            }
            let entries = get_region_raft_logs(&mut entries_map, op.region_id);
            entries.append(op);
        })?;
        info!(
            "epoch {} compact wal file generated {} files",
            epoch_id,
            entries_map.len()
        );
        for (region_id, entries) in entries_map {
            self.write_raft_log_file(epoch_idx, region_id, &entries)?;
        }
        let wal_filename = wal_file_name(&self.dir, epoch_id);
        let recycle_filename = recycle_file_name(&self.dir, epoch_id);
        fs::rename(wal_filename, recycle_filename)?;
        Ok(())
    }

    fn write_raft_log_file(
        &mut self,
        epoch_idx: usize,
        region_id: u64,
        entries: &RegionRaftLogs,
    ) -> Result<()> {
        let epoch_id = self.epoches[epoch_idx].id;
        let filename = raft_log_file_name(&self.dir, epoch_id, region_id, entries.range);
        let mut file = fs::File::create(filename)?;
        self.buf.truncate(0);
        for op in &entries.raft_logs {
            self.buf.put_u32_le(8 + op.data.len() as u32);
            self.buf.put_u32_le(op.term);
            self.buf.put_i32_le(op.e_type);
            self.buf.extend_from_slice(op.data.chunk());
        }
        let checksum = crc32c::crc32c(self.buf.chunk());
        self.buf.put_u32_le(checksum);
        file.write_all(self.buf.chunk())?;
        file.sync_all()?;
        self.epoches[epoch_idx]
            .raft_log_files
            .lock()
            .unwrap()
            .insert(region_id, entries.range);
        Ok(())
    }

    fn handle_truncate_task(&mut self, region_id: u64, index: u64) {
        self.truncated_idx.insert(region_id, index);
        let mut removed_epoch_ids = HashSet::new();
        for ep in &mut self.epoches {
            let mut raft_log_files = ep.raft_log_files.lock().unwrap();
            if let Some(range) = raft_log_files.get(&region_id) {
                if range.end_index <= index {
                    let filename = raft_log_file_name(&self.dir, ep.id, region_id, *range);
                    if let Err(err) = fs::remove_file(filename.clone()) {
                        error!("failed to remove rlog file {:?}, {:?}", filename, err);
                    } else {
                        info!(
                            "remove rlog file {}",
                            filename.file_name().unwrap().to_string_lossy()
                        );
                    }
                    raft_log_files.remove(&region_id);
                    if raft_log_files.len() == 0 {
                        removed_epoch_ids.insert(ep.id);
                    }
                } else {
                    info!(
                        "rlog file not deletable endIdx {}, truncateIdx {}",
                        range.end_index, index
                    );
                }
            }
        }
        if removed_epoch_ids.len() == 0 {
            return;
        }
        self.epoches.retain(|x| !removed_epoch_ids.contains(&x.id));
    }
}

pub(crate) fn raft_log_file_name(
    dir: &PathBuf,
    epoch_id: u32,
    region_id: u64,
    raft_log_range: RaftLogRange,
) -> PathBuf {
    dir.join(format!(
        "{:08x}_{:016x}_{:016x}_{:016x}.rlog",
        epoch_id, region_id, raft_log_range.start_index, raft_log_range.end_index
    ))
}

pub(crate) fn states_file_name(dir: &PathBuf, epoch_id: u32) -> PathBuf {
    dir.join(format!("{:08x}.states", epoch_id))
}

pub(crate) fn wal_file_name(dir: &Path, epoch_id: u32) -> PathBuf {
    dir.join(format!("{:08x}.wal", epoch_id))
}

pub(crate) fn recycle_file_name(dir: &PathBuf, epoch_id: u32) -> PathBuf {
    dir.join("recycle").join(format!("{:08x}.wal", epoch_id))
}

pub(crate) enum Task {
    Rotate {
        epoch_id: u32,
        states: BTreeMap<StateKey, Bytes>,
    },
    Truncate {
        region_id: u64,
        index: u64,
    },
    Close,
}
