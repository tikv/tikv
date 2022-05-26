// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{mpsc::Receiver, Arc},
};

use bytes::{Buf, BufMut, Bytes};
use file_system::{DirectWriter, IORateLimitMode, IOType};
use slog_global::*;
use tikv_util::time::Instant;

use crate::{log_batch::RaftLogBlock, write_batch::RegionBatch, *};

pub(crate) struct Worker {
    dir: PathBuf,
    epoches: Vec<Epoch>,
    truncated_idx: HashMap<u64, u64>,
    writer: DirectWriter,
    task_rx: Receiver<Task>,
    all_states: HashMap<u64, BTreeMap<Bytes, Bytes>>,
    buf: Vec<u8>,
}

impl Worker {
    pub(crate) fn new(
        dir: PathBuf,
        epoches: Vec<Epoch>,
        task_rx: Receiver<Task>,
        all_states: HashMap<u64, BTreeMap<Bytes, Bytes>>,
    ) -> Self {
        let rate_limiter = Arc::new(file_system::IORateLimiter::new(
            IORateLimitMode::WriteOnly,
            true,
            false,
        ));
        rate_limiter.set_io_rate_limit(128 * 1024 * 1024);
        let writer = DirectWriter::new(rate_limiter, IOType::Compaction);
        Self {
            dir,
            epoches,
            truncated_idx: HashMap::new(),
            writer,
            task_rx,
            all_states,
            buf: vec![],
        }
    }

    /// Reruns compaction tasks if needed.
    /// It's possible a rotation task is interrupted due to crash, we should rerun it.
    fn rerun_compact(&mut self) {
        // Don't compact the last epoch because it's highly probable to truncate raft logs in it soon.
        if self.epoches.len() < 2 {
            return;
        }
        for i in 0..self.epoches.len() - 1 {
            if self.epoches[i].has_wal_file {
                if let Err(err) = self.compact(i) {
                    error!("failed to compact epoch {} {:?}", self.epoches[i].id, err);
                }
            }
        }
    }

    pub(crate) fn run(&mut self) {
        self.rerun_compact();
        while let Ok(task) = self.task_rx.recv() {
            match task {
                Task::Rotate { epoch_id } => {
                    self.handle_rotate_task(epoch_id);
                }
                Task::Truncate {
                    region_id,
                    truncated_index,
                    truncated,
                } => {
                    self.handle_truncate_task(region_id, truncated_index, truncated);
                }
                Task::Close => return,
            }
        }
    }

    fn handle_rotate_task(&mut self, epoch_id: u32) {
        let mut epoch = Epoch::new(epoch_id);
        epoch.has_wal_file = true;
        self.epoches.push(epoch);
        // Don't compact the last epoch because it's highly probable to truncate raft logs in it soon.
        if self.epoches.len() >= 2 {
            let idx = self.epoches.len() - 2;
            let compact_id = self.epoches[idx].id;
            if let Err(err) = self.compact(idx) {
                error!("failed to compact epoch {} {:?}", compact_id, err);
            }
        }
    }

    fn write_all_states(&mut self, epoch_id: u32) -> Result<()> {
        self.buf.truncate(0);
        let header = StatesHeader::default();
        header.encode_to(&mut self.buf);
        for (id, region_states) in &self.all_states {
            for (k, v) in region_states {
                self.buf.put_u64_le(*id);
                self.buf.put_u16_le(k.len() as u16);
                self.buf.extend_from_slice(k.chunk());
                self.buf.put_u32_le(v.len() as u32);
                self.buf.extend_from_slice(v.chunk());
            }
        }
        let crc32 = crc32fast::hash(&self.buf[StatesHeader::len()..]);
        self.buf.put_u32_le(crc32);
        let filename = states_file_name(&self.dir, epoch_id);
        self.writer.write_to_file(&self.buf, &filename)?;
        info!("write state file for epoch {}", epoch_id);
        if epoch_id == 1 {
            return Ok(());
        }
        let old_epoch_state_file = states_file_name(&self.dir, epoch_id - 1);
        fs::remove_file(old_epoch_state_file)?;
        Ok(())
    }

    fn get_region_states(&mut self, region_id: u64) -> &mut BTreeMap<Bytes, Bytes> {
        self.all_states
            .entry(region_id)
            .or_insert_with(|| BTreeMap::new())
    }

    fn compact(&mut self, epoch_idx: usize) -> Result<()> {
        let epoch_id = self.epoches[epoch_idx].id;
        let mut batch = WriteBatch::default();
        let mut it = WALIterator::new(self.dir.clone(), epoch_id);
        it.iterate(|mut region_batch| {
            if let Some(truncated_idx) = self.truncated_idx.get(&region_batch.region_id) {
                region_batch.truncate(*truncated_idx);
            }
            batch.merge_region(region_batch);
        })?;
        let mut generated_files = 0;
        for (_, mut region_batch) in batch.regions {
            let new_states = std::mem::take(&mut region_batch.states);
            let region_states = self.get_region_states(region_batch.region_id);
            for (k, v) in &new_states {
                if v.is_empty() {
                    region_states.remove(k);
                } else {
                    region_states.insert(k.clone(), v.clone());
                }
            }
            if !region_batch.raft_logs.is_empty() {
                self.write_raft_log_file(epoch_idx, region_batch)?;
                generated_files += 1;
            }
        }
        info!(
            "epoch {} compact wal file generated {} files",
            epoch_id, generated_files,
        );
        self.write_all_states(epoch_id)?;
        let _ = file_system::sync_dir(self.dir.as_path());

        if let Err(e) = self.recycle_wal_file(epoch_id) {
            warn!("failed to recycle wal file"; "epoch_id" => epoch_id, "err" => %e);
        }
        Ok(())
    }

    fn recycle_wal_file(&self, epoch_id: u32) -> Result<()> {
        let wal_filename = wal_file_name(self.dir.as_path(), epoch_id);
        let recycled_count = self.dir.read_dir()?.count();
        if recycled_count >= 3 {
            fs::remove_file(wal_filename)?;
            let _ = file_system::sync_dir(self.dir.as_path());
            return Ok(());
        }

        let wal_filename = wal_file_name(self.dir.as_path(), epoch_id);
        let recycle_filename = recycle_file_name(self.dir.as_path(), epoch_id);
        fs::rename(wal_filename.as_path(), recycle_filename).map_err(|e| {
            let _ = fs::remove_file(wal_filename);
            e
        })?;
        let _ = file_system::sync_dir(self.dir.as_path());
        let _ = file_system::sync_dir(self.dir.join(RECYCLE_DIR).as_path());
        Ok(())
    }

    fn write_raft_log_file(&mut self, epoch_idx: usize, region_batch: RegionBatch) -> Result<()> {
        let epoch_id = self.epoches[epoch_idx].id;
        let first = region_batch.raft_logs.front().unwrap().index;
        let end = region_batch.raft_logs.back().unwrap().index + 1;
        let filename = raft_log_file_name(
            self.dir.as_path(),
            epoch_id,
            region_batch.region_id,
            first,
            end,
        );
        self.buf.truncate(0);
        let header = RlogHeader::default();
        header.encode_to(&mut self.buf);
        region_batch.encode_to(&mut self.buf);
        let checksum = crc32fast::hash(&self.buf[RlogHeader::len()..]);
        self.buf.put_u32_le(checksum);
        self.writer.write_to_file(&self.buf, &filename)?;
        self.epoches[epoch_idx]
            .raft_log_files
            .lock()
            .unwrap()
            .insert(region_batch.region_id, (first, end));
        Ok(())
    }

    fn handle_truncate_task(
        &mut self,
        region_id: u64,
        truncated_index: u64,
        truncated: Vec<RaftLogBlock>,
    ) {
        let timer = Instant::now_coarse();
        drop(truncated);
        ENGINE_TRUNCATE_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        self.truncated_idx.insert(region_id, truncated_index);
        let mut removed_epoch_ids = HashSet::new();
        let mut remove_cnt = 0;
        let mut retain_cnt = 0;
        for ep in &mut self.epoches {
            let mut raft_log_files = ep.raft_log_files.lock().unwrap();
            if let Some((first, end)) = raft_log_files.get(&region_id) {
                if *end <= truncated_index {
                    let filename =
                        raft_log_file_name(self.dir.as_path(), ep.id, region_id, *first, *end);
                    if let Err(err) = fs::remove_file(filename.clone()) {
                        error!("failed to remove rlog file {:?}, {:?}", filename, err);
                    }
                    raft_log_files.remove(&region_id);
                    if raft_log_files.len() == 0 {
                        removed_epoch_ids.insert(ep.id);
                    }
                    remove_cnt += 1;
                } else {
                    retain_cnt += 1;
                }
            }
        }
        if let Err(e) = file_system::sync_dir(self.dir.as_path()) {
            error!("failed to sync directory: {}", e);
        }
        info!(
            "region {} truncate raft log to {}, remove {} files, retain {} files",
            region_id, truncated_index, remove_cnt, retain_cnt
        );
        if removed_epoch_ids.is_empty() {
            return;
        }
        info!("remove epoches {:?}", &removed_epoch_ids);
        self.epoches.retain(|x| !removed_epoch_ids.contains(&x.id));
    }
}

pub(crate) fn raft_log_file_name(
    dir: &Path,
    epoch_id: u32,
    region_id: u64,
    first: u64,
    end: u64,
) -> PathBuf {
    dir.join(format!(
        "{:08x}_{:016x}_{:016x}_{:016x}.rlog",
        epoch_id, region_id, first, end,
    ))
}

/// Magic Number of rlog files. It's picked by running
///    echo rfengine.rlog | sha1sum
/// and taking the leading 64 bits.
const RLOG_MAGIC_NUMBER: u64 = 0x50ed2c1e89d6aa91;

#[derive(Clone, Copy)]
#[repr(u64)]
enum RlogVersion {
    V1 = 1,
}

pub(crate) struct RlogHeader {
    version: RlogVersion,
}

impl Default for RlogHeader {
    fn default() -> Self {
        Self {
            version: RlogVersion::V1,
        }
    }
}

impl RlogHeader {
    pub(crate) const fn len() -> usize {
        16 // magic number + version
    }

    fn encode_to(&self, buf: &mut Vec<u8>) {
        buf.put_u64_le(RLOG_MAGIC_NUMBER);
        buf.put_u64_le(self.version as u64)
    }

    pub(crate) fn decode(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::len() {
            return Err(Error::Corruption("states header mismatch".to_owned()));
        }
        let magic_number = buf.get_u64_le();
        if magic_number != RLOG_MAGIC_NUMBER {
            return Err(Error::Corruption("states magic number mismatch".to_owned()));
        }
        let version = buf.get_u64_le();
        if version != StatesVersion::V1 as u64 {
            return Err(Error::Corruption("states version mismatch".to_owned()));
        }
        Ok(Self {
            version: RlogVersion::V1,
        })
    }
}

pub(crate) fn states_file_name(dir: &Path, epoch_id: u32) -> PathBuf {
    dir.join(format!("{:08x}.states", epoch_id))
}

/// Magic Number of states files. It's picked by running
///    echo rfengine.states | sha1sum
/// and taking the leading 64 bits.
const STATES_MAGIC_NUMBER: u64 = 0xde2d5fe79dea8188;

#[derive(Clone, Copy)]
#[repr(u64)]
enum StatesVersion {
    V1 = 1,
}

pub(crate) struct StatesHeader {
    version: StatesVersion,
}

impl Default for StatesHeader {
    fn default() -> Self {
        Self {
            version: StatesVersion::V1,
        }
    }
}

impl StatesHeader {
    pub(crate) const fn len() -> usize {
        16 // magic number + version
    }

    fn encode_to(&self, buf: &mut Vec<u8>) {
        buf.put_u64_le(STATES_MAGIC_NUMBER);
        buf.put_u64_le(self.version as u64)
    }

    pub(crate) fn decode(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::len() {
            return Err(Error::Corruption("states header mismatch".to_owned()));
        }
        let magic_number = buf.get_u64_le();
        if magic_number != STATES_MAGIC_NUMBER {
            return Err(Error::Corruption("states magic number mismatch".to_owned()));
        }
        let version = buf.get_u64_le();
        if version != StatesVersion::V1 as u64 {
            return Err(Error::Corruption("states version mismatch".to_owned()));
        }
        Ok(Self {
            version: StatesVersion::V1,
        })
    }
}

pub(crate) fn wal_file_name(dir: &Path, epoch_id: u32) -> PathBuf {
    dir.join(format!("{:08x}.wal", epoch_id))
}

pub(crate) fn recycle_file_name(dir: &Path, epoch_id: u32) -> PathBuf {
    dir.join("recycle").join(format!("{:08x}.wal", epoch_id))
}

pub(crate) enum Task {
    Rotate {
        epoch_id: u32,
    },
    Truncate {
        region_id: u64,
        truncated_index: u64,
        truncated: Vec<RaftLogBlock>,
    },
    Close,
}
