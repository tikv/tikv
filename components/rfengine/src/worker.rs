// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::{
    collections::{HashMap, HashSet},
    fs, mem,
    path::Path,
    path::PathBuf,
    sync::mpsc::Receiver,
};

use bytes::{Buf, BufMut, Bytes};
use file_system::{DirectWriter, IORateLimitMode, IOType};
use slog_global::*;

use crate::*;

pub(crate) struct Worker {
    dir: PathBuf,
    epoches: Vec<Epoch>,
    region_data: Arc<dashmap::DashMap<u64, RwLock<RegionData>>>,
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
        region_data: Arc<dashmap::DashMap<u64, RwLock<RegionData>>>,
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
            region_data,
            truncated_idx: HashMap::new(),
            writer,
            task_rx,
            all_states,
            buf: vec![],
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
                Task::Rotate { epoch_id } => {
                    self.handle_rotate_task(epoch_id);
                }
                Task::Truncate { region_id } => {
                    self.handle_truncate_task(region_id);
                }
                Task::Close => return,
            }
        }
    }

    fn handle_rotate_task(&mut self, epoch_id: u32) {
        let mut epoch = Epoch::new(epoch_id);
        epoch.has_wal_file = true;
        self.epoches.push(epoch);
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
        for (id, region_states) in &self.all_states {
            for (k, v) in region_states {
                self.buf.put_u64_le(*id);
                self.buf.put_u16_le(k.len() as u16);
                self.buf.extend_from_slice(k.chunk());
                self.buf.put_u32_le(v.len() as u32);
                self.buf.extend_from_slice(v.chunk());
            }
        }
        let crc32 = crc32c::crc32c(&self.buf);
        self.buf.put_u32_le(crc32);
        let filename = states_file_name(&self.dir, epoch_id);
        self.writer.write_to_file(&self.buf, filename)?;
        info!("write state file for epoch {}", epoch_id);
        if epoch_id == 1 {
            return Ok(());
        }
        let old_epoch_state_file = states_file_name(&self.dir, epoch_id - 1);
        fs::remove_file(old_epoch_state_file)?;
        Ok(())
    }

    fn get_region_states(&mut self, region_id: u64) -> &mut BTreeMap<Bytes, Bytes> {
        if !self.all_states.contains_key(&region_id) {
            self.all_states.insert(region_id, BTreeMap::new());
        }
        self.all_states.get_mut(&region_id).unwrap()
    }

    fn compact(&mut self, epoch_idx: usize) -> Result<()> {
        let epoch_id = self.epoches[epoch_idx].id;
        let mut batch = WriteBatch::default();
        let mut it = WALIterator::new(self.dir.clone(), epoch_id);
        it.iterate(|mut region_data| {
            if let Some(truncated_idx) = self.truncated_idx.get(&region_data.region_id) {
                region_data.truncate(*truncated_idx);
            }
            batch.merge_region(&region_data);
        })?;
        let mut generated_files = 0;
        for (_, mut region_data) in batch.regions {
            let new_states = mem::replace(&mut region_data.states, BTreeMap::default());
            let region_states = self.get_region_states(region_data.region_id);
            for (k, v) in &new_states {
                if v.len() == 0 {
                    region_states.remove(k);
                } else {
                    region_states.insert(k.clone(), v.clone());
                }
            }
            if region_data.raft_logs.len() > 0 {
                self.write_raft_log_file(epoch_idx, &region_data)?;
                generated_files += 1;
            }
        }
        info!(
            "epoch {} compact wal file generated {} files",
            epoch_id, generated_files,
        );
        self.write_all_states(epoch_id)?;
        let wal_filename = wal_file_name(&self.dir, epoch_id);
        let recycle_filename = recycle_file_name(&self.dir, epoch_id);
        fs::rename(wal_filename, recycle_filename)?;
        Ok(())
    }

    fn write_raft_log_file(&mut self, epoch_idx: usize, region_data: &RegionData) -> Result<()> {
        let epoch_id = self.epoches[epoch_idx].id;
        let filename = raft_log_file_name(
            &self.dir,
            epoch_id,
            region_data.region_id,
            region_data.range,
        );
        self.buf.truncate(0);
        region_data.encode_to(&mut self.buf);
        let checksum = crc32c::crc32c(&self.buf);
        self.buf.put_u32_le(checksum);
        self.writer.write_to_file(&self.buf, filename)?;
        self.epoches[epoch_idx]
            .raft_log_files
            .lock()
            .unwrap()
            .insert(region_data.region_id, region_data.range);
        Ok(())
    }

    fn handle_truncate_task(&mut self, region_id: u64) {
        let map_ref = self.region_data.get(&region_id);
        if map_ref.is_none() {
            return;
        }
        let map_ref = map_ref.unwrap();
        let mut region_data = map_ref.write().unwrap();
        if !region_data.need_truncate() {
            return;
        }
        let index = region_data.truncated_idx;
        region_data.truncate(index);
        drop(region_data);
        self.truncated_idx.insert(region_id, index);
        let mut removed_epoch_ids = HashSet::new();
        let mut remove_cnt = 0;
        let mut retain_cnt = 0;
        for ep in &mut self.epoches {
            let mut raft_log_files = ep.raft_log_files.lock().unwrap();
            if let Some(range) = raft_log_files.get(&region_id) {
                if range.end_index <= index {
                    let filename = raft_log_file_name(&self.dir, ep.id, region_id, *range);
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
        info!("region {} truncate raft log to {}, remove {} files, retain {} files",
            region_id, index, remove_cnt, retain_cnt);
        if removed_epoch_ids.len() == 0 {
            return;
        }
        info!("remove epoches {:?}", &removed_epoch_ids);
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
    Rotate { epoch_id: u32 },
    Truncate { region_id: u64 },
    Close,
}
