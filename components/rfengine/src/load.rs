// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes};
use slog_global::info;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use crate::*;

const EPOCH_LEN: usize = 8;
const REGION_ID_LEN: usize = 16;
const START_INDEX_LEN: usize = 16;
const END_INDEX_LEN: usize = 16;
const REGION_ID_OFFSET: usize = EPOCH_LEN + 1;
const START_INDEX_OFFSET: usize = REGION_ID_OFFSET + 1 + REGION_ID_LEN;
const END_INDEX_OFFSET: usize = START_INDEX_OFFSET + 1 + START_INDEX_LEN;

pub(crate) struct Epoch {
    pub(crate) id: u32,
    pub(crate) has_state_file: bool,
    pub(crate) has_wal_file: bool,
    pub(crate) raft_log_files: Mutex<HashMap<u64, RaftLogRange>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RaftLogRange {
    pub(crate) start_index: u64,
    pub(crate) end_index: u64,
}

pub(crate) fn get_epoch(epoches: &mut HashMap<u32, Epoch>, epoch_id: u32) -> &mut Epoch {
    if epoches.contains_key(&epoch_id) {
        epoches.get_mut(&epoch_id).unwrap()
    } else {
        let ep = Epoch::new(epoch_id);
        epoches.insert(epoch_id, ep);
        epoches.get_mut(&epoch_id).unwrap()
    }
}

impl Epoch {
    pub(crate) fn new(id: u32) -> Self {
        Self {
            id,
            has_state_file: false,
            has_wal_file: false,
            raft_log_files: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn add_file(&mut self, filename: PathBuf) -> Result<()> {
        let extention = filename.extension().unwrap();
        if extention == "wal" {
            self.has_wal_file = true;
        } else if extention == "state" {
            self.has_state_file = true;
        } else if extention == "rlog" {
            let filename_str = filename.file_name().unwrap().to_str().unwrap();
            let region_id_buf = &filename_str[REGION_ID_OFFSET..REGION_ID_OFFSET + REGION_ID_LEN];
            let region_id = u64::from_str_radix(region_id_buf, 16)?;
            let start_index_buf =
                &filename_str[START_INDEX_OFFSET..START_INDEX_OFFSET + START_INDEX_LEN];
            let start_index = u64::from_str_radix(start_index_buf, 16)?;
            let end_index_buf = &filename_str[END_INDEX_OFFSET..END_INDEX_OFFSET + END_INDEX_LEN];
            let end_index = u64::from_str_radix(end_index_buf, 16)?;
            self.raft_log_files.lock().unwrap().insert(
                region_id,
                RaftLogRange {
                    start_index,
                    end_index,
                },
            );
        }
        Ok(())
    }
}

pub(crate) fn read_epoches(dir: &Path) -> Result<Vec<Epoch>> {
    let mut epoch_map = HashMap::new();
    let recycle_path = dir.join(RECYCLE_DIR);
    let entries = fs::read_dir(dir)?;
    for e in entries {
        let entry = e?;
        let path = entry.path();
        if path.starts_with(&recycle_path) {
            continue;
        }
        let filename = path.file_name().unwrap().to_str().unwrap();
        let epoch_id = u32::from_str_radix(&filename[..8], 16)?;
        let ep = get_epoch(&mut epoch_map, epoch_id);
        ep.add_file(path)?;
    }
    let mut epoches = Vec::new();
    for (_, v) in epoch_map.drain() {
        epoches.push(v);
    }
    epoches.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(epoches)
}

impl RFEngine {
    pub(crate) fn load_epoch(&mut self, ep: &Epoch) -> Result<u64> {
        info!(
            "load epoch {}, rlog files {}, has_wal {}, has_state {}",
            ep.id,
            ep.raft_log_files.lock().unwrap().len(),
            ep.has_wal_file,
            ep.has_state_file
        );
        let mut wal_off: u64 = 0;
        if ep.has_wal_file {
            wal_off = self.load_wal_file(ep.id)?;
        } else {
            let raft_log_files = ep.raft_log_files.lock().unwrap();
            for (k, v) in raft_log_files.iter() {
                self.load_raft_log_file(ep.id, *k, *v)?;
            }
        }
        if ep.has_state_file {
            self.load_state_file(ep.id)?;
        }
        Ok(wal_off)
    }

    pub(crate) fn load_wal_file(&mut self, epoch_id: u32) -> Result<u64> {
        let mut it = WALIterator::new(self.dir.clone(), epoch_id);
        let mut states = self.states.write().unwrap();
        let mut entries_map = self.entries_map.write().unwrap();
        it.iterate(|tp, entry| match tp {
            TYPE_STATE => {
                let (region_id, key, val) = parse_state(entry);
                let key = StateKey::new(region_id, key);
                if val.len() > 0 {
                    states.insert(key, Bytes::copy_from_slice(val));
                } else {
                    states.remove(&key);
                }
            }
            TYPE_RAFT_LOG => {
                let log_op = parse_log(entry);
                let entries = get_region_raft_logs(&mut entries_map, log_op.region_id);
                entries.append(log_op);
            }
            TYPE_TRUNCATE => {
                let (region_id, index) = parse_truncate(entry);
                let entries = get_region_raft_logs(&mut entries_map, region_id);
                let empty = entries.truncate(index);
                if empty {
                    entries_map.remove(&region_id);
                }
            }
            _ => panic!("unknown state"),
        })?;
        Ok(it.offset)
    }

    pub(crate) fn load_state_file(&mut self, epoch_id: u32) -> Result<()> {
        let filename = states_file_name(&self.dir, epoch_id);
        let bin = fs::read(filename)?;
        let mut data = bin.as_slice();
        let mut states = self.states.write().unwrap();
        while data.len() > 0 {
            let region_id = LittleEndian::read_u64(data);
            data = &data[8..];
            let key_len = LittleEndian::read_u16(data) as usize;
            data = &data[2..];
            let key = &data[..key_len];
            data = &data[key_len..];
            let val_len = LittleEndian::read_u32(data) as usize;
            data = &data[4..];
            let val = &data[..val_len];
            data = &data[val_len..];
            let state_key = StateKey::new(region_id, key);
            states.insert(state_key, Bytes::copy_from_slice(val));
        }
        Ok(())
    }

    pub(crate) fn load_raft_log_file(
        &mut self,
        epoch_id: u32,
        region_id: u64,
        raft_log_range: RaftLogRange,
    ) -> Result<()> {
        let rlog_filename = raft_log_file_name(&self.dir, epoch_id, region_id, raft_log_range);
        let bin = read_checksum_file(&rlog_filename)?;
        let mut data = bin.as_slice();
        let mut index = raft_log_range.start_index;
        let mut entries_map = self.entries_map.write().unwrap();
        while data.len() > 0 {
            let length = LittleEndian::read_u32(data) as usize;
            data = &data[4..];
            let mut entry = &data[..length];
            data = &data[length..];
            let term = LittleEndian::read_u32(entry);
            entry = &entry[4..];
            let e_type = LittleEndian::read_i32(entry);
            entry = &entry[4..];
            let op = RaftLogOp {
                region_id,
                index,
                term,
                e_type,
                data: Bytes::copy_from_slice(entry),
            };
            let entries = get_region_raft_logs(&mut entries_map, region_id);
            entries.append(op);
            index += 1;
        }
        assert_eq!(index, raft_log_range.end_index);
        Ok(())
    }
}

fn read_checksum_file(filename: &PathBuf) -> Result<Vec<u8>> {
    let mut bin = fs::read(filename)?;
    let checksum_off = bin.len() - 4;
    let checksum_expect = LittleEndian::read_u32(&bin[checksum_off..]);
    let checksum_got = crc32c::crc32c(&bin[..checksum_off]);
    if checksum_got != checksum_expect {
        return Err(Error::Checksum);
    }
    bin.truncate(checksum_off);
    Ok(bin)
}

pub(crate) const TYPE_STATE: u32 = 1;
pub(crate) const TYPE_RAFT_LOG: u32 = 2;
pub(crate) const TYPE_TRUNCATE: u32 = 3;
