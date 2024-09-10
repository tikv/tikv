// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{collections::HashSet, ops::Deref, sync::Arc};

use derive_more::Display;
use kvproto::brpb::{self, FileType};

use self::collector::CollectSubcompactionConfig;
use crate::{
    statistic::{LoadStatistic, SubcompactStatistic},
    storage::{Epoch, LogFile, LogFileId},
    util,
};

pub const SST_OUT_REL: &str = "outputs";
pub const META_OUT_REL: &str = "metas";

#[derive(Debug, Clone)]
pub struct Input {
    pub id: LogFileId,
    pub compression: brpb::CompressionType,
    pub crc64xor: u64,
    pub key_value_size: u64,
    pub num_of_entries: u64,
}

/// The group key of collecting subcompactions.
#[derive(Hash, Debug, PartialEq, Eq, Clone, Copy, Display)]
#[display(
    fmt = "key(r={},{},{:?},m?={},t={})",
    region_id,
    cf,
    ty,
    is_meta,
    table_id
)]
pub struct SubcompactionCollectKey {
    pub cf: &'static str,
    pub region_id: u64,
    pub ty: FileType,
    pub is_meta: bool,
    pub table_id: i64,
}

/// A subcompaction.
#[derive(Debug, Display, Clone)]
#[display(fmt = "compaction({},sz={})", subc_key, size)]
pub struct Subcompaction {
    pub inputs: Vec<Input>,
    pub size: u64,
    pub subc_key: SubcompactionCollectKey,

    pub input_max_ts: u64,
    pub input_min_ts: u64,
    pub compact_from_ts: u64,
    pub compact_to_ts: u64,
    pub min_key: Arc<[u8]>,
    pub max_key: Arc<[u8]>,
    pub epoch_hints: Vec<EpochHint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EpochHint {
    pub start_key: Arc<[u8]>,
    pub end_key: Arc<[u8]>,
    pub region_epoch: Epoch,
}

// "Embed" the subcompaction collect key field here.
impl Deref for Subcompaction {
    type Target = SubcompactionCollectKey;

    fn deref(&self) -> &Self::Target {
        &self.subc_key
    }
}

#[derive(Debug, Clone)]
pub struct SubcompactionResult {
    /// The origin subcompaction.
    pub origin: Subcompaction,
    /// The serializable metadata of this subcompaction.
    pub meta: brpb::LogFileSubcompaction,

    /// The expected crc64 for the generated SSTs.
    pub expected_crc64: Option<u64>,
    /// The expected key count for the generated SSTs.
    pub expected_keys: u64,
    /// The expected logical data size for the generated SSTs.
    pub expected_size: u64,

    pub load_stat: LoadStatistic,
    pub compact_stat: SubcompactStatistic,
}

impl SubcompactionResult {
    pub fn of(origin: Subcompaction) -> Self {
        Self {
            meta: Default::default(),
            expected_crc64: Some(0),
            expected_keys: Default::default(),
            expected_size: Default::default(),
            load_stat: Default::default(),
            compact_stat: Default::default(),
            origin,
        }
    }
}

#[derive(Debug)]
struct UnformedSubcompaction {
    size: u64,
    inputs: Vec<Input>,
    min_ts: u64,
    max_ts: u64,
    min_key: Arc<[u8]>,
    max_key: Arc<[u8]>,
    epoch_hints: HashSet<EpochHint>,
}

impl UnformedSubcompaction {
    /// create the initial state by a singleton file.
    fn by_file(file: &LogFile) -> Self {
        let epoch_hints = file.epoch_hints().collect();
        UnformedSubcompaction {
            size: file.file_real_size,
            inputs: vec![to_input(file)],
            min_ts: file.min_ts,
            max_ts: file.max_ts,
            min_key: file.min_key.clone(),
            max_key: file.max_key.clone(),
            epoch_hints,
        }
    }

    /// compose a real Subcompaction by the current state.
    fn compose(
        self,
        key: &SubcompactionCollectKey,
        cfg: &CollectSubcompactionConfig,
    ) -> Subcompaction {
        Subcompaction {
            inputs: self.inputs,
            size: self.size,
            input_min_ts: self.min_ts,
            input_max_ts: self.max_ts,
            min_key: self.min_key.clone(),
            max_key: self.max_key.clone(),
            compact_from_ts: cfg.compact_from_ts,
            compact_to_ts: cfg.compact_to_ts,
            subc_key: *key,
            epoch_hints: self.epoch_hints.into_iter().collect(),
        }
    }

    /// add a new file to the state.
    fn add_file(&mut self, file: LogFile) {
        self.epoch_hints.extend(file.epoch_hints());

        self.inputs.push(to_input(&file));
        self.size += file.file_real_size;
        self.min_ts = self.min_ts.min(file.min_ts);
        self.max_ts = self.max_ts.max(file.max_ts);
        if self.max_key < file.max_key {
            self.max_key = file.max_key;
        }
        if self.min_key > file.min_key {
            self.min_key = file.min_key;
        }
    }
}

impl SubcompactionCollectKey {
    /// extract the keys from the meta file.
    fn by_file(file: &LogFile) -> Self {
        SubcompactionCollectKey {
            is_meta: file.is_meta,
            region_id: file.region_id,
            cf: file.cf,
            ty: file.ty,
            table_id: file.table_id,
        }
    }
}

/// Convert a log file to an input of compaction.
fn to_input(file: &LogFile) -> Input {
    Input {
        id: file.id.clone(),
        compression: file.compression,
        crc64xor: file.crc64xor,
        key_value_size: file.hacky_key_value_size(),
        num_of_entries: file.number_of_entries as u64,
    }
}

pub mod collector;
pub mod exec;
pub mod meta;
