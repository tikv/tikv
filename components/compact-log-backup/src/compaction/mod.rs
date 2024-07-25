// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{ops::Deref, sync::Arc};

use derive_more::Display;
use kvproto::brpb::{self, FileType};

use crate::{
    statistic::{CompactStatistic, LoadStatistic},
    storage::LogFileId,
    util,
};

pub const SST_OUT_REL: &'static str = "outputs";
pub const META_OUT_REL: &'static str = "metas";

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
    pub compact_stat: CompactStatistic,
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

pub mod collector;
pub mod exec;
pub mod meta;
