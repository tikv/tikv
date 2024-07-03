use std::sync::Arc;

use derive_more::Display;
use kvproto::brpb::{self, FileType};

use crate::{
    statistic::{CompactStatistic, LoadStatistic},
    storage::LogFileId,
    util,
};

#[derive(Debug, Clone)]
pub struct Input {
    pub id: LogFileId,
    crc64xor: u64,
    key_value_size: u64,
    num_of_entries: u64,
}

#[derive(Debug, Display, Clone)]
#[display(fmt = "compaction(region={},size={},cf={})", region_id, size, cf)]
pub struct Subcompaction {
    pub inputs: Vec<Input>,
    pub size: u64,
    pub region_id: u64,
    pub cf: &'static str,
    pub input_max_ts: u64,
    pub input_min_ts: u64,
    pub compact_from_ts: u64,
    pub compact_to_ts: u64,
    pub min_key: Arc<[u8]>,
    pub max_key: Arc<[u8]>,
    pub ty: FileType,
}

#[derive(Debug)]
pub struct SubcompactionResult {
    pub origin: Subcompaction,
    pub meta: brpb::LogFileSubcompactionMeta,

    pub expected_crc64: Option<u64>,
    pub expected_keys: u64,
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
