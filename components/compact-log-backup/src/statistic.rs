// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::time::Duration;

use derive_more::{Add, AddAssign};

#[derive(Default, Debug, Add, AddAssign, Clone)]
pub struct LoadMetaStatistic {
    pub meta_files_in: u64,
    pub physical_bytes_loaded: u64,
    pub physical_data_files_in: u64,
    pub logical_data_files_in: u64,
    pub load_file_duration: Duration,

    pub prefetch_task_emitted: u64,
    pub prefetch_task_finished: u64,
    pub error_during_downloading: u64,
}

#[derive(Default, Debug, Add, AddAssign, Clone)]
pub struct LoadStatistic {
    pub files_in: u64,
    pub keys_in: u64,
    pub physical_bytes_in: u64,
    pub logical_key_bytes_in: u64,
    pub logical_value_bytes_in: u64,
    pub error_during_downloading: u64,
}

#[derive(Default, Debug, Add, AddAssign, Clone)]
pub struct CompactStatistic {
    pub keys_out: u64,
    pub physical_bytes_out: u64,
    pub logical_key_bytes_out: u64,
    pub logical_value_bytes_out: u64,

    pub write_sst_duration: Duration,
    pub load_duration: Duration,
    pub sort_duration: Duration,
    pub save_duration: Duration,

    pub empty_generation: u64,
}

#[derive(Default, Debug, Add, AddAssign, Clone)]
pub struct CollectCompactionStatistic {
    pub files_in: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub compactions_out: u64,

    pub files_filtered_out: u64,
}
