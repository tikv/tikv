// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::time::Duration;

use derive_more::{Add, AddAssign};

/// The statistic of loading metadata of compactions' source files.
#[derive(Default, Debug, Add, AddAssign, Clone)]
pub struct LoadMetaStatistic {
    /// How many meta files read?
    pub meta_files_in: u64,
    /// How many bytes read from remote, physically?
    pub physical_bytes_loaded: u64,
    /// How many physical data files' metadata we have processed?
    pub physical_data_files_in: u64,
    /// How many logical data files' (segments') metadata we have processed?
    pub logical_data_files_in: u64,
    /// How many time spent for loading remote files?
    pub load_file_duration: Duration,
    /// How many prefetch task spawned?
    pub prefetch_task_emitted: u64,
    /// How many spawned prefetch task finished?
    pub prefetch_task_finished: u64,
    /// How many errors happened during fetching from remote?
    pub error_during_downloading: u64,
}

/// The statistic of loading data files for a subcompaction.
#[derive(Default, Debug, Add, AddAssign, Clone)]
pub struct LoadStatistic {
    /// How many logical "files" we have loaded?
    pub files_in: u64,
    /// How many keys we have loaded?
    pub keys_in: u64,
    /// How many bytes we fetched from network, physically?
    pub physical_bytes_in: u64,
    /// How many bytes the keys we loaded have, in their original form?
    pub logical_key_bytes_in: u64,
    /// How many bytes the values we loaded have, without compression?
    pub logical_value_bytes_in: u64,
    /// How many errors happened during fetching from remote?
    pub error_during_downloading: u64,
}

/// The statistic of executing a subcompaction.
#[derive(Default, Debug, Add, AddAssign, Clone)]
pub struct CompactStatistic {
    /// How many keys we have yielded?
    pub keys_out: u64,
    /// How many bytes we have yielded, physically?
    pub physical_bytes_out: u64,
    /// How many bytes that all the keys we have yielded uses?
    pub logical_key_bytes_out: u64,
    /// How many bytes that all the values we have yielded uses?
    pub logical_value_bytes_out: u64,

    /// How many time we spent for writing the SST output?
    pub write_sst_duration: Duration,
    /// How many time we spent for reading the source of this subcompaction?
    pub load_duration: Duration,
    /// How many time we spent for sorting the inputs?
    pub sort_duration: Duration,
    /// How many time we spent for putting the artifacts to external storage?
    pub save_duration: Duration,

    /// How many subcompactions generates no thing?
    pub empty_generation: u64,
}

/// The statistic of collecting subcompactions.
#[derive(Default, Debug, Add, AddAssign, Clone)]
pub struct CollectCompactionStatistic {
    /// How many files we processed?
    pub files_in: u64,
    /// How many bytes the files we have processed have?
    pub bytes_in: u64,
    /// How many bytes the compactions we emitted need to handle?
    pub bytes_out: u64,
    /// How many compactions we have emitted?
    pub compactions_out: u64,

    /// How many files we have filtered out due to the TS range?
    pub files_filtered_out: u64,
}
