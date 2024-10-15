// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::time::Duration;

use chrono::{DateTime, Local};
use derive_more::{Add, AddAssign};
use serde::Serialize;

/// The statistic of an [`Execution`].
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CompactLogBackupStatistic {
    /// When we start the execution?
    pub start_time: DateTime<Local>,
    /// When it ends?
    pub end_time: DateTime<Local>,
    /// How many time we spent for the whole execution?
    pub time_taken: Duration,
    /// From which host we executed this compaction?
    pub exec_by: String,

    // Summary of statistics.
    pub load_stat: LoadStatistic,
    pub load_meta_stat: LoadMetaStatistic,
    pub collect_subcompactions_stat: CollectSubcompactionStatistic,
    pub subcompact_stat: SubcompactStatistic,
    pub prometheus: prom::SerAll,
}

/// The statistic of loading metadata of compactions' source files.
#[derive(Default, Debug, Add, AddAssign, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
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
#[derive(Default, Debug, Add, AddAssign, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]

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
#[derive(Default, Debug, Add, AddAssign, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SubcompactStatistic {
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
#[derive(Default, Debug, Add, AddAssign, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CollectSubcompactionStatistic {
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

pub mod prom {
    use prometheus::*;
    use serde::{ser::SerializeMap, Serialize};

    struct ShowPromHist<'a>(&'a Histogram);

    impl<'a> Serialize for ShowPromHist<'a> {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            use ::prometheus::core::Metric;
            let proto = self.0.metric();
            let hist = proto.get_histogram();
            let mut m = serializer.serialize_map(Some(hist.get_bucket().len() + 2))?;
            m.serialize_entry("count", &hist.get_sample_count())?;
            m.serialize_entry("sum", &hist.get_sample_sum())?;
            for bucket in hist.get_bucket() {
                m.serialize_entry(
                    &format!("le_{}", bucket.get_upper_bound()),
                    &bucket.get_cumulative_count(),
                )?;
            }
            m.end()
        }
    }

    /// SerAll is a placeholder type that when being serialized, it reads
    /// metrics registered to prometheus from the module and then serialize them
    /// to the result.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct SerAll;

    impl Serialize for SerAll {
        fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let histograms = [
                (
                    "read_meta_duration",
                    &*COMPACT_LOG_BACKUP_READ_META_DURATION,
                ),
                (
                    "load_a_file_duration",
                    &*COMPACT_LOG_BACKUP_LOAD_A_FILE_DURATION,
                ),
                ("load_duration", &*COMPACT_LOG_BACKUP_LOAD_DURATION),
                ("sort_duration", &*COMPACT_LOG_BACKUP_SORT_DURATION),
                ("save_duration", &*COMPACT_LOG_BACKUP_SAVE_DURATION),
                (
                    "write_sst_duration",
                    &*COMPACT_LOG_BACKUP_WRITE_SST_DURATION,
                ),
            ];

            let mut m = serializer.serialize_map(Some(histograms.len()))?;
            for (name, histogram) in histograms.iter() {
                m.serialize_entry(name, &ShowPromHist(histogram))?;
            }
            m.end()
        }
    }

    lazy_static::lazy_static! {
        // ==== The following metrics will be collected directly in the call site.
        pub static ref COMPACT_LOG_BACKUP_READ_META_DURATION: Histogram = register_histogram!(
            "compact_log_backup_read_meta_duration",
            "The duration of reading meta files.",
            exponential_buckets(0.001, 2.0, 13).unwrap()
        ).unwrap();

        pub static ref COMPACT_LOG_BACKUP_LOAD_A_FILE_DURATION: Histogram = register_histogram!(
            "compact_log_backup_load_a_file_duration",
            "The duration of loading a log file.",
            exponential_buckets(0.001, 2.0, 13).unwrap()
        ).unwrap();

        // ==== The following metrics will be collected in the hooks.
        pub static ref COMPACT_LOG_BACKUP_LOAD_DURATION: Histogram = register_histogram!(
            "compact_log_backup_load_duration",
            "The duration of loading log all log files for a compaction.",
            exponential_buckets(0.1, 1.5, 13).unwrap()
        ).unwrap();

        pub static ref COMPACT_LOG_BACKUP_SORT_DURATION: Histogram = register_histogram!(
            "compact_log_backup_sort_duration",
            "The duration of sorting contents.",
            exponential_buckets(0.1, 1.5, 13).unwrap()
        ).unwrap();

        pub static ref COMPACT_LOG_BACKUP_SAVE_DURATION: Histogram = register_histogram!(
            "compact_log_backup_save_duration",
            "The duration of saving log files.",
            exponential_buckets(0.01, 2.0, 13).unwrap()
        ).unwrap();

        pub static ref COMPACT_LOG_BACKUP_WRITE_SST_DURATION: Histogram = register_histogram!(
            "compact_log_backup_write_sst_duration",
            "The duration of writing SST files.",
            exponential_buckets(0.01, 2.0, 13).unwrap()
        ).unwrap();
    }
}
