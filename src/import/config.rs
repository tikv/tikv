// Copyright 2018 TiKV Project Authors.
use std::error::Error;
use std::result::Result;

use crate::util::config::{ReadableDuration, ReadableSize};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub import_dir: String,
    pub num_threads: usize,
    pub num_import_jobs: usize,
    pub num_import_sst_jobs: usize,
    pub max_prepare_duration: ReadableDuration,
    pub region_split_size: ReadableSize,
    pub stream_channel_window: usize,
    pub max_open_engines: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            import_dir: "/tmp/tikv/import".to_owned(),
            num_threads: 8,
            num_import_jobs: 8,
            num_import_sst_jobs: 2,
            max_prepare_duration: ReadableDuration::minutes(5),
            region_split_size: ReadableSize::mb(512),
            stream_channel_window: 128,
            max_open_engines: 8,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.num_threads == 0 {
            return Err("import.num_threads can not be 0".into());
        }
        if self.num_import_jobs == 0 {
            return Err("import.num_import_jobs can not be 0".into());
        }
        if self.num_import_sst_jobs == 0 {
            return Err("import.num_import_sst_jobs can not be 0".into());
        }
        if self.region_split_size.0 == 0 {
            return Err("import.region_split_size can not be 0".into());
        }
        if self.stream_channel_window == 0 {
            return Err("import.stream_channel_window can not be 0".into());
        }
        if self.max_open_engines == 0 {
            return Err("import.max_open_engines can not be 0".into());
        }
        Ok(())
    }
}
