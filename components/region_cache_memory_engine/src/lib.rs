// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(let_chains)]
#![feature(slice_pattern)]

use std::time::Duration;

use online_config::OnlineConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tikv_util::config::{ReadableDuration, ReadableSize};

mod background;
pub mod config;
mod engine;
mod keys;
mod memory_controller;
mod metrics;
mod perf_context;
mod range_manager;
mod read;
mod region_label;
mod write_batch;

pub use background::{BackgroundRunner, GcTask};
pub use engine::RangeCacheMemoryEngine;
pub use range_manager::RangeCacheStatus;
pub use write_batch::RangeCacheWriteBatch;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument: {0}")]
    InvalidArgument(String),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, OnlineConfig)]
#[serde(default, rename_all = "kebab-case")]
pub struct RangeCacheEngineConfig {
    pub enabled: bool,
    pub gc_interval: ReadableDuration,
    pub soft_limit_threshold: Option<ReadableSize>,
    pub hard_limit_threshold: Option<ReadableSize>,
}

impl Default for RangeCacheEngineConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            gc_interval: ReadableDuration(Duration::from_secs(60)),
            soft_limit_threshold: None,
            hard_limit_threshold: None,
        }
    }
}

impl RangeCacheEngineConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.enabled {
            return Ok(());
        }

        Ok(self.sanitize()?)
    }

    pub fn sanitize(&mut self) -> Result<(), Error> {
        if self.soft_limit_threshold.is_none() || self.hard_limit_threshold.is_none() {
            return Err(Error::InvalidArgument(
                "soft-limit-threshold or hard-limit-threshold not set".to_string(),
            ));
        }

        if self.soft_limit_threshold.as_ref().unwrap()
            >= self.hard_limit_threshold.as_ref().unwrap()
        {
            return Err(Error::InvalidArgument(format!(
                "soft-limit-threshold {:?} is larger or equal to hard-limit-threshold {:?}",
                self.soft_limit_threshold.as_ref().unwrap(),
                self.hard_limit_threshold.as_ref().unwrap()
            )));
        }

        Ok(())
    }

    pub fn soft_limit_threshold(&self) -> usize {
        self.soft_limit_threshold.map_or(0, |r| r.0 as usize)
    }

    pub fn hard_limit_threshold(&self) -> usize {
        self.hard_limit_threshold.map_or(0, |r| r.0 as usize)
    }

    pub fn config_for_test() -> RangeCacheEngineConfig {
        RangeCacheEngineConfig {
            enabled: true,
            gc_interval: ReadableDuration(Duration::from_secs(180)),
            soft_limit_threshold: Some(ReadableSize::gb(1)),
            hard_limit_threshold: Some(ReadableSize::gb(2)),
        }
    }
}
