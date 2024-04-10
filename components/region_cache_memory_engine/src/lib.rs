// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(let_chains)]
#![feature(slice_pattern)]

mod background;
mod engine;
pub mod keys;
use std::time::Duration;

use serde_derive::{Deserialize, Serialize};

pub mod region_label;
pub use engine::RangeCacheMemoryEngine;
pub mod range_manager;
mod write_batch;
use tikv_util::config::ReadableSize;
pub use write_batch::RangeCacheWriteBatch;
mod memory_controller;
pub use background::{BackgroundRunner, GcTask};
mod metrics;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument: {0}")]
    InvalidArgument(String),
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(default, rename_all = "kebab-case")]
pub struct RangeCacheEngineConfig {
    pub enabled: bool,
    pub gc_interval: Duration,
    soft_limit_threshold: Option<ReadableSize>,
    hard_limit_threshold: Option<ReadableSize>,
}

impl Default for RangeCacheEngineConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            gc_interval: Duration::from_secs(180),
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
            gc_interval: Duration::from_secs(180),
            soft_limit_threshold: Some(ReadableSize::gb(1)),
            hard_limit_threshold: Some(ReadableSize::gb(2)),
        }
    }
}
