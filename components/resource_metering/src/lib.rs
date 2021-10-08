// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(shrink_to)]
#![feature(hash_drain_filter)]

#[macro_use]
extern crate tikv_util;

use crate::cpu::recorder::RecorderHandle;
use crate::reporter::Task;

use std::sync::Arc;

use online_config::{ConfigChange, OnlineConfig};
use serde_derive::{Deserialize, Serialize};
use tikv_util::config::ReadableDuration;
use tikv_util::worker::Scheduler;

pub mod cpu;
pub mod reporter;

#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct ResourceMeteringTag {
    pub infos: Arc<TagInfos>,
}

impl ResourceMeteringTag {
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        Arc::new(TagInfos::from_rpc_context(context)).into()
    }
}

impl From<Arc<TagInfos>> for ResourceMeteringTag {
    fn from(infos: Arc<TagInfos>) -> Self {
        Self { infos }
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct TagInfos {
    pub store_id: u64,
    pub region_id: u64,
    pub peer_id: u64,
    pub extra_attachment: Vec<u8>,
}

impl TagInfos {
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        let peer = context.get_peer();
        TagInfos {
            store_id: peer.get_store_id(),
            peer_id: peer.get_id(),
            region_id: context.get_region_id(),
            extra_attachment: Vec::from(context.get_resource_group_tag()),
        }
    }
}

const MIN_PRECISION: ReadableDuration = ReadableDuration::secs(1);
const MAX_PRECISION: ReadableDuration = ReadableDuration::hours(1);
const MAX_MAX_RESOURCE_GROUPS: usize = 5_000;
const MIN_REPORT_RECEIVER_INTERVAL: ReadableDuration = ReadableDuration::secs(5);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub enabled: bool,

    pub receiver_address: String,
    pub report_receiver_interval: ReadableDuration,
    pub max_resource_groups: usize,

    pub precision: ReadableDuration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            enabled: true,
            receiver_address: "".to_string(),
            report_receiver_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2000,
            precision: ReadableDuration::secs(1),
        }
    }
}

impl Config {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if !self.receiver_address.is_empty() {
            tikv_util::config::check_addr(&self.receiver_address)?;
        }

        if self.precision < MIN_PRECISION || self.precision > MAX_PRECISION {
            return Err(format!(
                "precision must between {} and {}",
                MIN_PRECISION, MAX_PRECISION
            )
            .into());
        }

        if self.max_resource_groups > MAX_MAX_RESOURCE_GROUPS {
            return Err(format!(
                "max resource groups must between {} and {}",
                0, MAX_MAX_RESOURCE_GROUPS
            )
            .into());
        }

        if self.report_receiver_interval < MIN_REPORT_RECEIVER_INTERVAL
            || self.report_receiver_interval > self.precision * 500
        {
            return Err(format!(
                "report interval seconds must between {} and {}",
                MIN_REPORT_RECEIVER_INTERVAL,
                self.precision * 500
            )
            .into());
        }

        Ok(())
    }

    fn should_report(&self) -> bool {
        self.enabled && !self.receiver_address.is_empty() && self.max_resource_groups != 0
    }
}

pub struct ConfigManager {
    current_config: Config,
    scheduler: Scheduler<Task>,
    recorder: RecorderHandle,
}

impl ConfigManager {
    pub fn new(
        current_config: Config,
        scheduler: Scheduler<Task>,
        recorder: RecorderHandle,
    ) -> Self {
        ConfigManager {
            current_config,
            scheduler,
            recorder,
        }
    }
}

impl online_config::ConfigManager for ConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut new_config = self.current_config.clone();
        new_config.update(change);
        new_config.validate()?;

        if self.current_config.enabled != new_config.enabled {
            if new_config.enabled {
                self.recorder.resume();
            } else {
                self.recorder.pause();
            }
        }

        if self.current_config.precision != new_config.precision {
            self.recorder.set_precision(new_config.precision.0);
        }

        self.scheduler
            .schedule(Task::ConfigChange(new_config.clone()))
            .ok();
        self.current_config = new_config;

        Ok(())
    }
}
