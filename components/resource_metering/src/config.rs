// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use configuration::{ConfigChange, Configuration};
use serde_derive::{Deserialize, Serialize};
use tikv_util::config::ReadableDuration;

const MIN_PRECISION: ReadableDuration = ReadableDuration::secs(1);
const MAX_PRECISION: ReadableDuration = ReadableDuration::hours(1);
const MAX_MAX_RESOURCE_GROUPS: usize = 5_000;
const MIN_REPORT_AGENT_INTERVAL: ReadableDuration = ReadableDuration::secs(5);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub enabled: bool,

    pub agent_address: String,
    pub report_agent_interval: ReadableDuration,
    pub max_resource_groups: usize,

    pub precision: ReadableDuration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            enabled: false,
            agent_address: "".to_string(),
            report_agent_interval: ReadableDuration::minutes(1),
            max_resource_groups: 2000,
            precision: ReadableDuration::secs(1),
        }
    }
}

impl Config {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if !self.agent_address.is_empty() {
            tikv_util::config::check_addr(&self.agent_address)?;
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

        if self.report_agent_interval < MIN_REPORT_AGENT_INTERVAL
            || self.report_agent_interval > self.precision * 500
        {
            return Err(format!(
                "report interval seconds must between {} and {}",
                MIN_REPORT_AGENT_INTERVAL,
                self.precision * 500
            )
            .into());
        }

        Ok(())
    }
}

pub struct ConfigManagerBuilder {
    on_change_enabled: Vec<Box<dyn FnMut(bool) + Send + Sync>>,
    on_change_agent_address: Vec<Box<dyn FnMut(&str) + Send + Sync>>,
    on_change_report_agent_interval: Vec<Box<dyn FnMut(ReadableDuration) + Send + Sync>>,
    on_change_max_resource_groups: Vec<Box<dyn FnMut(usize) + Send + Sync>>,
    on_change_precision: Vec<Box<dyn FnMut(ReadableDuration) + Send + Sync>>,
}

impl ConfigManagerBuilder {
    pub fn new() -> Self {
        Self {
            on_change_enabled: vec![],
            on_change_agent_address: vec![],
            on_change_report_agent_interval: vec![],
            on_change_max_resource_groups: vec![],
            on_change_precision: vec![],
        }
    }

    pub fn on_change_enabled(&mut self, f: impl FnMut(bool) + 'static + Send + Sync) {
        self.on_change_enabled.push(Box::new(f));
    }

    pub fn on_change_agent_address(&mut self, f: impl FnMut(&str) + 'static + Send + Sync) {
        self.on_change_agent_address.push(Box::new(f));
    }

    pub fn on_change_report_agent_interval(
        &mut self,
        f: impl FnMut(ReadableDuration) + 'static + Send + Sync,
    ) {
        self.on_change_report_agent_interval.push(Box::new(f));
    }

    pub fn on_change_max_resource_groups(&mut self, f: impl FnMut(usize) + 'static + Send + Sync) {
        self.on_change_max_resource_groups.push(Box::new(f));
    }

    pub fn on_change_precision(&mut self, f: impl FnMut(ReadableDuration) + 'static + Send + Sync) {
        self.on_change_precision.push(Box::new(f));
    }

    pub fn build(self) -> ConfigManager {
        ConfigManager {
            current_config: Config::default(),
            on_change_enabled: self.on_change_enabled,
            on_change_agent_address: self.on_change_agent_address,
            on_change_report_agent_interval: self.on_change_report_agent_interval,
            on_change_max_resource_groups: self.on_change_max_resource_groups,
            on_change_precision: self.on_change_precision,
        }
    }
}

pub struct ConfigManager {
    current_config: Config,
    on_change_enabled: Vec<Box<dyn FnMut(bool) + Send + Sync>>,
    on_change_agent_address: Vec<Box<dyn FnMut(&str) + Send + Sync>>,
    on_change_report_agent_interval: Vec<Box<dyn FnMut(ReadableDuration) + Send + Sync>>,
    on_change_max_resource_groups: Vec<Box<dyn FnMut(usize) + Send + Sync>>,
    on_change_precision: Vec<Box<dyn FnMut(ReadableDuration) + Send + Sync>>,
}

impl configuration::ConfigManager for ConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut new_config = self.current_config.clone();
        new_config.update(change);
        new_config.validate()?;

        if self.current_config.enabled != new_config.enabled {
            self.on_change_enabled
                .iter_mut()
                .for_each(|f| f(new_config.enabled));
        }

        if self.current_config.agent_address != new_config.agent_address {
            self.on_change_agent_address
                .iter_mut()
                .for_each(|f| f(&new_config.agent_address));
        }

        if self.current_config.report_agent_interval != new_config.report_agent_interval {
            self.on_change_report_agent_interval
                .iter_mut()
                .for_each(|f| f(new_config.report_agent_interval));
        }

        if self.current_config.max_resource_groups != new_config.max_resource_groups {
            self.on_change_max_resource_groups
                .iter_mut()
                .for_each(|f| f(new_config.max_resource_groups));
        }

        if self.current_config.precision != new_config.precision {
            self.on_change_precision
                .iter_mut()
                .for_each(|f| f(new_config.precision));
        }

        self.current_config = new_config;

        Ok(())
    }
}
