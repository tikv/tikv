// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::{Collector, CollectorHandle, CpuRecords};

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use configuration::{ConfigChange, Configuration};
use grpcio::{ChannelBuilder, Environment};
use kvproto::resource_usage_agent::ResourceUsageAgentClient;
use security::SecurityManager;
use serde_derive::{Deserialize, Serialize};
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};

const MIN_PRECISION_SECONDS: u64 = 1;
const MAX_PRECISION_SECONDS: u64 = 24 * 60 * 60;
const MAX_MAX_RESOURCE_GROUPS: u64 = 10_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub enabled: bool,
    #[config(skip)]
    pub agent_address: String,
    pub precision_seconds: u64,
    pub max_resource_groups: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            enabled: false,
            agent_address: "".to_string(),
            precision_seconds: 1,
            max_resource_groups: 5000,
        }
    }
}

impl Config {
    pub fn validate(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if !self.agent_address.is_empty() {
            tikv_util::config::check_addr(&self.agent_address)?;
        }

        if self.precision_seconds < MIN_PRECISION_SECONDS
            || self.precision_seconds > MAX_PRECISION_SECONDS
        {
            return Err(format!(
                "precision seconds must between {} and {}",
                MIN_PRECISION_SECONDS, MAX_PRECISION_SECONDS
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

        Ok(())
    }
}

pub struct ConfigManager {
    current_config: Config,
    scheduler: Scheduler<Task>,
}

impl ConfigManager {
    pub fn new(current_config: Config, scheduler: Scheduler<Task>) -> Self {
        ConfigManager {
            current_config,
            scheduler,
        }
    }
}

impl configuration::ConfigManager for ConfigManager {
    fn dispatch(
        &mut self,
        change: ConfigChange,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.current_config.update(change);
        self.current_config.validate()?;

        self.scheduler
            .schedule(Task::ConfigChange(self.current_config.clone()))
            .ok();

        Ok(())
    }
}

pub struct CpuRecordsCollector {
    scheduler: Scheduler<Task>,
}

impl CpuRecordsCollector {
    pub fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }
}

impl Collector for CpuRecordsCollector {
    fn collect(&self, records: Arc<CpuRecords>) {
        self.scheduler.schedule(Task::CpuRecords(records)).ok();
    }
}

pub enum Task {
    ConfigChange(Config),
    CpuRecords(Arc<CpuRecords>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::ConfigChange(_) => {
                write!(f, "ConfigChange")?;
            }
            Task::CpuRecords(_) => {
                write!(f, "CpuRecords")?;
            }
        }

        Ok(())
    }
}

pub struct ResourceMeteringAgent {
    config: Config,

    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,

    // TODO: mock client for testing
    client: Option<ResourceUsageAgentClient>,
    cpu_records_collector: Option<CollectorHandle>,
}

impl ResourceMeteringAgent {
    pub fn new(
        config: Config,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        cpu_records_collector: CollectorHandle,
    ) -> Self {
        Self {
            config,
            env,
            security_mgr,
            client: None,
            cpu_records_collector: Some(cpu_records_collector),
        }
    }

    pub fn connect(&mut self) {
        if self.config.agent_address.is_empty() || !self.config.enabled {
            self.client = None;
            return;
        }

        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            self.security_mgr.connect(cb, &self.config.agent_address)
        };
        self.client = Some(ResourceUsageAgentClient::new(channel));
    }
}

impl Runnable for ResourceMeteringAgent {
    type Task = Task;

    fn run(&mut self, _task: Self::Task) {}

    fn shutdown(&mut self) {}
}

impl RunnableWithTimer for ResourceMeteringAgent {
    fn on_timeout(&mut self) {}

    fn get_interval(&self) -> Duration {
        Duration::default()
    }
}
