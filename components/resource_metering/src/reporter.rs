// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::{register_collector, Collector, CollectorHandle, CpuRecords};

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use collections::HashMap;
use configuration::{ConfigChange, Configuration};
use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{CollectCpuTimeRequest, ResourceUsageAgentClient};
use security::SecurityManager;
use serde_derive::{Deserialize, Serialize};
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};

const MIN_PRECISION_SECONDS: u64 = 1;
const MAX_PRECISION_SECONDS: u64 = 60 * 60;
const MAX_MAX_RESOURCE_GROUPS: usize = 5_000;
const MIN_REPORT_AGENT_INTERVAL_SECONDS: u64 = 15;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub enabled: bool,
    pub agent_address: String,
    pub report_agent_interval_seconds: u64,
    pub precision_seconds: u64,
    pub max_resource_groups: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            enabled: false,
            agent_address: "".to_string(),
            precision_seconds: 1,
            report_agent_interval_seconds: 60,
            max_resource_groups: 200,
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

        if self.report_agent_interval_seconds < MIN_REPORT_AGENT_INTERVAL_SECONDS
            || self.report_agent_interval_seconds > self.precision_seconds * 500
        {
            return Err(format!(
                "report interval seconds must between {} and {}",
                MIN_REPORT_AGENT_INTERVAL_SECONDS,
                self.precision_seconds * 500
            )
            .into());
        }

        Ok(())
    }

    fn should_report(&self) -> bool {
        self.enabled && !self.agent_address.is_empty() && self.max_resource_groups != 0
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

pub struct ResourceMeteringReporter {
    config: Config,

    env: Arc<Environment>,
    security_mgr: Arc<SecurityManager>,

    scheduler: Scheduler<Task>,

    // TODO: mock client for testing
    client: Option<ResourceUsageAgentClient>,
    cpu_records_collector: Option<CollectorHandle>,

    // resource_tag -> ([timestamp_secs], [cpu_time_ms], total_cpu_time_ms)
    records: HashMap<Vec<u8>, (Vec<u64>, Vec<u32>, u32)>,
    last_timestamp_secs: u64,

    find_top_k: Vec<u32>,
}

impl ResourceMeteringReporter {
    pub fn new(
        config: Config,
        scheduler: Scheduler<Task>,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        Self {
            config,
            env,
            security_mgr,
            scheduler,
            client: None,
            cpu_records_collector: None,
            records: HashMap::default(),
            last_timestamp_secs: 0,
            find_top_k: Vec::default(),
        }
    }

    pub fn init_reporter(&mut self, addr: &str) {
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            self.security_mgr.connect(cb, addr)
        };
        self.client = Some(ResourceUsageAgentClient::new(channel));
        if self.cpu_records_collector.is_none() {
            self.cpu_records_collector = Some(register_collector(Box::new(
                CpuRecordsCollector::new(self.scheduler.clone()),
            )));
        }
    }
}

impl Runnable for ResourceMeteringReporter {
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::ConfigChange(new_config) => {
                if !new_config.should_report() {
                    self.client.take();
                    self.cpu_records_collector.take();
                } else if new_config.agent_address != self.config.agent_address {
                    self.init_reporter(&new_config.agent_address);
                }

                self.config = new_config;
            }
            Task::CpuRecords(records) => {
                let timestamp_secs = records.begin_unix_time_ms / 1000;
                let mut should_push_record = false;
                if self.last_timestamp_secs + self.config.precision_seconds <= timestamp_secs {
                    self.last_timestamp_secs = timestamp_secs;
                    should_push_record = true;
                }

                for (tag, record) in &records.records {
                    let tag = &tag.infos.extra_attachment;
                    let ms = *record as u32;
                    match self.records.get_mut(tag) {
                        Some((ts, cpu_time, total)) => {
                            if should_push_record {
                                ts.push(timestamp_secs);
                                cpu_time.push(ms);
                            } else {
                                *cpu_time.last_mut().unwrap() += ms;
                            }
                            *total += ms;
                        }
                        None => {
                            self.records
                                .insert(tag.clone(), (vec![timestamp_secs], vec![ms], ms));
                        }
                    }
                }

                if self.records.len() > self.config.max_resource_groups {
                    self.find_top_k.clear();
                    for (_, _, total) in self.records.values() {
                        self.find_top_k.push(*total);
                    }
                    pdqselect::select_by(
                        &mut self.find_top_k,
                        self.config.max_resource_groups,
                        |a, b| b.cmp(a),
                    );
                    let kth = self.find_top_k[self.config.max_resource_groups];
                    self.records.retain(|_, (_, _, total)| *total >= kth);
                }
            }
        }
    }

    fn shutdown(&mut self) {
        self.cpu_records_collector.take();
        self.client.take();
    }
}

impl RunnableWithTimer for ResourceMeteringReporter {
    fn on_timeout(&mut self) {
        if self.records.is_empty() {
            return;
        }

        let records = std::mem::take(&mut self.records);
        if let Some(client) = self.client.as_ref() {
            match client.collect_cpu_time_opt(CallOption::default().timeout(Duration::from_secs(2)))
            {
                Ok((mut tx, rx)) => {
                    client.spawn(async move {
                        for (tag, (timestamp_list, cpu_time_ms_list, _)) in records {
                            let mut req = CollectCpuTimeRequest::default();
                            req.set_resource_tag(tag);
                            req.set_timestamp_list(timestamp_list);
                            req.set_cpu_time_ms_list(cpu_time_ms_list);
                            if tx.send((req, WriteFlags::default())).await.is_err() {
                                return;
                            }
                        }
                        if tx.close().await.is_err() {
                            return;
                        }
                        rx.await.ok();
                    });
                }
                Err(err) => {
                    warn!("failed to connect resource usage agent"; "error" => ?err);
                }
            }
        }
    }

    fn get_interval(&self) -> Duration {
        Duration::from_secs(self.config.report_agent_interval_seconds)
    }
}
