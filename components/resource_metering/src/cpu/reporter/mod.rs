// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod endpoint;
mod record;

use self::endpoint::Endpoint;
use self::record::Records;
use crate::cpu::collector::{register_collector, Collector, CollectorHandle};
use crate::cpu::recorder::CpuRecords;
use crate::Config;

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};

pub struct Reporter {
    config: Config,
    scheduler: Scheduler<Task>,
    endpoint: Option<Box<dyn Endpoint>>,
    collector: Option<CollectorHandle>,
    records: Records,
    instance_name: String,
}

impl Reporter {
    pub fn new(status_addr: &str, config: Config, scheduler: Scheduler<Task>) -> Self {
        let instance_name = instance_name(status_addr);

        let endpoint = config
            .should_report()
            .then(|| endpoint::init(&config.endpoint, &instance_name, &config.agent_address));

        let collector = config
            .should_report()
            .then(|| CollectorImpl::register(scheduler.clone()));

        Self {
            config,
            scheduler,
            endpoint,
            collector,
            instance_name,
            records: Records::default(),
        }
    }

    fn handle_report(&mut self) {
        if self.records.is_empty() {
            return;
        }

        // Whether endpoint exists or not, records should be taken in order to reset.
        let records = std::mem::take(&mut self.records);
        if let Some(endpoint) = self.endpoint.as_mut() {
            endpoint.report(records);
        }
    }

    fn handle_cpu_records(&mut self, raw_records: Arc<CpuRecords>) {
        self.records.append(raw_records);
        self.records.keep_top_k(self.config.max_resource_groups);
    }

    fn handle_config_change(&mut self, config: Config) {
        self.config = config;
        if !self.config.should_report() {
            self.reset();
            return;
        }

        if self.collector.is_none() {
            self.collector = Some(CollectorImpl::register(self.scheduler.clone()));
        }

        if let Some(ep) = &mut self.endpoint {
            if ep.name() == self.config.endpoint {
                ep.update(&self.config.agent_address);
                return;
            }
        }
        self.endpoint = Some(endpoint::init(
            &self.config.endpoint,
            &self.instance_name,
            &self.config.agent_address,
        ));
    }

    fn reset(&mut self) {
        self.endpoint.take();
        self.collector.take();
        self.records.clear();
    }
}

pub enum Task {
    ConfigChange(Config),
    CpuRecords(Arc<CpuRecords>),
}

impl Runnable for Reporter {
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::CpuRecords(raw_records) => self.handle_cpu_records(raw_records),
            Task::ConfigChange(config) => self.handle_config_change(config),
        }
    }

    fn shutdown(&mut self) {
        self.reset();
    }
}

impl RunnableWithTimer for Reporter {
    fn on_timeout(&mut self) {
        self.handle_report();
    }

    fn get_interval(&self) -> Duration {
        self.config.report_agent_interval.0
    }
}

pub struct CollectorImpl {
    scheduler: Scheduler<Task>,
}

impl CollectorImpl {
    pub fn register(scheduler: Scheduler<Task>) -> CollectorHandle {
        register_collector(Box::new(Self { scheduler }))
    }
}

impl Collector for CollectorImpl {
    fn collect(&self, records: Arc<CpuRecords>) {
        self.scheduler.schedule(Task::CpuRecords(records)).ok();
    }
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

impl Config {
    fn should_report(&self) -> bool {
        self.enabled && !self.agent_address.is_empty() && self.max_resource_groups != 0
    }
}

fn instance_name(status_addr: &str) -> String {
    let hostname = (|| {
        let hostname = hostname::get().ok()?;
        hostname.into_string().ok()
    })()
    .unwrap_or_else(|| "<unknown>".to_owned());

    let mut split = status_addr.split(':');
    let _host = split.next().expect("invalid status address");
    let port = split.next().expect("invalid status address");

    let mut instance_name = hostname;
    instance_name.push(':');
    instance_name.push_str(port);
    instance_name
}
