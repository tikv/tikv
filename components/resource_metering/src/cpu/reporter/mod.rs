// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod endpoint;
mod record;

use self::endpoint::grpc::GRPCEndpoint;
use self::endpoint::Endpoint;
use self::record::Records;
use crate::cpu::collector::{register_collector, Collector, CollectorHandle};
use crate::cpu::recorder::CpuRecords;
use crate::Config;

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};

pub struct ResourceMeteringReporter {
    config: Config,

    scheduler: Scheduler<Task>,

    endpoint: Option<Box<dyn Endpoint + 'static + Send>>,
    collector: Option<CollectorHandle>,

    records: Records,
}

impl ResourceMeteringReporter {
    pub fn new(config: Config, scheduler: Scheduler<Task>) -> Self {
        let endpoint = config
            .should_report()
            .then(|| Box::new(GRPCEndpoint::init(&config.agent_address)) as _);

        let collector = config
            .should_report()
            .then(|| CpuRecordsCollector::register(scheduler.clone()));

        Self {
            config,
            scheduler,
            endpoint,
            collector,
            records: Records::default(),
        }
    }
}

pub enum Task {
    ConfigChange(Config),
    CpuRecords(Arc<CpuRecords>),
}

impl Runnable for ResourceMeteringReporter {
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::ConfigChange(config) => {
                self.config = config;
                if !self.config.should_report() {
                    self.endpoint.take();
                    self.collector.take();
                    return;
                }

                if self.collector.is_none() {
                    self.collector = Some(CpuRecordsCollector::register(self.scheduler.clone()));
                }
                if self.endpoint.is_none() {
                    self.endpoint =
                        Some(Box::new(GRPCEndpoint::init(&self.config.agent_address)) as _);
                }

                self.endpoint
                    .as_mut()
                    .unwrap()
                    .update(&self.config.agent_address);
            }
            Task::CpuRecords(raw_records) => {
                self.records.append(raw_records);
                self.records.keep_top_k(self.config.max_resource_groups);
            }
        }
    }

    fn shutdown(&mut self) {
        self.collector.take();
        self.endpoint.take();
    }
}

impl RunnableWithTimer for ResourceMeteringReporter {
    fn on_timeout(&mut self) {
        if self.records.is_empty() {
            return;
        }

        let records = std::mem::take(&mut self.records);
        if let Some(endpoint) = self.endpoint.as_mut() {
            endpoint.report(records);
        }
    }

    fn get_interval(&self) -> Duration {
        self.config.report_agent_interval.0
    }
}

pub struct CpuRecordsCollector {
    scheduler: Scheduler<Task>,
}

impl CpuRecordsCollector {
    pub fn register(scheduler: Scheduler<Task>) -> CollectorHandle {
        register_collector(Box::new(Self { scheduler }))
    }
}

impl Collector for CpuRecordsCollector {
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
