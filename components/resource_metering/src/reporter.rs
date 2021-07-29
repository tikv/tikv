// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::collector::{register_collector, Collector, CollectorHandle};
use crate::row::collector::{register_row_collector, RowCollector, RowCollectorHandle};
use crate::row::recorder::{RowRecords, RowStats};

use crate::cpu::recorder::CpuRecords;
use crate::Config;

use std::fmt::{self, Display, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use collections::HashMap;
use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{CpuTimeRecord, ResourceUsageAgentClient};
use tikv_util::time::Duration;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};

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

pub struct RowRecordsCollector {
    scheduler: Scheduler<Task>,
}

impl RowRecordsCollector {
    pub fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }
}

impl RowCollector for RowRecordsCollector {
    fn collect_row(&self, records: Arc<RowRecords>) {
        self.scheduler.schedule(Task::RowRecords(records)).ok();
    }
}

pub enum Task {
    ConfigChange(Config),
    CpuRecords(Arc<CpuRecords>),
    RowRecords(Arc<RowRecords>),
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
            Task::RowRecords(_) => {
                write!(f, "RowRecords")?;
            }
        }

        Ok(())
    }
}

pub struct ResourceMeteringReporter {
    config: Config,
    env: Arc<Environment>,

    scheduler: Scheduler<Task>,

    // TODO: mock client for testing
    client: Option<ResourceUsageAgentClient>,
    reporting: Arc<AtomicBool>,
    cpu_records_collector: Option<CollectorHandle>,
    row_records_collector: Option<RowCollectorHandle>,

    // resource_tag -> ([timestamp_secs], [cpu_time_ms], total_cpu_time_ms)
    records: HashMap<Vec<u8>, ReportRecord>,
    // timestamp_secs -> OtherRecord
    others: HashMap<u64, OtherRecord>,
    find_top_k: Vec<u32>,
}

struct ReportRecord {
    timestamp_secs_list: Vec<u64>,
    cpu_time_ms_list: Vec<u32>,
    read_row_count_list: Vec<u64>,
    read_index_count_list: Vec<u64>,
    total_cpu_time_ms: u32,
}

impl ReportRecord {
    fn new() -> Self {
        Self {
            timestamp_secs_list: vec![],
            cpu_time_ms_list: vec![],
            read_row_count_list: vec![],
            read_index_count_list: vec![],
            total_cpu_time_ms: 0,
        }
    }
    fn add_cpu_time_ms(&mut self, timestamp_secs: u64, cpu_time_ms: u32) {
        if *self.timestamp_secs_list.last().unwrap() == timestamp_secs {
            *self.cpu_time_ms_list.last_mut().unwrap() += cpu_time_ms;
        } else {
            self.timestamp_secs_list.push(timestamp_secs);
            self.cpu_time_ms_list.push(cpu_time_ms);
        }
        self.total_cpu_time_ms += cpu_time_ms;
    }

    fn add_row_stats(&mut self, timestamp_secs: u64, row_stats: &RowStats) {
        if *self.timestamp_secs_list.last().unwrap() == timestamp_secs {
            *self.read_row_count_list.last_mut().unwrap() += row_stats.read_row_count;
            *self.read_index_count_list.last_mut().unwrap() += row_stats.read_index_count;
        } else {
            self.timestamp_secs_list.push(timestamp_secs);
            self.read_row_count_list.push(row_stats.read_row_count);
            self.read_index_count_list.push(row_stats.read_index_count);
        }
    }
}

#[derive(Debug, Default)]
struct OtherRecord {
    cpu_time_ms: u32,
    row_stats: RowStats,
}

impl OtherRecord {
    fn merge(&mut self, cpu_time_ms: u32, read_row_count: u64, read_index_count: u64) {
        self.cpu_time_ms += cpu_time_ms;
        self.row_stats.read_row_count += read_row_count;
        self.row_stats.read_index_count += read_index_count;
    }
}

impl ResourceMeteringReporter {
    pub fn new(config: Config, scheduler: Scheduler<Task>, env: Arc<Environment>) -> Self {
        Self {
            config,
            env,
            scheduler,
            client: None,
            reporting: Arc::new(AtomicBool::new(false)),
            cpu_records_collector: None,
            row_records_collector: None,
            records: HashMap::default(),
            others: HashMap::default(),
            find_top_k: Vec::default(),
        }
    }

    pub fn init_client(&mut self, addr: &str) {
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            cb.connect(addr)
        };
        self.client = Some(ResourceUsageAgentClient::new(channel));
        if self.cpu_records_collector.is_none() {
            self.cpu_records_collector = Some(register_collector(Box::new(
                CpuRecordsCollector::new(self.scheduler.clone()),
            )));
        }
        if self.row_records_collector.is_none() {
            self.row_records_collector = Some(register_row_collector(Box::new(
                RowRecordsCollector::new(self.scheduler.clone()),
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
                } else if new_config.agent_address != self.config.agent_address
                    || new_config.enabled != self.config.enabled
                {
                    self.init_client(&new_config.agent_address);
                }

                self.config = new_config;
            }
            Task::CpuRecords(records) => {
                let timestamp_secs = records.begin_unix_time_secs;

                for (tag, ms) in &records.records {
                    let tag = &tag.infos.extra_attachment;
                    if tag.is_empty() {
                        continue;
                    }

                    let ms = *ms as u32;
                    match self.records.get_mut(tag) {
                        Some(record) => {
                            record.add_cpu_time_ms(timestamp_secs, ms);
                        }
                        None => {
                            let mut record = ReportRecord::new();
                            record.add_cpu_time_ms(timestamp_secs, ms);
                            self.records.insert(tag.clone(), record);
                        }
                    }
                }

                if self.records.len() > self.config.max_resource_groups {
                    self.find_top_k.clear();
                    for record in self.records.values() {
                        self.find_top_k.push(record.total_cpu_time_ms);
                    }
                    pdqselect::select_by(
                        &mut self.find_top_k,
                        self.config.max_resource_groups,
                        |a, b| b.cmp(a),
                    );
                    let kth = self.find_top_k[self.config.max_resource_groups];
                    let others = &mut self.others;
                    self.records
                        .drain_filter(|_, record| record.total_cpu_time_ms <= kth)
                        .for_each(|(_, record)| {
                            record
                                .timestamp_secs_list
                                .into_iter()
                                .zip(
                                    record.cpu_time_ms_list.into_iter().zip(
                                        record
                                            .read_row_count_list
                                            .into_iter()
                                            .zip(record.read_index_count_list)
                                            .into_iter(),
                                    ),
                                )
                                .for_each(
                                    |(secs, (cpu_time, (read_row_cound, read_index_count)))| {
                                        (*others.entry(secs).or_insert(OtherRecord::default()))
                                            .merge(cpu_time, read_row_cound, read_index_count);
                                    },
                                )
                        });
                }
            }
            Task::RowRecords(records) => {
                let timestamp_secs = records.begin_unix_time_secs;
                for (tag, row_stats) in &records.records {
                    match self.records.get_mut(tag) {
                        Some(record) => {
                            record.add_row_stats(timestamp_secs, row_stats);
                        }
                        None => {
                            let mut record = ReportRecord::new();
                            record.add_row_stats(timestamp_secs, row_stats);
                            self.records.insert(tag.clone(), record);
                        }
                    }
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
            assert!(self.others.is_empty());
            return;
        }

        let records = std::mem::take(&mut self.records);
        let others = std::mem::take(&mut self.others);

        if self.reporting.load(SeqCst) {
            return;
        }

        if let Some(client) = self.client.as_ref() {
            match client.report_cpu_time_opt(CallOption::default().timeout(Duration::from_secs(2)))
            {
                Ok((mut tx, rx)) => {
                    self.reporting.store(true, SeqCst);
                    let reporting = self.reporting.clone();
                    client.spawn(async move {
                        defer!(reporting.store(false, SeqCst));

                        for (tag, record) in records {
                            let mut req = CpuTimeRecord::default();
                            req.set_resource_group_tag(tag);
                            req.set_record_list_timestamp_sec(record.timestamp_secs_list);
                            req.set_record_list_cpu_time_ms(record.cpu_time_ms_list);
                            req.set_record_list_scan_rows(record.read_row_count_list);
                            if tx.send((req, WriteFlags::default())).await.is_err() {
                                return;
                            }
                        }

                        // others

                        if !others.is_empty() {
                            let mut timestamp_secs_list = vec![];
                            let mut cpu_time_ms_list = vec![];
                            let mut read_row_count_list = vec![];
                            for (ts, record) in others {
                                timestamp_secs_list.push(ts);
                                cpu_time_ms_list.push(record.cpu_time_ms);
                                read_row_count_list.push(record.row_stats.read_row_count);
                            }
                            let mut req = CpuTimeRecord::default();
                            req.set_record_list_timestamp_sec(timestamp_secs_list);
                            req.set_record_list_cpu_time_ms(cpu_time_ms_list);
                            req.set_record_list_scan_rows(read_row_count_list);
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
        self.config.report_agent_interval.0
    }
}
