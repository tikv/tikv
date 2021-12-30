// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod collector_impl;
pub mod data_sink;
pub mod data_sink_reg;
pub mod pubsub;
pub mod single_target;

use crate::recorder::{CollectorGuard, CollectorRegHandle};
use crate::reporter::collector_impl::CollectorImpl;
use crate::reporter::data_sink_reg::{DataSinkId, DataSinkReg, DataSinkRegHandle};
use crate::{Config, DataSink, RawRecords, Records};

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use collections::HashMap;
use kvproto::resource_usage_agent::ResourceUsageRecord;
use tikv_util::time::Duration;
use tikv_util::warn;
use tikv_util::worker::{
    Builder as WorkerBuilder, LazyWorker, Runnable, RunnableWithTimer, Scheduler,
};

/// A structure for reporting statistics through [Client].
///
/// `Reporter` implements [Runnable] and [RunnableWithTimer] to handle [Task]s from
/// the [Scheduler]. It internally aggregates the reported [RawRecords] into [Records]
/// and upload them to the remote server through the `Client`.
///
/// [Runnable]: tikv_util::worker::Runnable
/// [RunnableWithTimer]: tikv_util::worker::RunnableWithTimer
/// [Scheduler]: tikv_util::worker::Scheduler
/// [RawRecords]: crate::model::RawRecords
/// [Records]: crate::model::Records
pub struct Reporter {
    config: Config,
    scheduler: Scheduler<Task>,
    collector_reg_handle: CollectorRegHandle,
    collector: Option<CollectorGuard>,

    data_sinks: HashMap<DataSinkId, Box<dyn DataSink>>,
    records: Records,
}

impl Runnable for Reporter {
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::Records(records) => self.handle_records(records),
            Task::ConfigChange(config) => self.handle_config_change(config),
            Task::DataSinkReg(data_sink_reg) => self.handle_data_sink_reg(data_sink_reg),
        }
    }

    fn shutdown(&mut self) {
        self.reset();
    }
}

impl RunnableWithTimer for Reporter {
    fn on_timeout(&mut self) {
        self.upload();
    }

    fn get_interval(&self) -> Duration {
        self.config.report_receiver_interval.0
    }
}

impl Reporter {
    pub fn new(
        config: Config,
        collector_reg_handle: CollectorRegHandle,
        scheduler: Scheduler<Task>,
    ) -> Self {
        Self {
            config,
            scheduler,
            collector: None,
            collector_reg_handle,

            data_sinks: HashMap::default(),
            records: Records::default(),
        }
    }

    fn handle_records(&mut self, records: Arc<RawRecords>) {
        let ts = records.begin_unix_time_secs;
        if self.config.max_resource_groups >= records.records.len() {
            self.records.append(ts, records.records.iter());
            return;
        }
        let (top, evicted) = records.top_k(self.config.max_resource_groups);
        self.records.append(ts, top);
        let others = self.records.others.entry(ts).or_default();
        evicted.for_each(|(_, v)| {
            others.merge(v);
        });
    }

    fn handle_config_change(&mut self, config: Config) {
        self.config = config;
    }

    fn handle_data_sink_reg(&mut self, data_sink_reg: DataSinkReg) {
        match data_sink_reg {
            DataSinkReg::Register { id, data_sink } => {
                if self.data_sinks.len() >= 10 {
                    warn!("too many datasinks"; "count" => self.data_sinks.len());
                    return;
                }
                self.data_sinks.insert(id, data_sink);

                if self.collector.is_none() {
                    let collector = Box::new(CollectorImpl::new(self.scheduler.clone()));
                    self.collector = Some(self.collector_reg_handle.register(collector, false));
                }
            }
            DataSinkReg::Deregister { id } => {
                self.data_sinks.remove(&id);

                if self.data_sinks.is_empty() {
                    self.collector = None;
                }
            }
        }
    }

    fn upload(&mut self) {
        if self.records.is_empty() {
            return;
        }
        // Whether endpoint exists or not, records should be taken in order to reset.
        let records = std::mem::take(&mut self.records);
        let report_data: Arc<Vec<ResourceUsageRecord>> = Arc::new(records.into());

        for data_sink in self.data_sinks.values_mut() {
            if let Err(err) = data_sink.try_send(report_data.clone()) {
                warn!("failed to send data to datasink"; "error" => ?err);
            }
        }
    }

    fn reset(&mut self) {
        self.collector.take();
        self.records.clear();
    }
}

/// `Task` represents a task scheduled in [Reporter].
pub enum Task {
    Records(Arc<RawRecords>),
    ConfigChange(Config),
    DataSinkReg(DataSinkReg),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Records(_) => {
                write!(f, "Records")?;
            }
            Task::ConfigChange(_) => {
                write!(f, "ConfigChange")?;
            }
            Task::DataSinkReg(_) => {
                write!(f, "DataSinkReg")?;
            }
        }
        Ok(())
    }
}

/// [ConfigChangeNotifier] for scheduling [Task::ConfigChange]
pub struct ConfigChangeNotifier {
    scheduler: Scheduler<Task>,
}

impl ConfigChangeNotifier {
    fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }

    pub fn notify(&self, config: Config) {
        if let Err(err) = self.scheduler.schedule(Task::ConfigChange(config)) {
            warn!("failed to schedule reporter::Task::ConfigChange"; "err" => ?err);
        }
    }
}

/// Constructs a default [Recorder], start it and return the corresponding [ConfigChangeNotifier], [DataSinkRegHandle] and [LazyWorker].
///
/// This function is intended to simplify external use.
pub fn init_reporter(
    config: Config,
    collector_reg_handle: CollectorRegHandle,
) -> (
    ConfigChangeNotifier,
    DataSinkRegHandle,
    Box<LazyWorker<Task>>,
) {
    let mut reporter_worker = WorkerBuilder::new("resource-metering-reporter")
        .pending_capacity(30)
        .create()
        .lazy_build("resource-metering-reporter");
    let reporter_scheduler = reporter_worker.scheduler();
    let data_sink_reg_handle = DataSinkRegHandle::new(reporter_scheduler.clone());
    let reporter = Reporter::new(config, collector_reg_handle, reporter_scheduler.clone());
    reporter_worker.start_with_timer(reporter);
    (
        ConfigChangeNotifier::new(reporter_scheduler),
        data_sink_reg_handle,
        Box::new(reporter_worker),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::{RawRecord, TagInfos};

    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;

    use collections::HashMap;
    use kvproto::resource_usage_agent::ResourceUsageRecord;
    use tikv_util::config::ReadableDuration;
    use tikv_util::worker::{LazyWorker, Runnable, RunnableWithTimer};

    #[derive(Default, Clone)]
    struct MockDataSink {
        op_count: Arc<AtomicUsize>,
    }

    impl DataSink for MockDataSink {
        fn try_send(&mut self, _records: Arc<Vec<ResourceUsageRecord>>) -> Result<()> {
            self.op_count.fetch_add(1, SeqCst);
            Ok(())
        }
    }

    #[test]
    fn test_reporter_basic() {
        let scheduler = LazyWorker::new("test-worker").scheduler();
        let collector_reg_handle = CollectorRegHandle::new_for_test();
        let mut r = Reporter::new(Config::default(), collector_reg_handle, scheduler);

        let client = MockDataSink::default();
        r.run(Task::DataSinkReg(DataSinkReg::Register {
            id: DataSinkId(1),
            data_sink: Box::new(client.clone()),
        }));
        r.run(Task::ConfigChange(Config {
            receiver_address: "abc".to_string(),
            report_receiver_interval: ReadableDuration::minutes(2),
            max_resource_groups: 3000,
            precision: ReadableDuration::secs(2),
        }));
        assert_eq!(r.get_interval(), Duration::from_secs(120));
        let mut records = HashMap::default();
        records.insert(
            Arc::new(TagInfos {
                store_id: 0,
                region_id: 0,
                peer_id: 0,
                extra_attachment: b"12345".to_vec(),
            }),
            RawRecord {
                cpu_time: 1,
                read_keys: 2,
                write_keys: 3,
            },
        );
        r.run(Task::Records(Arc::new(RawRecords {
            begin_unix_time_secs: 123,
            duration: Duration::default(),
            records,
        })));
        r.on_timeout();
        r.shutdown();
        assert_eq!(client.op_count.load(SeqCst), 1);
    }

    #[test]
    fn test_reporter_multiple_data_sinks() {
        let scheduler = LazyWorker::new("test-worker").scheduler();
        let collector_reg_handle = CollectorRegHandle::new_for_test();
        let mut r = Reporter::new(Config::default(), collector_reg_handle, scheduler);
        let ds1 = MockDataSink::default();
        let ds2 = MockDataSink::default();
        let ds3 = MockDataSink::default();
        r.run(Task::DataSinkReg(DataSinkReg::Register {
            id: DataSinkId(1),
            data_sink: Box::new(ds1.clone()),
        }));
        r.run(Task::DataSinkReg(DataSinkReg::Register {
            id: DataSinkId(2),
            data_sink: Box::new(ds2.clone()),
        }));
        r.run(Task::DataSinkReg(DataSinkReg::Register {
            id: DataSinkId(3),
            data_sink: Box::new(ds3.clone()),
        }));

        let mut records = HashMap::default();
        records.insert(
            Arc::new(TagInfos {
                store_id: 0,
                region_id: 0,
                peer_id: 0,
                extra_attachment: b"12345".to_vec(),
            }),
            RawRecord {
                cpu_time: 1,
                read_keys: 2,
                write_keys: 3,
            },
        );

        r.run(Task::Records(Arc::new(RawRecords {
            begin_unix_time_secs: 123,
            duration: Duration::default(),
            records: records.clone(),
        })));

        r.on_timeout();
        assert_eq!(ds1.op_count.load(SeqCst), 1);
        assert_eq!(ds2.op_count.load(SeqCst), 1);
        assert_eq!(ds3.op_count.load(SeqCst), 1);

        r.run(Task::DataSinkReg(DataSinkReg::Deregister {
            id: DataSinkId(2),
        }));

        r.run(Task::Records(Arc::new(RawRecords {
            begin_unix_time_secs: 123,
            duration: Duration::default(),
            records,
        })));

        r.on_timeout();
        assert_eq!(ds1.op_count.load(SeqCst), 2);
        assert_eq!(ds2.op_count.load(SeqCst), 1);
        assert_eq!(ds3.op_count.load(SeqCst), 2);

        r.shutdown();
    }
}
