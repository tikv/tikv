// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod data_sink;
pub mod single_target;

use crate::collector::{CollectorHandle, CollectorImpl, CollectorRegHandle};
use crate::{Config, DataSink, RawRecords, Records};

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use tikv_util::time::Duration;
use tikv_util::warn;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};

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
pub struct Reporter<D> {
    data_sink: D,
    config: Config,
    scheduler: Scheduler<Task>,
    collector_reg_handle: CollectorRegHandle,
    collector: Option<CollectorHandle>,
    records: Records,
}

impl<D> Runnable for Reporter<D>
where
    D: DataSink + Send,
{
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::Records(records) => self.handle_records(records),
            Task::ConfigChange(config) => self.handle_config_change(config),
        }
    }

    fn shutdown(&mut self) {
        self.reset();
    }
}

impl<D> RunnableWithTimer for Reporter<D>
where
    D: DataSink + Send,
{
    fn on_timeout(&mut self) {
        self.upload();
    }

    fn get_interval(&self) -> Duration {
        self.config.report_receiver_interval.0
    }
}

impl<D> Reporter<D>
where
    D: DataSink + Send,
{
    pub fn new(
        data_sink: D,
        config: Config,
        collector_reg_handle: CollectorRegHandle,
        scheduler: Scheduler<Task>,
    ) -> Self {
        let collector = config.should_report().then(|| {
            collector_reg_handle.register(Box::new(CollectorImpl::new(scheduler.clone())))
        });
        Self {
            data_sink,
            config,
            scheduler,
            collector,
            collector_reg_handle,
            records: Records::default(),
        }
    }

    fn handle_records(&mut self, records: Arc<RawRecords>) {
        self.records.append(records);
        self.records.keep_top_k(self.config.max_resource_groups);
    }

    fn handle_config_change(&mut self, config: Config) {
        self.config = config;
        if !self.config.should_report() {
            self.reset();
        }
        if self.collector.is_none() {
            self.collector = Some(
                self.collector_reg_handle
                    .register(Box::new(CollectorImpl::new(self.scheduler.clone()))),
            );
        }
    }

    fn upload(&mut self) {
        if self.records.is_empty() {
            return;
        }
        // Whether endpoint exists or not, records should be taken in order to reset.
        let records = std::mem::take(&mut self.records);
        let report_data = Arc::new(records.into());
        if let Err(err) = self.data_sink.try_send(report_data) {
            warn!("failed to send data to datasink"; "error" => ?err);
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
        }
        Ok(())
    }
}

// Helper functions.
impl Config {
    fn should_report(&self) -> bool {
        self.enabled && !self.receiver_address.is_empty() && self.max_resource_groups != 0
    }
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

    static OP_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct MockClient;

    impl DataSink for MockClient {
        fn try_send(&mut self, _records: Arc<Vec<ResourceUsageRecord>>) -> Result<()> {
            OP_COUNT.fetch_add(1, SeqCst);
            Ok(())
        }
    }

    #[test]
    fn test_reporter() {
        let scheduler = LazyWorker::new("test-worker").scheduler();
        let collector_reg_handle = CollectorRegHandle::new_for_test();
        let mut r = Reporter::new(
            MockClient,
            Config::default(),
            collector_reg_handle,
            scheduler,
        );
        r.run(Task::ConfigChange(Config {
            enabled: false,
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
        assert_eq!(OP_COUNT.load(SeqCst), 1);
    }
}
