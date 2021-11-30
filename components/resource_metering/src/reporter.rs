// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::{register_collector, CollectorHandle, CollectorImpl};
use crate::{Client, Config, RawRecords, Records};

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use tikv_util::time::Duration;
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
pub struct Reporter<C> {
    client: C,
    config: Config,
    scheduler: Scheduler<Task>,
    collector: Option<CollectorHandle>,
    records: Records,
}

impl<C> Runnable for Reporter<C>
where
    C: Client + Send,
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

impl<C> RunnableWithTimer for Reporter<C>
where
    C: Client + Send,
{
    fn on_timeout(&mut self) {
        self.upload();
    }

    fn get_interval(&self) -> Duration {
        self.config.report_receiver_interval.0
    }
}

impl<C> Reporter<C>
where
    C: Client + Send,
{
    pub fn new(client: C, config: Config, scheduler: Scheduler<Task>) -> Self {
        let collector = config
            .should_report()
            .then(|| register_collector(Box::new(CollectorImpl::new(scheduler.clone()))));
        Self {
            client,
            config,
            scheduler,
            collector,
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
            self.collector = Some(register_collector(Box::new(CollectorImpl::new(
                self.scheduler.clone(),
            ))));
        }
    }

    fn upload(&mut self) {
        if self.records.is_empty() {
            return;
        }
        // Whether endpoint exists or not, records should be taken in order to reset.
        let records = std::mem::take(&mut self.records);
        self.client
            .upload_records(&self.config.receiver_address, records);
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
    use crate::{RawRecord, ResourceMeteringTag, TagInfos};
    use collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use tikv_util::config::ReadableDuration;
    use tikv_util::worker::{LazyWorker, Runnable, RunnableWithTimer};

    static OP_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct MockClient;

    impl Client for MockClient {
        fn upload_records(&mut self, address: &str, _records: Records) {
            assert_eq!(address, "abc");
            OP_COUNT.fetch_add(1, SeqCst);
        }
    }

    #[test]
    fn test_reporter() {
        let scheduler = LazyWorker::new("test-worker").scheduler();
        let mut r = Reporter::new(MockClient, Config::default(), scheduler);
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
            ResourceMeteringTag {
                infos: Arc::new(TagInfos {
                    store_id: 0,
                    region_id: 0,
                    peer_id: 0,
                    extra_attachment: b"12345".to_vec(),
                }),
            },
            RawRecord { cpu_time: 1 },
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
