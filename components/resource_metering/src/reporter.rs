// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::collector::{register_collector, CollectorHandle, CollectorImpl};
use crate::metrics::REPORT_DATA_COUNTER;
use crate::{Client, Config, RawRecords, RecorderController, Records};

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use crossbeam::channel::{bounded, Receiver, Sender};
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
pub struct Reporter {
    client_registry: ClientRegistry,
    client_receiver: Receiver<Box<dyn Client>>,
    clients: Vec<Box<dyn Client>>,

    config: Config,
    _collector: CollectorHandle,
    records: Records,

    recorder_ctl: RecorderController,
}

impl Runnable for Reporter {
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

impl RunnableWithTimer for Reporter {
    fn on_timeout(&mut self) {
        self.handle_client_register();
        self.upload();
    }

    fn get_interval(&self) -> Duration {
        self.config.report_receiver_interval.0
    }
}

impl Reporter {
    pub fn new(
        clients: Vec<Box<dyn Client>>,
        config: Config,
        scheduler: Scheduler<Task>,
        recorder_ctl: RecorderController,
    ) -> Self {
        let clt = CollectorImpl::new(scheduler);
        let collector = register_collector(Box::new(clt));

        let (client_sender, client_receiver) = bounded(64);
        let client_registry = ClientRegistry::new(client_sender);

        Self {
            client_registry,
            client_receiver,
            clients,

            config,
            _collector: collector,
            records: Records::default(),

            recorder_ctl,
        }
    }

    pub fn client_registry(&self) -> ClientRegistry {
        self.client_registry.clone()
    }

    fn handle_records(&mut self, records: Arc<RawRecords>) {
        self.records.append(records);
        self.records.keep_top_k(self.config.max_resource_groups);
    }

    fn handle_config_change(&mut self, config: Config) {
        self.config = config;
    }

    fn handle_client_register(&mut self) {
        for c in self.client_receiver.try_iter() {
            self.clients.push(c);
        }

        // remove closed clients
        self.clients.drain_filter(|c| c.is_closed()).count();

        if self.clients.len() > 256 {
            warn!("too many clients"; "len" => self.clients.len());
        }

        let pending_cnt = self.clients.iter().filter(|c| c.is_pending()).count();
        let running_cnt = self.clients.len() - pending_cnt;

        // sampling can be paused if no clients case about the results
        if running_cnt > 0 {
            self.recorder_ctl.resume();
        } else {
            self.recorder_ctl.pause();
        }
    }

    fn upload(&mut self) {
        if self.records.is_empty() {
            return;
        }

        // No matter clients exist or not, records should be taken in order to reset.
        let records = Arc::new(std::mem::take(&mut self.records));
        REPORT_DATA_COUNTER
            .with_label_values(&["collected"])
            .inc_by(records.len() as _);
        for c in &mut self.clients {
            c.as_mut().upload_records(records.clone());
        }
    }

    fn reset(&mut self) {
        self.records.clear();
        self.clients.clear();
    }
}

#[derive(Clone)]
pub struct ClientRegistry {
    tx: Sender<Box<dyn Client>>,
}

impl ClientRegistry {
    pub fn new(tx: Sender<Box<dyn Client>>) -> Self {
        Self { tx }
    }

    pub fn register(&self, client: Box<dyn Client>) {
        self.tx.send(client).ok();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RawRecord, ResourceMeteringTag, TagInfos};

    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;

    use collections::HashMap;
    use tikv_util::config::ReadableDuration;
    use tikv_util::worker::{LazyWorker, Runnable, RunnableWithTimer};

    static OP_COUNT: AtomicUsize = AtomicUsize::new(0);

    struct MockClient;

    impl Client for MockClient {
        fn upload_records(&mut self, _records: Arc<Records>) {
            OP_COUNT.fetch_add(1, SeqCst);
        }
    }

    #[test]
    fn test_reporter() {
        let scheduler = LazyWorker::new("test-worker").scheduler();
        let mut r = Reporter::new(
            vec![Box::new(MockClient)],
            Config::default(),
            scheduler,
            RecorderController::default(),
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
