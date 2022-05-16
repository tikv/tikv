// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{ResourceUsageAgentClient, ResourceUsageRecord};
use tikv_util::{
    warn,
    worker::{Builder as WorkerBuilder, LazyWorker, Runnable, Scheduler},
};

use crate::{
    error::Result,
    metrics::{IGNORED_DATA_COUNTER, REPORT_DATA_COUNTER, REPORT_DURATION_HISTOGRAM},
    reporter::{
        data_sink::DataSink,
        data_sink_reg::{DataSinkGuard, DataSinkRegHandle},
    },
};

impl Runnable for SingleTargetDataSink {
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::Records(records) => self.handle_records(records),
            Task::ChangeAddress(address) => self.update_data_sink(address),
        }
    }

    fn shutdown(&mut self) {
        self.reset();
    }
}

/// `SingleTargetDataSink` is the default implementation of [DataSink], which uses gRPC
/// to report data to the remote end.
pub struct SingleTargetDataSink {
    scheduler: Scheduler<Task>,
    data_sink_reg: DataSinkRegHandle,
    data_sink: Option<DataSinkGuard>,

    env: Arc<Environment>,
    client: Option<ResourceUsageAgentClient>,
    limiter: Limiter,

    address: String,
}

impl SingleTargetDataSink {
    pub fn new(
        address: String,
        env: Arc<Environment>,
        data_sink_reg: DataSinkRegHandle,
        scheduler: Scheduler<Task>,
    ) -> Self {
        let mut single_target = Self {
            scheduler,
            data_sink_reg,
            data_sink: None,

            env,
            client: None,
            limiter: Limiter::default(),

            address: String::default(),
        };

        single_target.update_data_sink(address);
        single_target
    }

    fn handle_records(&mut self, records: Arc<Vec<ResourceUsageRecord>>) {
        let handle = self.limiter.try_acquire();
        if handle.is_none() {
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(records.len() as _);
            warn!("the last report has not been completed");
            return;
        }

        if self.address.is_empty() {
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(records.len() as _);
            warn!("the client of single target datasink is not ready");
            return;
        }

        if self.client.is_none() {
            let channel = {
                let cb = ChannelBuilder::new(self.env.clone())
                    .keepalive_time(Duration::from_secs(10))
                    .keepalive_timeout(Duration::from_secs(3));
                cb.connect(&self.address)
            };
            self.client = Some(ResourceUsageAgentClient::new(channel));
        }

        let client = self.client.as_ref().unwrap();
        let call_opt = CallOption::default().timeout(Duration::from_secs(2));
        let call = client.report_opt(call_opt);
        if let Err(err) = &call {
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(records.len() as _);
            warn!("failed to call report"; "err" => ?err);
            return;
        }
        let (mut tx, rx) = call.unwrap();
        client.spawn(async move {
            let _hd = handle;

            let _t = REPORT_DURATION_HISTOGRAM.start_timer();
            REPORT_DATA_COUNTER
                .with_label_values(&["to_send"])
                .inc_by(records.len() as _);
            for record in records.iter() {
                if let Err(err) = tx.send((record.clone(), WriteFlags::default())).await {
                    warn!("failed to send records"; "error" => ?err);
                    return;
                }
                REPORT_DATA_COUNTER.with_label_values(&["sent"]).inc();
            }
            if let Err(err) = tx.close().await {
                warn!("failed to close a grpc call"; "error" => ?err);
                return;
            }
            if let Err(err) = rx.await {
                warn!("failed to receive from a grpc call"; "error" => ?err);
            }
        });
    }

    fn update_data_sink(&mut self, new_address: String) {
        if new_address.is_empty() {
            self.reset();
            return;
        }

        if self.address != new_address {
            self.address = new_address;
            self.client = None; // discard previous connection
        }

        if self.data_sink.is_none() {
            let data_sink = Box::new(DataSinkImpl {
                scheduler: self.scheduler.clone(),
            });
            self.data_sink = Some(self.data_sink_reg.register(data_sink));
        }
    }

    fn reset(&mut self) {
        self.data_sink = None;
        self.client = None;
        self.address.clear();
    }
}

/// `Task` represents a task scheduled in [SingleTargetDataSink].
pub enum Task {
    Records(Arc<Vec<ResourceUsageRecord>>),
    ChangeAddress(String),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Records(_) => {
                write!(f, "Records")?;
            }
            Task::ChangeAddress(_) => {
                write!(f, "AddressChange")?;
            }
        }
        Ok(())
    }
}

/// A [DataSink] implementation for scheduling [Task::Records].
struct DataSinkImpl {
    scheduler: Scheduler<Task>,
}

impl DataSink for DataSinkImpl {
    fn try_send(&mut self, records: Arc<Vec<ResourceUsageRecord>>) -> Result<()> {
        let record_cnt = records.len();
        if self.scheduler.schedule(Task::Records(records)).is_err() {
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(record_cnt as _);
            Err("failed to schedule Task::Records".into())
        } else {
            Ok(())
        }
    }
}

/// [AddressChangeNotifier] for scheduling [Task::ChangeAddress]
pub struct AddressChangeNotifier {
    scheduler: Scheduler<Task>,
}

impl AddressChangeNotifier {
    fn new(scheduler: Scheduler<Task>) -> Self {
        Self { scheduler }
    }

    pub fn notify(&self, address: String) {
        if let Err(err) = self.scheduler.schedule(Task::ChangeAddress(address)) {
            warn!("failed to schedule Task::ChangeAddress"; "err" => ?err);
        }
    }
}

#[derive(Clone, Default)]
struct Limiter {
    is_acquired: Arc<AtomicBool>,
}

impl Limiter {
    pub fn try_acquire(&self) -> Option<Guard> {
        (!self.is_acquired.swap(true, Ordering::Relaxed)).then(|| Guard {
            acquired: self.is_acquired.clone(),
        })
    }
}

struct Guard {
    acquired: Arc<AtomicBool>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        assert!(self.acquired.swap(false, Ordering::Relaxed));
    }
}

/// Constructs a default [SingleTargetDataSink], start it and return the corresponding [AddressChangeNotifier]
/// and [LazyWorker].
///
/// This function is intended to simplify external use.
pub fn init_single_target(
    address: String,
    env: Arc<Environment>,
    data_sink_reg: DataSinkRegHandle,
) -> (AddressChangeNotifier, Box<LazyWorker<Task>>) {
    let mut single_target_worker = WorkerBuilder::new("resource-metering-single-target-data-sink")
        .pending_capacity(10)
        .create()
        .lazy_build("resource-metering-single-target-data-sink");
    let single_target_scheduler = single_target_worker.scheduler();
    let single_target =
        SingleTargetDataSink::new(address, env, data_sink_reg, single_target_scheduler.clone());
    single_target_worker.start(single_target);
    (
        AddressChangeNotifier::new(single_target_scheduler),
        Box::new(single_target_worker),
    )
}
