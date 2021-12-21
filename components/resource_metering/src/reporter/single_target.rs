// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::error::Result;
use crate::metrics::{IGNORED_DATA_COUNTER, REPORT_DATA_COUNTER, REPORT_DURATION_HISTOGRAM};
use crate::model::Records;
use crate::reporter::data_sink::DataSink;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{ResourceUsageAgentClient, ResourceUsageRecord};
use tikv_util::warn;

/// `SingleTargetDataSink` is the default implementation of [DataSink], which uses gRPC
/// to report data to the remote end.
pub struct SingleTargetDataSink {
    env: Arc<Environment>,

    address: Arc<ArcSwap<String>>,
    current_address: Arc<String>,

    client: Option<ResourceUsageAgentClient>,
    limiter: Limiter,
}

impl SingleTargetDataSink {
    pub fn new(address: Arc<ArcSwap<String>>, env: Arc<Environment>) -> Self {
        let current_address = address.load_full();
        Self {
            env,

            address,
            current_address,

            client: None,
            limiter: Limiter::default(),
        }
    }
}

impl DataSink for SingleTargetDataSink {
    fn try_send(&mut self, records: Records) -> Result<()> {
        let record_cnt = records.records.len() + if records.others.is_empty() { 0 } else { 1 };

        let new_address = self.address.load_full();
        if new_address.is_empty() {
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(record_cnt as _);
            return Err("receiver address is empty".into());
        }

        let handle = self.limiter.try_acquire();
        if handle.is_none() {
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(record_cnt as _);
            return Err("the last report has not been completed".into());
        }
        if new_address != self.current_address || self.client.is_none() {
            self.current_address = new_address;
            self.init_client();
        }
        let client = self.client.as_ref().unwrap();
        let call_opt = CallOption::default().timeout(Duration::from_secs(2));
        let call = client.report_opt(call_opt);
        if let Err(err) = &call {
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(record_cnt as _);
            return Err(format!("{}", err).into());
        }
        let (mut tx, rx) = call.unwrap();
        client.spawn(async move {
            let _hd = handle;

            let _t = REPORT_DURATION_HISTOGRAM.start_timer();
            let others = records.others;
            REPORT_DATA_COUNTER
                .with_label_values(&["to_send"])
                .inc_by(record_cnt as _);
            for (tag, record) in records.records {
                let mut req = ResourceUsageRecord::default();
                req.set_resource_group_tag(tag);
                req.set_record_list_timestamp_sec(record.timestamps);
                req.set_record_list_cpu_time_ms(record.cpu_time_list);
                if let Err(err) = tx.send((req, WriteFlags::default())).await {
                    warn!("failed to send records"; "error" => ?err);
                    return;
                }
                REPORT_DATA_COUNTER.with_label_values(&["sent"]).inc();
            }
            if !others.is_empty() {
                let mut req = ResourceUsageRecord::default();
                req.set_record_list_timestamp_sec(others.keys().copied().collect());
                req.set_record_list_cpu_time_ms(others.values().map(|r| r.cpu_time).collect());
                if let Err(err) = tx.send((req, WriteFlags::default())).await {
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

        Ok(())
    }
}

impl SingleTargetDataSink {
    pub fn set_env(&mut self, env: Arc<Environment>) {
        self.env = env;
    }

    fn init_client(&mut self) {
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            cb.connect(&self.current_address)
        };
        self.client = Some(ResourceUsageAgentClient::new(channel));
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
