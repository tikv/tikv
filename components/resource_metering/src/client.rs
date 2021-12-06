// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::metrics::{IGNORED_DATA_COUNTER, REPORT_DATA_COUNTER, REPORT_DURATION_HISTOGRAM};
use crate::model::Records;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{ResourceUsageAgentClient, ResourceUsageRecord};
use tikv_util::warn;

/// This trait abstracts the interface to communicate with the remote.
/// We can simply mock this interface to test without RPC.
pub trait Client: Send {
    fn upload_records(&mut self, records: Arc<Records>);

    // `is_pending` indicates that the client is not expecting to receive
    // records for now and may resume in the future.
    fn is_pending(&self) -> bool {
        false
    }

    // `is_down` indicates that the client has been closed and can be cleared.
    // Note that: once a client is down, it cannot go back to be up.
    fn is_down(&self) -> bool {
        false
    }
}

/// `GrpcClient` is the default implementation of [Client], which uses gRPC
/// to report data to the remote end.
#[derive(Clone)]
pub struct GrpcClient {
    env: Arc<Environment>,
    client: Option<ResourceUsageAgentClient>,
    limiter: Limiter,

    address: Arc<Mutex<String>>,
    prev_address: String,
}

impl Client for GrpcClient {
    fn upload_records(&mut self, records: Arc<Records>) {
        let address = { self.address.lock().unwrap().clone() };
        let record_cnt = records.records.len() + if records.others.is_empty() { 0 } else { 1 };

        if address.is_empty() {
            self.client.take();
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(record_cnt as _);
            warn!("receiver address is empty, discarding the new report data");
            return;
        }

        let handle = self.limiter.try_acquire();
        if handle.is_none() {
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(record_cnt as _);
            warn!("the last report has not been completed, discarding the new report data");
            return;
        }
        // address won't be empty here
        if self.client.is_none() || self.prev_address != address {
            self.init_client(&address);
            self.prev_address = address;
        }
        let client = self.client.as_ref().unwrap();
        let call_opt = CallOption::default().timeout(Duration::from_secs(2));
        let call = client.report_opt(call_opt);
        if let Err(err) = &call {
            IGNORED_DATA_COUNTER
                .with_label_values(&["report"])
                .inc_by(record_cnt as _);
            warn!("failed to connect to receiver"; "error" => ?err);
            return;
        }
        let (mut tx, rx) = call.unwrap();
        client.spawn(async move {
            let _hd = handle;

            let _t = REPORT_DURATION_HISTOGRAM.start_timer();
            REPORT_DATA_COUNTER
                .with_label_values(&["to_send"])
                .inc_by(record_cnt as _);
            for (tag, record) in &records.records {
                let mut req = ResourceUsageRecord::default();
                req.set_resource_group_tag(tag.clone());
                req.set_record_list_timestamp_sec(record.timestamps.clone());
                req.set_record_list_cpu_time_ms(record.cpu_time_list.clone());
                if let Err(err) = tx.send((req, WriteFlags::default())).await {
                    warn!("failed to send records"; "error" => ?err);
                    return;
                }
                REPORT_DATA_COUNTER.with_label_values(&["sent"]).inc();
            }
            let others = &records.others;
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
    }

    fn is_pending(&self) -> bool {
        self.address.lock().unwrap().is_empty()
    }
}

impl GrpcClient {
    pub fn new(env: Arc<Environment>, address: Arc<Mutex<String>>) -> Self {
        Self {
            env,
            client: None,
            limiter: Limiter::default(),
            address,
            prev_address: "".to_owned(),
        }
    }

    fn init_client(&mut self, address: &str) {
        assert!(!address.is_empty());
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            cb.connect(address)
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
