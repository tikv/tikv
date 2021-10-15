// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::model::Records;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{ResourceUsageAgentClient, ResourceUsageRecord};
use tikv_util::warn;

/// This trait abstracts the interface to communicate with the remote.
/// We can simply mock this interface to test without RPC.
pub trait Client {
    fn upload_records(&mut self, address: &str, records: Records);
}

/// `GrpcClient` is the default implementation of [Client], which uses gRPC
/// to report data to the remote end.
#[derive(Clone)]
pub struct GrpcClient {
    env: Arc<Environment>,
    address: String,
    client: Option<ResourceUsageAgentClient>,
    limiter: Limiter,
}

impl Default for GrpcClient {
    fn default() -> Self {
        Self {
            env: Arc::new(Environment::new(2)),
            address: "".to_owned(),
            client: None,
            limiter: Limiter::default(),
        }
    }
}

impl Client for GrpcClient {
    fn upload_records(&mut self, address: &str, records: Records) {
        if address.is_empty() {
            return;
        }
        let handle = self.limiter.try_acquire();
        if handle.is_none() {
            return;
        }
        if self.address != address || self.client.is_none() {
            self.address = address.to_owned();
            self.init_client();
        }
        let client = self.client.as_ref().unwrap();
        let call_opt = CallOption::default().timeout(Duration::from_secs(2));
        let call = client.report_opt(call_opt);
        if let Err(err) = &call {
            warn!("failed to connect to receiver"; "error" => ?err);
            return;
        }
        let (mut tx, rx) = call.unwrap();
        client.spawn(async move {
            let _hd = handle;
            let others = records.others;
            for (tag, record) in records.records {
                let mut req = ResourceUsageRecord::default();
                req.set_resource_group_tag(tag);
                req.set_record_list_timestamp_sec(record.timestamps);
                req.set_record_list_cpu_time_ms(record.cpu_time_list);
                if let Err(err) = tx.send((req, WriteFlags::default())).await {
                    warn!("failed to send records"; "error" => ?err);
                    return;
                }
            }
            if !others.is_empty() {
                let mut req = ResourceUsageRecord::default();
                req.set_record_list_timestamp_sec(others.keys().copied().collect());
                req.set_record_list_cpu_time_ms(others.values().map(|r| r.cpu_time).collect());
                if let Err(err) = tx.send((req, WriteFlags::default())).await {
                    warn!("failed to send records"; "error" => ?err);
                    return;
                }
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
}

impl GrpcClient {
    pub fn set_env(&mut self, env: Arc<Environment>) {
        self.env = env;
    }

    fn init_client(&mut self) {
        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            cb.connect(&self.address)
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
