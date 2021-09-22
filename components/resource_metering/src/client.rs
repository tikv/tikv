// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::CpuRecords;
use crate::summary::SummaryRecord;
use collections::HashMap;
use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{
    CpuTimeRecord, ResourceUsageAgentClient, SummaryRecord as PbSummaryRecord,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tikv_util::warn;

// The code here is migrated from zhongzc/topsql-vm in advance.
//
// TODO(mornyx): integration with zhongzc/topsql-vm.
//

/// This trait abstracts the interface to communicate with the remote.
/// We can simply mock this interface to test without RPC.
pub trait Client {
    fn upload_cpu_records(&mut self, _address: &str, _records: CpuRecords) {}

    fn upload_summary_records(
        &mut self,
        _address: &str,
        _records: HashMap<Vec<u8>, SummaryRecord>,
    ) {
    }
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
    fn upload_cpu_records(&mut self, address: &str, records: CpuRecords) {
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
        let call = client.report_cpu_time_opt(call_opt);
        if let Err(err) = &call {
            warn!("failed to connect to agent"; "error" => ?err);
            return;
        }
        let (mut tx, rx) = call.unwrap();
        client.spawn(async move {
            let _hd = handle;
            let others = records.others;
            let records = records.records;
            for (tag, (timestamp_list, cpu_time_ms_list, _)) in records {
                let mut req = CpuTimeRecord::default();
                req.set_resource_group_tag(tag);
                req.set_record_list_timestamp_sec(timestamp_list);
                req.set_record_list_cpu_time_ms(cpu_time_ms_list);
                if let Err(err) = tx.send((req, WriteFlags::default())).await {
                    warn!("failed to send cpu records"; "error" => ?err);
                    return;
                }
            }
            // others
            if !others.is_empty() {
                let timestamp_list = others.keys().cloned().collect::<Vec<_>>();
                let cpu_time_ms_list = others.values().cloned().collect::<Vec<_>>();
                let mut req = CpuTimeRecord::default();
                req.set_record_list_timestamp_sec(timestamp_list);
                req.set_record_list_cpu_time_ms(cpu_time_ms_list);
                if let Err(err) = tx.send((req, WriteFlags::default())).await {
                    warn!("failed to send cpu records"; "error" => ?err);
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

    fn upload_summary_records(&mut self, address: &str, records: HashMap<Vec<u8>, SummaryRecord>) {
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
        let call = client.report_summary_opt(call_opt);
        if let Err(err) = &call {
            warn!("failed to connect to agent"; "error" => ?err);
            return;
        }
        let (mut tx, rx) = call.unwrap();
        client.spawn(async move {
            let _hd = handle;
            for (tag, record) in records {
                let mut req = PbSummaryRecord::default();
                req.set_resource_group_tag(tag);
                req.set_read_keys(record.r_count.load(Ordering::Relaxed));
                req.set_write_keys(record.w_count.load(Ordering::Relaxed));
                if req.get_read_keys() > 0 || req.get_write_keys() > 0 {
                    if let Err(err) = tx.send((req, WriteFlags::default())).await {
                        warn!("failed to send summary record"; "error" => ?err);
                        return;
                    }
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
pub struct Limiter {
    is_acquired: Arc<AtomicBool>,
}

impl Limiter {
    pub fn try_acquire(&self) -> Option<Guard> {
        (!self.is_acquired.swap(true, Ordering::Relaxed)).then(|| Guard {
            acquired: self.is_acquired.clone(),
        })
    }
}

pub struct Guard {
    acquired: Arc<AtomicBool>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        assert!(self.acquired.swap(false, Ordering::Relaxed));
    }
}
