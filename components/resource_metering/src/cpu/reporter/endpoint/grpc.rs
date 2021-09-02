// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::util::Limiter;
use super::Endpoint;
use crate::cpu::reporter::record::Records;

use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{CpuTimeRecord, ResourceUsageAgentClient};
use tikv_util::warn;

pub struct GRPCEndpoint {
    env: Arc<Environment>,
    address: String,
    client: Option<ResourceUsageAgentClient>,
    limiter: Limiter,
}

impl Endpoint for GRPCEndpoint {
    fn report(&mut self, _instance_name: &str, address: &str, records: Records) {
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

    fn name(&self) -> &'static str {
        "grpc"
    }
}

impl GRPCEndpoint {
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

impl Default for GRPCEndpoint {
    fn default() -> Self {
        Self {
            env: Arc::new(Environment::new(2)),
            address: "".to_owned(),
            client: None,
            limiter: Limiter::default(),
        }
    }
}
