// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::util::Limiter;
use super::Endpoint;
use crate::cpu::reporter::record::Records;

use std::sync::Arc;
use std::time::Duration;

use futures::SinkExt;
use grpcio::{CallOption, ChannelBuilder, Environment, WriteFlags};
use kvproto::resource_usage_agent::{CpuTimeRecord, ResourceUsageAgentClient};

pub struct GRPCEndpoint {
    env: Arc<Environment>,
    address: String,
    client: ResourceUsageAgentClient,
    limiter: Limiter,
}

impl Endpoint for GRPCEndpoint {
    fn init(address: &str) -> Box<dyn Endpoint> {
        let env = Arc::new(Environment::new(2));
        let channel = {
            let cb = ChannelBuilder::new(env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            cb.connect(address)
        };
        let client = ResourceUsageAgentClient::new(channel);

        Box::new(Self {
            env,
            client,
            address: address.to_owned(),
            limiter: Limiter::default(),
        })
    }

    fn update(&mut self, address: &str) {
        if self.address == address {
            return;
        }

        let channel = {
            let cb = ChannelBuilder::new(self.env.clone())
                .keepalive_time(Duration::from_secs(10))
                .keepalive_timeout(Duration::from_secs(3));
            cb.connect(address)
        };
        self.client = ResourceUsageAgentClient::new(channel);
    }

    fn report(&mut self, records: Records) {
        let handle = self.limiter.try_acquire();
        if handle.is_none() {
            return;
        }

        let call_opt = CallOption::default().timeout(Duration::from_secs(2));
        let call = self.client.report_cpu_time_opt(call_opt);
        if let Err(err) = &call {
            warn!("failed to connect to agent"; "error" => ?err);
            return;
        }

        let (mut tx, rx) = call.unwrap();
        self.client.spawn(async move {
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
}
