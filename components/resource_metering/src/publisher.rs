// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::reporter::ClientRegistry;
use crate::{Client, Records, Reporter};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::channel::mpsc::{channel, Sender};
use futures::{SinkExt, StreamExt};
use grpcio::{RpcContext, ServerStreamingSink, WriteFlags};
use kvproto::resource_usage_agent::{CpuTimeRecord, Request};
use kvproto::resource_usage_agent_grpc::ResourceMeteringPubSub;
use tikv_util::{defer, warn};

#[derive(Clone)]
pub struct ResourceMeteringPublisher {
    client_registry: ClientRegistry,
}

impl ResourceMeteringPublisher {
    pub fn new(reporter: &Reporter) -> Self {
        Self {
            client_registry: reporter.client_registry(),
        }
    }
}

impl ResourceMeteringPubSub for ResourceMeteringPublisher {
    fn sub_cpu_time_record(
        &mut self,
        ctx: RpcContext,
        _: Request,
        mut sink: ServerStreamingSink<CpuTimeRecord>,
    ) {
        let is_closed = Arc::new(AtomicBool::new(false));
        let (tx, mut rx) = channel(1);

        self.client_registry.register(Box::new(PubClient {
            tx,
            is_closed: is_closed.clone(),
        }));

        ctx.spawn(async move {
            defer! {{
                is_closed.store(true, Ordering::SeqCst);
            }}
            loop {
                let records = rx.next().await;
                if records.is_none() {
                    break;
                }

                let records: Arc<Records> = records.unwrap();
                for (tag, record) in &records.records {
                    let mut req = CpuTimeRecord::default();
                    req.set_resource_group_tag(tag.clone());
                    req.set_record_list_timestamp_sec(record.timestamps.clone());
                    req.set_record_list_cpu_time_ms(record.cpu_time_list.clone());
                    if let Err(err) = sink.send((req, WriteFlags::default())).await {
                        warn!("failed to send records"; "error" => ?err);
                        return;
                    };
                }
                let others = &records.others;
                if !others.is_empty() {
                    let mut req = CpuTimeRecord::default();
                    req.set_record_list_timestamp_sec(others.keys().copied().collect());
                    req.set_record_list_cpu_time_ms(others.values().map(|r| r.cpu_time).collect());
                    if let Err(err) = sink.send((req, WriteFlags::default())).await {
                        warn!("failed to send records"; "error" => ?err);
                        return;
                    }
                }
            }
        });
    }
}

struct PubClient {
    tx: Sender<Arc<Records>>,
    is_closed: Arc<AtomicBool>,
}

impl Client for PubClient {
    fn upload_records(&mut self, records: Arc<Records>) {
        self.tx.try_send(records).ok();
    }

    fn is_pending(&self) -> bool {
        false
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::SeqCst)
    }
}
