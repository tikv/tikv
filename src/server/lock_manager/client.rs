// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use futures::{
    channel::mpsc::{self, UnboundedSender},
    future::{self, BoxFuture},
    sink::SinkExt,
    stream::{StreamExt, TryStreamExt},
};
use grpcio::{ChannelBuilder, EnvBuilder, Environment, WriteFlags};
use kvproto::deadlock::*;
use security::SecurityManager;

use super::{Error, Result};

type DeadlockFuture<T> = BoxFuture<'static, Result<T>>;

pub type Callback = Box<dyn Fn(DeadlockResponse) + Send>;

const CQ_COUNT: usize = 1;
const CLIENT_PREFIX: &str = "deadlock";

/// Builds the `Environment` of deadlock clients. All clients should use the same instance.
pub fn env() -> Arc<Environment> {
    Arc::new(
        EnvBuilder::new()
            .cq_count(CQ_COUNT)
            .name_prefix(thd_name!(CLIENT_PREFIX))
            .build(),
    )
}

#[derive(Clone)]
pub struct Client {
    client: DeadlockClient,
    sender: Option<UnboundedSender<DeadlockRequest>>,
}

impl Client {
    pub fn new(env: Arc<Environment>, security_mgr: Arc<SecurityManager>, addr: &str) -> Self {
        let cb = ChannelBuilder::new(env)
            .keepalive_time(Duration::from_secs(10))
            .keepalive_timeout(Duration::from_secs(3));
        let channel = security_mgr.connect(cb, addr);
        let client = DeadlockClient::new(channel);
        Self {
            client,
            sender: None,
        }
    }

    pub fn register_detect_handler(
        &mut self,
        cb: Callback,
    ) -> (DeadlockFuture<()>, DeadlockFuture<()>) {
        let (tx, rx) = mpsc::unbounded();
        let (sink, receiver) = self.client.detect().unwrap();
        let send_task = Box::pin(async move {
            let mut sink = sink.sink_map_err(Error::Grpc);
            let res = sink
                .send_all(&mut rx.map(|r| Ok((r, WriteFlags::default()))))
                .await
                .map(|_| {
                    info!("cancel detect sender");
                    sink.get_mut().cancel();
                });
            res
        });
        self.sender = Some(tx);

        let recv_task = Box::pin(receiver.map_err(Error::Grpc).try_for_each(move |resp| {
            cb(resp);
            future::ok(())
        }));

        (send_task, recv_task)
    }

    pub fn detect(&self, req: DeadlockRequest) -> Result<()> {
        self.sender
            .as_ref()
            .unwrap()
            .unbounded_send(req)
            .map_err(|e| Error::Other(box_err!(e)))
    }
}
