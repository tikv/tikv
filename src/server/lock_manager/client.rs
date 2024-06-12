// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use futures::{
    channel::mpsc::{self, UnboundedSender},
    future::{self, BoxFuture},
    sink::SinkExt,
    stream::{StreamExt, TryStreamExt},
};
use grpcio::{EnvBuilder, Environment};
use kvproto::deadlock::{deadlock_client::DeadlockClient, *};
use security::SecurityManager;
use tonic::transport::Channel;

use super::{Error, Result};

type DeadlockFuture<T> = BoxFuture<'static, Result<T>>;

pub type Callback = Box<dyn Fn(DeadlockResponse) + Send>;

const CQ_COUNT: usize = 1;
const CLIENT_PREFIX: &str = "deadlock";

/// Builds the `Environment` of deadlock clients. All clients should use the
/// same instance.
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
    client: DeadlockClient<Channel>,
    sender: Option<UnboundedSender<DeadlockRequest>>,
}

impl Client {
    pub fn new(
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        addr: &str,
        handle: tokio::runtime::Handle,
    ) -> Self {
        let channel = Channel::from_shared(addr.to_owned())
            .unwrap()
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(3))
            .executor(tikv_util::RuntimeExec::new(handle))
            .connect_lazy();
        let client = DeadlockClient::new(channel);
        Self {
            client,
            sender: None,
        }
    }

    pub fn register_detect_handler(
        &mut self,
    ) -> DeadlockFuture<tonic::Response<tonic::Streaming<DeadlockResponse>>> {
        let (tx, rx) = mpsc::unbounded();
        let mut client = self.client.clone();
        let send_task = Box::pin(async move { client.detect(rx).await.map_err(Error::Tonic) });
        self.sender = Some(tx);

        send_task
    }

    pub fn detect(&self, req: DeadlockRequest) -> Result<()> {
        self.sender
            .as_ref()
            .unwrap()
            .unbounded_send(req)
            .map_err(|e| Error::Other(box_err!(e)))
    }
}
