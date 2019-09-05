// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Error, Result};
use futures::unsync::mpsc::{self, UnboundedSender};
use futures::{Future, Sink, Stream};
use grpcio::{ChannelBuilder, EnvBuilder, Environment, WriteFlags};
use kvproto::deadlock::*;
use std::sync::Arc;
use std::time::Duration;
use tikv_util::security::SecurityManager;

type DeadlockFuture<T> = Box<dyn Future<Item = T, Error = Error>>;

pub type Callback = Box<dyn Fn(DeadlockResponse)>;

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
    addr: String,
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
            addr: addr.to_owned(),
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
        let send = sink
            .sink_map_err(Error::Grpc)
            .send_all(rx.then(|r| match r {
                Ok(r) => Ok((r, WriteFlags::default())),
                Err(()) => Err(Error::Other(box_err!("failed to recv detect request"))),
            }))
            .then(|res| match res {
                Ok((mut sink, _)) => {
                    info!("cancel detect sender");
                    sink.get_mut().cancel();
                    Ok(())
                }
                Err(e) => Err(e),
            });
        self.sender = Some(tx);

        let recv = receiver.map_err(Error::Grpc).for_each(move |resp| {
            cb(resp);
            Ok(())
        });
        (Box::new(send), Box::new(recv))
    }

    pub fn detect(&self, req: DeadlockRequest) -> Result<()> {
        self.sender
            .as_ref()
            .unwrap()
            .unbounded_send(req)
            .map_err(|e| Error::Other(box_err!(e)))
    }
}
