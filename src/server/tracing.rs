// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;
use minitrace::jaeger::thrift_compact_encode;
use minitrace::Collector;
use std::net::SocketAddr;
use std::ops::Deref;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::runtime::{Builder, Runtime};
/// Tracing Reporter
pub trait Reporter: Send + Sync {
    fn report(&self, collector: Option<Collector>);
}

impl<R, D> Reporter for D
where
    R: Reporter + ?Sized,
    D: Deref<Target = R> + Send + Sync,
{
    fn report(&self, collector: Option<Collector>) {
        self.deref().report(collector)
    }
}

/// A tracing reporter reports tracing results to Jaeger agent
pub struct JaegerReporter {
    agent: SocketAddr,
    runtime: Runtime,
    duration_threshold: Duration,
}

impl JaegerReporter {
    pub fn new(
        core_threads: usize,
        duration_threshold: Duration,
        agent: SocketAddr,
    ) -> Result<Self> {
        let runtime = Builder::new()
            .threaded_scheduler()
            .core_threads(core_threads)
            .enable_io()
            .build()?;

        Ok(Self {
            agent,
            runtime,
            duration_threshold,
        })
    }

    async fn report(agent: SocketAddr, collector: Collector, threshold: Duration) -> Result<()> {
        let local_addr: SocketAddr = if agent.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()?;
        let mut udp_socket = UdpSocket::bind(local_addr).await?;

        let trace_details = collector.collect();
        if Duration::from_nanos(trace_details.elapsed_ns) < threshold {
            return Ok(());
        }

        const BUFFER_SIZE: usize = 4096;
        let mut buf = Vec::with_capacity(BUFFER_SIZE);
        thrift_compact_encode(&mut buf, "TiKV", &trace_details, |_e| "*TODO*");
        udp_socket.send_to(&buf, agent).await?;
        Ok(())
    }
}

impl Reporter for JaegerReporter {
    fn report(&self, collector: Option<Collector>) {
        if let Some(collector) = collector {
            self.runtime
                .spawn(Self::report(self.agent, collector, self.duration_threshold));
        }
    }
}

/// A tracing reporter ignores all tracing results passed to it
#[derive(Clone, Copy)]
pub struct NullReporter;

impl NullReporter {
    pub fn new() -> Self {
        Self
    }
}

impl Reporter for NullReporter {
    fn report(&self, _collector: Option<Collector>) {}
}
