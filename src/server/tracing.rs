// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;
use kvproto::kvrpcpb::TraceContext;
use std::net::SocketAddr;
use std::ops::Deref;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::runtime::{Builder, Runtime};

use tikv_util::minitrace::cycle_to_realtime;
use tikv_util::minitrace::{self, Collector, Span};

/// Tracing Reporter
pub trait Reporter: Send + Sync {
    fn report(&self, trace_context: TraceContext, collector: Option<Collector>);
    fn is_null(&self) -> bool;
}

impl<R, D> Reporter for D
where
    R: Reporter + ?Sized,
    D: Deref<Target = R> + Send + Sync,
{
    fn report(&self, trace_context: TraceContext, collector: Option<Collector>) {
        self.deref().report(trace_context, collector)
    }

    fn is_null(&self) -> bool {
        self.deref().is_null()
    }
}

/// A tracing reporter reports tracing results to Jaeger agent
pub struct JaegerReporter {
    agent: SocketAddr,
    runtime: Runtime,
    duration_threshold: Duration,
    spans_max_length: usize,
}

impl JaegerReporter {
    pub fn new(
        core_threads: usize,
        duration_threshold: Duration,
        spans_max_length: usize,
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
            spans_max_length,
        })
    }

    async fn report(
        trace_context: TraceContext,
        agent: SocketAddr,
        mut spans: Vec<Span>,
        spans_max_length: usize,
        reporter: minitrace::report::Reporter,
    ) -> Result<()> {
        let local_addr: SocketAddr = if agent.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()?;
        let mut udp_socket = UdpSocket::bind(local_addr).await?;

        // Check if len of spans reaches `spans_max_length`
        if spans.len() > spans_max_length {
            spans.sort_unstable_by_key(|s| s.begin_cycles);
            spans.truncate(spans_max_length);
        }

        let external_trace = trace_context.get_is_trace_enabled();
        let bytes = reporter.encode(
            if external_trace {
                trace_context.get_trace_id() as _
            } else {
                rand::random()
            },
            spans,
        )?;

        udp_socket.send_to(&bytes, agent).await?;
        Ok(())
    }
}

impl Reporter for JaegerReporter {
    fn report(&self, trace_context: TraceContext, collector: Option<Collector>) {
        if let Some(collector) = collector {
            let mut spans = collector.collect();
            if spans.is_empty() {
                // Request is run too fast to collect spans
                return;
            }

            let mut root_index = 0;
            for (i, span) in spans.iter().enumerate() {
                if span.parent_id.0 == 0 {
                    root_index = i;
                }
            }
            spans.swap(0, root_index);

            // Check if duration reaches `duration_threshold`
            let duration_ns = cycle_to_realtime(spans[0].end_cycles).ns
                - cycle_to_realtime(spans[0].begin_cycles).ns;
            if Duration::from_nanos(duration_ns) < self.duration_threshold {
                // keep root span
                spans.truncate(1);
            }

            self.runtime.spawn(Self::report(
                trace_context,
                self.agent,
                spans,
                self.spans_max_length,
                minitrace::report::Reporter::new(self.agent, "TiKV"),
            ));
        }
    }

    fn is_null(&self) -> bool {
        false
    }
}

/// A tracing reporter drops all tracing results passed to it, like `/dev/null`
#[derive(Clone, Copy)]
pub struct NullReporter;

impl NullReporter {
    pub fn new() -> Self {
        Self
    }
}

impl Reporter for NullReporter {
    fn report(&self, _trace_context: TraceContext, _collector: Option<Collector>) {}

    fn is_null(&self) -> bool {
        true
    }
}
