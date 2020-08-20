// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;
use std::net::SocketAddr;
use std::ops::Deref;
use std::time::Duration;
use tikv_util::minitrace::{jaeger::thrift_compact_encode, Collector, Event, TraceDetails};
use tokio::net::UdpSocket;
use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "protobuf-codec")]
use protobuf::ProtobufEnum;

/// Tracing Reporter
pub trait Reporter: Send + Sync {
    fn report(&self, collector: Option<Collector>);
    fn is_null(&self) -> bool;
}

impl<R, D> Reporter for D
where
    R: Reporter + ?Sized,
    D: Deref<Target = R> + Send + Sync,
{
    fn report(&self, collector: Option<Collector>) {
        self.deref().report(collector)
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
        agent: SocketAddr,
        mut trace_details: TraceDetails,
        spans_max_length: usize,
    ) -> Result<()> {
        let local_addr: SocketAddr = if agent.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()?;
        let mut udp_socket = UdpSocket::bind(local_addr).await?;

        // Check if len of spans reaches `spans_max_length`
        if trace_details.spans.len() > spans_max_length {
            trace_details.spans.sort_unstable_by_key(|s| s.begin_cycles);
            trace_details.spans.truncate(spans_max_length);
        }

        const BUFFER_SIZE: usize = 4096;
        let mut buf = Vec::with_capacity(BUFFER_SIZE);
        thrift_compact_encode(
            &mut buf,
            "TiKV",
            &trace_details,
            // transform numerical event to string
            |event| {
                #[cfg(feature = "protobuf-codec")]
                return Event::enum_descriptor_static()
                    .value_by_number(event as _)
                    .name();
                #[cfg(feature = "prost-codec")]
                return format!(
                    "{:?}",
                    Event::from_i32(event as _).unwrap_or(Event::Unknown)
                );
            },
            // transform encoded property in protobuf to key-value strings
            |bytes| {
                if let Ok(mut property) = protobuf::parse_from_bytes::<tipb::TracingProperty>(bytes)
                {
                    let value = property.take_value();

                    let key = property.get_key();
                    #[cfg(feature = "protobuf-codec")]
                    return (
                        key.enum_descriptor().value_by_number(key as _).name(),
                        value,
                    );
                    #[cfg(feature = "prost-codec")]
                    return (format!("{:?}", key), value);
                }

                ("Unknown".into(), "Unknown".into())
            },
        );
        udp_socket.send_to(&buf, agent).await?;
        Ok(())
    }
}

impl Reporter for JaegerReporter {
    fn report(&self, collector: Option<Collector>) {
        if let Some(collector) = collector {
            let trace_details = collector.collect();

            // Check if duration reaches `duration_threshold`
            if Duration::from_nanos(trace_details.elapsed_ns) < self.duration_threshold {
                return;
            }

            self.runtime.spawn(Self::report(
                self.agent,
                trace_details,
                self.spans_max_length,
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
    fn report(&self, _collector: Option<Collector>) {}

    fn is_null(&self) -> bool {
        true
    }
}
