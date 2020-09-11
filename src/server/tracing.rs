// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;
use kvproto::kvrpcpb::TraceContext;
use std::net::SocketAddr;
use std::ops::Deref;
use std::time::Duration;
use tikv_util::minitrace::{
    jaeger::{thrift_compact_encode, JaegerSpanInfo, ReferenceType},
    Collector, Event, State, TraceDetails,
};
use tokio::net::UdpSocket;
use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "protobuf-codec")]
use protobuf::ProtobufEnum;

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

        let external_trace = trace_context.get_is_trace_enabled();

        const BUFFER_SIZE: usize = 4096;
        let mut buf = Vec::with_capacity(BUFFER_SIZE);
        thrift_compact_encode(
            &mut buf,
            "TiKV",
            0,
            if external_trace {
                trace_context.get_trace_id() as _
            } else {
                rand::random()
            },
            &trace_details,
            |s| JaegerSpanInfo {
                self_id: if external_trace {
                    (trace_context.get_span_id_prefix() as i64) << 32 | s.id as i64
                } else {
                    s.id as _
                },
                parent_id: if external_trace {
                    if s.state == State::Root {
                        trace_context.get_parent_span_id() as _
                    } else {
                        (trace_context.get_span_id_prefix() as i64) << 32 | s.related_id as i64
                    }
                } else {
                    s.related_id as _
                },
                reference_type: ReferenceType::FollowFrom,
                operation_name: {
                    #[cfg(feature = "protobuf-codec")]
                    {
                        Event::enum_descriptor_static()
                            .value_by_number(s.event as _)
                            .name()
                    }
                    #[cfg(feature = "prost-codec")]
                    {
                        Event::from_i32(s.event as _)
                            .map(|e| format!("{:?}", e))
                            .unwrap_or(String::new())
                    }
                },
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
    fn report(&self, trace_context: TraceContext, collector: Option<Collector>) {
        if let Some(collector) = collector {
            let mut trace_details = collector.collect();
            if trace_details.spans.is_empty() {
                // Request is run too fast to collect spans
                return;
            }

            let total_cycles = (trace_details.elapsed_ns as u128
                * trace_details.cycles_per_second as u128)
                / 1_000_000_000u128;

            let mut root_index = 0;
            for (i, span) in trace_details.spans.iter().enumerate() {
                if span.state == State::Root {
                    root_index = i;
                }
            }
            trace_details.spans.swap(0, root_index);

            // reuse root span to represent the whole process
            trace_details.spans[0].elapsed_cycles = total_cycles as u64;

            // Check if duration reaches `duration_threshold`
            if Duration::from_nanos(trace_details.elapsed_ns) < self.duration_threshold {
                // keep root span
                trace_details.spans.truncate(1);
            }

            self.runtime.spawn(Self::report(
                trace_context,
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
    fn report(&self, _trace_context: TraceContext, _collector: Option<Collector>) {}

    fn is_null(&self) -> bool {
        true
    }
}
