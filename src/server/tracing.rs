// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::Result;
use kvproto::kvrpcpb::TraceContext;
use std::cell::Cell;
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::AtomicU16;
use std::time::Duration;
use tikv_util::minitrace::{jaeger::thrift_compact_encode, Collector, Event, State, TraceDetails};
use tokio::net::UdpSocket;
use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "protobuf-codec")]
use protobuf::ProtobufEnum;
use std::collections::HashMap;

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

        if trace_context.get_is_trace_enabled() {
            Self::id_remap(&trace_context, &mut trace_details);
        }

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
            0,
            if trace_context.get_is_trace_enabled() {
                trace_context.get_trace_id() as _
            } else {
                rand::random()
            },
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

    fn id_remap(trace_context: &TraceContext, trace_details: &mut TraceDetails) {
        let mut id_mapper = HashMap::with_capacity(trace_details.spans.len());
        let id_prefix = trace_context.get_span_id_prefix();
        for span in &trace_details.spans {
            id_mapper.insert(span.id, (id_prefix as u64) << 32 | unique_u32() as u64);
        }

        // remap self if and parent id
        for span in &mut trace_details.spans {
            span.id = id_mapper[&span.id];
            if span.state != State::Root {
                span.related_id = id_mapper[&span.related_id];
            } else {
                span.state = State::Scheduling;
                span.related_id = trace_context.get_parent_span_id();
            }
        }

        // remap relevant span id of property
        for span_id in &mut trace_details.properties.span_ids {
            *span_id = id_mapper[span_id];
        }
    }
}

impl Reporter for JaegerReporter {
    fn report(&self, trace_context: TraceContext, collector: Option<Collector>) {
        if let Some(collector) = collector {
            let trace_details = collector.collect();

            // Check if duration reaches `duration_threshold`
            if Duration::from_nanos(trace_details.elapsed_ns) < self.duration_threshold {
                return;
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

static GLOBAL_ID_COUNTER: AtomicU16 = AtomicU16::new(0);

#[inline]
fn next_global_id_prefix() -> u16 {
    GLOBAL_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::AcqRel)
}

thread_local! {
    static ID_PREFIX: Cell<(u16, u16)> = Cell::new((0, 0));
}

fn unique_u32() -> u32 {
    ID_PREFIX.with(|c| {
        let (mut id_prefix, mut id_suffix) = c.get();
        if id_suffix == std::u16::MAX {
            id_suffix = 0;
            id_prefix = next_global_id_prefix();
        } else {
            id_suffix += 1;
        }
        c.set((id_prefix, id_suffix));
        (id_prefix as u32) << 16 | id_suffix as u32
    })
}
