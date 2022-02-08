// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
// use std::lazy::Lazy;
use std::time::Duration;

use arc_swap::ArcSwap;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::prelude::*;
use kvproto::tracepb;
use minitrace::prelude::*;
use online_config::{ConfigChange, OnlineConfig};
use serde_json::json;
use tikv_util::time::{duration_to_sec, Instant};

use crate::server::metrics::{GrpcTypeKind, GRPC_MSG_HISTOGRAM_STATIC};

type CollectTask = (Collector, Vec<tracepb::RemoteParentSpan>, Duration);

const CHANNEL_SIZE: usize = 1024;

pub fn init_tracing(
    config: &TracingConfig,
) -> (TracingService, TracerFactory, TracingConfigManager) {
    let config = Arc::new(ArcSwap::from_pointee(config.clone()));
    let (tx, rx) = channel(CHANNEL_SIZE);

    (
        TracingService { rx },
        TracerFactory {
            config: config.clone(),
            tx,
        },
        TracingConfigManager { config },
    )
}

pub struct TracingService {
    rx: Receiver<CollectTask>,
}

impl TracingService {
    // TODO (andylokandy): implement a real pubsub service
    pub fn register_service(self) {
        std::thread::spawn(move || {
            let print_to_slow_log = self.rx.for_each_concurrent(
                None,
                |(collector, remote_parent_spans, total_duration)| async move {
                    let mut report = TracingReport {
                        spans: collector.collect().await,
                        remote_parent_spans,
                    };
                    warn!(
                        "[tracing] slow request deteted";
                        "total_duration" => ?total_duration,
                        "report" => serde_json::to_string(&report.to_json()).unwrap_or_default()
                    );
                },
            );
            futures::executor::block_on(print_to_slow_log);
        });
    }
}

#[macro_export]
macro_rules! new_request_tracer {
    ($req_name: ident, $req: expr, $factory: expr) => {{
        let trace_ctx = $req.mut_context().take_trace_context();
        let factory = $factory;
        let config = factory.config.load();

        TracerFactory::new_tracer(
            stringify!($req_name),
            $crate::server::metrics::GrpcTypeKind::$req_name,
            trace_ctx,
            factory.tx.clone(),
            &config,
        )
    }};
}

#[derive(Clone)]
pub struct TracerFactory {
    pub config: Arc<ArcSwap<TracingConfig>>,
    pub tx: Sender<CollectTask>,
}

impl TracerFactory {
    pub fn new_tracer(
        req_name: &'static str,
        req_tag: GrpcTypeKind,
        trace_ctx: tracepb::TraceContext,
        tx: Sender<CollectTask>,
        config: &TracingConfig,
    ) -> RequestTracer {
        let global_enabled = config.enable;
        let client_enabled = !trace_ctx.remote_parent_spans.is_empty();
        let begin_instant = Instant::now_coarse();

        if global_enabled && client_enabled {
            let (root_span, collector) = Span::root_with_args(
                req_name,
                CollectArgs::default().max_span_count(Some(config.max_span_count)),
            );

            let inner = RequestTracerInner {
                remote_parent_spans: trace_ctx.remote_parent_spans.into_vec(),
                duration_threshold: Duration::from_millis(trace_ctx.duration_threshold_ms.into()),
                tx,
                collector,
            };
            RequestTracer {
                root_span,
                req_tag,
                begin_instant,
                inner: Some(inner),
            }
        } else {
            RequestTracer {
                root_span: Span::new_noop(),
                req_tag,
                begin_instant,
                inner: None,
            }
        }
    }
}

#[must_use]
pub struct RequestTracer {
    pub root_span: Span,
    req_tag: GrpcTypeKind,
    begin_instant: Instant,
    inner: Option<RequestTracerInner>,
}

impl RequestTracer {
    pub fn new_noop() -> Self {
        RequestTracer {
            root_span: Span::new_noop(),
            req_tag: GrpcTypeKind::invalid,
            begin_instant: Instant::now_coarse(),
            inner: None,
        }
    }
}

pub struct RequestTracerInner {
    remote_parent_spans: Vec<tracepb::RemoteParentSpan>,
    tx: Sender<CollectTask>,
    collector: Collector,
    duration_threshold: Duration,
}

impl RequestTracer {
    pub fn finish(self) {
        drop(self.root_span);
        let duration = self.begin_instant.saturating_elapsed();
        GRPC_MSG_HISTOGRAM_STATIC
            .get(self.req_tag)
            .observe(duration_to_sec(duration));
        if let Some(mut inner) = self.inner {
            if duration >= inner.duration_threshold {
                let collect_task = (inner.collector, inner.remote_parent_spans, duration);
                // TODO (andylokandy): Add metric for dropped reports
                inner.tx.try_send(collect_task).ok();
            }
        }
    }
}

/// Clone the tracer with necessary information for metrics but not the information for
/// tracing, because the tracing collector is not cloneable.
impl Clone for RequestTracer {
    fn clone(&self) -> Self {
        RequestTracer {
            root_span: Span::new_noop(),
            req_tag: self.req_tag,
            begin_instant: self.begin_instant,
            inner: None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TracingConfig {
    // Experimental: planned to be removed in the future
    pub enable: bool,
    pub max_span_count: usize,
}

impl Default for TracingConfig {
    fn default() -> Self {
        TracingConfig {
            enable: false,
            max_span_count: 100,
        }
    }
}

pub struct TracingConfigManager {
    config: Arc<ArcSwap<TracingConfig>>,
}

impl online_config::ConfigManager for TracingConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
        let mut new_config = (**self.config.load()).clone();
        new_config.update(change);
        self.config.store(Arc::new(new_config));
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct TracingReport {
    remote_parent_spans: Vec<tracepb::RemoteParentSpan>,
    spans: Vec<SpanRecord>,
}

impl TracingReport {
    pub fn to_json(&mut self) -> serde_json::Value {
        self.spans.sort_by_key(|span| span.begin_unix_time_ns);

        json!({
            "trace_id": self.remote_parent_spans.get(0).map(|parent| parent.trace_id).unwrap_or(0),
            "parent_id": self.remote_parent_spans.get(0).map(|parent| parent.span_id).unwrap_or(0),
            "spans": self.spans.iter().map(Self::span_to_json).collect::<Vec<_>>()
        })
    }

    fn span_to_json(span: &SpanRecord) -> serde_json::Value {
        json!({
            "span_id": span.id,
            "event": span.event.to_string(),
            "begin_unix_ns": span.begin_unix_time_ns,
            "duration_ns": span.duration_ns,
            "properties": Self::properties_to_json(&span.properties)
        })
    }

    fn properties_to_json(properties: &[(&str, String)]) -> serde_json::Value {
        let map: serde_json::Map<_, _> = properties
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone().into()))
            .collect();
        map.into()
    }
}
