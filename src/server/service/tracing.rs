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
use tikv_util::config::ReadableDuration;
use tikv_util::time::{duration_to_sec, Instant};

use crate::server::metrics::{GrpcTypeKind, GRPC_MSG_HISTOGRAM_STATIC};

type CollectTask = (Collector, Vec<tracepb::RemoteParentSpan>, Duration);

// static SPAN_ID_RANDOM_BITS: Lazy<u32> = Lazy::new(|| rand::thread_rng().gen());
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
                        total_duration,
                    };
                    warn!("[tracing] slow request deteted"; "report" => serde_json::to_string(&report.to_json()).unwrap_or_default());
                },
            );
            futures::executor::block_on(print_to_slow_log);
        });
    }
}

#[derive(Clone)]
pub struct TracerFactory {
    pub config: Arc<ArcSwap<TracingConfig>>,
    pub tx: Sender<CollectTask>,
}

#[macro_export]
macro_rules! new_request_tracer {
    ($req_name: ident, $req: expr, $handle: expr) => {{
        let trace_ctx = $req.mut_context().take_trace_context();
        let handle = $handle;
        let config = handle.config.load();
        let duration_threshold = config.duration_threshold.$req_name.0;

        RequestTracer::new(
            stringify!($req_name),
            $crate::server::metrics::GrpcTypeKind::$req_name,
            duration_threshold,
            trace_ctx,
            handle.tx.clone(),
            &config,
        )
    }};
}

#[must_use]
pub struct RequestTracer {
    pub root_span: Span,
    req_tag: GrpcTypeKind,
    begin_instant: Instant,
    inner: Option<RequestTracerInner>,
}

impl RequestTracer {
    #[doc(hidden)]
    pub fn new(
        req_name: &'static str,
        req_tag: GrpcTypeKind,
        duration_threshold: Duration,
        trace_ctx: tracepb::TraceContext,
        tx: Sender<CollectTask>,
        config: &TracingConfig,
    ) -> Self {
        let global_enabled = config.enable;
        let client_enabled = !trace_ctx.remote_parent_spans.is_empty();
        let begin_instant = Instant::now_coarse();

        if global_enabled
        /* && client_enabled */
        {
            let (root_span, collector) = Span::root_with_args(
                req_name,
                CollectArgs::default().max_span_count(Some(config.max_span_count)),
            );

            let inner = RequestTracerInner {
                remote_parent_spans: trace_ctx.remote_parent_spans.into(),
                tx,
                collector,
                duration_threshold,
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
    #[online_config(submodule)]
    pub duration_threshold: TracingDurationThreshold,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct TracingDurationThreshold {
    pub kv_get: ReadableDuration,
    pub kv_scan: ReadableDuration,
    pub kv_prewrite: ReadableDuration,
    pub kv_pessimistic_lock: ReadableDuration,
    pub kv_pessimistic_rollback: ReadableDuration,
    pub kv_commit: ReadableDuration,
    pub kv_cleanup: ReadableDuration,
    pub kv_batch_get: ReadableDuration,
    pub kv_batch_rollback: ReadableDuration,
    pub kv_txn_heart_beat: ReadableDuration,
    pub kv_check_txn_status: ReadableDuration,
    pub kv_check_secondary_locks: ReadableDuration,
    pub kv_scan_lock: ReadableDuration,
    pub kv_resolve_lock: ReadableDuration,
    pub kv_delete_range: ReadableDuration,
    pub kv_import: ReadableDuration,
    pub kv_gc: ReadableDuration,
    pub coprocessor: ReadableDuration,
    pub mvcc_get_by_key: ReadableDuration,
    pub mvcc_get_by_start_ts: ReadableDuration,
    pub raw_get: ReadableDuration,
    pub raw_batch_get: ReadableDuration,
    pub raw_scan: ReadableDuration,
    pub raw_batch_scan: ReadableDuration,
    pub raw_put: ReadableDuration,
    pub raw_batch_put: ReadableDuration,
    pub raw_delete: ReadableDuration,
    pub raw_batch_delete: ReadableDuration,
    pub raw_delete_range: ReadableDuration,
    pub raw_get_key_ttl: ReadableDuration,
    pub raw_compare_and_swap: ReadableDuration,
    pub raw_checksum: ReadableDuration,
    pub raw_coprocessor: ReadableDuration,
    pub invalid: ReadableDuration,
}

impl Default for TracingConfig {
    fn default() -> Self {
        TracingConfig {
            enable: false,
            max_span_count: 100,
            duration_threshold: TracingDurationThreshold::default(),
        }
    }
}

impl Default for TracingDurationThreshold {
    fn default() -> Self {
        TracingDurationThreshold {
            kv_get: ReadableDuration(Duration::from_millis(50)),
            kv_scan: ReadableDuration(Duration::from_millis(50)),
            kv_prewrite: ReadableDuration(Duration::from_millis(50)),
            kv_pessimistic_lock: ReadableDuration(Duration::from_millis(50)),
            kv_pessimistic_rollback: ReadableDuration(Duration::from_millis(50)),
            kv_commit: ReadableDuration(Duration::from_millis(50)),
            kv_cleanup: ReadableDuration(Duration::from_millis(50)),
            kv_batch_get: ReadableDuration(Duration::from_millis(50)),
            kv_batch_rollback: ReadableDuration(Duration::from_millis(50)),
            kv_txn_heart_beat: ReadableDuration(Duration::from_millis(50)),
            kv_check_txn_status: ReadableDuration(Duration::from_millis(50)),
            kv_check_secondary_locks: ReadableDuration(Duration::from_millis(50)),
            kv_scan_lock: ReadableDuration(Duration::from_millis(50)),
            kv_resolve_lock: ReadableDuration(Duration::from_millis(50)),
            kv_delete_range: ReadableDuration(Duration::from_millis(50)),
            kv_import: ReadableDuration(Duration::from_millis(50)),
            kv_gc: ReadableDuration(Duration::from_millis(50)),
            coprocessor: ReadableDuration(Duration::from_millis(50)),
            mvcc_get_by_key: ReadableDuration(Duration::from_millis(50)),
            mvcc_get_by_start_ts: ReadableDuration(Duration::from_millis(50)),
            raw_get: ReadableDuration(Duration::from_millis(50)),
            raw_batch_get: ReadableDuration(Duration::from_millis(50)),
            raw_scan: ReadableDuration(Duration::from_millis(50)),
            raw_batch_scan: ReadableDuration(Duration::from_millis(50)),
            raw_put: ReadableDuration(Duration::from_millis(50)),
            raw_batch_put: ReadableDuration(Duration::from_millis(50)),
            raw_delete: ReadableDuration(Duration::from_millis(50)),
            raw_batch_delete: ReadableDuration(Duration::from_millis(50)),
            raw_delete_range: ReadableDuration(Duration::from_millis(50)),
            raw_get_key_ttl: ReadableDuration(Duration::from_millis(50)),
            raw_compare_and_swap: ReadableDuration(Duration::from_millis(50)),
            raw_checksum: ReadableDuration(Duration::from_millis(50)),
            raw_coprocessor: ReadableDuration(Duration::from_millis(50)),
            invalid: ReadableDuration(Duration::from_millis(50)),
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
    total_duration: Duration,
}

impl TracingReport {
    pub fn to_json(&mut self) -> serde_json::Value {
        self.spans.sort_by_key(|span| span.begin_unix_time_ns);

        json!({
            "trace_id": self.remote_parent_spans.get(0).map(|parent| parent.trace_id).unwrap_or(0),
            "parent_id": self.remote_parent_spans.get(0).map(|parent| parent.span_id).unwrap_or(0),
            "spans": self.spans.iter().map( Self::span_to_json).collect::<Vec<_>>()
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
