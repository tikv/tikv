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
use tikv_util::config::ReadableDuration;
use tikv_util::time::{duration_to_sec, Instant};

use crate::server::metrics::{GrpcTypeKind, GRPC_MSG_HISTOGRAM_STATIC};

// static SPAN_ID_RANDOM_BITS: Lazy<u32> = Lazy::new(|| rand::thread_rng().gen());
const CHANNEL_SIZE: usize = 1024;

pub fn init_tracing(
    config: &TracingConfig,
) -> (TracingService, TracingHandle, TracingConfigManager) {
    let config = Arc::new(ArcSwap::from_pointee(config.clone()));
    let (tx, rx) = channel(CHANNEL_SIZE);

    (
        TracingService { rx },
        TracingHandle {
            config: config.clone(),
            tx,
        },
        TracingConfigManager { config },
    )
}

pub struct TracingService {
    rx: Receiver<TracingReport>,
}

impl TracingService {
    // TODO (andylokandy): implement a real pubsub service
    pub fn register_service(self) {
        std::thread::spawn(move || {
            let print_to_slow_log = self.rx.for_each(|mut report| {
                report.spans.sort_by_key(|span| span.begin_unix_time_ns);
                warn!("[tracing] slow request deteted"; "report" => ?report);
                futures::future::ready(())
            });
            futures::executor::block_on(print_to_slow_log);
        });
    }
}

#[derive(Clone, Debug)]
pub struct TracingHandle {
    pub config: Arc<ArcSwap<TracingConfig>>,
    pub tx: Sender<TracingReport>,
}

#[derive(Clone, Debug)]
pub struct TracingReport {
    remote_parent_spans: Vec<tracepb::RemoteParentSpan>,
    spans: Vec<SpanRecord>,
    total_duration: Duration,
}

#[macro_export]
macro_rules! new_request_tracer {
    ($req_name: ident, $req: expr, $handle: expr) => {{
        let trace_ctx = $req.mut_context().take_tracing_context();
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
        trace_ctx: tracepb::TracingContext,
        tx: Sender<TracingReport>,
        config: &TracingConfig,
    ) -> Self {
        let global_enabled = config.enable;
        let client_enabled = !trace_ctx.remote_parent_span.is_empty();
        let begin_instant = Instant::now_coarse();

        if global_enabled && client_enabled {
            let (root_span, collector) = Span::root(req_name);

            let inner = RequestTracerInner {
                remote_parent_spans: trace_ctx.remote_parent_span.into(),
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
    tx: Sender<TracingReport>,
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
            if duration > inner.duration_threshold {
                let spans = inner.collector.collect();
                let report = TracingReport {
                    remote_parent_spans: inner.remote_parent_spans,
                    spans,
                    total_duration: duration,
                };
                // TODO (andylokandy): Add metric for dropped reports
                inner.tx.try_send(report).ok();
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default, OnlineConfig)]
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

impl Default for TracingDurationThreshold {
    fn default() -> Self {
        TracingDurationThreshold {
            kv_get: ReadableDuration(Duration::from_millis(5)),
            kv_scan: ReadableDuration(Duration::from_millis(5)),
            kv_prewrite: ReadableDuration(Duration::from_millis(5)),
            kv_pessimistic_lock: ReadableDuration(Duration::from_millis(5)),
            kv_pessimistic_rollback: ReadableDuration(Duration::from_millis(5)),
            kv_commit: ReadableDuration(Duration::from_millis(5)),
            kv_cleanup: ReadableDuration(Duration::from_millis(5)),
            kv_batch_get: ReadableDuration(Duration::from_millis(5)),
            kv_batch_rollback: ReadableDuration(Duration::from_millis(5)),
            kv_txn_heart_beat: ReadableDuration(Duration::from_millis(5)),
            kv_check_txn_status: ReadableDuration(Duration::from_millis(5)),
            kv_check_secondary_locks: ReadableDuration(Duration::from_millis(5)),
            kv_scan_lock: ReadableDuration(Duration::from_millis(5)),
            kv_resolve_lock: ReadableDuration(Duration::from_millis(5)),
            kv_delete_range: ReadableDuration(Duration::from_millis(5)),
            kv_import: ReadableDuration(Duration::from_millis(5)),
            kv_gc: ReadableDuration(Duration::from_millis(5)),
            coprocessor: ReadableDuration(Duration::from_millis(5)),
            mvcc_get_by_key: ReadableDuration(Duration::from_millis(5)),
            mvcc_get_by_start_ts: ReadableDuration(Duration::from_millis(5)),
            raw_get: ReadableDuration(Duration::from_millis(5)),
            raw_batch_get: ReadableDuration(Duration::from_millis(5)),
            raw_scan: ReadableDuration(Duration::from_millis(5)),
            raw_batch_scan: ReadableDuration(Duration::from_millis(5)),
            raw_put: ReadableDuration(Duration::from_millis(5)),
            raw_batch_put: ReadableDuration(Duration::from_millis(5)),
            raw_delete: ReadableDuration(Duration::from_millis(5)),
            raw_batch_delete: ReadableDuration(Duration::from_millis(5)),
            raw_delete_range: ReadableDuration(Duration::from_millis(5)),
            raw_get_key_ttl: ReadableDuration(Duration::from_millis(5)),
            raw_compare_and_swap: ReadableDuration(Duration::from_millis(5)),
            raw_checksum: ReadableDuration(Duration::from_millis(5)),
            raw_coprocessor: ReadableDuration(Duration::from_millis(5)),
            invalid: ReadableDuration(Duration::from_millis(5)),
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
