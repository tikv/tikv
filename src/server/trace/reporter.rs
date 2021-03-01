// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use collections::hash_set_with_capacity;
use kvproto::kvrpcpb::TraceContext;
use tikv_util::trace::{self, span::Span, CollectArgs, Collector};
use tikv_util::worker::{Runnable, Scheduler};

pub struct ReporterBuilder {
    subscribers: Vec<Box<dyn Subscriber>>,
    duration_threshold: Duration,
    spans_max_length: Option<usize>,
}

impl ReporterBuilder {
    pub fn new() -> Self {
        Self {
            subscribers: vec![],
            duration_threshold: Duration::default(),
            spans_max_length: None,
        }
    }

    pub fn build(self) -> Reporter {
        Reporter {
            subscribers: self.subscribers,
            duration_threshold: self.duration_threshold,
            spans_max_length: self.spans_max_length,
        }
    }

    pub fn register(&mut self, subscriber: impl Subscriber) {
        self.subscribers.push(Box::new(subscriber));
    }

    pub fn duration_threshold(&mut self, value: Duration) {
        self.duration_threshold = value;
    }

    pub fn spans_max_length(&mut self, value: usize) {
        self.spans_max_length = Some(value);
    }
}

pub struct Reporter {
    subscribers: Vec<Box<dyn Subscriber>>,
    duration_threshold: Duration,
    spans_max_length: Option<usize>,
}

impl Reporter {
    pub fn collect(
        &self,
        trace_context: &TraceContext,
        collector: Option<Collector>,
    ) -> Option<Vec<Span>> {
        if let Some(collector) = collector {
            let mut spans = collector.collect_with_args(
                CollectArgs::default().duration_threshold(self.duration_threshold),
            );

            if spans.is_empty() {
                // Request runs too fast to collect spans
                return None;
            } else {
                self.polish_spans(&mut spans);
            }

            for subscriber in &self.subscribers {
                subscriber.report(
                    if trace_context.get_enable() {
                        trace_context.get_trace_id()
                    } else {
                        rand::random()
                    },
                    trace_context.get_root_parent_span_id(),
                    trace_context.get_span_id_prefix(),
                    &spans,
                );
            }

            Some(spans)
        } else {
            None
        }
    }

    pub fn subscribers_is_empty(&self) -> bool {
        self.subscribers.is_empty()
    }

    fn polish_spans(&self, spans: &mut Vec<Span>) {
        if let Some(max_length) = self.spans_max_length {
            if spans.len() > max_length {
                spans.sort_by_key(|s| s.begin_unix_time_ns);
                spans.truncate(max_length);
            }
        }

        // Repair spans have unresolved parent id
        let mut root_id = spans[0].id;
        let mut id_set = hash_set_with_capacity(spans.len());
        for span in spans.iter_mut() {
            id_set.insert(span.id);
            if span.parent_id == 0 {
                root_id = span.id;
            }
        }

        for span in spans.iter_mut() {
            if span.parent_id != 0 && !id_set.contains(&span.parent_id) {
                span.parent_id = root_id;
            }
        }
    }
}

pub trait Subscriber: Send + Sync + 'static {
    fn report(&self, trace_id: u64, root_parent_span_id: u64, span_id_prefix: u32, spans: &[Span]);
}

pub struct JaegerSubscriber {
    scheduler: Scheduler<JaegerReportArgs>,
    agent: SocketAddr,
}

impl JaegerSubscriber {
    pub fn new(scheduler: Scheduler<JaegerReportArgs>, agent: SocketAddr) -> Self {
        Self { scheduler, agent }
    }
}

impl Subscriber for JaegerSubscriber {
    fn report(&self, trace_id: u64, root_parent_span_id: u64, span_id_prefix: u32, spans: &[Span]) {
        if let Ok(bytes) = trace::Reporter::encode(
            "TiKV".to_owned(),
            trace_id,
            root_parent_span_id,
            span_id_prefix,
            &spans,
        ) {
            self.scheduler
                .schedule(JaegerReportArgs {
                    bytes,
                    agent: self.agent,
                })
                .ok();
        }
    }
}

pub struct JaegerReportArgs {
    pub bytes: Vec<u8>,
    pub agent: SocketAddr,
}

impl std::fmt::Display for JaegerReportArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[jaeger reporter] report")
    }
}

pub struct JaegerReportRunner;

impl Runnable for JaegerReportRunner {
    type Task = JaegerReportArgs;

    fn run(&mut self, args: Self::Task) {
        let local_addr: SocketAddr = if args.agent.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        }
        .parse()
        .unwrap();
        if let Ok(socket) = UdpSocket::bind(local_addr) {
            socket.send_to(&args.bytes, args.agent).ok();
        }
    }
}
