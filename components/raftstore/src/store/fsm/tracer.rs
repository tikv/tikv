// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::sync::Arc;

use tikv_util::trace::*;

thread_local! {
    static RAFT_TRACER: RefCell<RaftTracer> = RefCell::new(RaftTracer {
        local_collector: None,
        spans: vec![],
    });
}

pub struct RaftTracer {
    local_collector: Option<LocalCollector>,
    spans: Vec<Span>,
}

impl RaftTracer {
    pub fn begin() {
        RAFT_TRACER.with(|tracer| {
            let mut tracer = tracer.borrow_mut();
            tracer.local_collector = Some(LocalCollector::start());
        });
    }

    pub fn add_span(span: Span) {
        RAFT_TRACER.with(|tracer| {
            let mut tracer = tracer.borrow_mut();
            tracer.spans.push(span);
        });
    }

    pub fn end() {
        RAFT_TRACER.with(|tracer| {
            let mut tracer = tracer.borrow_mut();
            let local_spans = tracer
                .local_collector
                .take()
                .expect("empty local_collector")
                .collect();
            if !tracer.spans.is_empty() {
                let local_spans = Arc::new(local_spans);
                for span in tracer.spans.split_off(0) {
                    span.mount_local_spans(local_spans.clone());
                }
            }
        });
    }
}

thread_local! {
    static APPLY_TRACER: RefCell<ApplyTracer> = RefCell::new(ApplyTracer { local_collector: None, spans: vec![] });
}

pub struct ApplyTracer {
    local_collector: Option<LocalCollector>,
    spans: Vec<Arc<LocalSpans>>,
}

impl ApplyTracer {
    pub fn begin() {
        APPLY_TRACER.with(|tracer| {
            let mut tracer = tracer.borrow_mut();
            tracer.local_collector = Some(LocalCollector::start());
        });
    }

    pub fn truncate() {
        APPLY_TRACER.with(|tracer| {
            let mut tracer = tracer.borrow_mut();

            let local_spans = tracer
                .local_collector
                .take()
                .expect("empty local_collector")
                .collect();
            if !local_spans.spans.is_empty() {
                tracer.spans.push(Arc::new(local_spans));
            }

            tracer.local_collector = Some(LocalCollector::start());
        });
    }

    pub fn partial_submit(f: impl FnOnce(&[Arc<LocalSpans>])) {
        APPLY_TRACER.with(|tracer| {
            let tracer = tracer.borrow();

            if !tracer.spans.is_empty() {
                f(tracer.spans.as_slice());
            }
        });
    }

    pub fn end() {
        APPLY_TRACER.with(|tracer| {
            let mut tracer = tracer.borrow_mut();
            let _ = tracer
                .local_collector
                .take()
                .expect("empty local_collector")
                .collect();
            tracer.spans.clear();
        })
    }
}
