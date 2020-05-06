#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod collector;
pub mod future;
pub mod time_measure;
pub mod util;

pub use tracer_attribute;

use collector::*;
pub use collector::{Collector, CollectorType};
use time_measure::*;
pub type SpanID = snowflake::ProcessUniqueId;

pub const TIME_MEASURE_TYPE: TimeMeasureType = TimeMeasureType::Instant;
pub const COLLECTOR_TYPE: CollectorType = CollectorType::Channel;

#[derive(Debug)]
pub struct Span {
    pub tag: &'static str,
    pub id: SpanID,
    pub parent: Option<SpanID>,
    pub elapsed_start: std::time::Duration,
    pub elapsed_end: std::time::Duration,
}

pub struct SpanInfo {
    tag: &'static str,
    parent: Option<SpanID>,
}

thread_local! {
    static SPAN_STACK: std::cell::RefCell<Vec<&'static SpanGuard>> = std::cell::RefCell::new(Vec::with_capacity(1024));
}

#[inline]
pub fn new_span_root(
    tag: &'static str,
    tx: CollectorTx,
    time_measure_type: TimeMeasureType,
) -> SpanGuard {
    let root_time = TimeMeasure::root(time_measure_type);
    let time_handle = TimeMeasureHandle {
        root: root_time,
        start: std::time::Duration::new(0, 0),
    };
    let info = SpanInfo { tag, parent: None };

    SpanGuard {
        id: SpanID::new(),
        time_handle,
        tx,
        info,
    }
}

#[inline]
pub fn new_span(tag: &'static str) -> OSpanGuard {
    if let Some(parent) = SPAN_STACK.with(|spans| {
        let spans = spans.borrow();
        spans.last().cloned()
    }) {
        let root_time = parent.time_handle.root;
        let time_handle = root_time.start();
        let tx = parent.tx.clone();

        let info = SpanInfo {
            tag,
            parent: Some(parent.id),
        };

        OSpanGuard(Some(SpanGuard {
            id: SpanID::new(),
            time_handle,
            tx,
            info,
        }))
    } else {
        OSpanGuard(None)
    }
}

pub struct SpanGuard {
    id: SpanID,
    time_handle: TimeMeasureHandle,
    tx: CollectorTx,
    info: SpanInfo,
}

impl SpanGuard {
    #[inline]
    pub fn enter(&self) -> Entered<'_> {
        Entered::new(self)
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        let (elapsed_start, elapsed_end) = self.time_handle.end();

        let _ = self.tx.push(Span {
            tag: self.info.tag,
            id: self.id,
            parent: self.info.parent,
            elapsed_start,
            elapsed_end,
        });
    }
}

pub struct OSpanGuard(Option<SpanGuard>);

impl OSpanGuard {
    #[inline]
    pub fn enter(&self) -> Option<Entered<'_>> {
        self.0.as_ref().map(|s| s.enter())
    }
}

pub struct Entered<'a> {
    guard: &'a SpanGuard,
}

impl<'a> Entered<'a> {
    fn new(span_guard: &'a SpanGuard) -> Self {
        SPAN_STACK.with(|spans| {
            spans
                .borrow_mut()
                .push(unsafe { std::mem::transmute(span_guard) });
        });

        Entered { guard: span_guard }
    }
}

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        let guard = SPAN_STACK
            .with(|spans| spans.borrow_mut().pop())
            .expect("corrupted stack");

        assert_eq!(guard.id, self.guard.id, "corrupted stack");
    }
}
