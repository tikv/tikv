#[macro_use]
extern crate lazy_static;

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod future;
pub mod time_measure;
pub mod util;

pub use tracer_attribute;

use time_measure::*;

pub type ID = usize;
const FACTOR: usize = 100;
pub const TIME_MEASURE_TYPE: TimeMeasureType = TimeMeasureType::Instant;

#[derive(Debug)]
pub struct Span {
    pub tag: &'static str,
    pub id: ID,
    pub parent: Option<ID>,
    pub elapsed_start: std::time::Duration,
    pub elapsed_end: std::time::Duration,
}

pub struct SpanInner {
    sender: crossbeam::channel::Sender<Span>,
    root: TimeMeasureRoot,
    time_handle: TimeMeasureHandle,
}

pub struct ArcSpanInner {
    tag: &'static str,
    ref_count: std::sync::atomic::AtomicUsize,
    id: Option<ID>,
    parent: Option<ID>,
    span_inner: Vec<SpanInner>,
}

impl Drop for ArcSpanInner {
    fn drop(&mut self) {
        for span in self.span_inner.drain(..) {
            let (start, end) = span.time_handle.end();
            let _ = span.sender.try_send(Span {
                tag: self.tag,
                id: self.id.unwrap(),
                parent: self.parent,
                elapsed_start: start,
                elapsed_end: end,
            });
        }

        if let Some(parent_id) = self.parent {
            let arc_span = REGISTRY.spans.get(parent_id).expect("can not get parent");

            if arc_span
                .ref_count
                .fetch_sub(1, std::sync::atomic::Ordering::Release)
                == 1
            {
                drop(arc_span);
                std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
                let mut arc_span = REGISTRY.spans.take(parent_id).expect("can not get span");
                arc_span.id = Some(parent_id);
            }
        }
    }
}

thread_local! {
    static SPAN_STACK: std::cell::RefCell<Vec<usize>> = std::cell::RefCell::new(vec![]);
}

pub struct Registry {
    spans: sharded_slab::Slab<ArcSpanInner>,
}

lazy_static! {
    static ref REGISTRY: Registry = Registry {
        spans: sharded_slab::Slab::new()
    };
}

pub fn new_span_root(
    tag: &'static str,
    sender: crossbeam::channel::Sender<Span>,
    time_measure_type: TimeMeasureType,
) -> SpanGuard {
    let mut span_inners = Vec::with_capacity(FACTOR);

    for _ in 0..FACTOR {
        let root = TimeMeasure::root(time_measure_type);
        span_inners.push(SpanInner {
            sender: sender.clone(),
            root,
            time_handle: root.start(),
        });
    }

    let id = REGISTRY
        .spans
        .insert(ArcSpanInner {
            tag,
            id: None,
            parent: None,
            ref_count: std::sync::atomic::AtomicUsize::new(1),
            span_inner: span_inners,
        })
        .expect("full");

    SpanGuard { id }
}

pub fn new_span(tag: &'static str) -> OSpanGuard {
    if let Some(parent_id) = SPAN_STACK.with(|spans| {
        let spans = spans.borrow();
        spans.last().cloned()
    }) {
        let mut span_inners = Vec::with_capacity(FACTOR);

        let parent_arc_span = REGISTRY
            .spans
            .get(parent_id)
            .expect("can not get parent span");

        parent_arc_span
            .ref_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        for parent_span in parent_arc_span.span_inner.iter() {
            let root = parent_span.root;
            span_inners.push(SpanInner {
                sender: parent_span.sender.clone(),
                root,
                time_handle: root.start(),
            })
        }

        let id = REGISTRY
            .spans
            .insert(ArcSpanInner {
                tag,
                id: None,
                parent: Some(parent_id),
                ref_count: std::sync::atomic::AtomicUsize::new(1),
                span_inner: span_inners,
            })
            .expect("full");

        OSpanGuard(Some(SpanGuard { id }))
    } else {
        OSpanGuard(None)
    }
}

pub struct SpanGuard {
    id: ID,
}

impl SpanGuard {
    pub fn enter(&self) -> Entered<'_> {
        SPAN_STACK.with(|spans| {
            spans.borrow_mut().push(self.id);
        });

        Entered { guard: &self }
    }
}

pub struct OSpanGuard(Option<SpanGuard>);

impl OSpanGuard {
    pub fn enter(&self) -> Option<Entered<'_>> {
        self.0.as_ref().map(|s| s.enter())
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        let arc_span = REGISTRY.spans.get(self.id).expect("can not get span");

        if arc_span
            .ref_count
            .fetch_sub(1, std::sync::atomic::Ordering::Release)
            == 1
        {
            drop(arc_span);
            std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
            let mut arc_span = REGISTRY.spans.take(self.id).expect("can not get span");
            arc_span.id = Some(self.id);
        }
    }
}

pub struct Entered<'a> {
    guard: &'a SpanGuard,
}

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        let id = SPAN_STACK
            .with(|spans| spans.borrow_mut().pop())
            .expect("corrupted stack");

        assert_eq!(id, self.guard.id, "corrupted stack");
    }
}
