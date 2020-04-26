#[macro_use]
extern crate lazy_static;

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod future;
pub mod util;
pub type ID = usize;
pub use tracer_attribute;

#[derive(Debug)]
pub struct Span {
    pub tag: &'static str,
    pub id: ID,
    pub parent: Option<ID>,
    pub elapsed_start: std::time::Duration,
    pub elapsed_end: std::time::Duration,
}

pub struct SpanInner {
    tag: &'static str,
    sender: crossbeam::channel::Sender<Span>,
    id: Option<ID>,
    parent: Option<ID>,
    root_start_time: std::time::Instant,
    elapsed_start: std::time::Duration,
    ref_count: std::sync::atomic::AtomicUsize,
}

impl Drop for SpanInner {
    fn drop(&mut self) {
        let _ = self.sender.try_send(Span {
            tag: self.tag,
            id: self.id.unwrap(),
            parent: self.parent,
            elapsed_start: self.elapsed_start,
            elapsed_end: self.root_start_time.elapsed(),
        });

        if let Some(parent_id) = self.parent {
            let span = REGISTRY.spans.get(parent_id).expect("can not get parent");

            if span
                .ref_count
                .fetch_sub(1, std::sync::atomic::Ordering::Release)
                == 1
            {
                drop(span);
                std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
                let mut span = REGISTRY.spans.take(parent_id).expect("can not get span");
                span.id = Some(parent_id);
            }
        }
    }
}

thread_local! {
    static SPAN_STACK: std::cell::RefCell<Vec<usize>> = std::cell::RefCell::new(vec![]);
}

pub struct Registry {
    spans: sharded_slab::Slab<SpanInner>,
}

lazy_static! {
    static ref REGISTRY: Registry = Registry {
        spans: sharded_slab::Slab::new()
    };
}

pub fn new_span_root(tag: &'static str, sender: crossbeam::channel::Sender<Span>) -> SpanGuard {
    let id = REGISTRY
        .spans
        .insert(SpanInner {
            tag,
            sender,
            id: None,
            parent: None,
            root_start_time: std::time::Instant::now(),
            elapsed_start: std::time::Duration::new(0, 0),
            ref_count: std::sync::atomic::AtomicUsize::new(1),
        })
        .expect("full");

    SpanGuard { id }
}

pub fn new_span(tag: &'static str) -> OSpanGuard {
    if let Some(parent_id) = SPAN_STACK.with(|spans| {
        let spans = spans.borrow();
        spans.last().cloned()
    }) {
        let span = REGISTRY
            .spans
            .get(parent_id)
            .expect("can not get parent span");

        span.ref_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let sender = span.sender.clone();

        let id = REGISTRY
            .spans
            .insert(SpanInner {
                tag,
                sender,
                id: None,
                parent: Some(parent_id),
                root_start_time: span.root_start_time,
                elapsed_start: span.root_start_time.elapsed(),
                ref_count: std::sync::atomic::AtomicUsize::new(1),
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
        let span = REGISTRY.spans.get(self.id).expect("can not get span");

        if span
            .ref_count
            .fetch_sub(1, std::sync::atomic::Ordering::Release)
            == 1
        {
            drop(span);
            std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
            let mut span = REGISTRY.spans.take(self.id).expect("can not get span");
            span.id = Some(self.id);
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
