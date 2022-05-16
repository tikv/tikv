// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This module provides some utilities to define the tree hierarchy to trace memory.
//!
//! A memory trace is a tree that records how much memory its children and itself
//! uses, It doesn't need to match any function stacktrace, instead it should
//! have logically meaningful layout.
//!
//! For example, memory usage should be divided into several components under the
//! root scope: TiDB EndPoint, Transaction, Raft, gRPC etc. TiDB EndPoint can divide
//! its children by queries, while Raft can divide memory by store and apply. Name
//! are defined as number for better performance. In practice, it can be mapped to
//! enumerates instead.
//!
//! To define a memory trace tree, we can use the `mem_trace` macro. The `mem_trace`
//! macro constructs every node as a `MemoryTrace` which implements `MemoryTrace` trait.
//! We can also define a specified tree node by implementing `MemoryTrace` trait.

use std::{
    fmt::{self, Debug, Display, Formatter},
    num::NonZeroU64,
    ops::{Add, Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

type HashMap<K, V> =
    std::collections::HashMap<K, V, std::hash::BuildHasherDefault<fxhash::FxHasher>>;

#[derive(Debug, PartialEq, Copy, Clone, Hash, Eq)]
pub enum Id {
    Name(&'static str),
    Number(u64),
}

impl Id {
    pub fn name(&self) -> String {
        match self {
            Id::Name(s) => s.to_string(),
            Id::Number(n) => n.to_string(),
        }
    }

    pub fn readable_name(&self) -> String {
        match self {
            Id::Name(s) => {
                let mut s = s.replace('_', " ");
                s.make_ascii_lowercase();
                s
            }
            Id::Number(n) => n.to_string(),
        }
    }
}

impl From<&'static str> for Id {
    #[inline]
    fn from(s: &'static str) -> Self {
        Id::Name(s)
    }
}

impl From<u64> for Id {
    #[inline]
    fn from(id: u64) -> Self {
        Id::Number(id)
    }
}

impl From<NonZeroU64> for Id {
    #[inline]
    fn from(id: NonZeroU64) -> Self {
        Id::Number(id.into())
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Id::Number(n) => write!(f, "{}", n),
            Id::Name(n) => write!(f, "{}", n),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TraceEvent {
    Add(usize),
    Sub(usize),
    Reset(usize),
}

impl Default for TraceEvent {
    fn default() -> Self {
        TraceEvent::Add(0)
    }
}

impl Add for TraceEvent {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        match (self, other) {
            (TraceEvent::Add(l), TraceEvent::Add(r)) => TraceEvent::Add(l + r),
            (TraceEvent::Sub(l), TraceEvent::Sub(r)) => TraceEvent::Sub(l + r),
            (TraceEvent::Add(l), TraceEvent::Sub(r)) | (TraceEvent::Sub(r), TraceEvent::Add(l)) => {
                if l > r {
                    TraceEvent::Add(l - r)
                } else {
                    TraceEvent::Sub(r - l)
                }
            }
            // `r` should be smaller than `v` which it's promised by caller.
            (TraceEvent::Reset(v), TraceEvent::Sub(r)) => TraceEvent::Reset(v - r),
            (TraceEvent::Reset(v), TraceEvent::Add(r)) => TraceEvent::Reset(v + r),
            (_, e @ TraceEvent::Reset(_)) => e,
        }
    }
}

pub struct MemoryTrace {
    pub id: Id,
    trace: AtomicUsize,
    children: HashMap<Id, Arc<MemoryTrace>>,
}

impl MemoryTrace {
    pub fn new(id: impl Into<Id>) -> MemoryTrace {
        MemoryTrace {
            id: id.into(),
            trace: std::sync::atomic::AtomicUsize::default(),
            children: HashMap::default(),
        }
    }

    pub fn trace(&self, event: TraceEvent) {
        match event {
            TraceEvent::Add(val) => {
                self.trace.fetch_add(val, Ordering::Relaxed);
            }
            TraceEvent::Sub(val) => {
                self.trace.fetch_sub(val, Ordering::Relaxed);
            }
            TraceEvent::Reset(val) => {
                self.trace.swap(val, Ordering::Relaxed);
            }
        }
    }

    pub fn trace_guard<T: Default>(
        self: &Arc<MemoryTrace>,
        item: T,
        size: usize,
    ) -> MemoryTraceGuard<T> {
        self.trace(TraceEvent::Add(size));
        let node = Some(self.clone());
        MemoryTraceGuard { item, size, node }
    }

    pub fn snapshot(&self) -> MemoryTraceSnapshot {
        MemoryTraceSnapshot {
            id: self.id,
            trace: self.trace.load(Ordering::Relaxed),
            children: self.children.values().map(|c| c.snapshot()).collect(),
        }
    }

    pub fn sub_trace(&self, id: Id) -> Arc<MemoryTrace> {
        self.children.get(&id).cloned().unwrap()
    }

    pub fn add_sub_trace(&mut self, id: Id, trace: Arc<MemoryTrace>) {
        self.children.insert(id, trace);
    }

    // TODO: Maybe need a cache to reduce read cost.
    pub fn sum(&self) -> usize {
        let sum: usize = self.children.values().map(|c| c.sum()).sum();
        sum + self.trace.load(Ordering::Relaxed)
    }

    pub fn name(&self) -> String {
        self.id.name()
    }

    pub fn get_children_ids(&self) -> Vec<Id> {
        let mut ids = vec![];
        for id in self.children.keys() {
            ids.push(*id);
        }
        ids
    }
}

pub struct MemoryTraceSnapshot {
    pub id: Id,
    pub trace: usize,
    pub children: Vec<MemoryTraceSnapshot>,
}

/// Define the tree hierarchy of memory usage for a module.
/// For example there is a module:
///   - root
///     - mid1
///        - leaf1
///        - leaf2
///     - mid2
///        - leaf3
///
/// Its defination could be:
///   mem_trace!(root, [(mid1, [leaf1, leaf2]), (mid2, [leaf3])])
#[macro_export]
macro_rules! mem_trace {
    ($name: ident) => {
        {
            use tikv_alloc::trace::MemoryTrace;

            std::sync::Arc::new(MemoryTrace::new(stringify!($name)))
        }
    };
    ($name: ident, [$($child:tt),+]) => {
        {
            let mut node = mem_trace!($name);
            $(
                let child = mem_trace!($child);
                std::sync::Arc::get_mut(&mut node).unwrap().add_sub_trace(child.id, child);
            )*
            node
        }
    };
    (($name: ident, [$($child:tt),+])) => {
        mem_trace!($name, [$($child),*])
    }
}

pub struct MemoryTraceGuard<T: Default> {
    item: T,
    size: usize,
    node: Option<Arc<MemoryTrace>>,
}

impl<T: Default> MemoryTraceGuard<T> {
    pub fn map<F, U: Default>(mut self, f: F) -> MemoryTraceGuard<U>
    where
        F: FnOnce(T) -> U,
    {
        let item = std::mem::take(&mut self.item);
        MemoryTraceGuard {
            item: f(item),
            size: self.size,
            node: self.node.take(),
        }
    }

    pub fn consume(&mut self) -> T {
        if let Some(node) = self.node.take() {
            node.trace(TraceEvent::Sub(self.size));
        }
        std::mem::take(&mut self.item)
    }
}

impl<T: Default> Drop for MemoryTraceGuard<T> {
    fn drop(&mut self) {
        if let Some(node) = self.node.take() {
            node.trace(TraceEvent::Sub(self.size));
        }
    }
}

impl<T: Default> From<T> for MemoryTraceGuard<T> {
    fn from(item: T) -> Self {
        MemoryTraceGuard {
            item,
            size: 0,
            node: None,
        }
    }
}

impl<T: Default> Deref for MemoryTraceGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.item
    }
}

impl<T: Default> DerefMut for MemoryTraceGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.item
    }
}

impl<T: Default> Debug for MemoryTraceGuard<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryTraceGuard")
            .field("size", &self.size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        self as tikv_alloc,
        trace::{Id, TraceEvent},
    };

    #[test]
    fn test_id_name() {
        let id = Id::Name("test_id");
        assert_eq!(id.name(), "test_id");
        let id = Id::Number(100);
        assert_eq!(id.name(), "100");
    }

    #[test]
    fn test_id_readable_name() {
        let id = Id::Name("test_id");
        assert_eq!(id.readable_name(), "test id");
        let id = Id::Number(100);
        assert_eq!(id.readable_name(), "100");
    }

    #[test]
    fn test_mem_trace_macro() {
        let trace = mem_trace!(root, [(mid1, [leaf1, leaf2]), (mid2, [leaf3])]);
        trace
            .sub_trace(Id::Name("mid1"))
            .sub_trace(Id::Name("leaf2"))
            .trace(TraceEvent::Add(1));
        trace
            .sub_trace(Id::Name("mid2"))
            .sub_trace(Id::Name("leaf3"))
            .trace(TraceEvent::Add(10));
        trace
            .sub_trace(Id::Name("mid2"))
            .trace(TraceEvent::Add(100));
        assert_eq!(111, trace.sum());
    }

    #[test]
    fn test_trace_event_add() {
        assert_eq!(TraceEvent::Add(1) + TraceEvent::Add(1), TraceEvent::Add(2));
        assert_eq!(TraceEvent::Sub(1) + TraceEvent::Sub(1), TraceEvent::Sub(2));
        assert_eq!(TraceEvent::Add(1) + TraceEvent::Sub(1), TraceEvent::Sub(0));
        assert_eq!(TraceEvent::Sub(1) + TraceEvent::Add(1), TraceEvent::Sub(0));
        assert_eq!(TraceEvent::Add(1) + TraceEvent::Sub(2), TraceEvent::Sub(1));
        assert_eq!(TraceEvent::Sub(2) + TraceEvent::Add(1), TraceEvent::Sub(1));
        assert_eq!(TraceEvent::Sub(2) + TraceEvent::Add(3), TraceEvent::Add(1));

        assert_eq!(
            TraceEvent::Add(1) + TraceEvent::Reset(3),
            TraceEvent::Reset(3)
        );
        assert_eq!(
            TraceEvent::Sub(1) + TraceEvent::Reset(3),
            TraceEvent::Reset(3)
        );
        assert_eq!(
            TraceEvent::Reset(3) + TraceEvent::Add(1),
            TraceEvent::Reset(4)
        );
        assert_eq!(
            TraceEvent::Reset(3) + TraceEvent::Sub(1),
            TraceEvent::Reset(2)
        );
    }
}
