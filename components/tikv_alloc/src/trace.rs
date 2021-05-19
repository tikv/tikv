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
//! macro constructs every node as a `MemoryTraceNode` which implements `MemoryTrace` trait.
//! We can also define a specified tree node by implementing `MemoryTrace` trait.

use std::num::NonZeroU64;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Weak,
};
use std::{
    cmp,
    fmt::{self, Display},
};

type HashMap<K, V> =
    std::collections::HashMap<K, V, std::hash::BuildHasherDefault<fxhash::FxHasher>>;

#[derive(Debug, PartialEq, Copy, Clone, Hash, Eq)]
pub enum Id {
    Name(&'static str),
    Number(u64),
}

impl Id {
    pub fn readable_name(&self) -> String {
        match self {
            Id::Name(s) => {
                let mut s = s.to_string();
                s.make_ascii_lowercase();
                let mut res = String::with_capacity(s.len());
                for p in s.split('_') {
                    res.push_str(p);
                    res.push(' ');
                }
                res
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

#[derive(Clone, Copy)]
pub enum TraceEvent {
    Add(usize),
    Sub(usize),
    Reset(usize),
}

pub trait MemoryTrace {
    fn trace(&self, event: TraceEvent);
    fn snapshot(&self, parent: &mut MemoryTraceSnapshot);
    fn sub_trace(&self, id: Id) -> Arc<dyn MemoryTrace + Send + Sync>;
    fn add_sub_trace(&mut self, id: Id, trace: Arc<dyn MemoryTrace + Send + Sync>);
}

pub struct MemoryTraceNode {
    pub id: Id,
    parent: Option<Weak<dyn MemoryTrace + Send + Sync>>,
    trace: AtomicUsize,
    children: HashMap<Id, Arc<dyn MemoryTrace + Send + Sync>>,
}

impl MemoryTraceNode {
    pub fn new(id: impl Into<Id>) -> MemoryTraceNode {
        MemoryTraceNode {
            id: id.into(),
            parent: None,
            trace: std::sync::atomic::AtomicUsize::default(),
            children: HashMap::default(),
        }
    }

    fn parent_trace(&self, event: TraceEvent) {
        if let Some(parent) = self.parent.as_ref() {
            if let Some(p) = parent.upgrade() {
                p.trace(event);
            }
        }
    }

    pub fn set_parent(&mut self, parent: &Arc<dyn MemoryTrace + Send + Sync>) {
        self.parent = Some(Arc::downgrade(parent));
    }
}

impl MemoryTrace for MemoryTraceNode {
    fn trace(&self, event: TraceEvent) {
        match event {
            TraceEvent::Add(val) => {
                self.trace.fetch_add(val, Ordering::Relaxed);
                self.parent_trace(event);
            }
            TraceEvent::Sub(val) => {
                self.trace.fetch_sub(val, Ordering::Relaxed);
                self.parent_trace(event);
            }
            TraceEvent::Reset(val) => {
                let prev = self.trace.swap(val, Ordering::Relaxed);
                match prev.cmp(&val) {
                    cmp::Ordering::Greater => self.parent_trace(TraceEvent::Sub(prev - val)),
                    cmp::Ordering::Less => self.parent_trace(TraceEvent::Add(val - prev)),
                    cmp::Ordering::Equal => (),
                }
            }
        }
    }

    fn snapshot(&self, parent: &mut MemoryTraceSnapshot) {
        let mut snap = MemoryTraceSnapshot {
            id: self.id,
            trace: self.trace.load(Ordering::Relaxed),
            children: vec![],
        };
        for child in self.children.values() {
            child.snapshot(&mut snap);
        }
        parent.children.push(snap);
    }

    fn sub_trace(&self, id: Id) -> Arc<dyn MemoryTrace + Send + Sync> {
        self.children.get(&id).cloned().unwrap()
    }

    fn add_sub_trace(&mut self, id: Id, trace: Arc<dyn MemoryTrace + Send + Sync>) {
        self.children.insert(id, trace);
    }
}

pub struct MemoryTraceSnapshot {
    pub id: Id,
    pub trace: usize,
    pub children: Vec<MemoryTraceSnapshot>,
}

/// Define the tree hierarchy of memory usage for a module.
/// For example there is a module:
///   root
///     - mid1
///       - leaf1
///       - leaf2
///     - mid2
///       - leaf3
/// Its defination could be:
///   mem_trace!(root, (mid1, [leaf1, leaf2]), (mid2, [leaf3]))
#[macro_export]
macro_rules! mem_trace {
    ($name: ident) => {
        {
            use tikv_alloc::trace::MemoryTraceNode;

            std::sync::Arc::new(MemoryTraceNode::new(stringify!($name)))
        }
    };
    ($name: ident, [$($child:tt),+]) => {
        {
            use tikv_alloc::trace::MemoryTrace;

            let mut node = mem_trace!($name);
            $(
                let mut child = mem_trace!($child);
                unsafe {
                    std::sync::Arc::get_mut_unchecked(&mut child).set_parent(&(node.clone() as Arc<dyn MemoryTrace + Send + Sync>));
                    std::sync::Arc::get_mut_unchecked(&mut node).add_sub_trace(child.id, child);
                }
            )*
            node
        }
    };
    (($name: ident, [$($child:tt),+])) => {
        mem_trace!($name, [$($child),*])
    }
}
