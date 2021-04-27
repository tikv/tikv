// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::num::NonZeroU64;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Weak,
};
use std::{
    cmp,
    fmt::{self, Display},
};

pub type HashMap<K, V> =
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
    Add,
    Sub,
    Reset,
}

pub trait MemoryTrace {
    fn trace(&self, event: TraceEvent, val: usize);
    fn snapshot(&self, parent: &mut MemoryTraceSnapshot);
    fn sub_trace(&self, id: Id) -> Option<Arc<dyn MemoryTrace>>;
}

pub struct MemoryTraceNode {
    id: Id,
    parent: Option<Weak<MemoryTraceNode>>,
    trace: AtomicUsize,
    children: HashMap<Id, Arc<dyn MemoryTrace>>,
}

impl MemoryTraceNode {
    fn parent_trace(&self, event: TraceEvent, val: usize) {
        if let Some(parent) = self.parent.as_ref() {
            if let Some(p) = parent.upgrade() {
                p.trace(event, val);
            }
        }
    }
}

impl MemoryTrace for MemoryTraceNode {
    fn trace(&self, event: TraceEvent, val: usize) {
        match event {
            TraceEvent::Add => {
                self.trace.fetch_add(val, Ordering::Relaxed);
                self.parent_trace(event, val);
            }
            TraceEvent::Sub => {
                self.trace.fetch_sub(val, Ordering::Relaxed);
                self.parent_trace(event, val);
            }
            TraceEvent::Reset => {
                let prev = self.trace.swap(val, Ordering::Relaxed);
                match prev.cmp(&val) {
                    cmp::Ordering::Greater => self.parent_trace(TraceEvent::Sub, prev - val),
                    cmp::Ordering::Less => self.parent_trace(TraceEvent::Add, val - prev),
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
        for (_, child) in &self.children {
            child.snapshot(&mut snap);
        }
        parent.children.push(snap);
    }

    fn sub_trace(&self, id: Id) -> Option<Arc<dyn MemoryTrace>> {
        self.children.get(&id).cloned()
    }
}

pub struct MemoryTraceSnapshot {
    pub id: Id,
    pub trace: usize,
    pub children: Vec<MemoryTraceSnapshot>,
}

macro_rules! mem_trace {
    ($name: ident) => {
        {
            Arc::new(MemoryTraceNode {
                id: stringify!($name).into(),
                parent: None,
                trace: AtomicUsize::default(),
                children: Vec::new(),
            })
        }
    };
    ($name: ident, [$($child:tt),+]) => {
        {
            let mut node = mem_trace!($name);
            $(
                let mut child = mem_trace!($child);
                unsafe {
                    std::sync::Arc::get_mut_unchecked(&mut child).parent = Some(Arc::downgrade(&node));
                    std::sync::Arc::get_mut_unchecked(&mut node).children.push(child);
                }
            )*
            node
        }
    };
    (($name: ident, [$($child:tt),+])) => {
        mem_trace!($name, [$($child),*])
    }
}
