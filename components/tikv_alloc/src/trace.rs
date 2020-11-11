// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use std::fmt::{self, Display};
use std::num::NonZeroU64;
use std::sync::Mutex;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Id {
    Name(&'static str),
    Number(u64),
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

#[derive(Debug)]
pub struct MemoryTrace {
    id: Id,
    size: Option<usize>,
    children: Vec<MemoryTrace>,
}

impl MemoryTrace {
    fn new(id: impl Into<Id>) -> MemoryTrace {
        MemoryTrace {
            id: id.into(),
            size: None,
            children: vec![],
        }
    }

    pub fn add_sub_trace(&mut self, id: impl Into<Id>) -> &mut MemoryTrace {
        self.children.push(MemoryTrace::new(id));
        self.children.last_mut().unwrap()
    }

    pub fn add_sub_trace_with_capacity(
        &mut self,
        id: impl Into<Id>,
        cap: usize,
    ) -> &mut MemoryTrace {
        let mut trace = MemoryTrace::new(id);
        trace.children.reserve(cap);
        self.children.push(trace);
        self.children.last_mut().unwrap()
    }

    pub fn set_size(&mut self, size: usize) {
        self.size = Some(size);
    }

    pub fn add_size(&mut self, size: usize) {
        self.size = Some(self.size.unwrap_or_default() + size);
    }

    pub fn size(&self) -> usize {
        self.size.unwrap_or_default()
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn children(&self) -> &[MemoryTrace] {
        &self.children
    }
}

pub trait MemoryTraceProvider {
    fn trace(&mut self, dump: &mut MemoryTrace);
}

pub struct MemoryTraceManager {
    providers: Vec<Box<dyn MemoryTraceProvider + Send>>,
}

impl MemoryTraceManager {
    pub fn snapshot(&mut self) -> MemoryTrace {
        let mut trace = MemoryTrace::new("TiKV");
        for provider in &mut self.providers {
            provider.trace(&mut trace);
        }
        trace
    }

    pub fn register_provider(&mut self, provider: Box<dyn MemoryTraceProvider + Send>) {
        self.providers.push(provider);
    }
}

lazy_static! {
    static ref GLOBAL_MEMORY_TRACE_MANAGER: Mutex<MemoryTraceManager> =
        Mutex::new(MemoryTraceManager { providers: vec![] });
}

pub fn register_provider(provider: Box<dyn MemoryTraceProvider + Send>) {
    let mut manager = GLOBAL_MEMORY_TRACE_MANAGER.lock().unwrap();
    manager.register_provider(provider);
}

pub fn snapshot() -> MemoryTrace {
    let mut manager = GLOBAL_MEMORY_TRACE_MANAGER.lock().unwrap();
    manager.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Provider {
        id: Id,
        children: Vec<Provider>,
        size: usize,
    }

    impl MemoryTraceProvider for Provider {
        fn trace(&mut self, dump: &mut MemoryTrace) {
            let subtrace = dump.add_sub_trace(self.id);
            subtrace.set_size(self.size);
            for child in &mut self.children {
                child.trace(subtrace);
            }
            let total = subtrace.size();
            dump.add_size(total);
        }
    }

    #[test]
    fn test_basic() {
        let region_1 = Provider {
            id: Id::Number(1),
            children: vec![],
            size: 2,
        };
        let raft_provider = Provider {
            id: Id::Name("raft"),
            children: vec![region_1],
            size: 3,
        };
        super::register_provider(Box::new(raft_provider));
        let trace = super::snapshot();
        assert_eq!(trace.size(), 5);
        assert_eq!(trace.id, Id::Name("TiKV"));
        assert_eq!(trace.children.len(), 1);
        let c = &trace.children[0];
        assert_eq!(c.size(), 5);
        assert_eq!(c.id, Id::Name("raft"));
        assert_eq!(c.children.len(), 1);
        let c = &c.children[0];
        assert_eq!(c.size(), 2);
        assert_eq!(c.id, Id::Number(1));
        assert!(c.children.is_empty());
    }
}
