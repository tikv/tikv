// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{alloc::Layout, ptr, sync::Arc};

use super::{
    list::{Node, ReclaimableNode},
    memory_control::MemoryLimiter,
};
use crate::skiplist::list::U_SIZE;

#[derive(Clone)]
pub struct Arena<M: MemoryLimiter> {
    pub limiter: Arc<M>,
}

impl<M: MemoryLimiter> Arena<M> {
    pub fn new(limiter: Arc<M>) -> Self {
        Arena { limiter }
    }

    /// Alloc 8-byte aligned memory.
    pub fn alloc(&self, size: usize) -> usize {
        // todo: alloc returns Result
        assert!(self.limiter.acquire(size));

        let layout = Layout::from_size_align(size, U_SIZE).unwrap();
        let addr = unsafe { std::alloc::alloc(layout) as usize };
        self.limiter.allocated(addr, size);
        addr
    }

    pub fn free(&self, ptr: *const Node) {
        let ptr = ptr as *mut Node;
        let size = {
            let node = unsafe { &*ptr };
            node.size()
        };

        self.limiter.freed(ptr as usize, size);
        let layout = Layout::from_size_align(size, U_SIZE).unwrap();
        unsafe {
            if !(*ptr).key.is_empty() {
                ptr::drop_in_place(&mut (*ptr).key);
            }
            if !(*ptr).value.is_empty() {
                ptr::drop_in_place(&mut (*ptr).value);
            }
            std::alloc::dealloc(ptr as *mut u8, layout);
        }
        self.limiter.reclaim(size);
    }
}
