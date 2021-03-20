// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicPtr, Ordering};

type AllocFn = unsafe fn(Layout) -> *mut u8;
type DeallocFn = unsafe fn(*mut u8, Layout);

/// Used to initialize the plugin's allocator.
///
/// A `HostAllocatorPtr` contains the relevant pointers to initialize the allocator of
/// to plugin. It will be passed from TiKV to the plugin.
#[repr(C)]
pub struct HostAllocatorPtr {
    pub alloc_fn: AllocFn,
    pub dealloc_fn: DeallocFn,
}

/// An allocator that forwards invocations to the host (TiKV) of the plugin.
pub struct HostAllocator {
    alloc_fn: AtomicPtr<AllocFn>,
    dealloc_fn: AtomicPtr<DeallocFn>,
}

impl HostAllocator {
    /// Creates a new [`HostAllocator`].
    ///
    /// The internal function pointers are set to `nullptr`, so any attempt to allocate memory
    /// before a call to [`set_allocator()`] will result in a segmentation fault.
    pub const fn new() -> Self {
        HostAllocator {
            alloc_fn: AtomicPtr::new(std::ptr::null_mut()),
            dealloc_fn: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Updates the function pointers of the [`HostAllocator`] to the given [`HostAllocatorPtr`].
    /// This function needs to be called before _any_ allocation with this allocator is performed,
    /// because otherwise the [`HostAllocator`] is in an invalid state.
    pub fn set_allocator(&self, allocator: HostAllocatorPtr) {
        self.alloc_fn
            .store(allocator.alloc_fn as *mut _, Ordering::SeqCst);
        self.dealloc_fn
            .store(allocator.dealloc_fn as *mut _, Ordering::SeqCst);
    }
}

unsafe impl GlobalAlloc for HostAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        (*self.alloc_fn.load(Ordering::Relaxed))(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        (*self.dealloc_fn.load(Ordering::Relaxed))(ptr, layout)
    }
}
