// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct Limiter {
    is_acquired: Arc<AtomicBool>,
}

impl Limiter {
    pub fn try_acquire(&self) -> Option<Handle> {
        (!self.is_acquired.swap(true, Ordering::Relaxed)).then(|| Handle {
            acquired: self.is_acquired.clone(),
        })
    }
}

pub struct Handle {
    acquired: Arc<AtomicBool>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        assert!(self.acquired.swap(false, Ordering::Relaxed));
    }
}
