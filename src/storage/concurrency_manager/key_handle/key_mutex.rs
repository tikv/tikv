// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use event_listener::Event;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct KeyMutex {
    mutex_locked: AtomicBool,
    mutex_released_event: Event,
}

impl KeyMutex {
    pub fn new() -> Self {
        KeyMutex {
            mutex_locked: AtomicBool::new(false),
            mutex_released_event: Event::new(),
        }
    }

    pub async fn mutex_lock(&self) {
        loop {
            if self
                .mutex_locked
                .compare_and_swap(false, true, Ordering::SeqCst)
            {
                // current value of locked is true, listen to the release event
                self.mutex_released_event.listen().await;
            } else {
                break;
            }
        }
    }

    pub fn unlock(&self) {
        self.mutex_locked.store(false, Ordering::SeqCst);
        self.mutex_released_event.notify(1);
    }
}
