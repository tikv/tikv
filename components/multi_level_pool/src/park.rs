// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::queue::ArrayQueue;
use parking_lot::{Condvar, Mutex};

use std::sync::Arc;

#[derive(Clone)]
pub struct Parker(Arc<ParkerInner>);

struct ParkerInner {
    notified: Mutex<bool>,
    condvar: Condvar,
}

impl Parker {
    pub fn new() -> Parker {
        let inner = ParkerInner {
            notified: Mutex::new(false),
            condvar: Condvar::new(),
        };
        Parker(Arc::new(inner))
    }

    pub fn park(&self, sleepers: &ArrayQueue<Parker>) {
        let mut notified = self.0.notified.lock();
        if *notified {
            *notified = false;
        } else {
            sleepers.push(self.clone()).expect("sleeper queue is empty");
            self.0.condvar.wait(&mut notified);
        }
    }

    pub fn unpark(self) {
        *self.0.notified.lock() = true;
        self.0.condvar.notify_one();
    }
}
