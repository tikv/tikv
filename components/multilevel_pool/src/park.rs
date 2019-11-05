// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crossbeam::queue::ArrayQueue;
use parking_lot::{Condvar, Mutex};

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

struct ParkPair {
    notified: Mutex<bool>,
    condvar: Condvar,
}

pub struct Parker {
    sleepers: Arc<ArrayQueue<Unparker>>,
    park_pair: Arc<ParkPair>,
}

impl Parker {
    pub fn new(sleepers: Arc<ArrayQueue<Unparker>>) -> Parker {
        Parker {
            sleepers,
            park_pair: Arc::new(ParkPair {
                notified: Mutex::new(false),
                condvar: Condvar::new(),
            }),
        }
    }

    pub fn park(&self) {
        let mut notified = self.park_pair.notified.lock();
        if *notified {
            *notified = false;
        } else {
            self.sleepers
                .push(Unparker(self.park_pair.clone()))
                .expect("sleeper queue is empty");
            self.park_pair.condvar.wait(&mut notified);
        }
    }
}

pub struct Unparker(Arc<ParkPair>);

impl Unparker {
    pub fn unpark(self) {
        *self.0.notified.lock() = true;
        self.0.condvar.notify_one();
    }
}
