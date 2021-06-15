// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Weak};

pub fn make_pair() -> (Alive, Probe) {
    let flag = Arc::new(0);
    let weak = Arc::downgrade(&flag);

    (Alive(flag), Probe(weak))
}

pub struct Alive(Arc<u32>);

#[derive(Clone)]
pub struct Probe(Weak<u32>);

impl Probe {
    pub fn is_alive(&self) -> bool {
        self.0.upgrade().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_liveness_probe() {
        let (alive, probe) = make_pair();
        let wg = crossbeam::sync::WaitGroup::new();
        let wg0 = wg.clone();
        let handle = std::thread::spawn(move || {
            let _alive = alive;
            wg0.wait();
        });

        assert!(probe.is_alive());
        drop(wg);
        handle.join().unwrap();
        assert!(!probe.is_alive());
    }
}
