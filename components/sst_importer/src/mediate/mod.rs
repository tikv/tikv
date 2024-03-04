// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
    time::{Duration, Instant},
};

use futures::compat::Future01CompatExt;
use uuid::Uuid;

mod observer;
pub use observer::IngestObserver;

#[derive(Clone)]
pub enum Event {
    Pause {
        region_id: u64,
        uuid: Uuid,
        deadline: Instant,
    },
    Continue {
        region_id: u64,
        uuid: Uuid,
    },
}

#[derive(Debug)]
struct LeaseState {
    deadline: Instant,
    ref_count: Arc<AtomicU64>,
}

impl LeaseState {
    fn ref_(&self) -> LeaseRef {
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        LeaseRef {
            lease: LeaseState {
                deadline: self.deadline,
                ref_count: self.ref_count.clone(),
            },
        }
    }

    fn has_ref(&self) -> bool {
        self.ref_count.load(Ordering::SeqCst) != 0
    }

    fn expire(&mut self) {
        self.deadline = Instant::now() - Duration::from_secs(1);
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.deadline
    }
}

#[derive(Debug)]
pub struct LeaseRef {
    lease: LeaseState,
}

impl LeaseRef {
    pub fn is_expired(&self) -> bool {
        self.lease.is_expired()
    }
}

impl Drop for LeaseRef {
    fn drop(&mut self) {
        self.lease.ref_count.fetch_sub(1, Ordering::SeqCst);
    }
}

pub trait Observer: Sync + Send {
    fn update(&self, event: Event);
    fn query(&self, region_id: u64, uuid: &Uuid) -> Option<LeaseRef>;
    // Returns a valid lease uuid for a region.
    // Called by Resolver before advancing resolved_ts.
    fn get_region_lease(&self, region_id: u64) -> Option<Uuid>;
    fn gc(&self);
}

pub trait Mediator: Sync + Send {
    fn pause(&self, region_id: u64, uuid: Uuid, deadline: Instant);
    fn continues(&self, region_id: u64, uuid: Uuid);
    fn register(&mut self, comp: Arc<dyn Observer>);
    fn gc(&self);
}

#[derive(Default)]
pub struct IngestMediator {
    comps: Vec<Arc<dyn Observer>>,
}

impl Mediator for IngestMediator {
    fn pause(&self, region_id: u64, uuid: Uuid, deadline: Instant) {
        let event = Event::Pause {
            region_id,
            uuid,
            deadline,
        };
        for comp in &self.comps {
            comp.update(event.clone())
        }
    }

    fn continues(&self, region_id: u64, uuid: Uuid) {
        let event = Event::Continue { region_id, uuid };
        for comp in &self.comps {
            comp.update(event.clone())
        }
    }

    fn register(&mut self, comp: Arc<dyn Observer>) {
        self.comps.push(comp)
    }

    fn gc(&self) {
        for comp in &self.comps {
            comp.gc();
        }
    }
}

pub async fn periodic_gc_mediator(mediator: Weak<dyn Mediator>, duration: Duration) {
    loop {
        let Some(m) = mediator.upgrade() else {
            return;
        };
        m.gc();
        let _ = tikv_util::timer::GLOBAL_TIMER_HANDLE
            .delay(Instant::now() + duration)
            .compat()
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use futures::executor::block_on;

    use super::*;

    #[test]
    fn test_lease_ref_is_expired() {
        let ttl = Duration::from_millis(200);
        let lease = LeaseState {
            deadline: Instant::now() + ttl,
            ref_count: Arc::default(),
        };
        let ref_ = lease.ref_();
        assert!(!ref_.is_expired());
        std::thread::sleep(2 * ttl);
        assert!(ref_.is_expired());
    }

    #[test]
    fn test_lease_ref_count() {
        let lease = LeaseState {
            deadline: Instant::now(),
            ref_count: Arc::default(),
        };
        assert!(!lease.has_ref());

        let ref1 = lease.ref_();
        assert!(lease.has_ref());
        let ref2 = lease.ref_();
        assert_eq!(lease.ref_count.load(Ordering::SeqCst), 2);

        drop(ref1);
        assert_eq!(lease.ref_count.load(Ordering::SeqCst), 1);
        drop(ref2);
        assert_eq!(lease.ref_count.load(Ordering::SeqCst), 0);
        assert!(!lease.has_ref());
    }

    #[test]
    fn test_gc() {
        struct Mock {
            gc_count: AtomicU64,
        }
        impl Observer for Mock {
            fn update(&self, _: Event) {}
            fn query(&self, _: u64, _: &Uuid) -> Option<LeaseRef> {
                None
            }
            fn get_region_lease(&self, _: u64) -> Option<Uuid> {
                None
            }
            fn gc(&self) {
                self.gc_count.fetch_add(1, Ordering::SeqCst);
            }
        }

        let mock = Arc::new(Mock {
            gc_count: AtomicU64::new(0),
        });
        let mut mediator = IngestMediator::default();
        mediator.register(mock.clone());
        let mediator = Arc::new(mediator);
        let mediator_weak = Arc::downgrade(&mediator);
        thread::spawn(move || {
            block_on(periodic_gc_mediator(
                mediator_weak,
                Duration::from_millis(100),
            ))
        });
        thread::sleep(Duration::from_millis(500));
        assert!(mock.gc_count.load(Ordering::SeqCst) > 2);
    }
}
