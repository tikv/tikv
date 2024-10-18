// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains the implementation of the mediator pattern for the
//! sst_importer component in TiKV. It provides the `Mediator` trait and the
//! `IngestMediator` struct, which act as a central hub for communication
//! between different observers. Observers can register with the mediator and
//! receive events through the `Observer` trait.
//! Together, they resolves the compatibility issue between the sst_importer
//! and resolved_ts.

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

/// The event type that the mediator can send to observers.
#[derive(Clone)]
pub enum Event {
    Acquire {
        region_id: u64,
        uuid: Uuid,
        deadline: Instant,
    },
    Release {
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

/// A reference to a lease. It should be hold when a long time import RPC is in
/// progress.
#[derive(Debug)]
pub struct LeaseRef {
    lease: LeaseState,
}

impl LeaseRef {
    /// Checks if the lease is expired.
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
    /// Returns a lease specified by region_id and uuid if the lease is valid.
    fn get_lease(&self, region_id: u64, uuid: &Uuid) -> Option<LeaseRef>;
    /// Returns a valid lease uuid for a region.
    /// Called by Resolver before advancing resolved_ts.
    fn get_region_lease(&self, region_id: u64) -> Option<Uuid>;
    /// Garbage collection and it should never block.
    fn gc(&self);
}

pub trait Mediator: Sync + Send {
    fn acquire(&self, region_id: u64, uuid: Uuid, deadline: Instant);
    fn release(&self, region_id: u64, uuid: Uuid);
    fn register(&mut self, comp: Arc<dyn Observer>);
    fn gc(&self);
}

#[derive(Default)]
pub struct IngestMediator {
    comps: Vec<Arc<dyn Observer>>,
}

impl Mediator for IngestMediator {
    fn acquire(&self, region_id: u64, uuid: Uuid, deadline: Instant) {
        let event = Event::Acquire {
            region_id,
            uuid,
            deadline,
        };
        for comp in &self.comps {
            comp.update(event.clone())
        }
    }

    fn release(&self, region_id: u64, uuid: Uuid) {
        let event = Event::Release { region_id, uuid };
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

/// Periodically triggers garbage collection on the mediator.
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
            fn get_lease(&self, _: u64, _: &Uuid) -> Option<LeaseRef> {
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
