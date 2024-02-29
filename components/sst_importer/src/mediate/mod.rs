// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use collections::{HashMap, HashMapEntry};
use uuid::Uuid;

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
}

pub trait Mediator: Sync + Send {
    fn pause(&self, region_id: u64, uuid: Uuid, deadline: Instant);
    fn continues(&self, region_id: u64, uuid: Uuid);
    fn register(&mut self, comp: Arc<dyn Observer>);
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
}

#[derive(Default)]
pub struct IngestObserver {
    // region_id -> (uuid -> deadline)
    sst_leases: RwLock<HashMap<u64, HashMap<Uuid, LeaseState>>>,
}

impl Observer for IngestObserver {
    fn update(&self, event: Event) {
        match event {
            Event::Pause {
                region_id,
                uuid,
                deadline,
            } => {
                self.upsert_lease(region_id, uuid, deadline);
            }
            Event::Continue { region_id, uuid } => {
                self.expire_lease(region_id, &[uuid]);
            }
        }
        // TODO: batch clean up expired leases.
    }

    fn query(&self, region_id: u64, uuid: &Uuid) -> Option<LeaseRef> {
        let ssts = self.sst_leases.read().unwrap();
        let Some(leases) = ssts.get(&region_id) else {
            return None;
        };
        warn!("dbg query"; "region_id" => region_id, "leases" => format!("{:?}", ssts.get(&region_id)));
        leases.get(uuid).map(|lease| lease.ref_())
    }

    fn get_region_lease(&self, region_id: u64) -> Option<Uuid> {
        let ssts = self.sst_leases.read().unwrap();
        let leases = ssts.get(&region_id)?;
        for (uuid, lease) in leases {
            if !lease.is_expired() || lease.has_ref() {
                return Some(*uuid);
            }
        }
        None
    }

    // fn query(&self, region_id: u64, uuid: Option<Uuid>) -> Option<Instant> {
    //     let mut expired_leases = vec![];
    //     let mut valid_lease = None;
    //     {
    //         let now = Instant::now();
    //         let ssts = self.sst_leases.read().unwrap();
    //         let Some(leases) = ssts.get(&region_id) else {
    //             return None;
    //         };
    //         for (uuid, deadline) in leases {
    //             if now < *deadline {
    //                 valid_lease = Some(*deadline);
    //             } else {
    //                 expired_leases.push(*uuid);
    //                 info!("ingest lease expired";
    //                     "region_id" => region_id,
    //                     "uuid" => ?uuid,
    //                 );
    //             }
    //         }
    //     }
    //     if !expired_leases.is_empty() {
    //         // Clean up expired leases.
    //         self.expire_lease(region_id, &expired_leases);
    //     }
    //     valid_lease
    // }
}

impl IngestObserver {
    fn upsert_lease(&self, region_id: u64, uuid: Uuid, deadline: Instant) {
        let mut ssts = self.sst_leases.write().unwrap();
        let region_leases = ssts.entry(region_id).or_default();
        match region_leases.entry(uuid) {
            HashMapEntry::Vacant(e) => {
                e.insert(LeaseState {
                    deadline,
                    ref_count: Arc::default(),
                });
            }
            HashMapEntry::Occupied(mut e) => {
                // Update deadline and keep ref_count as it is.
                e.get_mut().deadline = deadline;
            }
        };
        warn!("dbg upsert"; "region_id" => region_id, "leases" => format!("{:?}", ssts.get(&region_id)));
    }
    fn expire_lease(&self, region_id: u64, uuids: &[Uuid]) {
        let mut ssts = self.sst_leases.write().unwrap();
        if let HashMapEntry::Occupied(mut leases) = ssts.entry(region_id) {
            for uuid in uuids {
                if let Some(lease) = leases.get_mut().get_mut(uuid) {
                    if lease.has_ref() {
                        // Do not remove a lease that has refs.
                        lease.expire();
                    } else {
                        leases.get_mut().remove(uuid);
                    }
                }
            }
            if leases.get().is_empty() {
                leases.remove();
            }
        } else {
            warn!("ingest lease not found"; "region_id" => region_id);
        };
        const MIN_SHRINK_CAP: usize = 1024;
        if ssts.capacity() > MIN_SHRINK_CAP && ssts.capacity() > ssts.len() * 2 {
            ssts.shrink_to(MIN_SHRINK_CAP);
        }
        warn!("dbg expire"; "region_id" => region_id, "leases" => format!("{:?}", ssts.get(&region_id)));
    }
}

#[cfg(test)]
mod tests {
    use std::{assert_matches::assert_matches, thread, time::Duration};

    use super::*;

    #[test]
    fn test_observer_get_region_lease() {
        let observer = IngestObserver::default();
        let region_id = 1;
        assert!(observer.get_region_lease(region_id).is_none());

        let uuid = Uuid::new_v4();
        let deadline = Instant::now() + Duration::from_secs(60);
        observer.update(Event::Pause {
            region_id,
            uuid,
            deadline,
        });
        assert_eq!(observer.get_region_lease(region_id).unwrap(), uuid);

        observer.update(Event::Continue { region_id, uuid });
        assert!(observer.get_region_lease(region_id).is_none());

        observer.update(Event::Pause {
            region_id,
            uuid,
            deadline: Instant::now(),
        });
        thread::sleep(Duration::from_millis(200));
        assert!(observer.get_region_lease(region_id).is_none());
    }

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
    fn test_observer_upsert_lease() {
        let observer = IngestObserver::default();
        let region_id = 1;
        let uuid1 = Uuid::new_v4();
        let deadline1 = Instant::now();
        let uuid2 = Uuid::new_v4();
        let deadline2 = Instant::now();
        observer.update(Event::Pause {
            region_id,
            uuid: uuid1,
            deadline: deadline1,
        });
        observer.update(Event::Pause {
            region_id,
            uuid: uuid2,
            deadline: deadline2,
        });

        let assert_ref_count = |uuid, count: u64| {
            assert_eq!(
                observer.sst_leases.read().unwrap()[&region_id][&uuid]
                    .ref_count
                    .load(Ordering::SeqCst),
                count,
            );
        };

        let ref1 = observer.query(region_id, &uuid1).unwrap();
        assert_eq!(ref1.lease.deadline, deadline1);
        assert_ref_count(uuid1, 1);
        let ref2 = observer.query(region_id, &uuid2).unwrap();
        assert_eq!(ref2.lease.deadline, deadline2);

        // Make sure upsert does not overwrite ref_count.
        let deadline3 = Instant::now();
        observer.update(Event::Pause {
            region_id,
            uuid: uuid1,
            deadline: deadline3,
        });
        let ref3 = observer.query(region_id, &uuid1).unwrap();
        assert_eq!(ref3.lease.deadline, deadline3);
        assert_ref_count(uuid1, 2);
    }

    #[test]
    fn test_observer_expire_lease() {
        let observer = IngestObserver::default();
        let region_id = 1;
        let uuid1 = Uuid::new_v4();
        let deadline1 = Instant::now();
        let uuid2 = Uuid::new_v4();
        let deadline2 = Instant::now();
        observer.update(Event::Pause {
            region_id,
            uuid: uuid1,
            deadline: deadline1,
        });
        observer.update(Event::Pause {
            region_id,
            uuid: uuid2,
            deadline: deadline2,
        });

        // Hold a ref to uuid1.
        let ref1 = observer.query(region_id, &uuid1).unwrap();
        assert_eq!(ref1.lease.deadline, deadline1);

        observer.update(Event::Continue {
            region_id,
            uuid: uuid1,
        });
        observer.update(Event::Continue {
            region_id,
            uuid: uuid2,
        });

        // Make sure expire does not remove a lease that has refs.
        let ref11 = observer.query(region_id, &uuid1).unwrap();
        // Make sure the unremoved lease is indeed expired.
        assert!(ref11.is_expired());

        assert_matches!(observer.query(region_id, &uuid2), None);
    }
}
