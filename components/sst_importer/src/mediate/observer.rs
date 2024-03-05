// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, RwLock},
    time::Instant,
};

use collections::{HashMap, HashMapEntry};
use uuid::Uuid;

use super::*;

const MIN_SHRINK_CAP: usize = 1024;

// region_id -> (uuid -> deadline)
#[derive(Default)]
struct SstLeases(HashMap<u64, HashMap<Uuid, LeaseState>>);

impl SstLeases {
    fn upsert_lease(&mut self, region_id: u64, uuid: Uuid, deadline: Instant) {
        let ssts = &mut self.0;
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
    }
    fn expire_lease(&mut self, region_id: u64, uuid: &Uuid) {
        let ssts = &mut self.0;
        if let HashMapEntry::Occupied(mut leases) = ssts.entry(region_id) {
            if let Some(lease) = leases.get_mut().get_mut(uuid) {
                if lease.has_ref() {
                    // Do not remove a lease that has refs.
                    lease.expire();
                } else {
                    leases.get_mut().remove(uuid);
                }
            }
            if leases.get().is_empty() {
                leases.remove();
            }
        } else {
            warn!("sst lease not found"; "region_id" => region_id, "uuid" => ?uuid);
        };
    }

    fn gc(&mut self) {
        let ssts = &mut self.0;
        let mut empty_regions = vec![];
        for (region_id, leases) in ssts.iter_mut() {
            leases.retain(|_, lease| {
                // Remove a lease that has expired and no ref.
                lease.has_ref() || !lease.is_expired()
            });
            if leases.is_empty() {
                empty_regions.push(*region_id);
            }
        }
        // Remove regions that have no lease.
        for region_id in empty_regions {
            ssts.remove(&region_id);
        }
        if ssts.capacity() > MIN_SHRINK_CAP && ssts.capacity() > ssts.len() * 2 {
            ssts.shrink_to(MIN_SHRINK_CAP);
        }
    }
}

#[derive(Default)]
pub struct IngestObserver {
    sst_leases: RwLock<SstLeases>,
}

impl Observer for IngestObserver {
    fn update(&self, event: Event) {
        match event {
            Event::Acquire {
                region_id,
                uuid,
                deadline,
            } => {
                self.upsert_lease(region_id, uuid, deadline);
            }
            Event::Release { region_id, uuid } => {
                self.expire_lease(region_id, &uuid);
            }
        }
    }

    fn get_lease(&self, region_id: u64, uuid: &Uuid) -> Option<LeaseRef> {
        let ssts = self.sst_leases.read().unwrap();
        let Some(leases) = ssts.0.get(&region_id) else {
            return None;
        };
        leases.get(uuid).map(|lease| lease.ref_())
    }

    fn get_region_lease(&self, region_id: u64) -> Option<Uuid> {
        let ssts = self.sst_leases.read().unwrap();
        let leases = ssts.0.get(&region_id)?;
        for (uuid, lease) in leases {
            if !lease.is_expired() || lease.has_ref() {
                return Some(*uuid);
            }
        }
        None
    }

    fn gc(&self) {
        let Ok(mut leases) = self.sst_leases.try_write() else {
            return;
        };
        leases.gc()
    }
}

impl IngestObserver {
    fn upsert_lease(&self, region_id: u64, uuid: Uuid, deadline: Instant) {
        let mut ssts = self.sst_leases.write().unwrap();
        ssts.upsert_lease(region_id, uuid, deadline)
    }
    fn expire_lease(&self, region_id: u64, uuid: &Uuid) {
        let mut ssts = self.sst_leases.write().unwrap();
        ssts.expire_lease(region_id, uuid)
    }
}

#[cfg(test)]
mod tests {
    use std::{assert_matches::assert_matches, sync::mpsc::channel, thread, time::Duration};

    use super::*;

    #[test]
    fn test_observer_get_region_lease() {
        let observer = IngestObserver::default();
        let region_id = 1;
        assert!(observer.get_region_lease(region_id).is_none());

        let uuid = Uuid::new_v4();
        let deadline = Instant::now() + Duration::from_secs(60);
        observer.update(Event::Acquire {
            region_id,
            uuid,
            deadline,
        });
        assert_eq!(observer.get_region_lease(region_id).unwrap(), uuid);

        observer.update(Event::Release { region_id, uuid });
        assert!(observer.get_region_lease(region_id).is_none());

        observer.update(Event::Acquire {
            region_id,
            uuid,
            deadline: Instant::now(),
        });
        thread::sleep(Duration::from_millis(200));
        assert!(observer.get_region_lease(region_id).is_none());
    }

    #[test]
    fn test_observer_upsert_lease() {
        let observer = IngestObserver::default();
        let region_id = 1;
        let uuid1 = Uuid::new_v4();
        let deadline1 = Instant::now();
        let uuid2 = Uuid::new_v4();
        let deadline2 = Instant::now();
        observer.update(Event::Acquire {
            region_id,
            uuid: uuid1,
            deadline: deadline1,
        });
        observer.update(Event::Acquire {
            region_id,
            uuid: uuid2,
            deadline: deadline2,
        });

        let assert_ref_count = |uuid, count: u64| {
            assert_eq!(
                observer.sst_leases.read().unwrap().0[&region_id][&uuid]
                    .ref_count
                    .load(Ordering::SeqCst),
                count,
            );
        };

        let ref1 = observer.get_lease(region_id, &uuid1).unwrap();
        assert_eq!(ref1.lease.deadline, deadline1);
        assert_ref_count(uuid1, 1);
        let ref2 = observer.get_lease(region_id, &uuid2).unwrap();
        assert_eq!(ref2.lease.deadline, deadline2);

        // Make sure upsert does not overwrite ref_count.
        let deadline3 = Instant::now();
        observer.update(Event::Acquire {
            region_id,
            uuid: uuid1,
            deadline: deadline3,
        });
        let ref3 = observer.get_lease(region_id, &uuid1).unwrap();
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
        observer.update(Event::Acquire {
            region_id,
            uuid: uuid1,
            deadline: deadline1,
        });
        observer.update(Event::Acquire {
            region_id,
            uuid: uuid2,
            deadline: deadline2,
        });

        // Hold a ref to uuid1.
        let ref1 = observer.get_lease(region_id, &uuid1).unwrap();
        assert_eq!(ref1.lease.deadline, deadline1);

        observer.update(Event::Release {
            region_id,
            uuid: uuid1,
        });
        observer.update(Event::Release {
            region_id,
            uuid: uuid2,
        });

        // Make sure expire does not remove a lease that has refs.
        let ref11 = observer.get_lease(region_id, &uuid1).unwrap();
        // Make sure the unremoved lease is indeed expired.
        assert!(ref11.is_expired());

        assert_matches!(observer.get_lease(region_id, &uuid2), None);
    }

    #[test]
    fn test_observer_gc() {
        let observer = IngestObserver::default();

        let region_id1 = 1;
        let uuid11 = Uuid::new_v4();
        let deadline11 = Instant::now() + Duration::from_secs(60);
        let uuid12 = Uuid::new_v4();
        let deadline12 = Instant::now() + Duration::from_secs(60);
        observer.update(Event::Acquire {
            region_id: region_id1,
            uuid: uuid11,
            deadline: deadline11,
        });
        observer.update(Event::Acquire {
            region_id: region_id1,
            uuid: uuid12,
            deadline: deadline12,
        });

        let region_id2 = 2;
        let uuid2 = Uuid::new_v4();
        let deadline2 = Instant::now() + Duration::from_secs(60);
        observer.update(Event::Acquire {
            region_id: region_id2,
            uuid: uuid2,
            deadline: deadline2,
        });

        // Gc does not remove valid leases.
        observer.gc();
        observer.get_lease(region_id1, &uuid11).unwrap();
        observer.get_lease(region_id1, &uuid12).unwrap();
        observer.get_lease(region_id2, &uuid2).unwrap();

        // Gc does not remove leases that have refs.
        let ref2 = observer.get_lease(region_id2, &uuid2).unwrap();
        observer.update(Event::Release {
            region_id: region_id2,
            uuid: uuid2,
        });
        observer.gc();
        observer.get_lease(region_id2, &uuid2).unwrap();

        // Gc does remove regions that has no valid lease.
        drop(ref2);
        observer.gc();
        assert!(observer.get_lease(region_id2, &uuid2).is_none());

        // Gc can handle concurrent leases.
        observer.update(Event::Release {
            region_id: region_id1,
            uuid: uuid12,
        });
        observer.gc();
        observer.get_lease(region_id1, &uuid11).unwrap();
        assert!(observer.get_lease(region_id1, &uuid12).is_none());

        // Gc reclaims memory.
        observer
            .sst_leases
            .write()
            .unwrap()
            .0
            .reserve(MIN_SHRINK_CAP * 2);
        observer.gc();
        let cap = observer.sst_leases.write().unwrap().0.capacity();
        assert!(cap < MIN_SHRINK_CAP * 2);

        // Gc never block.
        let observer = Arc::new(observer);
        let _guard = observer.sst_leases.write().unwrap();
        let observer_ = observer.clone();
        let (tx, rx) = channel();
        thread::spawn(move || {
            observer_.gc();
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
}
