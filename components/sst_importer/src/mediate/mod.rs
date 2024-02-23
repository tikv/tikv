// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use collections::{HashMap, HashMapEntry};
use tikv_util::time::Instant;
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

pub trait Observer: Sync + Send {
    fn update(&self, event: Event);
    // Called by Resolver before advancing resolved_ts.
    // Return its pause lease if there is any.
    fn query(&self, region_id: u64, uuid: &Uuid) -> Option<Instant>;
    fn query_region(&self, region_id: u64) -> Vec<(Uuid, Instant)>;
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
    sst_leases: RwLock<HashMap<u64, HashMap<Uuid, Instant>>>,
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

    fn query(&self, region_id: u64, uuid: &Uuid) -> Option<Instant> {
        let ssts = self.sst_leases.read().unwrap();
        let Some(leases) = ssts.get(&region_id) else {
            return None;
        };
        leases.get(uuid).cloned()
    }

    fn query_region(&self, region_id: u64) -> Vec<(Uuid, Instant)> {
        let ssts = self.sst_leases.read().unwrap();
        ssts.get(&region_id).map_or_else(Vec::new, |leases| {
            leases.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        })
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
        match ssts.get_mut(&region_id) {
            Some(leases) => {
                leases.insert(uuid, deadline);
            }
            None => {
                let mut leases = HashMap::default();
                leases.insert(uuid, deadline);
                ssts.insert(region_id, leases);
            }
        }
    }
    fn expire_lease(&self, region_id: u64, uuids: &[Uuid]) {
        let mut ssts = self.sst_leases.write().unwrap();
        match ssts.entry(region_id) {
            HashMapEntry::Occupied(mut leases) => {
                for uuid in uuids {
                    leases.get_mut().remove(&uuid);
                }
                if leases.get().is_empty() {
                    leases.remove();
                }
            }
            HashMapEntry::Vacant(_) => {
                warn!("ingest lease not found"; "region_id" => region_id);
            }
        };
        const MIN_SHRINK_CAP: usize = 1024;
        if ssts.capacity() > MIN_SHRINK_CAP && ssts.capacity() > ssts.len() * 2 {
            ssts.shrink_to(MIN_SHRINK_CAP);
        }
    }
}
