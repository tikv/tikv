// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use collections::HashMap;

#[derive(Clone)]
pub enum Event {
    Pause { region_id: u64, lease: Instant },
    Continue { region_id: u64 },
}

pub trait Observer: Sync + Send {
    fn update(&self, event: Event);
    // Called by Resolver before advancing resolved_ts.
    // Return its pause lease if there is any.
    fn query(&self, region_id: u64) -> Option<Instant>;
}

pub trait Mediator: Sync + Send {
    fn pause(&self, region_id: u64, lease: Duration);
    fn continues(&self, region_id: u64);
    fn register(&mut self, comp: Arc<dyn Observer>);
}

#[derive(Default)]
pub struct IngestMediator {
    comps: Vec<Arc<dyn Observer>>,
}

impl Mediator for IngestMediator {
    fn pause(&self, region_id: u64, lease: Duration) {
        let lease = Instant::now() + lease;
        let event = Event::Pause { region_id, lease };
        for comp in &self.comps {
            comp.update(event.clone())
        }
    }

    fn continues(&self, region_id: u64) {
        let event = Event::Continue { region_id };
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
    ssts: RwLock<HashMap<u64, Instant>>,
}

impl Observer for IngestObserver {
    fn update(&self, event: Event) {
        let mut ssts = self.ssts.write().unwrap();
        match event {
            Event::Pause { region_id, lease } => {
                ssts.insert(region_id, lease);
            }
            Event::Continue { region_id } => {
                ssts.remove(&region_id);
                const MIN_SHRINK_CAP: usize = 1024;
                if ssts.capacity() > MIN_SHRINK_CAP && ssts.capacity() > ssts.len() * 2 {
                    ssts.shrink_to(MIN_SHRINK_CAP);
                }
            }
        }
        // TODO: batch clean up expired leases.
    }

    fn query(&self, region_id: u64) -> Option<Instant> {
        let lease = self.ssts.read().unwrap().get(&region_id).cloned();
        if let Some(lease) = lease
            && Instant::now() > lease
        {
            // Lease expired, clean up.
            self.update(Event::Continue { region_id });
            info!("ingest lease expired"; "region_id" => region_id);
        }
        lease
    }
}
