// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{hash_map::RandomState, HashMap},
    time::Duration,
};

use crate::errors::{Error, Result};
use futures::{channel::mpsc, executor::block_on, StreamExt};
use raft::StateRole;
use raftstore::{coprocessor::RegionInfoProvider, RegionInfo};

use tikv_util::{box_err, time::Instant, warn};
use tokio::sync::{Mutex, RwLock};
use txn_types::{Key, TimeStamp};

/// wrap a user key with encoded data key.
pub fn wrap_key(v: Vec<u8>) -> Vec<u8> {
    // TODO: encode in place.
    let key = Key::from_raw(v.as_slice()).into_encoded();
    key
}

/// decode ts from a key, and transform the error to the crate error.
pub fn get_ts(key: &Key) -> Result<TimeStamp> {
    key.decode_ts().map_err(|err| {
        Error::Other(box_err!(
            "failed to get ts from key '{}': {}",
            log_wrappers::Value::key(key.as_encoded().as_slice()),
            err
        ))
    })
}

pub fn redact<'a>(key: &'a impl AsRef<[u8]>) -> log_wrappers::Value<'a> {
    log_wrappers::Value::key(key.as_ref())
}

/// RegionPager seeks regions with leader role in the range.
pub struct RegionPager<P> {
    regions: P,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    reach_last_region: bool,
}

impl<P: RegionInfoProvider> RegionPager<P> {
    pub fn scan_from(regions: P, start_key: Vec<u8>, end_key: Vec<u8>) -> Self {
        Self {
            regions,
            start_key,
            end_key,
            reach_last_region: false,
        }
    }

    pub fn next_page(&mut self, size: usize) -> Result<Vec<RegionInfo>> {
        if self.start_key >= self.end_key || self.reach_last_region {
            return Ok(vec![]);
        }

        let (mut tx, rx) = mpsc::channel(size);
        let end_key = self.end_key.clone();
        self.regions
            .seek_region(
                &self.start_key,
                Box::new(move |i| {
                    let r = i
                        .filter(|r| r.role == StateRole::Leader)
                        .take(size)
                        .take_while(|r| r.region.start_key < end_key)
                        .try_for_each(|r| tx.try_send(r.clone()));
                    if let Err(_err) = r {
                        warn!("failed to scan region and send to initlizer")
                    }
                }),
            )
            .map_err(|err| {
                Error::Other(box_err!(
                    "failed to seek region for start key {}: {}",
                    redact(&self.start_key),
                    err
                ))
            })?;
        let collected_regions = block_on(rx.collect::<Vec<_>>());
        self.start_key = collected_regions
            .last()
            .map(|region| region.region.end_key.to_owned())
            // no leader region found.
            .unwrap_or(vec![]);
        if self.start_key.is_empty() {
            self.reach_last_region = true;
        }
        Ok(collected_regions)
    }
}

/// StopWatch is a utility for record time cost in multi-stage tasks.
/// NOTE: Maybe it should be generic over somewhat Clock type?
pub struct StopWatch(Instant);

impl StopWatch {
    /// Create a new stopwatch via current time.
    pub fn new() -> Self {
        Self(Instant::now_coarse())
    }

    /// Get time elapsed since last lap (or creation if the first time).
    pub fn lap(&mut self) -> Duration {
        let elapsed = self.0.saturating_elapsed();
        self.0 = Instant::now_coarse();
        elapsed
    }
}

/// Slot is a shareable slot in the slot map.
pub type Slot<T> = Mutex<T>;

/// SlotMap is a trivial concurrent map which sharding over each key.
/// NOTE: Maybe we can use dashmap for replacing the RwLock.
pub type SlotMap<K, V, S = RandomState> = RwLock<HashMap<K, Slot<V>, S>>;
