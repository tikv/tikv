// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use dashmap::{
    mapref::{entry::Entry, one::RefMut},
    DashMap,
};
use kvproto::metapb::Region;
use raftstore::coprocessor::*;
use resolved_ts::Resolver;
use tikv_util::{info, warn};
use txn_types::TimeStamp;

use crate::{debug, metrics::TRACK_REGION, utils};

/// A utility to tracing the regions being subscripted.
#[derive(Clone, Default, Debug)]
pub struct SubscriptionTracer(Arc<DashMap<u64, RegionSubscription>>);

pub struct RegionSubscription {
    pub meta: Region,
    pub(crate) handle: ObserveHandle,
    pub(crate) resolver: TwoPhaseResolver,
}

impl std::fmt::Debug for RegionSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RegionSubscription")
            .field(&self.meta.get_id())
            .field(&self.handle)
            .finish()
    }
}

impl RegionSubscription {
    pub fn new(region: Region, handle: ObserveHandle, start_ts: Option<TimeStamp>) -> Self {
        let resolver = TwoPhaseResolver::new(region.get_id(), start_ts);
        Self {
            handle,
            meta: region,
            resolver,
        }
    }

    pub fn stop(&mut self) {
        self.handle.stop_observing();
    }

    pub fn is_observing(&self) -> bool {
        self.handle.is_observing()
    }

    pub fn resolver(&mut self) -> &mut TwoPhaseResolver {
        &mut self.resolver
    }

    pub fn handle(&self) -> &ObserveHandle {
        &self.handle
    }
}

#[derive(PartialEq, Eq)]
pub enum CheckpointType {
    MinTs,
    StartTsOfInitialScan,
    StartTsOfTxn(Option<Arc<[u8]>>),
}

impl std::fmt::Debug for CheckpointType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MinTs => write!(f, "MinTs"),
            Self::StartTsOfInitialScan => write!(f, "StartTsOfInitialScan"),
            Self::StartTsOfTxn(arg0) => f
                .debug_tuple("StartTsOfTxn")
                .field(&format_args!(
                    "{}",
                    utils::redact(&arg0.as_ref().map(|x| x.as_ref()).unwrap_or(&[]))
                ))
                .finish(),
        }
    }
}

pub struct ResolveResult {
    pub region: Region,
    pub checkpoint: TimeStamp,
    pub checkpoint_type: CheckpointType,
}

impl std::fmt::Debug for ResolveResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolveResult")
            .field("region", &self.region.get_id())
            .field("checkpoint", &self.checkpoint)
            .field("checkpoint_type", &self.checkpoint_type)
            .finish()
    }
}

impl ResolveResult {
    fn resolve(sub: &mut RegionSubscription, min_ts: TimeStamp) -> Self {
        let ts = sub.resolver.resolve(min_ts);
        let ty = if ts == min_ts {
            CheckpointType::MinTs
        } else if sub.resolver.in_phase_one() {
            CheckpointType::StartTsOfInitialScan
        } else {
            CheckpointType::StartTsOfTxn(sub.resolver.sample_far_lock())
        };
        Self {
            region: sub.meta.clone(),
            checkpoint: ts,
            checkpoint_type: ty,
        }
    }
}

impl SubscriptionTracer {
    /// clear the current `SubscriptionTracer`.
    pub fn clear(&self) {
        self.0.retain(|_, v| {
            v.stop();
            TRACK_REGION.dec();
            false
        });
    }

    // Register a region as tracing.
    // The `start_ts` is used to tracking the progress of initial scanning.
    // Note: the `None` case of `start_ts` is for testing / refresh region status
    // when split / merge, maybe we'd better provide some special API for those
    // cases and remove the `Option`?
    pub fn register_region(
        &self,
        region: &Region,
        handle: ObserveHandle,
        start_ts: Option<TimeStamp>,
    ) {
        info!("start listen stream from store"; "observer" => ?handle, "region_id" => %region.get_id());
        TRACK_REGION.inc();
        if let Some(mut o) = self.0.insert(
            region.get_id(),
            RegionSubscription::new(region.clone(), handle, start_ts),
        ) {
            TRACK_REGION.dec();
            o.stop();
        }
    }

    /// try advance the resolved ts with the min ts of in-memory locks.
    /// returns the regions and theirs resolved ts.
    pub fn resolve_with(&self, min_ts: TimeStamp) -> Vec<ResolveResult> {
        self.0
            .iter_mut()
            // Don't advance the checkpoint ts of removed region.
            .map(|mut s| ResolveResult::resolve(s.value_mut(), min_ts))
            .collect()
    }

    #[inline(always)]
    pub fn warn_if_gap_too_huge(&self, ts: TimeStamp) {
        let gap = TimeStamp::physical_now() - ts.physical();
        if gap >= 10 * 60 * 1000
        // 10 mins
        {
            let far_resolver = self
                .0
                .iter()
                .min_by_key(|r| r.value().resolver.resolved_ts());
            warn!("log backup resolver ts advancing too slow";
            "far_resolver" => %{match far_resolver {
                Some(r) => format!("{:?}", r.value().resolver),
                None => "BUG[NoResolverButResolvedTSDoesNotAdvance]".to_owned()
            }},
            "gap" => ?Duration::from_millis(gap),
            );
        }
    }

    /// try to mark a region no longer be tracked by this observer.
    /// returns whether success (it failed if the region hasn't been observed
    /// when calling this.)
    pub fn deregister_region_if(
        &self,
        region: &Region,
        if_cond: impl FnOnce(&RegionSubscription, &Region) -> bool,
    ) -> bool {
        let region_id = region.get_id();
        let remove_result = self.0.entry(region_id);
        match remove_result {
            Entry::Occupied(mut x) => {
                if if_cond(x.get(), region) {
                    TRACK_REGION.dec();
                    x.get_mut().stop();
                    let v = x.remove();
                    info!("stop listen stream from store"; "observer" => ?v, "region_id"=> %region_id);
                    return true;
                }
                false
            }
            Entry::Vacant(_) => false,
        }
    }

    /// try update the subscription status by the new region info.
    ///
    /// # return
    ///
    /// Whether the status can be updated internally without
    /// deregister-and-register.
    pub fn try_update_region(&self, new_region: &Region) -> bool {
        let mut sub = match self.get_subscription_of(new_region.get_id()) {
            Some(sub) => sub,
            None => {
                warn!("backup stream observer refreshing void subscription."; utils::slog_region(new_region));
                return true;
            }
        };

        let mut subscription = sub.value_mut();

        let old_epoch = subscription.meta.get_region_epoch();
        let new_epoch = new_region.get_region_epoch();
        if old_epoch.version == new_epoch.version {
            subscription.meta = new_region.clone();
            return true;
        }

        false
    }

    /// check whether the region_id should be observed by this observer.
    pub fn is_observing(&self, region_id: u64) -> bool {
        let sub = self.0.get_mut(&region_id);
        match sub {
            Some(mut sub) if !sub.is_observing() => {
                sub.value_mut().stop();
                false
            }
            Some(_) => true,
            None => false,
        }
    }

    pub fn get_subscription_of(
        &self,
        region_id: u64,
    ) -> Option<RefMut<'_, u64, RegionSubscription>> {
        self.0.get_mut(&region_id)
    }
}

/// This enhanced version of `Resolver` allow some unordered lock events.  
/// The name "2-phase" means this is used for 2 *concurrency* phases of
/// observing a region:
/// 1. Doing the initial scanning.
/// 2. Listening at the incremental data.
///
/// ```text
/// +->(Start TS Of Task)            +->(Task registered to KV)
/// +--------------------------------+------------------------>
/// ^~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^ ^~~~~~~~~~~~~~~~~~~~~~~~~
/// |                                 +-> Phase 2: Listening incremental data.
/// +-> Phase 1: Initial scanning scans writes between start ts and now.
/// ```
///
/// In backup-stream, we execute these two tasks parallel. Which may make some
/// race conditions:
/// - When doing initial scanning, there may be a flush triggered, but the
///   default resolver would probably resolved to the tip of incremental events.
/// - When doing initial scanning, we meet and track a lock already meet by the
///   incremental events, then the default resolver cannot untrack this lock any
///   more.
///
/// This version of resolver did some change for solve these problems:
/// - The resolver won't advance the resolved ts to greater than `stable_ts` if
///   there is some. This can help us prevent resolved ts from advancing when
///   initial scanning hasn't finished yet.
/// - When we `untrack` a lock haven't been tracked, this would record it, and
///   skip this lock if we want to track it then. This would be safe because:
///   - untracking a lock not be tracked is no-op for now.
///   - tracking a lock have already being untracked (unordered call of `track`
///     and `untrack`) wouldn't happen at phase 2 for same region. but only when
///     phase 1 and phase 2 happened concurrently, at that time, we wouldn't and
///     cannot advance the resolved ts.
pub struct TwoPhaseResolver {
    resolver: Resolver,
    future_locks: Vec<FutureLock>,
    /// When `Some`, is the start ts of the initial scanning.
    /// And implies the phase 1 (initial scanning) is keep running
    /// asynchronously.
    stable_ts: Option<TimeStamp>,
}

enum FutureLock {
    Lock(Vec<u8>, TimeStamp),
    Unlock(Vec<u8>),
}

impl std::fmt::Debug for FutureLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lock(arg0, arg1) => f
                .debug_tuple("Lock")
                .field(&format_args!("{}", utils::redact(arg0)))
                .field(arg1)
                .finish(),
            Self::Unlock(arg0) => f
                .debug_tuple("Unlock")
                .field(&format_args!("{}", utils::redact(arg0)))
                .finish(),
        }
    }
}

impl TwoPhaseResolver {
    /// try to get one of the key of the oldest lock in the resolver.
    pub fn sample_far_lock(&self) -> Option<Arc<[u8]>> {
        let (_, keys) = self.resolver.locks().first_key_value()?;
        keys.iter().next().cloned()
    }

    pub fn in_phase_one(&self) -> bool {
        self.stable_ts.is_some()
    }

    pub fn track_phase_one_lock(&mut self, start_ts: TimeStamp, key: Vec<u8>) {
        if !self.in_phase_one() {
            warn!("backup stream tracking lock as if in phase one"; "start_ts" => %start_ts, "key" => %utils::redact(&key))
        }
        self.resolver.track_lock(start_ts, key, None)
    }

    pub fn track_lock(&mut self, start_ts: TimeStamp, key: Vec<u8>) {
        if self.in_phase_one() {
            self.future_locks.push(FutureLock::Lock(key, start_ts));
            return;
        }
        self.resolver.track_lock(start_ts, key, None)
    }

    pub fn untrack_lock(&mut self, key: &[u8]) {
        // If we are still in phase one, tracking all unpaired locks.
        if self.in_phase_one() {
            self.future_locks.push(FutureLock::Unlock(key.to_owned()));
            return;
        }
        self.resolver.untrack_lock(key, None)
    }

    fn handle_future_lock(&mut self, lock: FutureLock) {
        match lock {
            FutureLock::Lock(key, ts) => self.resolver.track_lock(ts, key, None),
            FutureLock::Unlock(key) => self.resolver.untrack_lock(&key, None),
        }
    }

    pub fn resolve(&mut self, min_ts: TimeStamp) -> TimeStamp {
        if let Some(stable_ts) = self.stable_ts {
            return min_ts.min(stable_ts);
        }

        self.resolver.resolve(min_ts)
    }

    pub fn resolved_ts(&self) -> TimeStamp {
        if let Some(stable_ts) = self.stable_ts {
            return stable_ts;
        }

        self.resolver.resolved_ts()
    }

    pub fn new(region_id: u64, stable_ts: Option<TimeStamp>) -> Self {
        Self {
            resolver: Resolver::new(region_id),
            future_locks: Default::default(),
            stable_ts,
        }
    }

    pub fn phase_one_done(&mut self) {
        debug!("phase one done"; "resolver" => ?self.resolver, "future_locks" => ?self.future_locks);
        for lock in std::mem::take(&mut self.future_locks).into_iter() {
            self.handle_future_lock(lock);
        }
        let ts = self.stable_ts.take();
        match ts {
            Some(ts) => {
                // advance the internal resolver.
                // the start ts of initial scanning would be a safe ts for min ts
                // -- because is used to be a resolved ts.
                self.resolver.resolve(ts);
            }
            None => {
                warn!("BUG: a two-phase resolver is executing phase_one_done when not in phase one"; "resolver" => ?self)
            }
        }
    }
}

impl std::fmt::Debug for TwoPhaseResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TwoPhaseResolver")
            .field(&self.stable_ts)
            .field(&self.resolver)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use kvproto::metapb::{Region, RegionEpoch};
    use raftstore::coprocessor::ObserveHandle;
    use txn_types::TimeStamp;

    use super::{SubscriptionTracer, TwoPhaseResolver};

    #[test]
    fn test_two_phase_resolver() {
        let key = b"somewhere_over_the_rainbow";
        let ts = TimeStamp::new;
        let mut r = TwoPhaseResolver::new(42, Some(ts(42)));
        r.track_phase_one_lock(ts(48), key.to_vec());
        // When still in phase one, the resolver should not be advanced.
        r.untrack_lock(&key[..]);
        assert_eq!(r.resolve(ts(50)), ts(42));

        // Even new lock tracked...
        r.track_lock(ts(52), key.to_vec());
        r.untrack_lock(&key[..]);
        assert_eq!(r.resolve(ts(53)), ts(42));

        // When pahse one done, the resolver should be resovled properly.
        r.phase_one_done();
        assert_eq!(r.resolve(ts(54)), ts(54));

        // It should be able to track incremental locks.
        r.track_lock(ts(55), key.to_vec());
        assert_eq!(r.resolve(ts(56)), ts(55));
        r.untrack_lock(&key[..]);
        assert_eq!(r.resolve(ts(57)), ts(57));
    }

    fn region(id: u64, version: u64, conf_version: u64) -> Region {
        let mut r = Region::new();
        let mut e = RegionEpoch::new();
        e.set_version(version);
        e.set_conf_ver(conf_version);
        r.set_id(id);
        r.set_region_epoch(e);
        r
    }

    #[test]
    fn test_delay_remove() {
        let subs = SubscriptionTracer::default();
        let handle = ObserveHandle::new();
        subs.register_region(&region(1, 1, 1), handle, Some(TimeStamp::new(42)));
        assert!(subs.get_subscription_of(1).is_some());
        assert!(subs.is_observing(1));
        subs.deregister_region_if(&region(1, 1, 1), |_, _| true);
        assert!(!subs.is_observing(1));
    }

    #[test]
    fn test_cal_checkpoint() {
        let subs = SubscriptionTracer::default();
        subs.register_region(
            &region(1, 1, 1),
            ObserveHandle::new(),
            Some(TimeStamp::new(42)),
        );
        subs.register_region(&region(2, 2, 1), ObserveHandle::new(), None);
        subs.register_region(
            &region(3, 4, 1),
            ObserveHandle::new(),
            Some(TimeStamp::new(88)),
        );
        subs.get_subscription_of(3)
            .unwrap()
            .resolver
            .phase_one_done();
        subs.register_region(
            &region(4, 8, 1),
            ObserveHandle::new(),
            Some(TimeStamp::new(92)),
        );
        let mut region4_sub = subs.get_subscription_of(4).unwrap();
        region4_sub.resolver.phase_one_done();
        region4_sub
            .resolver
            .track_lock(TimeStamp::new(128), b"Alpi".to_vec());
        subs.register_region(&region(5, 8, 1), ObserveHandle::new(), None);
        subs.deregister_region_if(&region(5, 8, 1), |_, _| true);
        drop(region4_sub);

        let mut rs = subs
            .resolve_with(TimeStamp::new(1000))
            .into_iter()
            .map(|r| (r.region, r.checkpoint, r.checkpoint_type))
            .collect::<Vec<_>>();
        rs.sort_by_key(|k| k.0.get_id());
        use crate::subscription_track::CheckpointType::*;
        assert_eq!(
            rs,
            vec![
                (region(1, 1, 1), 42.into(), StartTsOfInitialScan),
                (region(2, 2, 1), 1000.into(), MinTs),
                (region(3, 4, 1), 1000.into(), MinTs),
                (
                    region(4, 8, 1),
                    128.into(),
                    StartTsOfTxn(Some(Arc::from(b"Alpi".as_slice())))
                ),
            ]
        );
    }
}
