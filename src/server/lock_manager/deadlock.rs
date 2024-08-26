// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    fmt::{self, Display, Formatter},
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex},
};

use collections::{HashMap, HashSet};
use engine_traits::KvEngine;
use futures::{
    future::{self, FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::TryStreamExt,
};
use futures_util::stream;
use grpcio::{
    self, DuplexSink, Environment, RequestStream, RpcContext, RpcStatus, RpcStatusCode, UnarySink,
    WriteFlags,
};
use itertools::Itertools;
use kvproto::{deadlock::*, metapb::Region};
use pd_client::{PdClient, INVALID_ID};
use raft::StateRole;
use raftstore::{
    coprocessor::{
        BoxRegionChangeObserver, BoxRoleObserver, Coprocessor, CoprocessorHost, ObserverContext,
        RegionChangeEvent, RegionChangeObserver, RoleChange, RoleObserver,
    },
    store::util::is_region_initialized,
};
use security::SecurityManager;
use tikv_util::{
    future::paired_future_callback,
    time::{Duration, Instant},
    worker::{FutureRunnable, FutureScheduler, Stopped},
};
use tokio::task::spawn_local;
use txn_types::TimeStamp;

use super::{
    client::{self, Client},
    config::Config,
    metrics::*,
    waiter_manager::Scheduler as WaiterMgrScheduler,
    Error, Result,
};
use crate::{
    server::resolve::StoreAddrResolver,
    storage::lock_manager::{DiagnosticContext, KeyLockWaitInfo, LockDigest},
};

/// `Locks` is a set of locks belonging to one transaction.
struct Locks {
    ts: TimeStamp,
    // (hash, key)
    // The `key` is recorded as diagnostic information. There may be multiple keys with the same
    // hash, but it should be enough if we record only one of them.
    keys: Vec<(u64, Vec<u8>)>,
    resource_group_tag: Vec<u8>,
    last_detect_time: Instant,
}

impl Locks {
    /// Creates a new `Locks`.
    fn new(
        ts: TimeStamp,
        hash: u64,
        key: Vec<u8>,
        resource_group_tag: Vec<u8>,
        last_detect_time: Instant,
    ) -> Self {
        Self {
            ts,
            keys: vec![(hash, key)],
            resource_group_tag,
            last_detect_time,
        }
    }

    fn lock_matches(lhs_hash: u64, lhs_key: &[u8], rhs_hash: u64, rhs_key: &[u8]) -> bool {
        if lhs_hash != rhs_hash {
            return false;
        }
        // If either of the keys is missing, this may caused by TiKV nodes in too-old
        // versions in the cluster, which may happen theoretically if some user
        // rolling-upgrades the cluster from a too-old version (before 5.1). In this
        // case, skip checking equality of the keys.
        if lhs_key.is_empty() || rhs_key.is_empty() {
            return true;
        }

        lhs_key == rhs_key
    }

    /// Pushes the `hash` if not exist and updates `last_detect_time`.
    fn push(&mut self, lock_hash: u64, key: Vec<u8>, now: Instant) -> bool {
        self.last_detect_time = now;

        if !self
            .keys
            .iter()
            .any(|(hash, k)| Self::lock_matches(*hash, k, lock_hash, &key))
        {
            self.keys.push((lock_hash, key));
            true
        } else {
            false
        }
    }

    /// Removes the lock-waiting relationship by given hash and key.
    ///
    /// Returns the removed lock_hash and key if an entry is successfully
    /// removed. The return value is useful when sometimes the `key` is
    /// missing due to remote TiKV nodes in out-of-date versions.
    fn remove(&mut self, lock_hash: u64, key: &[u8]) -> Option<(u64, Vec<u8>)> {
        if let Some(idx) = self
            .keys
            .iter()
            .position(|(hash, k)| Self::lock_matches(*hash, k, lock_hash, &key))
        {
            return Some(self.keys.swap_remove(idx));
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns true if the `Locks` is expired.
    fn is_expired(&self, now: Instant, ttl: Duration) -> bool {
        now.saturating_duration_since(self.last_detect_time) >= ttl
    }

    /// Generate a `WaitForEntry` for the lock.
    fn to_wait_for_entry(&self, waiter_ts: TimeStamp) -> WaitForEntry {
        let mut entry = WaitForEntry::default();
        entry.set_txn(waiter_ts.into_inner());
        entry.set_wait_for_txn(self.ts.into_inner());
        entry.set_key_hash(self.keys[0].0);
        entry.set_key(self.keys[0].1.clone());
        entry.set_resource_group_tag(self.resource_group_tag.clone());
        entry
    }
}

type KeyReverseMap = HashMap<Vec<u8>, HashMap<TimeStamp, HashSet<TimeStamp>>>;

/// Used to detect the deadlock of wait-for-lock in the cluster.
pub struct DetectTable {
    /// Keeps the DAG of wait-for-lock. Every edge from `txn_ts` to `lock_ts`
    /// has a survival time -- `ttl`. When checking the deadlock, if the ttl
    /// has elpased, the corresponding edge will be removed.
    /// `last_detect_time` is the start time of the edge. `Detect` requests will
    /// refresh it.
    // waiting_txn_ts => (lock_ts => Locks)
    wait_for_map: HashMap<TimeStamp, HashMap<TimeStamp, Locks>>,

    /// For finding lock-waiting and lock-holding transactions by key.
    ///
    /// Theoretically, keys occurring in `wait_for_map` should be unique.
    /// However, considering that in some rare cases, it's possible that the
    /// leader leader of some region is transferring between nodes; with
    /// uncertain network latency, different lock-waiting relationship on a
    /// same key can be received at the same time, while the detector leader has
    /// no way to determine which one is the latest. Considering this, if
    /// `key_reverse_map` is fully synced with `wait_for_map`, then a key is
    /// possible to map to multiple existing locks, each of which has its
    /// own set of waiting transactions.
    // key => (lock_ts => {waiting_txn_ts})
    // TODO: Use the pre-calculated lock hash instead to reduce the overhead of hash calculation.
    key_reverse_map: KeyReverseMap,

    /// The ttl of every edge.
    ttl: Duration,

    /// The time of last `active_expire`.
    last_active_expire: Instant,

    now: Instant,
}

impl DetectTable {
    /// Creates a auto-expiring detect table.
    pub fn new(ttl: Duration) -> Self {
        Self {
            wait_for_map: HashMap::default(),
            key_reverse_map: HashMap::default(),
            ttl,
            last_active_expire: Instant::now_coarse(),
            now: Instant::now_coarse(),
        }
    }

    /// Returns the key and its hash which causes deadlock, and the current wait
    /// chain that forms the deadlock with `txn_ts`'s waiting for txn at
    /// `lock_ts`. Note that the current detecting edge is not included in
    /// the returned wait chain. This is intended to reduce RPC message size
    /// since the information about current detecting txn is included in a
    /// separated field.
    pub fn detect(
        &mut self,
        txn_ts: TimeStamp,
        lock_ts: TimeStamp,
        lock_hash: u64,
        lock_key: &[u8],
        resource_group_tag: &[u8],
    ) -> Option<(u64, Vec<u8>, Vec<WaitForEntry>)> {
        let _timer = DETECT_DURATION_HISTOGRAM.start_coarse_timer();
        TASK_COUNTER_METRICS.detect.inc();

        self.now = Instant::now_coarse();
        self.active_expire();

        // If `txn_ts` is waiting for `lock_ts`, it won't cause deadlock.
        if self.register_if_existed(txn_ts, lock_ts, lock_hash, &lock_key, resource_group_tag) {
            return None;
        }

        if let Some((deadlock_key_hash, deadlock_key, wait_chain)) = self.do_detect(txn_ts, lock_ts)
        {
            ERROR_COUNTER_METRICS.deadlock.inc();
            return Some((deadlock_key_hash, deadlock_key, wait_chain));
        }
        self.register(txn_ts, lock_ts, lock_hash, &lock_key, resource_group_tag);
        None
    }

    /// Checks if there is a path from `wait_for_ts` to `txn_ts`.
    /// If deadlock is detected, returns (key hash, key, the current wait chain)
    /// where the key is that blocking another transaction.
    fn do_detect(
        &mut self,
        txn_ts: TimeStamp,
        wait_for_ts: TimeStamp,
    ) -> Option<(u64, Vec<u8>, Vec<WaitForEntry>)> {
        let now = self.now;
        let ttl = self.ttl;

        let mut stack = vec![wait_for_ts];
        // Memorize the pushed vertexes to avoid duplicate search, and maps to the
        // predecessor of the vertex.
        // Since the graph is a DAG instead of a tree, a vertex may have multiple
        // predecessors. But it's ok if we only remember one: for each vertex,
        // if it has a route to the goal (txn_ts), we must be able to find the
        // goal and exit this function before visiting the vertex one more time.
        let mut pushed: HashMap<TimeStamp, TimeStamp> = HashMap::default();
        pushed.insert(wait_for_ts, TimeStamp::zero());
        while let Some(curr_ts) = stack.pop() {
            if let Some(wait_for) = self.wait_for_map.get_mut(&curr_ts) {
                // Remove expired edges.
                let expired_locks = wait_for.extract_if(|_, locks| locks.is_expired(now, ttl));
                // Remove the entries in `key_reverse_map` corresponding to expired locks (if
                // any).
                for (lock_ts, locks) in expired_locks {
                    for (_, key) in locks.keys {
                        Self::remove_key_reverse_index(
                            &mut self.key_reverse_map,
                            &key,
                            lock_ts,
                            curr_ts,
                        );
                    }
                }

                if wait_for.is_empty() {
                    self.wait_for_map.remove(&curr_ts);
                } else {
                    for (lock_ts, locks) in wait_for {
                        let lock_ts = *lock_ts;

                        if lock_ts == txn_ts {
                            let (hash, key) = locks.keys[0].clone();
                            let last_entry = locks.to_wait_for_entry(curr_ts);
                            let mut wait_chain =
                                self.generate_wait_chain(wait_for_ts, curr_ts, pushed);
                            wait_chain.push(last_entry);
                            return Some((hash, key, wait_chain));
                        }

                        #[allow(clippy::map_entry)]
                        if !pushed.contains_key(&lock_ts) {
                            stack.push(lock_ts);
                            pushed.insert(lock_ts, curr_ts);
                        }
                    }
                }
            }
        }
        None
    }

    /// Generate the wait chain after deadlock is detected. This function is
    /// part of implementation of `do_detect`. It assumes there's a path
    /// from `start` to `end` in the waiting graph, and every single edge
    /// `V1 -> V2` has an entry in `vertex_predecessors_map` so that
    /// `vertex_predecessors_map[V2] == V1`, and `vertex_predecessors_map[V1] ==
    /// 0`.
    fn generate_wait_chain(
        &self,
        start: TimeStamp,
        end: TimeStamp,
        vertex_predecessors_map: HashMap<TimeStamp, TimeStamp>,
    ) -> Vec<WaitForEntry> {
        // It's rare that a deadlock formed by too many transactions. Preallocating a
        // few elements should be enough in most cases.
        let mut wait_chain = Vec::with_capacity(3);

        let mut lock_ts = end;
        loop {
            let waiter_ts = *vertex_predecessors_map.get(&lock_ts).unwrap();
            if waiter_ts.is_zero() {
                assert_eq!(lock_ts, start);
                break;
            }
            let locks = self
                .wait_for_map
                .get(&waiter_ts)
                .unwrap()
                .get(&lock_ts)
                .unwrap();

            let entry = locks.to_wait_for_entry(waiter_ts);
            wait_chain.push(entry);

            // Move backward
            lock_ts = waiter_ts;
        }

        wait_chain.reverse();
        wait_chain
    }

    /// Returns true and adds to the detect table if `txn_ts` is waiting for
    /// `lock_ts`.
    fn register_if_existed(
        &mut self,
        txn_ts: TimeStamp,
        lock_ts: TimeStamp,
        lock_hash: u64,
        key: &[u8],
        resource_group_tag: &[u8],
    ) -> bool {
        if let Some(wait_for) = self.wait_for_map.get_mut(&txn_ts) {
            if let Some(locks) = wait_for.get_mut(&lock_ts) {
                let pushed = locks.push(lock_hash, key.to_vec(), self.now);
                locks.resource_group_tag = resource_group_tag.to_vec();

                if pushed {
                    Self::insert_key_reverse_index(&mut self.key_reverse_map, key, lock_ts, txn_ts);
                }

                return true;
            }
        }
        false
    }

    /// Adds to the detect table. The edge from `txn_ts` to `lock_ts` must not
    /// exist.
    fn register(
        &mut self,
        txn_ts: TimeStamp,
        lock_ts: TimeStamp,
        lock_hash: u64,
        key: &[u8],
        resource_group_tag: &[u8],
    ) {
        let wait_for = self.wait_for_map.entry(txn_ts).or_default();
        assert!(!wait_for.contains_key(&lock_ts));
        let locks = Locks::new(
            lock_ts,
            lock_hash,
            key.to_vec(),
            resource_group_tag.to_vec(),
            self.now,
        );
        wait_for.insert(locks.ts, locks);

        Self::insert_key_reverse_index(&mut self.key_reverse_map, key, lock_ts, txn_ts);
    }

    /// Removes the corresponding wait_for_entry.
    fn clean_up_wait_for(&mut self, txn_ts: TimeStamp, lock_digest: LockDigest, key: &[u8]) {
        if let Some(wait_for) = self.wait_for_map.get_mut(&txn_ts) {
            if let Some(locks) = wait_for.get_mut(&lock_digest.ts) {
                let removed_lock = locks.remove(lock_digest.hash, key);
                if locks.is_empty() {
                    wait_for.remove(&lock_digest.ts);
                    if wait_for.is_empty() {
                        self.wait_for_map.remove(&txn_ts);
                    }
                }

                // If the lock-waiting is registered by a TiKV node in new version, but
                // the `clean_up_wait_for` is sent from a node in old version (before v5.1), the
                // parameter `key` can be empty while the `wait_for_map` has stored the key.
                // In this case, use the key from the deleted entry from `locks`.
                if let Some((_, removed_key)) = removed_lock {
                    Self::remove_key_reverse_index(
                        &mut self.key_reverse_map,
                        &removed_key,
                        lock_digest.ts,
                        txn_ts,
                    )
                }
            }
        }
        TASK_COUNTER_METRICS.clean_up_wait_for.inc();
    }

    /// Replaces the specific lock on the specific key, and re-detect all
    /// waiters on this lock. For each detected deadlocks, returns a tuple
    /// `(txn_id, key_hash, key, wait_chain, resource_group_tag)`. Note that
    /// the `key_hash` and `key` differs from the one given in parameter. Same
    /// as the `detect` function's return value, the key is the one on which
    /// the transaction aborting by the deadlock blocks another transaction
    /// in the deadlock circle.
    fn replace_lock_by_key(
        &mut self,
        lock_hash: u64,
        key: Vec<u8>,
        old_lock_ts: TimeStamp,
        new_lock_ts: TimeStamp,
    ) -> Vec<(TimeStamp, u64, Vec<u8>, Vec<WaitForEntry>, Vec<u8>)> {
        if old_lock_ts == new_lock_ts {
            return vec![];
        }

        // Find a list of transactions that is blocked by `old_lock_ts` on `key`.
        let locks_and_waiters_of_key = match self.key_reverse_map.get_mut(&key) {
            Some(v) => v,
            None => return vec![],
        };
        let waiting_txn = match locks_and_waiters_of_key.remove(&old_lock_ts) {
            Some(v) => v,
            None => return vec![],
        };

        let mut result = vec![];

        // For each txn, update their entries in `wait_for_map` to change their blockers
        // from `old_lock_ts` to `new_lock_ts`.
        for txn_ts in &waiting_txn {
            // Unwrap as `wait_for_map` and `key_reverse_map` should be synchronized.
            let blockers = self.wait_for_map.get_mut(txn_ts).unwrap();
            let old_locks = blockers.get_mut(&old_lock_ts).unwrap();
            assert!(old_locks.remove(lock_hash, &key).is_some());
            let resource_group_tag = old_locks.resource_group_tag.clone();
            if old_locks.is_empty() {
                blockers.remove(&old_lock_ts);
            }

            // let new_locks = blockers
            //     .entry(new_lock_ts)
            //     .and_modify(|locks| {
            //         // It's possible that the new lock-waiting info already exists.
            // Ignore it in         // this case.
            //         locks.push(lock_hash, key.clone(), now);
            //     })
            //     .or_insert_with(|| Locks::new(new_lock_ts, lock_hash, key.clone(),
            // vec![], now)); new_locks.resource_group_tag = resource_group_tag;

            if let Some((deadlock_key_hash, deadlock_key, wait_chain)) =
                self.detect(*txn_ts, new_lock_ts, lock_hash, &key, &resource_group_tag)
            {
                result.push((
                    *txn_ts,
                    deadlock_key_hash,
                    deadlock_key,
                    wait_chain,
                    resource_group_tag,
                ));
            }
        }
        // // Insert the waiting_txn back to `key_reverse_map`, but waiting for
        // // `new_lock_ts` instead. If the `new_lock_ts` entry already exists for
        // // the key, merge the two set.
        // match locks_and_waiters_of_key.entry(new_lock_ts) {
        //     Entry::Occupied(mut e) => {
        //         e.get_mut().extend(waiting_txn);
        //     }
        //     Entry::Vacant(e) => {
        //         e.insert(waiting_txn);
        //     }
        // }
        return result;
    }

    /// Insert an entry to te `key_reverse_map`, but skip if the `key` is
    /// missing, which may be caused by TiKV nodes in old version.
    ///
    /// Panics if the entry already exists.
    fn insert_key_reverse_index(
        key_reverse_map: &mut KeyReverseMap,
        key: &[u8],
        lock_ts: TimeStamp,
        txn_ts: TimeStamp,
    ) {
        if key.is_empty() {
            return;
        }
        let waiting_txns_on_key = key_reverse_map
            .entry(key.to_vec())
            .or_default()
            .entry(lock_ts)
            .or_default();
        assert!(waiting_txns_on_key.insert(txn_ts));
    }

    /// Remove an entry from the `key_reverse_map`, but skip if the `key` is
    /// missing. Panics if the entry doesn't exist in the map.
    fn remove_key_reverse_index(
        key_reverse_map: &mut KeyReverseMap,
        key: &[u8],
        lock_ts: TimeStamp,
        txn_ts: TimeStamp,
    ) {
        if key.is_empty() {
            return;
        }

        if let Some(locks_on_key) = key_reverse_map.get_mut(key) {
            if let Some(waiting_txn_set) = locks_on_key.get_mut(&lock_ts) {
                waiting_txn_set.remove(&txn_ts);
                if waiting_txn_set.is_empty() {
                    locks_on_key.remove(&lock_ts);
                    if locks_on_key.is_empty() {
                        key_reverse_map.remove(key);
                    }
                }
            }
        }
    }

    /// Removes the entries of the transaction.
    fn clean_up(&mut self, txn_ts: TimeStamp) {
        if let Some(entry) = self.wait_for_map.remove(&txn_ts) {
            for (lock_ts, locks) in entry {
                for (_, key) in locks.keys {
                    Self::remove_key_reverse_index(
                        &mut self.key_reverse_map,
                        &key,
                        lock_ts,
                        txn_ts,
                    );
                }
            }
        }

        TASK_COUNTER_METRICS.clean_up.inc();
    }

    /// Clears the whole detect table.
    fn clear(&mut self) {
        self.wait_for_map.clear();
        self.key_reverse_map.clear();
    }

    /// Reset the ttl
    fn reset_ttl(&mut self, ttl: Duration) {
        self.ttl = ttl;
    }

    /// The threshold of detect table size to trigger `active_expire`.
    const ACTIVE_EXPIRE_THRESHOLD: usize = 100000;
    /// The interval between `active_expire`.
    const ACTIVE_EXPIRE_INTERVAL: Duration = Duration::from_secs(3600);

    /// Iterates the whole table to remove all expired entries.
    fn active_expire(&mut self) {
        if self.wait_for_map.len() >= Self::ACTIVE_EXPIRE_THRESHOLD
            && self.now.saturating_duration_since(self.last_active_expire)
                >= Self::ACTIVE_EXPIRE_INTERVAL
        {
            let now = self.now;
            let ttl = self.ttl;

            let removed_locks = self.wait_for_map.iter_mut().flat_map(|(txn_ts, wait_for)| {
                // Extract expired (lock_ts, locks) pairs, and attach the txn_ts to each of
                // them.
                wait_for
                    .extract_if(|_, locks| locks.is_expired(now, ttl))
                    .map(|(lock_ts, locks)| (*txn_ts, lock_ts, locks))
            });
            // Then, for each key in removed locks, remove its corresponding entry in
            // `key_reverse_map`.
            for (txn_ts, lock_ts, locks) in removed_locks {
                for (_, key) in locks.keys.into_iter() {
                    Self::remove_key_reverse_index(
                        &mut self.key_reverse_map,
                        &key,
                        lock_ts,
                        txn_ts,
                    );
                }
            }
            self.wait_for_map.retain(|_, wait_for| !wait_for.is_empty());
            self.last_active_expire = self.now;
        }
    }
}

/// The role of the detector.
#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum Role {
    /// The node is the leader of the detector.
    Leader,
    /// The node is a follower of the leader.
    #[default]
    Follower,
}

impl From<StateRole> for Role {
    fn from(role: StateRole) -> Role {
        match role {
            StateRole::Leader => Role::Leader,
            _ => Role::Follower,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DetectType {
    Detect,
    CleanUpWaitFor,
    CleanUp,
}

pub enum Task {
    /// The detect request of itself.
    Detect {
        tp: DetectType,
        txn_ts: TimeStamp,
        wait_info: Option<KeyLockWaitInfo>,
        // Only valid when `tp == Detect`.
        diag_ctx: DiagnosticContext,
    },
    /// Replace locks on specific key to another transaction's lock, update all
    /// its waiters and redo deadlock check.
    ReplaceLocksByKeys { items: Vec<ReplaceLockByKeyItem> },
    /// The detect request of other nodes.
    DetectRpc {
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    },
    /// If the node has the leader region and the role of the node changes,
    /// a `ChangeRole` task will be scheduled.
    ///
    /// It's the only way to change the node from leader to follower, and vice
    /// versa.
    ChangeRole(Role),
    /// Change the ttl of DetectTable
    ChangeTtl(Duration),
    // Task only used for test
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(u64) + Send>),
    #[cfg(test)]
    GetRole(Box<dyn FnOnce(Role) + Send>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Detect {
                tp,
                txn_ts,
                wait_info,
                ..
            } => write!(
                f,
                "Detect {{ tp: {:?}, txn_ts: {}, wait_info: {:?} }}",
                tp, txn_ts, wait_info
            ),
            Task::ReplaceLocksByKeys { items } => {
                write!(f, "ReplaeLocksByKeys {{ items: {:?} }}", items,)
            }
            Task::DetectRpc { .. } => write!(f, "Detect Rpc"),
            Task::ChangeRole(role) => write!(f, "ChangeRole {{ role: {:?} }}", role),
            Task::ChangeTtl(ttl) => write!(f, "ChangeTtl {{ ttl: {:?} }}", ttl),
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(_) => write!(f, "Validate dead lock config"),
            #[cfg(test)]
            Task::GetRole(_) => write!(f, "Get role of the deadlock detector"),
        }
    }
}

/// `Scheduler` is the wrapper of the `FutureScheduler<Task>` to simplify
/// scheduling tasks to the deadlock detector.
#[derive(Clone)]
pub struct Scheduler(FutureScheduler<Task>);

impl Scheduler {
    pub fn new(scheduler: FutureScheduler<Task>) -> Self {
        Self(scheduler)
    }

    fn notify_scheduler(&self, task: Task) {
        // Only when the deadlock detector is stopped, an error will be returned.
        // So there is no need to handle the error.
        if let Err(Stopped(task)) = self.0.schedule(task) {
            error!("failed to send task to deadlock_detector"; "task" => %task);
        }
    }

    pub fn detect(
        &self,
        txn_ts: TimeStamp,
        wait_info: KeyLockWaitInfo,
        diag_ctx: DiagnosticContext,
    ) {
        // TODO: Support detect many keys in a batch
        self.notify_scheduler(Task::Detect {
            tp: DetectType::Detect,
            txn_ts,
            wait_info: Some(wait_info),
            diag_ctx,
        });
    }

    pub fn clean_up_wait_for(&self, start_ts: TimeStamp, wait_info: KeyLockWaitInfo) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::CleanUpWaitFor,
            txn_ts: start_ts,
            wait_info: Some(wait_info),
            diag_ctx: DiagnosticContext::default(),
        });
    }

    pub fn clean_up(&self, txn_ts: TimeStamp) {
        self.notify_scheduler(Task::Detect {
            tp: DetectType::CleanUp,
            txn_ts,
            wait_info: None,
            diag_ctx: DiagnosticContext::default(),
        });
    }

    pub fn replace_locks_by_keys(&self, items: Vec<ReplaceLockByKeyItem>) {
        self.notify_scheduler(Task::ReplaceLocksByKeys { items });
    }

    fn change_role(&self, role: Role) {
        self.notify_scheduler(Task::ChangeRole(role));
    }

    pub fn change_ttl(&self, t: Duration) {
        self.notify_scheduler(Task::ChangeTtl(t));
    }

    #[cfg(any(test, feature = "testexport"))]
    pub fn validate(&self, f: Box<dyn FnOnce(u64) + Send>) {
        self.notify_scheduler(Task::Validate(f));
    }

    #[cfg(test)]
    pub fn get_role(&self, f: Box<dyn FnOnce(Role) + Send>) {
        self.notify_scheduler(Task::GetRole(f));
    }
}

/// The leader region is the region containing the LEADER_KEY and the leader of
/// the leader region is also the leader of the deadlock detector.
const LEADER_KEY: &[u8] = b"";

/// `RoleChangeNotifier` observes region or role change events of raftstore. If
/// the region is the leader region and the role of this node is changed, a
/// `ChangeRole` task will be scheduled to the deadlock detector. It's the only
/// way to change the node from the leader of deadlock detector to follower, and
/// vice versa.
#[derive(Clone)]
pub(crate) struct RoleChangeNotifier {
    /// The id of the valid leader region.
    // raftstore.coprocessor needs it to be Sync + Send.
    leader_region_id: Arc<Mutex<u64>>,
    scheduler: Scheduler,
}

impl RoleChangeNotifier {
    fn is_leader_region(region: &Region) -> bool {
        // The key range of a new created region is empty which misleads the leader
        // of the deadlock detector stepping down.
        //
        // If the peers of a region is not empty, the region info is complete.
        is_region_initialized(region)
            && region.get_start_key() <= LEADER_KEY
            && (region.get_end_key().is_empty() || LEADER_KEY < region.get_end_key())
    }

    pub(crate) fn new(scheduler: Scheduler) -> Self {
        Self {
            leader_region_id: Arc::new(Mutex::new(INVALID_ID)),
            scheduler,
        }
    }

    pub(crate) fn register(self, host: &mut CoprocessorHost<impl KvEngine>) {
        host.registry
            .register_role_observer(1, BoxRoleObserver::new(self.clone()));
        host.registry
            .register_region_change_observer(1, BoxRegionChangeObserver::new(self));
    }
}

impl Coprocessor for RoleChangeNotifier {}

impl RoleObserver for RoleChangeNotifier {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, role_change: &RoleChange) {
        let region = ctx.region();
        // A region is created first, so the leader region id must be valid.
        if Self::is_leader_region(region)
            && *self.leader_region_id.lock().unwrap() == region.get_id()
        {
            self.scheduler.change_role(role_change.state.into());
        }
    }
}

impl RegionChangeObserver for RoleChangeNotifier {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        role: StateRole,
    ) {
        let region = ctx.region();
        if Self::is_leader_region(region) {
            match event {
                RegionChangeEvent::Create | RegionChangeEvent::Update(_) => {
                    *self.leader_region_id.lock().unwrap() = region.get_id();
                    self.scheduler.change_role(role.into());
                }
                RegionChangeEvent::Destroy => {
                    // When one region is merged to target region, it will be destroyed.
                    // If the leader region is merged to the target region and the node
                    // is also the leader of the target region, the RoleChangeNotifier will
                    // receive one RegionChangeEvent::Update of the target region and one
                    // RegionChangeEvent::Destroy of the leader region. To prevent the
                    // destroy event misleading the leader stepping down, it saves the
                    // valid leader region id and only when the id equals to the destroyed
                    // region id, it sends a ChangeRole(Follower) task to the deadlock detector.
                    let mut leader_region_id = self.leader_region_id.lock().unwrap();
                    if *leader_region_id == region.get_id() {
                        *leader_region_id = INVALID_ID;
                        self.scheduler.change_role(Role::Follower);
                    }
                }
                RegionChangeEvent::UpdateBuckets(_) => {}
            }
        }
    }
}

struct Inner {
    /// The role of the deadlock detector. Default is `Role::Follower`.
    role: Role,

    detect_table: DetectTable,
}

/// Detector is used to detect deadlocks between transactions. There is a leader
/// in the cluster which collects all `wait_for_entry` from other followers.
pub struct Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: PdClient + 'static,
{
    /// The store id of the node.
    store_id: u64,
    /// Used to create clients to the leader.
    env: Arc<Environment>,
    /// The leader's id and address if exists.
    leader_info: Option<(u64, String)>,
    /// The connection to the leader.
    leader_client: Option<Client>,
    /// Used to get the leader of leader region from PD.
    pd_client: Arc<P>,
    /// Used to resolve store address.
    resolver: S,
    /// Used to connect other nodes.
    security_mgr: Arc<SecurityManager>,
    /// Used to schedule Deadlock msgs to the waiter manager.
    waiter_mgr_scheduler: WaiterMgrScheduler,

    inner: Rc<RefCell<Inner>>,
}

unsafe impl<S, P> Send for Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: PdClient + 'static,
{
}

impl<S, P> Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: PdClient + 'static,
{
    pub fn new(
        store_id: u64,
        pd_client: Arc<P>,
        resolver: S,
        security_mgr: Arc<SecurityManager>,
        waiter_mgr_scheduler: WaiterMgrScheduler,
        cfg: &Config,
    ) -> Self {
        assert!(store_id != INVALID_ID);
        Self {
            store_id,
            env: client::env(),
            leader_info: None,
            leader_client: None,
            pd_client,
            resolver,
            security_mgr,
            waiter_mgr_scheduler,
            inner: Rc::new(RefCell::new(Inner {
                role: Role::Follower,
                detect_table: DetectTable::new(cfg.wait_for_lock_timeout.into()),
            })),
        }
    }

    /// Returns true if it is the leader of the deadlock detector.
    fn is_leader(&self) -> bool {
        self.inner.borrow().role == Role::Leader
    }

    /// Resets to the initial state.
    fn reset(&mut self, role: Role) {
        let mut inner = self.inner.borrow_mut();
        inner.detect_table.clear();
        inner.role = role;
        self.leader_client.take();
        self.leader_info.take();
    }

    /// Refreshes the leader info. Returns true if the leader exists.
    fn refresh_leader_info(&mut self) -> bool {
        assert!(!self.is_leader());
        match self.get_leader_info() {
            Ok(Some((leader_id, leader_addr))) => {
                self.update_leader_info(leader_id, leader_addr);
            }
            Ok(None) => {
                // The leader is gone, reset state.
                info!("no leader");
                self.reset(Role::Follower);
            }
            Err(e) => {
                error!("get leader info failed"; "err" => ?e);
            }
        };
        self.leader_info.is_some()
    }

    /// Gets leader info from PD.
    fn get_leader_info(&self) -> Result<Option<(u64, String)>> {
        let leader = self.pd_client.get_region_info(LEADER_KEY)?.leader;
        match leader {
            Some(leader) => {
                let leader_id = leader.get_store_id();
                let leader_addr = self.resolve_store_address(leader_id)?;
                Ok(Some((leader_id, leader_addr)))
            }

            None => {
                ERROR_COUNTER_METRICS.leader_not_found.inc();
                Ok(None)
            }
        }
    }

    /// Resolves store address.
    fn resolve_store_address(&self, store_id: u64) -> Result<String> {
        match wait_op!(|cb| self
            .resolver
            .resolve(store_id, cb)
            .map_err(|e| Error::Other(box_err!(e))))
        {
            Some(Ok(addr)) => Ok(addr),
            _ => Err(box_err!("failed to resolve store address")),
        }
    }

    /// Updates the leader info.
    fn update_leader_info(&mut self, leader_id: u64, leader_addr: String) {
        match self.leader_info {
            Some((id, ref addr)) if id == leader_id && *addr == leader_addr => {
                debug!("leader not change"; "leader_id" => leader_id, "leader_addr" => %leader_addr);
            }
            _ => {
                // The leader info is stale if the leader is itself.
                if leader_id == self.store_id {
                    info!("stale leader info");
                } else {
                    info!("leader changed"; "leader_id" => leader_id, "leader_addr" => %leader_addr);
                    self.leader_client.take();
                    self.leader_info.replace((leader_id, leader_addr));
                }
            }
        }
    }

    /// Resets state if role changes.
    fn change_role(&mut self, role: Role) {
        if self.inner.borrow().role != role {
            match role {
                Role::Leader => {
                    info!("became the leader of deadlock detector!"; "self_id" => self.store_id);
                    DETECTOR_LEADER_GAUGE.set(1);
                }
                Role::Follower => {
                    info!("changed from the leader of deadlock detector to follower!"; "self_id" => self.store_id);
                    DETECTOR_LEADER_GAUGE.set(0);
                }
            }
        }
        // If the node is a follower, it will receive a `ChangeRole(Follower)` msg when
        // the leader is changed. It should reset itself even if the role of the
        // node is not changed.
        self.reset(role);
    }

    /// Reconnects the leader. The leader info must exist.
    fn reconnect_leader(&mut self) {
        assert!(self.leader_client.is_none() && self.leader_info.is_some());
        ERROR_COUNTER_METRICS.reconnect_leader.inc();
        let (leader_id, leader_addr) = self.leader_info.as_ref().unwrap();
        // Create the connection to the leader and registers the callback to receive
        // the deadlock response.
        let mut leader_client = Client::new(
            Arc::clone(&self.env),
            Arc::clone(&self.security_mgr),
            leader_addr,
        );
        let waiter_mgr_scheduler = self.waiter_mgr_scheduler.clone();
        let (send, recv) = leader_client.register_detect_handler(Box::new(move |mut resp| {
            let entry = resp.take_entry();
            let txn = entry.txn.into();
            let lock = LockDigest {
                ts: entry.wait_for_txn.into(),
                hash: entry.key_hash,
            };
            let mut wait_chain: Vec<_> = resp.take_wait_chain().into();
            let key = entry.get_key().to_vec();
            wait_chain.push(entry);
            waiter_mgr_scheduler.deadlock(
                txn,
                key,
                lock,
                resp.get_deadlock_key_hash(),
                resp.deadlock_key,
                wait_chain,
            )
        }));
        spawn_local(send.map_err(|e| error!("leader client failed"; "err" => ?e)));
        // No need to log it again.
        spawn_local(recv.map_err(|_| ()));

        self.leader_client = Some(leader_client);
        info!("reconnect leader succeeded"; "leader_id" => leader_id);
    }

    /// Returns true if sends successfully.
    ///
    /// If the client is None, reconnects the leader first, then sends the
    /// request to the leader. If sends failed, sets the client to None for
    /// retry.
    fn send_detect_request_to_leader(
        &mut self,
        tp: DetectType,
        txn_ts: TimeStamp,
        wait_info: &Option<KeyLockWaitInfo>,
        diag_ctx: DiagnosticContext,
    ) -> bool {
        assert!(!self.is_leader() && self.leader_info.is_some());

        if self.leader_client.is_none() {
            self.reconnect_leader();
        }
        if let Some(leader_client) = &self.leader_client {
            let tp = match tp {
                DetectType::Detect => DeadlockRequestType::Detect,
                DetectType::CleanUpWaitFor => DeadlockRequestType::CleanUpWaitFor,
                DetectType::CleanUp => DeadlockRequestType::CleanUp,
            };
            let mut entry = WaitForEntry::default();
            entry.set_txn(txn_ts.into_inner());
            if let Some(wait_info) = wait_info.as_ref() {
                entry.set_wait_for_txn(wait_info.lock_digest.ts.into_inner());
                entry.set_key_hash(wait_info.lock_digest.hash);
                entry.set_key(wait_info.key.to_raw().unwrap());
            }
            entry.set_resource_group_tag(diag_ctx.resource_group_tag);
            let mut req = DeadlockRequest::default();
            req.set_tp(tp);
            req.set_entry(entry);
            if leader_client.send(req).is_ok() {
                return true;
            }
            // The client is disconnected. Take it for retry.
            self.leader_client.take();
        }
        false
    }

    /// Send `replace_locks_by_keys` request to the leader.
    ///
    /// Returns `Ok` if the request is sent to the client's channel. Otherwise,
    /// the `items` will be returned back in the `Err`, so that the caller
    /// can retry sending without cloning `items`.
    fn send_replace_locks_by_keys_to_leader(
        &mut self,
        items: Vec<ReplaceLockByKeyItem>,
    ) -> std::result::Result<(), Vec<ReplaceLockByKeyItem>> {
        assert!(!self.is_leader() && self.leader_info.is_some());

        if self.leader_client.is_none() {
            self.reconnect_leader();
        }
        if let Some(leader_client) = &self.leader_client {
            let mut replace_locks_by_keys_req = ReplaceLocksByKeysRequest::default();
            replace_locks_by_keys_req.set_items(items.into());
            let mut req = DeadlockRequest::default();
            req.set_tp(DeadlockRequestType::ReplaceLockByKey);
            req.set_replace_locks_by_keys(replace_locks_by_keys_req);

            match leader_client.send(req) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    self.leader_client.take();
                    return Err(e
                        .into_inner()
                        .take_replace_locks_by_keys()
                        .take_items()
                        .into_vec());
                }
            }
        }
        Err(items)
    }

    fn handle_detect_locally(
        &self,
        tp: DetectType,
        txn_ts: TimeStamp,
        wait_info: Option<KeyLockWaitInfo>,
        diag_ctx: DiagnosticContext,
    ) {
        let detect_table = &mut self.inner.borrow_mut().detect_table;
        match tp {
            DetectType::Detect => {
                let wait_info = wait_info.unwrap();
                let key = wait_info.key.to_raw().unwrap();
                if let Some((deadlock_key_hash, deadlock_key, mut wait_chain)) = detect_table
                    .detect(
                        txn_ts,
                        wait_info.lock_digest.ts,
                        wait_info.lock_digest.hash,
                        &key,
                        &diag_ctx.resource_group_tag,
                    )
                {
                    let mut last_entry = WaitForEntry::default();
                    last_entry.set_txn(txn_ts.into_inner());
                    last_entry.set_wait_for_txn(wait_info.lock_digest.ts.into_inner());
                    last_entry.set_key_hash(wait_info.lock_digest.hash);
                    last_entry.set_key(key.clone());
                    last_entry.set_resource_group_tag(diag_ctx.resource_group_tag);
                    wait_chain.push(last_entry);
                    self.waiter_mgr_scheduler.deadlock(
                        txn_ts,
                        key,
                        wait_info.lock_digest,
                        deadlock_key_hash,
                        deadlock_key,
                        wait_chain,
                    );
                }
            }
            DetectType::CleanUpWaitFor => {
                let wait_info = wait_info.unwrap();
                detect_table.clean_up_wait_for(
                    txn_ts,
                    wait_info.lock_digest,
                    &wait_info.key.to_raw().unwrap(),
                )
            }
            DetectType::CleanUp => detect_table.clean_up(txn_ts),
        }
    }

    /// Handles detect requests of itself.
    fn handle_detect(
        &mut self,
        tp: DetectType,
        txn_ts: TimeStamp,
        wait_info: Option<KeyLockWaitInfo>,
        diag_ctx: DiagnosticContext,
    ) {
        if self.is_leader() {
            self.handle_detect_locally(tp, txn_ts, wait_info, diag_ctx);
        } else {
            for _ in 0..2 {
                // TODO: If the leader hasn't been elected, it requests Pd for
                // each detect request. Maybe need flow control here.
                //
                // Refresh leader info when the connection to the leader is disconnected.
                if self.leader_client.is_none() && !self.refresh_leader_info() {
                    break;
                }
                if self.send_detect_request_to_leader(tp, txn_ts, &wait_info, diag_ctx.clone()) {
                    return;
                }
                // Because the client is asynchronous, it won't be closed until
                // failing to send a request. So retry to
                // refresh the leader info and send it again.
            }
            // If a request which causes deadlock is dropped, it leads to the waiter
            // timeout. TiDB will retry to acquire the lock and detect deadlock
            // again.
            warn!("detect request dropped"; "tp" => ?tp, "txn_ts" => txn_ts, "wait_info" => ?wait_info);
            ERROR_COUNTER_METRICS.dropped.inc();
        }
    }

    fn handle_replace_locks_by_keys_locally(&mut self, items: Vec<ReplaceLockByKeyItem>) {
        let detect_table = &mut self.inner.borrow_mut().detect_table;
        for item in items {
            let deadlocks = detect_table.replace_lock_by_key(
                item.key_hash,
                item.key.clone(),
                item.old_lock_ts.into(),
                item.new_lock_ts.into(),
            );
            for (txn_ts, deadlock_key_hash, deadlock_key, mut wait_chain, resource_group_tag) in
                deadlocks
            {
                let mut last_entry = WaitForEntry::default();
                last_entry.set_txn(txn_ts.into_inner());
                last_entry.set_wait_for_txn(item.new_lock_ts);
                last_entry.set_key_hash(item.key_hash);
                last_entry.set_key(item.key.clone());
                last_entry.set_resource_group_tag(resource_group_tag);
                wait_chain.push(last_entry);
                self.waiter_mgr_scheduler.deadlock(
                    txn_ts,
                    item.key.clone(),
                    LockDigest {
                        ts: item.new_lock_ts.into(),
                        hash: item.key_hash,
                    },
                    deadlock_key_hash,
                    deadlock_key,
                    wait_chain,
                );
            }
        }
    }

    fn handle_replace_locks_by_keys(&mut self, mut items: Vec<ReplaceLockByKeyItem>) {
        if items.is_empty() {
            return;
        }

        if self.is_leader() {
            self.handle_replace_locks_by_keys_locally(items);
        } else {
            for _ in 0..2 {
                // TODO: If the leader hasn't been elected, it requests Pd for
                // each detect request. Maybe need flow control here.
                //
                // Refresh leader info when the connection to the leader is disconnected.
                if self.leader_client.is_none() && !self.refresh_leader_info() {
                    break;
                }
                match self.send_replace_locks_by_keys_to_leader(items) {
                    Ok(()) => return,
                    Err(i) => items = i,
                }
            }
            warn!("replace_locks_by_keys request dropped"; "items" => ?items);
            ERROR_COUNTER_METRICS.dropped.inc();
        }
    }

    /// Handles detect requests of other nodes.
    fn handle_detect_rpc(
        &self,
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    ) {
        // TODO: Support batch checking.
        if !self.is_leader() {
            let status = RpcStatus::with_message(
                RpcStatusCode::FAILED_PRECONDITION,
                "I'm not the leader of deadlock detector".to_string(),
            );
            spawn_local(sink.fail(status).map_err(|_| ()));
            ERROR_COUNTER_METRICS.not_leader.inc();
            return;
        }

        let inner = Rc::clone(&self.inner);
        let mut s = stream
            .map_err(Error::Grpc)
            .try_filter_map(move |mut req| {
                // It's possible the leader changes after registering this handler.
                let mut inner = inner.borrow_mut();
                if inner.role != Role::Leader {
                    ERROR_COUNTER_METRICS.not_leader.inc();
                    return future::ready(Err(Error::Other(box_err!("leader changed"))));
                }
                let WaitForEntry {
                    txn,
                    wait_for_txn,
                    key_hash,
                    key,
                    resource_group_tag,
                    ..
                } = req.get_entry();
                let detect_table = &mut inner.detect_table;
                let res: Option<Pin<Box<dyn futures::Stream<Item = _>>>> = match req.get_tp() {
                    DeadlockRequestType::Detect => {
                        if let Some((deadlock_key_hash, deadlock_key, wait_chain)) = detect_table
                            .detect(
                                txn.into(),
                                wait_for_txn.into(),
                                *key_hash,
                                key,
                                resource_group_tag,
                            )
                        {
                            let mut resp = DeadlockResponse::default();
                            resp.set_entry(req.take_entry());
                            resp.set_deadlock_key_hash(deadlock_key_hash);
                            resp.set_deadlock_key(deadlock_key);
                            resp.set_wait_chain(wait_chain.into());
                            Some(Box::pin(stream::once(future::ok((
                                resp,
                                WriteFlags::default(),
                            )))))
                        } else {
                            None
                        }
                    }
                    DeadlockRequestType::CleanUpWaitFor => {
                        detect_table.clean_up_wait_for(
                            txn.into(),
                            LockDigest {
                                ts: wait_for_txn.into(),
                                hash: *key_hash,
                            },
                            key,
                        );
                        None
                    }
                    DeadlockRequestType::CleanUp => {
                        detect_table.clean_up(txn.into());
                        None
                    }
                    DeadlockRequestType::ReplaceLockByKey => {
                        let items = req.take_replace_locks_by_keys().take_items();
                        let result = items.into_iter().flat_map(move |item| {
                            detect_table
                                .replace_lock_by_key(
                                    item.key_hash,
                                    item.key.clone(),
                                    item.old_lock_ts.into(),
                                    item.new_lock_ts.into(),
                                )
                                .into_iter()
                                .map(move |(
                                        txn_ts,
                                        deadlock_key_hash,
                                        deadlock_key,
                                        wait_chain,
                                        resource_group_tag,
                                    )| {
                                        let mut entry = WaitForEntry::default();
                                        entry.set_key(item.key.clone());
                                        entry.set_key_hash(item.key_hash);
                                        entry.set_txn(txn_ts.into_inner());
                                        entry.set_wait_for_txn(item.new_lock_ts);
                                        entry.set_resource_group_tag(resource_group_tag);

                                        let mut resp = DeadlockResponse::default();
                                        resp.set_entry(entry);
                                        resp.set_deadlock_key_hash(deadlock_key_hash);
                                        resp.set_deadlock_key(deadlock_key);
                                        resp.set_wait_chain(wait_chain.into());

                                        Ok((resp, WriteFlags::default()))
                                    },
                                )
                            })
                            // Needs to drain the iterator to remove the reference to `inner`.
                            .collect_vec();
                        Some(Box::pin(stream::iter(result)))
                    }
                };
                future::ok(res)
            })
            .try_flatten();
        let send_task = async move {
            let mut sink = sink.sink_map_err(Error::from);
            sink.send_all(&mut s).await?;
            sink.close().await?;
            Result::Ok(())
        }
        .map_err(|e| warn!("deadlock detect rpc stream disconnected"; "error" => ?e));
        spawn_local(send_task);
    }

    fn handle_change_role(&mut self, role: Role) {
        debug!("handle change role"; "role" => ?role);
        self.change_role(role);
    }

    fn handle_change_ttl(&mut self, ttl: Duration) {
        let mut inner = self.inner.borrow_mut();
        inner.detect_table.reset_ttl(ttl);
        info!("Deadlock detector config changed"; "ttl" => ?ttl);
    }
}

impl<S, P> FutureRunnable<Task> for Detector<S, P>
where
    S: StoreAddrResolver + 'static,
    P: PdClient + 'static,
{
    fn run(&mut self, task: Task) {
        match task {
            Task::Detect {
                tp,
                txn_ts,
                wait_info,
                diag_ctx,
            } => {
                self.handle_detect(tp, txn_ts, wait_info, diag_ctx);
            }
            Task::ReplaceLocksByKeys { items } => {
                self.handle_replace_locks_by_keys(items);
            }
            Task::DetectRpc { stream, sink } => {
                self.handle_detect_rpc(stream, sink);
            }
            Task::ChangeRole(role) => self.handle_change_role(role),
            Task::ChangeTtl(ttl) => self.handle_change_ttl(ttl),
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(f) => f(self.inner.borrow().detect_table.ttl.as_millis() as u64),
            #[cfg(test)]
            Task::GetRole(f) => f(self.inner.borrow().role),
        }
    }
}

#[derive(Clone)]
pub struct Service {
    waiter_mgr_scheduler: WaiterMgrScheduler,
    detector_scheduler: Scheduler,
}

impl Service {
    pub fn new(waiter_mgr_scheduler: WaiterMgrScheduler, detector_scheduler: Scheduler) -> Self {
        Self {
            waiter_mgr_scheduler,
            detector_scheduler,
        }
    }
}

impl Deadlock for Service {
    // TODO: remove it
    fn get_wait_for_entries(
        &mut self,
        ctx: RpcContext<'_>,
        _req: WaitForEntriesRequest,
        sink: UnarySink<WaitForEntriesResponse>,
    ) {
        let (cb, f) = paired_future_callback();
        if !self.waiter_mgr_scheduler.dump_wait_table(cb) {
            let status = RpcStatus::with_message(
                RpcStatusCode::RESOURCE_EXHAUSTED,
                "waiter manager has stopped".to_owned(),
            );
            ctx.spawn(sink.fail(status).map(|_| ()))
        } else {
            ctx.spawn(
                f.map_err(Error::from)
                    .map_ok(|v| {
                        let mut resp = WaitForEntriesResponse::default();
                        resp.set_entries(v.into());
                        resp
                    })
                    .and_then(|resp| sink.success(resp).map_err(Error::Grpc))
                    .unwrap_or_else(|e| debug!("get_wait_for_entries failed"; "err" => ?e)),
            );
        }
    }

    fn detect(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<DeadlockRequest>,
        sink: DuplexSink<DeadlockResponse>,
    ) {
        let task = Task::DetectRpc { stream, sink };
        if let Err(Stopped(Task::DetectRpc { sink, .. })) = self.detector_scheduler.0.schedule(task)
        {
            let status = RpcStatus::with_message(
                RpcStatusCode::RESOURCE_EXHAUSTED,
                "deadlock detector has stopped".to_owned(),
            );
            ctx.spawn(sink.fail(status).map(|_| ()));
        }
    }
}

#[cfg(test)]
pub mod tests {
    use engine_test::kv::KvTestEngine;
    use futures::executor::block_on;
    use raftstore::coprocessor::RegionChangeReason;
    use security::SecurityConfig;
    use tikv_util::worker::FutureWorker;

    use super::*;
    use crate::server::resolve;

    #[test]
    fn test_detect_table() {
        let mut detect_table = DetectTable::new(Duration::from_secs(10));

        // Deadlock: 1 -> 2 -> 1
        assert_eq!(detect_table.detect(1.into(), 2.into(), 2, &[], &[]), None);
        assert_eq!(
            detect_table
                .detect(2.into(), 1.into(), 1, &[], &[])
                .unwrap()
                .0,
            2
        );
        // Deadlock: 1 -> 2 -> 3 -> 1
        assert_eq!(detect_table.detect(2.into(), 3.into(), 3, &[], &[]), None);
        assert_eq!(
            detect_table
                .detect(3.into(), 1.into(), 1, &[], &[])
                .unwrap()
                .0,
            3
        );
        detect_table.clean_up(2.into());
        assert_eq!(detect_table.wait_for_map.contains_key(&2.into()), false);

        // After cycle is broken, no deadlock.
        assert_eq!(detect_table.detect(3.into(), 1.into(), 1, &[], &[]), None);
        assert_eq!(detect_table.wait_for_map.get(&3.into()).unwrap().len(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .keys
                .len(),
            1
        );

        // Different key_hash grows the list.
        assert_eq!(detect_table.detect(3.into(), 1.into(), 2, &[], &[]), None);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .keys
                .len(),
            2
        );

        // Same key_hash doesn't grow the list.
        assert_eq!(detect_table.detect(3.into(), 1.into(), 2, &[], &[]), None);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .keys
                .len(),
            2
        );

        // Different lock_ts grows the map.
        assert_eq!(detect_table.detect(3.into(), 2.into(), 2, &[], &[]), None);
        assert_eq!(detect_table.wait_for_map.get(&3.into()).unwrap().len(), 2);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&2.into())
                .unwrap()
                .keys
                .len(),
            1
        );

        // Clean up entries shrinking the map.
        detect_table.clean_up_wait_for(
            3.into(),
            LockDigest {
                ts: 1.into(),
                hash: 1,
            },
            b"k1",
        );
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&3.into())
                .unwrap()
                .get(&1.into())
                .unwrap()
                .keys
                .len(),
            1
        );
        detect_table.clean_up_wait_for(
            3.into(),
            LockDigest {
                ts: 1.into(),
                hash: 2,
            },
            b"k2",
        );
        assert_eq!(detect_table.wait_for_map.get(&3.into()).unwrap().len(), 1);
        detect_table.clean_up_wait_for(
            3.into(),
            LockDigest {
                ts: 2.into(),
                hash: 2,
            },
            b"k2",
        );
        assert_eq!(detect_table.wait_for_map.contains_key(&3.into()), false);

        // Clean up non-exist entry
        detect_table.clean_up(3.into());
        detect_table.clean_up_wait_for(
            3.into(),
            LockDigest {
                ts: 1.into(),
                hash: 1,
            },
            b"k2",
        );
    }

    #[test]
    fn test_detect_table_expire() {
        let mut detect_table = DetectTable::new(Duration::from_millis(100));

        // Deadlock
        assert!(
            detect_table
                .detect(1.into(), 2.into(), 1, &[], &[])
                .is_none()
        );
        assert!(
            detect_table
                .detect(2.into(), 1.into(), 2, &[], &[])
                .is_some()
        );
        // After sleep, the expired entry has been removed. So there is no deadlock.
        std::thread::sleep(Duration::from_millis(500));
        assert_eq!(detect_table.wait_for_map.len(), 1);
        assert!(
            detect_table
                .detect(2.into(), 1.into(), 2, &[], &[])
                .is_none()
        );
        assert_eq!(detect_table.wait_for_map.len(), 1);

        // `Detect` updates the last_detect_time, so the entry won't be removed.
        detect_table.clear();
        assert!(
            detect_table
                .detect(1.into(), 2.into(), 1, &[], &[])
                .is_none()
        );
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            detect_table
                .detect(1.into(), 2.into(), 1, &[], &[])
                .is_none()
        );
        assert!(
            detect_table
                .detect(2.into(), 1.into(), 2, &[], &[])
                .is_some()
        );

        // Remove expired entry shrinking the map.
        detect_table.clear();
        assert!(
            detect_table
                .detect(1.into(), 2.into(), 1, &[], &[])
                .is_none()
        );
        assert!(
            detect_table
                .detect(1.into(), 3.into(), 1, &[], &[])
                .is_none()
        );
        assert_eq!(detect_table.wait_for_map.len(), 1);
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            detect_table
                .detect(1.into(), 3.into(), 2, &[], &[])
                .is_none()
        );
        assert!(
            detect_table
                .detect(2.into(), 1.into(), 2, &[], &[])
                .is_none()
        );
        assert_eq!(detect_table.wait_for_map.get(&1.into()).unwrap().len(), 1);
        assert_eq!(
            detect_table
                .wait_for_map
                .get(&1.into())
                .unwrap()
                .get(&3.into())
                .unwrap()
                .keys
                .len(),
            2
        );
        std::thread::sleep(Duration::from_millis(500));
        assert!(
            detect_table
                .detect(3.into(), 2.into(), 3, &[], &[])
                .is_none()
        );
        assert_eq!(detect_table.wait_for_map.len(), 2);
        assert!(
            detect_table
                .detect(3.into(), 1.into(), 3, &[], &[])
                .is_none()
        );
        assert_eq!(detect_table.wait_for_map.len(), 1);
    }

    #[test]
    fn test_deadlock_generating_wait_chain() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct Edge<'a> {
            ts: u64,
            lock_ts: u64,
            hash: u64,
            key: &'a [u8],
            tag: &'a [u8],
        }

        let new_edge = |ts, lock_ts, hash, key, tag| Edge {
            ts,
            lock_ts,
            hash,
            key,
            tag,
        };

        // Detect specified edges sequentially, and expects the last one will cause the
        // deadlock.
        let test_once = |edges: &[Edge<'_>]| {
            let mut detect_table = DetectTable::new(Duration::from_millis(100));
            let mut edge_map = HashMap::default();

            for e in &edges[0..edges.len() - 1] {
                assert!(
                    detect_table
                        .detect(e.ts.into(), e.lock_ts.into(), e.hash, e.key, e.tag)
                        .is_none()
                );
                edge_map.insert((e.ts, e.lock_ts), *e);
            }

            let last = edges.last().unwrap();
            let (_, _, wait_chain) = detect_table
                .detect(
                    last.ts.into(),
                    last.lock_ts.into(),
                    last.hash,
                    last.key,
                    last.tag,
                )
                .unwrap();

            // Walk through the wait chain
            let mut current_position = last.lock_ts;
            for (i, entry) in wait_chain.iter().enumerate() {
                let edge = Edge {
                    ts: entry.get_txn(),
                    lock_ts: entry.get_wait_for_txn(),
                    hash: entry.get_key_hash(),
                    key: entry.get_key(),
                    tag: entry.get_resource_group_tag(),
                };
                let expect_edge = edge_map.get(&(edge.ts, edge.lock_ts)).unwrap();
                assert_eq!(
                    edge, *expect_edge,
                    "failed at item {}, full wait chain {:?}",
                    i, wait_chain
                );
                assert_eq!(
                    edge.ts, current_position,
                    "failed at item {}, full wait chain {:?}",
                    i, wait_chain
                );
                current_position = edge.lock_ts;
            }
            assert_eq!(
                current_position, last.ts,
                "incorrect wait chain {:?}",
                wait_chain
            );
        };

        test_once(&[
            new_edge(1, 2, 11, b"k1", b"tag1"),
            new_edge(2, 1, 12, b"k2", b"tag2"),
        ]);

        test_once(&[
            new_edge(1, 2, 11, b"k1", b"tag1"),
            new_edge(2, 3, 12, b"k2", b"tag2"),
            new_edge(3, 1, 13, b"k3", b"tag3"),
        ]);

        test_once(&[
            new_edge(1, 2, 11, b"k12", b"tag12"),
            new_edge(2, 3, 12, b"k23", b"tag23"),
            new_edge(2, 4, 13, b"k24", b"tag24"),
            new_edge(4, 1, 14, b"k41", b"tag41"),
        ]);

        test_once(&[
            new_edge(1, 2, 11, b"k12", b"tag12"),
            new_edge(1, 3, 12, b"k13", b"tag13"),
            new_edge(2, 4, 13, b"k24", b"tag24"),
            new_edge(3, 5, 14, b"k35", b"tag35"),
            new_edge(2, 5, 15, b"k25", b"tag25"),
            new_edge(5, 6, 16, b"k56", b"tag56"),
            new_edge(6, 1, 17, b"k61", b"tag61"),
        ]);

        use rand::seq::SliceRandom;
        let mut case = vec![
            new_edge(1, 2, 11, b"k12", b"tag12"),
            new_edge(1, 3, 12, b"k13", b"tag13"),
            new_edge(2, 4, 13, b"k24", b"tag24"),
            new_edge(3, 5, 14, b"k35", b"tag35"),
            new_edge(2, 5, 15, b"k25", b"tag25"),
            new_edge(5, 6, 16, b"k56", b"tag56"),
        ];
        case.shuffle(&mut rand::thread_rng());
        case.push(new_edge(6, 1, 17, b"k61", b"tag61"));
        test_once(&case);
    }

    pub(crate) struct MockPdClient;

    impl PdClient for MockPdClient {}

    fn start_deadlock_detector(
        host: &mut CoprocessorHost<KvTestEngine>,
    ) -> (FutureWorker<Task>, Scheduler) {
        let waiter_mgr_worker = FutureWorker::new("dummy-waiter-mgr");
        let waiter_mgr_scheduler = WaiterMgrScheduler::new(waiter_mgr_worker.scheduler());
        let mut detector_worker = FutureWorker::new("test-deadlock-detector");
        let detector_runner = Detector::new(
            1,
            Arc::new(MockPdClient {}),
            resolve::MockStoreAddrResolver::default(),
            Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()),
            waiter_mgr_scheduler,
            &Config::default(),
        );
        let detector_scheduler = Scheduler::new(detector_worker.scheduler());
        let role_change_notifier = RoleChangeNotifier::new(detector_scheduler.clone());
        role_change_notifier.register(host);
        detector_worker.start(detector_runner).unwrap();
        (detector_worker, detector_scheduler)
    }

    // Region with non-empty peers is valid.
    fn new_region(id: u64, start_key: &[u8], end_key: &[u8], valid: bool) -> Region {
        let mut region = Region::default();
        region.set_id(id);
        region.set_start_key(start_key.to_vec());
        region.set_end_key(end_key.to_vec());
        if valid {
            region.set_peers(vec![kvproto::metapb::Peer::default()].into());
        }
        region
    }

    #[test]
    fn test_role_change_notifier() {
        let mut host = CoprocessorHost::default();
        let (mut worker, scheduler) = start_deadlock_detector(&mut host);

        let mut region = new_region(1, b"", b"", true);
        let invalid = new_region(2, b"", b"", false);
        let other = new_region(3, b"0", b"", true);
        let follower_roles = [
            StateRole::Follower,
            StateRole::PreCandidate,
            StateRole::Candidate,
        ];
        let events = [
            RegionChangeEvent::Create,
            RegionChangeEvent::Update(RegionChangeReason::ChangePeer),
            RegionChangeEvent::Destroy,
        ];
        let check_role = |role| {
            let (tx, f) = paired_future_callback();
            scheduler.get_role(tx);
            assert_eq!(block_on(f).unwrap(), role);
        };

        // Region changed
        for &event in &events[..2] {
            for &follower_role in &follower_roles {
                host.on_region_changed(&region, event, follower_role);
                check_role(Role::Follower);
                host.on_region_changed(&invalid, event, StateRole::Leader);
                check_role(Role::Follower);
                host.on_region_changed(&other, event, StateRole::Leader);
                check_role(Role::Follower);
                host.on_region_changed(&region, event, StateRole::Leader);
                check_role(Role::Leader);
                host.on_region_changed(&invalid, event, follower_role);
                check_role(Role::Leader);
                host.on_region_changed(&other, event, follower_role);
                check_role(Role::Leader);
                host.on_region_changed(&region, event, follower_role);
                check_role(Role::Follower);
            }
        }
        host.on_region_changed(&region, RegionChangeEvent::Create, StateRole::Leader);
        host.on_region_changed(&invalid, RegionChangeEvent::Destroy, StateRole::Leader);
        host.on_region_changed(&other, RegionChangeEvent::Destroy, StateRole::Leader);
        check_role(Role::Leader);
        host.on_region_changed(&region, RegionChangeEvent::Destroy, StateRole::Leader);
        check_role(Role::Follower);
        // Leader region id is changed.
        region.set_id(2);
        host.on_region_changed(
            &region,
            RegionChangeEvent::Update(RegionChangeReason::ChangePeer),
            StateRole::Leader,
        );
        // Destroy the previous leader region.
        region.set_id(1);
        host.on_region_changed(&region, RegionChangeEvent::Destroy, StateRole::Leader);
        check_role(Role::Leader);

        // Role changed
        let region = new_region(1, b"", b"", true);
        host.on_region_changed(&region, RegionChangeEvent::Create, StateRole::Follower);
        check_role(Role::Follower);
        for &follower_role in &follower_roles {
            host.on_role_change(&region, RoleChange::new(follower_role));
            check_role(Role::Follower);
            host.on_role_change(&invalid, RoleChange::new(StateRole::Leader));
            check_role(Role::Follower);
            host.on_role_change(&other, RoleChange::new(StateRole::Leader));
            check_role(Role::Follower);
            host.on_role_change(&region, RoleChange::new(StateRole::Leader));
            check_role(Role::Leader);
            host.on_role_change(&invalid, RoleChange::new(follower_role));
            check_role(Role::Leader);
            host.on_role_change(&other, RoleChange::new(follower_role));
            check_role(Role::Leader);
            host.on_role_change(&region, RoleChange::new(follower_role));
            check_role(Role::Follower);
        }

        worker.stop();
    }
}
