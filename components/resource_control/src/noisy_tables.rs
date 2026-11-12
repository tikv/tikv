// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The demoted-table set: the TiDB tables a blamed resource group has
//! proposed to. Regions holding one of them run on the low-priority store
//! pool.
//!
//! One lock owns the whole lifecycle. A mark lands in `pending`; the control
//! tick's `publish` merges `pending` into `published`; `clear` empties both.
//! `publish` and `clear` also write the derived `has_published` flag, the
//! generation and the gauge while still holding the write lock, so no reader
//! or concurrent transition can observe the set and its metadata
//! disagreeing. Logging is left to the caller, outside the lock.
//!
//! The blamed-group set lives in its own lock in `ResourceGroupManager`, and
//! neither lock is held while the other is taken. A mark that raced a
//! `clear` can leave one stale pending table; the next tick either clears it
//! again or demotes that table for one episode. Neither is a safety issue.

use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use fail::fail_point;
use parking_lot::RwLock;
use prometheus::IntGauge;

use crate::metrics;

#[derive(Default)]
struct State {
    published: BTreeSet<i64>,
    pending: BTreeSet<i64>,
    /// Bumped whenever `published` changes.
    generation: u64,
}

/// What a `publish` did, for the caller to log.
pub struct Published {
    pub added: Vec<i64>,
    pub total: usize,
}

pub struct NoisyTables {
    state: RwLock<State>,
    /// Published count, for operators. Injected so tests can own one.
    gauge: &'static IntGauge,
    // Mirrors of `state`, written only under its write lock so they can never
    // run ahead of or lag the set, and read on the hot path without it.
    has_published: AtomicBool,
    generation: AtomicU64,
}

impl Default for NoisyTables {
    fn default() -> Self {
        Self::new()
    }
}

impl NoisyTables {
    pub fn new() -> Self {
        Self::with_gauge(&metrics::NOISY_TABLES_GAUGE)
    }

    fn with_gauge(gauge: &'static IntGauge) -> Self {
        NoisyTables {
            state: RwLock::new(State::default()),
            gauge,
            has_published: AtomicBool::new(false),
            generation: AtomicU64::new(0),
        }
    }

    /// Whether `table_id` is published. Lock-free when nothing is.
    pub fn contains(&self, table_id: i64) -> bool {
        if !self.has_published.load(Ordering::Relaxed) {
            return false;
        }
        self.state.read().published.contains(&table_id)
    }

    /// Whether any published table lies in `first..=last`. Lock-free when
    /// nothing is published, a logarithmic lookup otherwise.
    pub fn any_in(&self, first: i64, last: i64) -> bool {
        if !self.has_published.load(Ordering::Relaxed) {
            return false;
        }
        self.state
            .read()
            .published
            .range(first..=last)
            .next()
            .is_some()
    }

    /// Changes whenever [`Self::contains`] may answer differently. Acquire
    /// pairs with the Release stores in `publish` and `clear`, so a reader
    /// that sees a new generation also sees the set it describes.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Queues `table_id` for the next publish. Returns whether it was queued.
    ///
    /// Hot path: a published or already-pending table costs one read lock.
    pub fn mark(&self, table_id: i64) -> bool {
        {
            let state = self.state.read();
            if state.published.contains(&table_id) || state.pending.contains(&table_id) {
                return false;
            }
        }
        self.state.write().pending.insert(table_id)
    }

    #[cfg(test)]
    pub fn pending_len(&self) -> usize {
        self.state.read().pending.len()
    }

    /// Adds the pending marks to the published set. Adds rather than
    /// replaces: a demoted region's reads stop reaching `propose`, so it
    /// stops being marked, and replacing made demotion oscillate. `clear` is
    /// the only release.
    pub fn publish(&self) -> Option<Published> {
        let mut state = self.state.write();
        if state.pending.is_empty() {
            return None;
        }
        fail_point!("noisy_tables_publish_before_merge");
        let added: Vec<i64> = std::mem::take(&mut state.pending).into_iter().collect();
        let before = state.published.len();
        state.published.extend(added.iter().copied());
        let total = state.published.len();
        if total != before {
            state.generation += 1;
            self.commit(&state);
        }
        Some(Published { added, total })
    }

    /// Drops pending and published marks. Returns how many published tables
    /// were released.
    pub fn clear(&self) -> usize {
        let mut state = self.state.write();
        state.pending.clear();
        let released = state.published.len();
        if released > 0 {
            state.published.clear();
            state.generation += 1;
        }
        self.commit(&state);
        released
    }

    /// Writes the derived metadata from the authoritative state. Called with
    /// the write guard held, so a transition and its metadata are one step.
    fn commit(&self, state: &State) {
        self.gauge.set(state.published.len() as i64);
        self.has_published
            .store(!state.published.is_empty(), Ordering::Relaxed);
        self.generation.store(state.generation, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A gauge this test alone writes; the real one is process-global.
    fn with_own_gauge() -> NoisyTables {
        let gauge = Box::leak(Box::new(
            IntGauge::new("noisy_tables_test", "per-test gauge").unwrap(),
        ));
        NoisyTables::with_gauge(gauge)
    }

    /// Set, flag, generation and gauge are checked together after every
    /// transition.
    #[test]
    fn test_metadata_tracks_the_set_through_every_transition() {
        let t = with_own_gauge();
        assert!(!t.contains(7));
        assert_eq!(t.generation(), 0);
        assert_eq!(t.gauge.get(), 0);

        assert!(t.mark(7));
        assert!(!t.contains(7), "pending is not published");
        assert!(!t.mark(7), "already pending");
        assert_eq!(t.gauge.get(), 0);

        let p = t.publish().expect("something was pending");
        assert_eq!(p.added, vec![7]);
        assert_eq!(p.total, 1);
        assert!(t.contains(7));
        assert!(t.any_in(0, 10));
        assert!(!t.any_in(8, 10));
        assert!(t.has_published.load(Ordering::Relaxed));
        assert_eq!(t.generation(), 1);
        assert_eq!(t.gauge.get(), 1);
        assert!(!t.mark(7), "already published");

        assert!(t.mark(9));
        t.publish();
        assert_eq!(t.generation(), 2);
        assert_eq!(t.gauge.get(), 2);

        assert_eq!(t.clear(), 2);
        assert!(!t.contains(7));
        assert!(!t.has_published.load(Ordering::Relaxed));
        assert_eq!(t.generation(), 3);
        assert_eq!(t.gauge.get(), 0);
        assert_eq!(t.pending_len(), 0);

        // A clear with nothing published leaves everything as it was.
        assert_eq!(t.clear(), 0);
        assert_eq!(t.generation(), 3);
        assert_eq!(t.gauge.get(), 0);
    }
}
