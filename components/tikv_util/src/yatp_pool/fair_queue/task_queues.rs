// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeMap, time::Duration};

use rand::Rng;
use yatp::queue::multilevel::LEVEL_NUM;

use super::scheduler::FairTaskCell;

/// Accumulated running-time thresholds used by YATP's task history to select
/// levels 1 and 2. These do not set the duration of a worker's dispatch slice.
/// Tasks with an explicit fixed level bypass these thresholds.
pub(super) const LEVEL_TIME_THRESHOLDS: [Duration; LEVEL_NUM - 1] =
    [Duration::from_millis(5), Duration::from_millis(100)];

// This policy has exactly three levels; the threshold array's length checks
// that contract against YATP's LEVEL_NUM at compile time.
const LEVEL_SELECTION_TOTAL: u32 = 100;
const LEVEL0_CHANCE: u32 = 80;
const LEVEL1_CHANCE: u32 = 16;
const LEVEL1_CUMULATIVE_CHANCE: u32 = LEVEL0_CHANCE + LEVEL1_CHANCE;

/// Multilevel task ordering within one group queue. The scheduler owns
/// synchronization and assigns task levels; this queue preserves sequence
/// order within each level.
/// Yielded or woken tasks get a new sequence and join their level's tail.
/// Ordered maps also allow immediate cancellation by the scheduler's stored
/// (level, sequence) key and tail selection for admission eviction.
#[derive(Default)]
pub(super) struct TaskQueues {
    levels: [BTreeMap<u64, FairTaskCell>; LEVEL_NUM],
}

impl TaskQueues {
    /// Inserts a task using the scheduler's unique enqueue sequence.
    pub(super) fn push(&mut self, level: usize, sequence: u64, entry: FairTaskCell) {
        self.levels[level].insert(sequence, entry);
    }

    /// Picks an initial level with probabilities 80%, 16%, and 4%, then
    /// searches subsequent levels cyclically when the selected level is
    /// empty. These probabilities do not specify CPU shares or a wait bound.
    pub(super) fn pop(&mut self) -> Option<(usize, FairTaskCell)> {
        let first = Self::level_for_roll(rand::thread_rng().gen_range(0..LEVEL_SELECTION_TOTAL));
        self.pop_from(first)
    }

    pub(super) fn remove(&mut self, level: usize, sequence: u64) -> Option<FairTaskCell> {
        self.levels[level].remove(&sequence)
    }

    /// Returns the newest task in the lowest-priority nonempty level. Evicting
    /// its tail preserves older queued work that has already waited for
    /// service.
    pub(super) fn eviction_candidate(&self) -> Option<u64> {
        self.levels
            .iter()
            .rev()
            .find_map(|queue| queue.last_key_value().map(|(_, entry)| entry.record.id))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.levels.iter().all(BTreeMap::is_empty)
    }

    /// Transfers ownership so callers can destroy tasks after releasing locks.
    pub(super) fn drain(&mut self) -> impl Iterator<Item = FairTaskCell> + use<> {
        std::mem::take(&mut self.levels)
            .into_iter()
            .flat_map(BTreeMap::into_values)
    }

    fn level_for_roll(roll: u32) -> usize {
        debug_assert!(roll < LEVEL_SELECTION_TOTAL);
        if roll < LEVEL0_CHANCE {
            0
        } else if roll < LEVEL1_CUMULATIVE_CHANCE {
            1
        } else {
            LEVEL_NUM - 1
        }
    }

    fn pop_from(&mut self, first: usize) -> Option<(usize, FairTaskCell)> {
        (0..LEVEL_NUM).find_map(|offset| {
            let level = (first + offset) % LEVEL_NUM;
            self.levels[level]
                .pop_first()
                .map(|(_, entry)| (level, entry))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Weak};

    use yatp::{
        queue::{Extras, multilevel::TaskLevelManager},
        task::future::TaskCell,
    };

    use super::{
        super::{GroupPolicy, task::TaskRecord},
        *,
    };

    fn entry(id: u64) -> FairTaskCell {
        let policy = Arc::new(GroupPolicy::new(0, "default".into(), 8, 10_000));
        let history = Arc::new(TaskLevelManager::new(LEVEL_TIME_THRESHOLDS, None));
        FairTaskCell {
            task_cell: TaskCell::new(async {}, Extras::new_multilevel(id, None)),
            record: TaskRecord::new(id, policy, history, 0, Weak::new(), false),
        }
    }

    fn pop_id(queue: &mut TaskQueues, first: usize) -> Option<(usize, u64)> {
        queue
            .pop_from(first)
            .map(|(level, entry)| (level, entry.record.id))
    }

    #[test]
    fn fifo_within_each_level() {
        for level in 0..LEVEL_NUM {
            let mut queue = TaskQueues::default();
            queue.push(level, 20, entry(2));
            queue.push(level, 10, entry(1));
            queue.push(level, 30, entry(3));
            for id in 1..=3 {
                assert_eq!(pop_id(&mut queue, level), Some((level, id)));
            }
            assert!(queue.is_empty());
            assert!(queue.pop().is_none());
        }
    }

    #[test]
    fn empty_levels_follow_cyclic_order() {
        let mut queue = TaskQueues::default();
        queue.push(0, 1, entry(1));
        queue.push(2, 2, entry(2));
        assert_eq!(pop_id(&mut queue, 1), Some((2, 2)));
        assert_eq!(pop_id(&mut queue, 2), Some((0, 1)));
        assert!(queue.is_empty());

        for first in 0..LEVEL_NUM {
            let available = (first + LEVEL_NUM - 1) % LEVEL_NUM;
            queue.push(available, 3, entry(3));
            assert_eq!(pop_id(&mut queue, first), Some((available, 3)));
        }
    }

    #[test]
    fn remove_targets_exact_level_and_sequence() {
        let mut queue = TaskQueues::default();
        queue.push(0, 10, entry(1));
        queue.push(0, 20, entry(2));
        queue.push(1, 10, entry(3));
        assert!(queue.remove(2, 10).is_none());
        assert_eq!(queue.remove(0, 10).unwrap().record.id, 1);
        assert!(queue.remove(0, 10).is_none());
        assert_eq!(pop_id(&mut queue, 0), Some((0, 2)));
        assert_eq!(pop_id(&mut queue, 1), Some((1, 3)));
        assert!(queue.is_empty());
    }

    #[test]
    fn eviction_prefers_tail_of_lowest_nonempty_level() {
        let mut queue = TaskQueues::default();
        queue.push(0, 30, entry(1));
        queue.push(1, 1, entry(2));
        queue.push(2, 10, entry(3));
        queue.push(2, 20, entry(4));
        assert_eq!(queue.eviction_candidate(), Some(4));
        assert_eq!(queue.remove(2, 20).unwrap().record.id, 4);
        assert_eq!(queue.eviction_candidate(), Some(3));
        queue.remove(2, 10);
        assert_eq!(queue.eviction_candidate(), Some(2));
        queue.remove(1, 1);
        assert_eq!(queue.eviction_candidate(), Some(1));
        queue.remove(0, 30);
        assert_eq!(queue.eviction_candidate(), None);
    }

    #[test]
    fn selection_probabilities_are_80_16_4() {
        let mut counts = [0; LEVEL_NUM];
        for roll in 0..LEVEL_SELECTION_TOTAL {
            counts[TaskQueues::level_for_roll(roll)] += 1;
        }
        assert_eq!(counts, [80, 16, 4]);
        for (roll, level) in [(0, 0), (79, 0), (80, 1), (95, 1), (96, 2), (99, 2)] {
            assert_eq!(TaskQueues::level_for_roll(roll), level);
        }
    }

    #[test]
    fn drain_moves_entries_out_of_queue() {
        let mut queue = TaskQueues::default();
        queue.push(0, 30, entry(1));
        queue.push(1, 20, entry(2));
        queue.push(2, 10, entry(3));
        let drained = queue.drain();
        assert!(queue.is_empty());
        // The detached iterator must not retain a borrow of the queue.
        queue.push(0, 40, entry(4));
        let mut ids: Vec<_> = drained.map(|entry| entry.record.id).collect();
        ids.sort_unstable();
        assert_eq!(ids, [1, 2, 3]);
        assert_eq!(pop_id(&mut queue, 0), Some((0, 4)));
        assert!(queue.drain().next().is_none());
    }
}
