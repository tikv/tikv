// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use collections::{HashMap, HashMapEntry};
use kvproto::{
    metapb,
    replication_modepb::{ReplicationMode, ReplicationStatus, StoreDrAutoSyncStatus},
};
use tikv_util::info;

/// A registry that maps store to a group.
///
/// A group ID is generated according to the label value. Stores with same
/// label value will be mapped to the same group ID.
#[derive(Default, Debug)]
pub struct StoreGroup {
    labels: HashMap<u64, Vec<metapb::StoreLabel>>,
    label_ids: HashMap<String, u64>,
    stores: HashMap<u64, u64>,
    label_key: String,
    version: u64,
    dirty: bool,
}

impl StoreGroup {
    /// Backs store labels.
    ///
    /// When using majority mode, labels still need to be backup for future
    /// usage. Should only call it in majority mode.
    pub fn backup_store_labels(&mut self, store: &mut metapb::Store) {
        if self
            .labels
            .get(&store.id)
            .map_or(false, |l| store.get_labels() == &**l)
        {
            return;
        }

        let labels = store.take_labels();
        info!("backup store labels"; "store_id" => store.id, "labels" => ?labels);
        self.labels.insert(store.id, labels.into());
        self.dirty = true;
    }

    /// Registers the store with given label value.
    ///
    /// # Panics
    ///
    /// Panics if the store is registered twice with different store ID.
    pub fn register_store(
        &mut self,
        store_id: u64,
        labels: Vec<metapb::StoreLabel>,
    ) -> Option<u64> {
        info!("associated store labels"; "store_id" => store_id, "labels" => ?labels);
        let key = &self.label_key;
        match self.stores.entry(store_id) {
            HashMapEntry::Occupied(o) => {
                if let Some(l) = labels.iter().find(|l| l.key == *key) {
                    assert_eq!(
                        self.label_ids.get(&l.value),
                        Some(o.get()),
                        "store {:?}",
                        store_id
                    );
                } else {
                    panic!("label {} not found in store {} {:?}", key, store_id, labels);
                }
                Some(*o.get())
            }
            HashMapEntry::Vacant(v) => {
                let group_id = {
                    if let Some(l) = labels.iter().find(|l| l.key == *key) {
                        let id = self.label_ids.len() as u64 + 1;
                        if let Some(id) = self.label_ids.get(&l.value) {
                            Some(*id)
                        } else {
                            self.label_ids.insert(l.value.clone(), id);
                            Some(id)
                        }
                    } else {
                        None
                    }
                };
                self.labels.insert(store_id, labels);
                if let Some(group_id) = group_id {
                    v.insert(group_id);
                }
                group_id
            }
        }
    }

    /// Gets the group ID of store.
    ///
    /// Different version may indicates different label key. If version is less than
    /// recorded one, then label key has to be changed, new value can't be mixed with
    /// old values, so `None` is returned. If version is larger, then label key must
    /// still matches. Because `recalculate` is called before updating regions'
    /// replication status, so unchanged recorded version means unchanged label key.
    #[inline]
    pub fn group_id(&self, version: u64, store_id: u64) -> Option<u64> {
        if version < self.version {
            None
        } else {
            self.stores.get(&store_id).cloned()
        }
    }

    /// Clears id caches and recalculates if necessary.
    ///
    /// If label key is not changed, the function is no-op.
    fn recalculate(&mut self, status: &ReplicationStatus) {
        if status.get_mode() == ReplicationMode::Majority {
            return;
        }
        if status.get_dr_auto_sync().label_key == self.label_key && !self.dirty {
            return;
        }
        self.dirty = false;
        let state_id = status.get_dr_auto_sync().state_id;
        if state_id <= self.version {
            // It should be checked by caller.
            panic!(
                "invalid state id detected: {} <= {}",
                state_id, self.version
            );
        }
        self.stores.clear();
        self.label_ids.clear();
        self.label_key = status.get_dr_auto_sync().label_key.clone();
        self.version = state_id;
        for (store_id, labels) in &self.labels {
            if let Some(l) = labels.iter().find(|l| l.key == self.label_key) {
                let mut group_id = self.label_ids.len() as u64 + 1;
                if let Some(id) = self.label_ids.get(&l.value) {
                    group_id = *id;
                } else {
                    self.label_ids.insert(l.value.clone(), group_id);
                }
                self.stores.insert(*store_id, group_id);
            }
        }
    }
}

/// A global state that stores current replication mode and related metadata.
#[derive(Default, Debug)]
pub struct GlobalReplicationState {
    status: ReplicationStatus,
    pub group: StoreGroup,
    group_buffer: Vec<(u64, u64)>,
}

impl GlobalReplicationState {
    pub fn status(&self) -> &ReplicationStatus {
        &self.status
    }

    pub fn set_status(&mut self, status: ReplicationStatus) {
        self.status = status;
        self.group.recalculate(&self.status);
    }

    pub fn calculate_commit_group(
        &mut self,
        version: u64,
        peers: &[metapb::Peer],
    ) -> &mut Vec<(u64, u64)> {
        self.group_buffer.clear();
        for p in peers {
            if let Some(group_id) = self.group.group_id(version, p.store_id) {
                self.group_buffer.push((p.id, group_id));
            }
        }
        &mut self.group_buffer
    }

    pub fn store_dr_autosync_status(&self) -> Option<StoreDrAutoSyncStatus> {
        match self.status.get_mode() {
            ReplicationMode::DrAutoSync => {
                let mut s = StoreDrAutoSyncStatus::new();
                s.set_state(self.status.get_dr_auto_sync().get_state());
                s.set_state_id(self.status.get_dr_auto_sync().get_state_id());
                Some(s)
            }
            ReplicationMode::Majority => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic;

    use kvproto::{
        metapb,
        replication_modepb::{ReplicationMode, ReplicationStatus},
    };

    use super::*;
    use crate::store::util::new_peer;

    fn new_label(key: &str, value: &str) -> metapb::StoreLabel {
        metapb::StoreLabel {
            key: key.to_owned(),
            value: value.to_owned(),
            ..Default::default()
        }
    }

    fn new_store(id: u64, labels: Vec<metapb::StoreLabel>) -> metapb::Store {
        let mut store = metapb::Store {
            id,
            ..Default::default()
        };
        store.set_labels(labels.into());
        store
    }

    fn new_status(state_id: u64, key: &str) -> ReplicationStatus {
        let mut status = ReplicationStatus::default();
        status.set_mode(ReplicationMode::DrAutoSync);
        status.mut_dr_auto_sync().state_id = state_id;
        status.mut_dr_auto_sync().label_key = key.to_owned();
        status
    }

    #[test]
    fn test_group_register() {
        let mut state = GlobalReplicationState::default();
        let label1 = new_label("zone", "label 1");
        assert_eq!(None, state.group.register_store(1, vec![label1.clone()]));
        let label2 = new_label("zone", "label 2");
        assert_eq!(None, state.group.register_store(2, vec![label2.clone()]));
        assert_eq!(state.group.group_id(0, 2), None);
        let label3 = new_label("host", "label 3");
        let label4 = new_label("host", "label 4");
        state.group.register_store(3, vec![label1.clone(), label3]);
        state.group.register_store(4, vec![label2, label4.clone()]);
        for i in 1..=4 {
            assert_eq!(None, state.group.group_id(0, i));
        }

        let mut status = new_status(1, "zone");
        state.group.recalculate(&status);
        assert_eq!(Some(2), state.group.group_id(1, 1));
        assert_eq!(Some(1), state.group.group_id(1, 2));
        assert_eq!(Some(2), state.group.group_id(1, 3));
        assert_eq!(Some(1), state.group.group_id(1, 4));
        assert_eq!(None, state.group.group_id(0, 1));
        assert_eq!(Some(2), state.group.register_store(5, vec![label1, label4]));
        assert_eq!(
            Some(3),
            state
                .group
                .register_store(6, vec![new_label("zone", "label 5")])
        );

        let gb = state.calculate_commit_group(1, &[new_peer(1, 1), new_peer(2, 2), new_peer(3, 3)]);
        assert_eq!(*gb, vec![(1, 2), (2, 1), (3, 2)]);

        // Switches back to majority will not clear group id.
        status = ReplicationStatus::default();
        state.set_status(status);
        let gb = state.calculate_commit_group(1, &[new_peer(1, 1), new_peer(2, 2), new_peer(3, 3)]);
        assert_eq!(*gb, vec![(1, 2), (2, 1), (3, 2)]);

        status = new_status(3, "zone");
        state.set_status(status);
        let gb = state.calculate_commit_group(3, &[new_peer(1, 1), new_peer(2, 2), new_peer(3, 3)]);
        assert_eq!(*gb, vec![(1, 2), (2, 1), (3, 2)]);

        status = new_status(4, "host");
        state.set_status(status);
        let gb = state.calculate_commit_group(4, &[new_peer(1, 1), new_peer(2, 2), new_peer(3, 3)]);
        assert_eq!(*gb, vec![(3, 2)]);
    }

    fn validate_state(state: &GlobalReplicationState, version: u64, expected: &[u64]) {
        for (i, exp) in expected.iter().enumerate() {
            let exp = if *exp != 0 { Some(*exp) } else { None };
            assert_eq!(exp, state.group.group_id(version, i as u64 + 1), "{}", i);
        }
    }

    #[test]
    fn test_backup_store_labels() {
        let mut state = GlobalReplicationState::default();
        let label1 = new_label("zone", "label 1");
        let mut store = new_store(1, vec![label1.clone()]);
        state.group.backup_store_labels(&mut store);
        let label2 = new_label("zone", "label 2");
        store = new_store(2, vec![label2.clone()]);
        state.group.backup_store_labels(&mut store);
        let label3 = new_label("host", "label 3");
        let label4 = new_label("host", "label 4");
        store = new_store(3, vec![label1.clone(), label3.clone()]);
        state.group.backup_store_labels(&mut store);
        store = new_store(4, vec![label2.clone(), label4.clone()]);
        state.group.backup_store_labels(&mut store);
        for i in 1..=4 {
            assert_eq!(None, state.group.group_id(0, i));
        }
        let mut status = new_status(1, "zone");
        state.group.recalculate(&status);
        let mut expected = vec![2, 1, 2, 1];
        validate_state(&state, 1, &expected);

        // Backup store will not assign id immediately.
        store = new_store(5, vec![label2, label4.clone()]);
        state.group.backup_store_labels(&mut store);
        assert_eq!(None, state.group.group_id(0, 5));
        status = new_status(2, "zone");
        state.group.recalculate(&status);
        // Even though key is not changed, the backup store should be recalculated.
        expected.push(1);
        validate_state(&state, 2, &expected);

        status = new_status(3, "host");
        state.group.recalculate(&status);
        expected = vec![0, 0, 2, 1, 1];
        validate_state(&state, 3, &expected);

        // If a store has no group id, it can still updates the labels.
        assert_eq!(
            Some(1),
            state.group.register_store(1, vec![label1.clone(), label4])
        );
        // But a calculated group id can't be changed.
        let res = panic_hook::recover_safe(move || {
            state
                .group
                .register_store(1, vec![label1.clone(), label3.clone()])
        });
        assert!(res.is_err(), "existing group id can't be changed.");
    }
}
