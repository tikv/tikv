// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb;
use kvproto::replication_modepb::ReplicationStatus;
use tikv_util::collections::{HashMap, HashMapEntry};

/// A registry that maps store to a group.
///
/// A group ID is generated according to the label value. Stores with same
/// label value will be mapped to the same group ID.
#[derive(Default, Debug)]
pub struct StoreGroup {
    labels: HashMap<String, u64>,
    stores: HashMap<u64, u64>,
}

impl StoreGroup {
    /// Registers the store with given label value.
    ///
    /// # Panics
    ///
    /// Panics if the store is registered twice with different store ID.
    pub fn register_store(&mut self, store_id: u64, label: String) -> u64 {
        debug!("associated {} {}", store_id, label);
        match self.stores.entry(store_id) {
            HashMapEntry::Occupied(o) => {
                assert_eq!(
                    self.labels.get(&label),
                    Some(o.get()),
                    "store {:?}",
                    store_id
                );
                *o.get()
            }
            HashMapEntry::Vacant(v) => {
                let len = self.labels.len() as u64 + 1;
                let id = *self.labels.entry(label).or_insert(len);
                v.insert(id);
                id
            }
        }
    }

    /// Gets the group ID of store.
    #[inline]
    pub fn group_id(&self, store_id: u64) -> Option<u64> {
        self.stores.get(&store_id).cloned()
    }
}

/// A global state that stores current replication mode and related metadata.
#[derive(Default, Debug)]
pub struct GlobalReplicationState {
    pub status: ReplicationStatus,
    pub group: StoreGroup,
    pub group_buffer: Vec<(u64, u64)>,
}

impl GlobalReplicationState {
    pub fn calculate_commit_group(&mut self, peers: &[metapb::Peer]) {
        self.group_buffer.clear();
        for p in peers {
            if let Some(label_id) = self.group.group_id(p.store_id) {
                self.group_buffer.push((p.id, label_id));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::util::new_peer;

    #[test]
    fn test_group_register() {
        let mut state = GlobalReplicationState::default();
        let group_id = state.group.register_store(1, "label 1".to_owned());
        assert_eq!(state.group.group_id(1), Some(group_id));
        assert_eq!(
            state.group.register_store(2, "label 1".to_owned()),
            group_id
        );
        assert_eq!(state.group.group_id(2), Some(group_id));
        assert_eq!(
            group_id,
            state.group.register_store(1, "label 1".to_owned())
        );
        assert_ne!(
            group_id,
            state.group.register_store(3, "label 2".to_owned())
        );

        state.calculate_commit_group(&[new_peer(1, 1), new_peer(2, 2), new_peer(3, 3)]);
        assert_eq!(state.group_buffer, vec![(1, 1), (2, 1), (3, 2)]);
    }
}
