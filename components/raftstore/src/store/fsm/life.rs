// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains functions that relates to peer liftime management and
//! are shared with raftstore and raftstore v2.

use engine_traits::{KvEngine, CF_RAFT};
use kvproto::raft_serverpb::{ExtraMessageType, PeerState, RaftMessage, RegionLocalState};
use tikv_util::warn;

use crate::store::util::is_epoch_stale;

/// Tell leader that `to_peer` from `tombstone_msg` is destroyed.
pub fn build_peer_destroyed_report(tombstone_msg: &mut RaftMessage) -> Option<RaftMessage> {
    let to_region_id = if tombstone_msg.has_extra_msg() {
        assert_eq!(
            tombstone_msg.get_extra_msg().get_type(),
            ExtraMessageType::MsgGcPeerRequest
        );
        tombstone_msg
            .get_extra_msg()
            .get_check_gc_peer()
            .get_from_region_id()
    } else {
        tombstone_msg.get_region_id()
    };
    if to_region_id == 0 || tombstone_msg.get_from_peer().get_id() == 0 {
        return None;
    }
    let mut msg = RaftMessage::default();
    msg.set_region_id(to_region_id);
    msg.set_from_peer(tombstone_msg.take_to_peer());
    msg.set_to_peer(tombstone_msg.take_from_peer());
    msg.mut_extra_msg()
        .set_type(ExtraMessageType::MsgGcPeerResponse);
    Some(msg)
}

/// Forward the destroy request from target peer to merged source peer.
pub fn forward_destroy_to_source_peer<T: FnOnce(RaftMessage)>(msg: &RaftMessage, forward: T) {
    let extra_msg = msg.get_extra_msg();
    // Instead of respond leader directly, send a message to target region to
    // double check it's really destroyed.
    let check_gc_peer = extra_msg.get_check_gc_peer();
    let mut tombstone_msg = RaftMessage::default();
    tombstone_msg.set_region_id(check_gc_peer.get_check_region_id());
    tombstone_msg.set_from_peer(msg.get_from_peer().clone());
    tombstone_msg.set_to_peer(check_gc_peer.get_check_peer().clone());
    tombstone_msg.set_region_epoch(check_gc_peer.get_check_region_epoch().clone());
    tombstone_msg.set_is_tombstone(true);
    // No need to set epoch as we don't know what it is.
    // This message will not be handled by `on_gc_peer_request` due to
    // `is_tombstone` being true.
    tombstone_msg
        .mut_extra_msg()
        .set_type(ExtraMessageType::MsgGcPeerRequest);
    tombstone_msg
        .mut_extra_msg()
        .mut_check_gc_peer()
        .set_from_region_id(check_gc_peer.get_from_region_id());
    forward(tombstone_msg);
}

pub fn handle_tombstone_message_on_learner<EK: KvEngine>(
    engine: &EK,
    store_id: u64,
    mut msg: RaftMessage,
) -> Option<RaftMessage> {
    let region_id = msg.get_region_id();
    let region_state_key = keys::region_state_key(region_id);
    let local_state: RegionLocalState = match engine.get_msg_cf(CF_RAFT, &region_state_key) {
        Ok(Some(s)) => s,
        e => {
            warn!(
                "[store {}] failed to get regions state of {:?}: {:?}",
                store_id,
                msg.get_region_id(),
                e
            );
            // A peer may never be created if its parent peer skips split by
            // applying a new snapshot.
            return build_peer_destroyed_report(&mut msg);
        }
    };

    if local_state.get_state() != PeerState::Tombstone {
        return None;
    }

    // In v2, we rely on leader to confirm destroy actively.
    let local_epoch = local_state.get_region().get_region_epoch();
    // The region in this peer is already destroyed
    if msg.get_region_epoch() == local_epoch || is_epoch_stale(msg.get_region_epoch(), local_epoch)
    {
        return build_peer_destroyed_report(&mut msg);
    }

    None
}
