// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering;

use engine_traits::RaftEngineDebug;

use super::{
    common::*, mock_core::*, mock_engine_store_server::into_engine_store_server_wrap, mock_ffi::*,
};
use crate::mock_cluster;

pub(crate) unsafe extern "C" fn ffi_query_fap_snapshot_state(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
    _peer_id: u64,
    index: u64,
    term: u64,
) -> interfaces_ffi::FapSnapshotState {
    let store = into_engine_store_server_wrap(arg1);
    match (*store.engine_store_server).tmp_fap_regions.get(&region_id) {
        Some(e) => {
            if index == 0 && term == 0 {
                debug!("ffi_query_fap_snapshot_state: found unchecked snapshot";
                    "region_id" => region_id,
                    "index" => index,
                    "term" => term,
                );
                interfaces_ffi::FapSnapshotState::Persisted
            } else if e.apply_state.get_applied_index() == index && e.applied_term == term {
                debug!("ffi_query_fap_snapshot_state: found matched snapshot";
                    "region_id" => region_id,
                    "index" => index,
                    "term" => term,
                );
                interfaces_ffi::FapSnapshotState::Persisted
            } else {
                debug!("ffi_query_fap_snapshot_state: mismatch snapshot";
                    "region_id" => region_id,
                    "index" => index,
                    "term" => term,
                    "actual_index" => e.apply_state.get_applied_index(),
                    "actual_term" => e.applied_term
                );
                interfaces_ffi::FapSnapshotState::NotFound
            }
        }
        None => interfaces_ffi::FapSnapshotState::NotFound,
    }
}

pub(crate) unsafe extern "C" fn ffi_kvstore_region_exists(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
) -> bool {
    let store = into_engine_store_server_wrap(arg1);
    let res = (*store.engine_store_server)
        .kvstore
        .contains_key(&region_id);
    debug!("ffi_kvstore_region_exists {} {}", region_id, res);
    res
}

pub(crate) unsafe extern "C" fn ffi_clear_fap_snapshot(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    debug!("ffi_clear_fap_snapshot clean";
        "region_id" => region_id
    );
    (*store.engine_store_server)
        .tmp_fap_regions
        .remove(&region_id);
}

pub(crate) unsafe extern "C" fn ffi_apply_fap_snapshot(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
    peer_id: u64,
    assert_exist: u8,
    _index: u64,
    _term: u64,
) -> u8 {
    let store = into_engine_store_server_wrap(arg1);
    let new_region = match (*store.engine_store_server)
        .tmp_fap_regions
        .remove(&region_id)
    {
        Some(e) => e,
        None => {
            info!("not a fap snapshot";
                "region_id" => region_id,
                "peer_id" => peer_id,
                "assert_exist" => assert_exist,
            );
            if assert_exist != 0 {
                panic!("should exist region_id={} peed_id={}", region_id, peer_id);
            }
            return 0;
        }
    };
    (*store.engine_store_server)
        .kvstore
        .insert(region_id, new_region);
    let target_region = (*store.engine_store_server)
        .kvstore
        .get_mut(&region_id)
        .unwrap();
    crate::write_snapshot_to_db_data(
        &mut (*store.engine_store_server),
        target_region,
        String::from("fast-add-peer"),
    );
    1
}

#[allow(clippy::redundant_closure_call)]
pub(crate) unsafe extern "C" fn ffi_fast_add_peer(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
    new_peer_id: u64,
) -> interfaces_ffi::FastAddPeerRes {
    let store = into_engine_store_server_wrap(arg1);
    let cluster_ext = &*(store.cluster_ext_ptr as *const mock_cluster::ClusterExt);
    let store_id = (*store.engine_store_server).id;
    (*store.engine_store_server).mutate_region_states(region_id, |e: &mut RegionStats| {
        e.fast_add_peer_count.fetch_add(1, Ordering::SeqCst);
        e.started_fast_add_peers.lock().unwrap().insert(region_id);
    });

    let failed_add_peer_res =
        |status: interfaces_ffi::FastAddPeerStatus| interfaces_ffi::FastAddPeerRes {
            status,
            apply_state: create_cpp_str(None),
            region: create_cpp_str(None),
        };
    let from_store = (|| {
        fail::fail_point!("fap_mock_add_peer_from_id", |t| {
            let t = t.unwrap().parse::<u64>().unwrap();
            t
        });
        1
    })();
    let block_wait: bool = (|| {
        fail::fail_point!("fap_mock_block_wait", |t| {
            let t = t.unwrap().parse::<u64>().unwrap();
            t
        });
        0
    })() != 0;
    let force_wait_for_data: bool = (|| {
        fail::fail_point!("fap_mock_force_wait_for_data", |t| {
            let t = t.unwrap().parse::<u64>().unwrap();
            t
        });
        0
    })() != 0;
    let fail_after_write: bool = (|| {
        fail::fail_point!("fap_mock_fail_after_write", |t| {
            let t = t.unwrap().parse::<u64>().unwrap();
            t
        });
        0
    })() != 0;
    debug!("recover from remote peer: enter from {} to {}", from_store, store_id; "region_id" => region_id);

    if force_wait_for_data {
        debug!("recover from remote peer: force_wait_for_data from {} to {}", from_store, store_id; "region_id" => region_id);
        return failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::WaitForData);
    }
    for retry in 0..300 {
        let mut ret: Option<interfaces_ffi::FastAddPeerRes> = None;
        if retry > 0 {
            std::thread::sleep(std::time::Duration::from_millis(30));
        }
        cluster_ext.access_ffi_helpers(&mut |guard: &mut HashMap<u64, crate::mock_cluster::FFIHelperSet>| {
            debug!("recover from remote peer: preparing from {} to {}, persist and check source", from_store, store_id; "region_id" => region_id);
            let source_server = match guard.get_mut(&from_store) {
                Some(s) => &mut s.engine_store_server,
                None => {
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::NoSuitable));
                    return;
                }
            };
            let source_engines = match source_server.engines.clone() {
                Some(s) => s,
                None => {
                    error!("recover from remote peer: failed get source engine"; "region_id" => region_id);
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData));
                    return
                }
            };
            // TODO We must ask the remote peer to persist before get a snapshot.
            let source_region = match source_server.kvstore.get(&region_id) {
                Some(s) => s,
                None => {
                    error!("recover from remote peer: failed read source region info"; "region_id" => region_id);
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData));
                    return;
                }
            };
            let region_local_state: RegionLocalState = match general_get_region_local_state(
                &source_engines.kv,
                region_id,
            ) {
                Some(x) => x,
                None => {
                    debug!("recover from remote peer: preparing from {} to {}:{}, not region state", from_store, store_id, new_peer_id; "region_id" => region_id);
                    // We don't return BadData here, since the data may not be persisted.
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::WaitForData));
                    return;
                }
            };
            let new_region_meta = region_local_state.get_region();
            let peer_state = region_local_state.get_state();
            // Validation
            match peer_state {
                PeerState::Tombstone | PeerState::Applying => {
                    // Note in real implementation, we will avoid selecting this peer.
                    error!("recover from remote peer: preparing from {} to {}:{}, error peer state {:?}", from_store, store_id, new_peer_id, peer_state; "region_id" => region_id);
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData));
                    return;
                }
                _ => {
                    info!("recover from remote peer: preparing from {} to {}:{}, ok peer state {:?}", from_store, store_id, new_peer_id, peer_state; "region_id" => region_id);
                }
            };
            if !engine_store_ffi::core::validate_remote_peer_region(
                new_region_meta,
                store_id,
                new_peer_id,
            ) {
                debug!("recover from remote peer: preparing from {} to {}, failed for applied conf change for peer {} region_meta {:?}", from_store, store_id, new_peer_id, new_region_meta; "region_id" => region_id);
                ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::WaitForData));
                return;
            }

            // TODO check commit_index and applied_index here
            debug!("recover from remote peer: preparing from {} to {}, check target", from_store, store_id; "region_id" => region_id);
            let mut new_region = make_new_region(
                Some(new_region_meta.clone()),
                Some((*store.engine_store_server).id),
            );
            let target_engines = match (*store.engine_store_server).engines.clone() {
                Some(s) => s,
                None => {
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::OtherError));
                    return;
                }
            };
            debug!("recover from remote peer: meta from {} to {}", from_store, store_id; "region_id" => region_id);
            // Must first dump meta then data, otherwise data may lag behind.
            // We can see a raft log hole at applied_index otherwise.
            let apply_state: RaftApplyState = match general_get_apply_state(
                &source_engines.kv,
                region_id,
            ) {
                Some(x) => x,
                None => {
                    error!("recover from remote peer: failed read apply state"; "region_id" => region_id);
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData));
                    return;
                }
            };
            new_region.set_applied(apply_state.get_applied_index(), source_region.applied_term);
            debug!("recover from remote peer: begin data from {} to {}", from_store, store_id; 
                "region_id" => region_id,
                "apply_state" => ?apply_state,
            );
            // TODO In TiFlash we should take care of write batch size
            if let Err(e) = copy_data_from(
                &source_engines,
                &target_engines,
                &source_region,
                &mut new_region,
            ) {
                error!("recover from remote peer: inject error {:?}", e; "region_id" => region_id);
                ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::FailedInject));
                return;
            }

            if fail_after_write {
                let mut raft_wb = target_engines.raft.log_batch(1024);
                let mut entries: Vec<raft::eraftpb::Entry> = Default::default();
                target_engines
                    .raft
                    .get_all_entries_to(region_id, &mut entries)
                    .unwrap();
                let l = entries.len();
                // Manually delete one raft log
                // let from = entries.get(l - 2).unwrap().get_index();
                let from = 7;
                let to = entries.get(l - 1).unwrap().get_index() + 1;
                debug!("recover from remote peer: simulate error from {} to {}", from_store, store_id;
                    "region_id" => region_id,
                    "from" => from,
                    "to" => to,
                );
                // raft_wb.cut_logs(region_id, from, to);
                target_engines.raft.gc(region_id, from, to, &mut raft_wb).unwrap();
                target_engines.raft.consume(&mut raft_wb, true).unwrap();
            }
            let apply_state_bytes = apply_state.write_to_bytes().unwrap();
            let region_bytes = region_local_state.get_region().write_to_bytes().unwrap();
            let apply_state_ptr = create_cpp_str(Some(apply_state_bytes));
            let region_ptr = create_cpp_str(Some(region_bytes));

            (*store.engine_store_server).tmp_fap_regions.insert(new_region_meta.get_id(), Box::new(new_region));
            // Check if we have commit_index.
            debug!("recover from remote peer: ok from {} to {}", from_store, store_id; "region_id" => region_id);

            ret = Some(interfaces_ffi::FastAddPeerRes {
                status: interfaces_ffi::FastAddPeerStatus::Ok,
                apply_state: apply_state_ptr,
                region: region_ptr,
            });
        });
        if let Some(r) = ret {
            match r.status {
                interfaces_ffi::FastAddPeerStatus::WaitForData => {
                    if block_wait {
                        continue;
                    } else {
                        (*store.engine_store_server).mutate_region_states(
                            region_id,
                            |e: &mut RegionStats| {
                                e.finished_fast_add_peer_count
                                    .fetch_add(1, Ordering::SeqCst);
                            },
                        );
                        return r;
                    }
                }
                _ => {
                    (*store.engine_store_server).mutate_region_states(
                        region_id,
                        |e: &mut RegionStats| {
                            e.finished_fast_add_peer_count
                                .fetch_add(1, Ordering::SeqCst);
                        },
                    );
                    return r;
                }
            }
        }
    }
    error!("recover from remote peer: failed after retry"; "region_id" => region_id);
    (*store.engine_store_server).mutate_region_states(region_id, |e: &mut RegionStats| {
        e.finished_fast_add_peer_count
            .fetch_add(1, Ordering::SeqCst);
    });
    failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData)
}
