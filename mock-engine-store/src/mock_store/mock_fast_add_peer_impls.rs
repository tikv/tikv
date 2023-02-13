// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::Ordering;

use super::{
    common::*, mock_core::*, mock_engine_store_server::into_engine_store_server_wrap, mock_ffi::*,
};
use crate::{mock_cluster, node::NodeCluster};

#[allow(clippy::redundant_closure_call)]
pub(crate) unsafe extern "C" fn ffi_fast_add_peer(
    arg1: *mut interfaces_ffi::EngineStoreServerWrap,
    region_id: u64,
    new_peer_id: u64,
) -> interfaces_ffi::FastAddPeerRes {
    let store = into_engine_store_server_wrap(arg1);
    let cluster = &*(store.cluster_ptr as *const mock_cluster::Cluster<NodeCluster>);
    let store_id = (*store.engine_store_server).id;
    (*store.engine_store_server).mutate_region_states(region_id, |e: &mut RegionStats| {
        e.fast_add_peer_count.fetch_add(1, Ordering::SeqCst);
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
    let fail_after_write: bool = (|| {
        fail::fail_point!("fap_mock_fail_after_write", |t| {
            let t = t.unwrap().parse::<u64>().unwrap();
            t
        });
        0
    })() != 0;
    debug!("recover from remote peer: enter from {} to {}", from_store, store_id; "region_id" => region_id);

    for retry in 0..300 {
        let mut ret: Option<interfaces_ffi::FastAddPeerRes> = None;
        if retry > 0 {
            std::thread::sleep(std::time::Duration::from_millis(30));
        }
        cluster.access_ffi_helpers(&mut |guard: &mut HashMap<u64, crate::mock_cluster::FFIHelperSet>| {
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
                debug!("recover from remote peer: preparing from {} to {}, not applied conf change {}", from_store, store_id, new_peer_id; "region_id" => region_id);
                ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::WaitForData));
                return;
            }
            // TODO check commit_index and applied_index here
            debug!("recover from remote peer: preparing from {} to {}, check target", from_store, store_id; "region_id" => region_id);
            let new_region = make_new_region(
                Some(new_region_meta.clone()),
                Some((*store.engine_store_server).id),
            );
            (*store.engine_store_server)
                .kvstore
                .insert(region_id, Box::new(new_region));
            let target_engines = match (*store.engine_store_server).engines.clone() {
                Some(s) => s,
                None => {
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::OtherError));
                    return;
                }
            };
            let target_region = match (*store.engine_store_server).kvstore.get_mut(&region_id) {
                Some(s) => s,
                None => {
                    ret = Some(failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData));
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
            debug!("recover from remote peer: begin data from {} to {}", from_store, store_id; 
                "region_id" => region_id,
                "apply_state" => ?apply_state,
            );
            // TODO In TiFlash we should take care of write batch size
            if let Err(e) = copy_data_from(
                &source_engines,
                &target_engines,
                &source_region,
                target_region,
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
                        return r;
                    }
                }
                _ => return r,
            }
        }
    }
    error!("recover from remote peer: failed after retry"; "region_id" => region_id);
    failed_add_peer_res(interfaces_ffi::FastAddPeerStatus::BadData)
}
