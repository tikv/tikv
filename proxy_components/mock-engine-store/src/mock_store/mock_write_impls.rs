// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use super::{
    common::*,
    mock_core::*,
    mock_engine_store_server::{
        into_engine_store_server_wrap, move_data_from, write_to_db_data, EngineStoreServer,
        EngineStoreServerWrap,
    },
};

pub unsafe extern "C" fn ffi_handle_admin_raft_cmd(
    arg1: *const interfaces_ffi::EngineStoreServerWrap,
    arg2: interfaces_ffi::BaseBuffView,
    arg3: interfaces_ffi::BaseBuffView,
    arg4: interfaces_ffi::RaftCmdHeader,
) -> interfaces_ffi::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    let mut req = kvproto::raft_cmdpb::AdminRequest::default();
    let mut resp = kvproto::raft_cmdpb::AdminResponse::default();
    req.merge_from_bytes(arg2.to_slice()).unwrap();
    resp.merge_from_bytes(arg3.to_slice()).unwrap();
    store.handle_admin_raft_cmd(&req, &resp, arg4)
}

pub unsafe extern "C" fn ffi_handle_write_raft_cmd(
    arg1: *const interfaces_ffi::EngineStoreServerWrap,
    arg2: interfaces_ffi::WriteCmdsView,
    arg3: interfaces_ffi::RaftCmdHeader,
) -> interfaces_ffi::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    store.handle_write_raft_cmd(arg2, arg3)
}

impl EngineStoreServerWrap {
    unsafe fn handle_admin_raft_cmd(
        &mut self,
        req: &kvproto::raft_cmdpb::AdminRequest,
        resp: &kvproto::raft_cmdpb::AdminResponse,
        header: interfaces_ffi::RaftCmdHeader,
    ) -> interfaces_ffi::EngineStoreApplyRes {
        let region_id = header.region_id;
        let node_id = (*self.engine_store_server).id;
        info!("handle_admin_raft_cmd";
            "node_id"=>node_id,
            "request"=>?req,
            "response"=>?resp,
            "header"=>?header,
            "region_id"=>header.region_id,
        );
        let do_handle_admin_raft_cmd =
            move |region: &mut Box<MockRegion>, engine_store_server: &mut EngineStoreServer| {
                if region.apply_state.get_applied_index() >= header.index {
                    // If it is a old entry.
                    error!("obsolete admin index";
                    "apply_state"=>?region.apply_state,
                    "header"=>?header,
                    "node_id"=>node_id,
                    );
                    panic!("observe obsolete admin index");
                    // return interfaces_ffi::EngineStoreApplyRes::None;
                }
                match req.get_cmd_type() {
                    AdminCmdType::ChangePeer | AdminCmdType::ChangePeerV2 => {
                        let new_region_meta = resp.get_change_peer().get_region();
                        let old_peer_id = {
                            let old_region =
                                engine_store_server.kvstore.get_mut(&region_id).unwrap();
                            old_region.region = new_region_meta.clone();
                            region.set_applied(header.index, header.term);
                            old_region.peer.get_store_id()
                        };

                        let mut do_remove = true;
                        if old_peer_id != 0 {
                            for peer in new_region_meta.get_peers().iter() {
                                if peer.get_store_id() == old_peer_id {
                                    // Should not remove region
                                    do_remove = false;
                                }
                            }
                        } else {
                            // If old_peer_id is 0, seems old_region.peer is not set, just neglect
                            // for convenience.
                            do_remove = false;
                        }
                        if do_remove {
                            let removed = engine_store_server.kvstore.remove(&region_id);
                            // We need to also remove apply state, thus we need to know peer_id
                            debug!(
                                "Remove region {:?} peer_id {} at node {}, for new meta {:?}",
                                removed.unwrap().region,
                                old_peer_id,
                                node_id,
                                new_region_meta
                            );
                        }
                    }
                    AdminCmdType::BatchSplit => {
                        let regions = resp.get_splits().regions.as_ref();

                        for i in 0..regions.len() {
                            let region_meta = regions.get(i).unwrap();
                            if region_meta.id == region_id {
                                // This is the derived region
                                debug!(
                                    "region {} is derived by split at peer {} with meta {:?}",
                                    region_meta.id, node_id, region_meta
                                );
                                assert!(engine_store_server.kvstore.contains_key(&region_meta.id));
                                engine_store_server
                                    .kvstore
                                    .get_mut(&region_meta.id)
                                    .unwrap()
                                    .region = region_meta.clone();
                            } else {
                                // Should split data into new region
                                debug!(
                                    "new region {} generated by split at peer {} with meta {:?}",
                                    region_meta.id, node_id, region_meta
                                );
                                let new_region =
                                    make_new_region(Some(region_meta.clone()), Some(node_id));

                                // No need to split data because all KV are stored in the same
                                // RocksDB. TODO But we still need
                                // to clean all in-memory data.
                                // We can't assert `region_meta.id` is brand new here
                                engine_store_server
                                    .kvstore
                                    .insert(region_meta.id, Box::new(new_region));
                            }
                        }

                        {
                            // Move data
                            let region_ids =
                                regions.iter().map(|r| r.get_id()).collect::<Vec<u64>>();
                            move_data_from(engine_store_server, region_id, region_ids.as_slice());
                        }
                    }
                    AdminCmdType::PrepareMerge => {
                        let tikv_region = resp.get_split().get_left();

                        let _target = req.prepare_merge.as_ref().unwrap().target.as_ref();
                        let region_meta = &mut (engine_store_server
                            .kvstore
                            .get_mut(&region_id)
                            .unwrap()
                            .region);

                        // Increase self region conf version and version
                        let region_epoch = region_meta.region_epoch.as_mut().unwrap();
                        let new_version = region_epoch.version + 1;
                        region_epoch.set_version(new_version);
                        // TODO this check may fail
                        // assert_eq!(tikv_region.get_region_epoch().get_version(), new_version);
                        let conf_version = region_epoch.conf_ver + 1;
                        region_epoch.set_conf_ver(conf_version);
                        assert_eq!(tikv_region.get_region_epoch().get_conf_ver(), conf_version);

                        {
                            let region = engine_store_server.kvstore.get_mut(&region_id).unwrap();
                            region.set_applied(header.index, header.term);
                        }
                        // We don't handle MergeState and PeerState here
                    }
                    AdminCmdType::CommitMerge => {
                        fail::fail_point!("ffi_before_commit_merge", |_| {
                            return interfaces_ffi::EngineStoreApplyRes::Persist;
                        });
                        let (target_id, source_id) =
                            { (region_id, req.get_commit_merge().get_source().get_id()) };
                        {
                            let target_region =
                                &mut (engine_store_server.kvstore.get_mut(&region_id).unwrap());

                            let target_region_meta = &mut target_region.region;
                            let target_version =
                                target_region_meta.get_region_epoch().get_version();
                            let source_region = req.get_commit_merge().get_source();
                            let source_version = source_region.get_region_epoch().get_version();

                            let new_version = std::cmp::max(source_version, target_version) + 1;
                            target_region_meta
                                .mut_region_epoch()
                                .set_version(new_version);
                            assert_eq!(
                                target_region_meta.get_region_epoch().get_version(),
                                new_version
                            );

                            // No need to merge data
                            let source_at_left = if source_region.get_start_key().is_empty() {
                                true
                            } else if target_region_meta.get_start_key().is_empty() {
                                false
                            } else {
                                source_region
                                    .get_end_key()
                                    .cmp(target_region_meta.get_start_key())
                                    == std::cmp::Ordering::Equal
                            };

                            // The validation of applied result on TiFlash's side.
                            let tikv_target_region_meta = resp.get_split().get_left();
                            if source_at_left {
                                target_region_meta
                                    .set_start_key(source_region.get_start_key().to_vec());
                                assert_eq!(
                                    tikv_target_region_meta.get_start_key(),
                                    target_region_meta.get_start_key()
                                );
                            } else {
                                target_region_meta
                                    .set_end_key(source_region.get_end_key().to_vec());
                                assert_eq!(
                                    tikv_target_region_meta.get_end_key(),
                                    target_region_meta.get_end_key()
                                );
                            }
                            target_region.set_applied(header.index, header.term);
                        }
                        {
                            move_data_from(engine_store_server, source_id, &[target_id]);
                        }
                        let to_remove = req.get_commit_merge().get_source().get_id();
                        engine_store_server.kvstore.remove(&to_remove);
                    }
                    AdminCmdType::RollbackMerge => {
                        let region = engine_store_server.kvstore.get_mut(&region_id).unwrap();
                        let region_meta = &mut region.region;
                        let new_version = region_meta.get_region_epoch().get_version() + 1;
                        let region_epoch = region_meta.region_epoch.as_mut().unwrap();
                        region_epoch.set_version(new_version);

                        region.set_applied(header.index, header.term);
                    }
                    AdminCmdType::CompactLog => {
                        // We can always do compact, since a executed CompactLog must follow a
                        // successful persist.
                        let region = engine_store_server.kvstore.get_mut(&region_id).unwrap();
                        let state = &mut region.apply_state;
                        let compact_index = req.get_compact_log().get_compact_index();
                        let compact_term = req.get_compact_log().get_compact_term();
                        state.mut_truncated_state().set_index(compact_index);
                        state.mut_truncated_state().set_term(compact_term);
                        region.set_applied(header.index, header.term);
                    }
                    _ => {
                        region.set_applied(header.index, header.term);
                    }
                }
                // Do persist or not
                match req.get_cmd_type() {
                    AdminCmdType::CompactLog => {
                        fail::fail_point!("no_persist_compact_log", |_| {
                            // Persist data, but don't persist meta.
                            interfaces_ffi::EngineStoreApplyRes::None
                        });
                        interfaces_ffi::EngineStoreApplyRes::Persist
                    }
                    AdminCmdType::PrepareFlashback | AdminCmdType::FinishFlashback => {
                        fail::fail_point!("no_persist_flashback", |_| {
                            // Persist data, but don't persist meta.
                            interfaces_ffi::EngineStoreApplyRes::None
                        });
                        interfaces_ffi::EngineStoreApplyRes::Persist
                    }
                    _ => interfaces_ffi::EngineStoreApplyRes::Persist,
                }
            };

        let res = match (*self.engine_store_server).kvstore.entry(region_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                do_handle_admin_raft_cmd(o.get_mut(), &mut (*self.engine_store_server))
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                // Currently in tests, we don't handle commands like BatchSplit,
                // and sometimes we don't bootstrap region 1,
                // so it is normal if we find no region.
                warn!("region {} not found, create for {}", region_id, node_id);
                let new_region = v.insert(Default::default());
                assert!((*self.engine_store_server).kvstore.contains_key(&region_id));
                do_handle_admin_raft_cmd(new_region, &mut (*self.engine_store_server))
            }
        };

        let region = match (*self.engine_store_server).kvstore.get_mut(&region_id) {
            Some(r) => Some(r),
            None => {
                warn!(
                    "still can't find region {} for {}, may be remove due to confchange",
                    region_id, node_id
                );
                None
            }
        };
        if res == interfaces_ffi::EngineStoreApplyRes::Persist {
            // Persist tells ApplyDelegate to do a commit.
            // So we also need a persist of actual data on engine-store' side.
            if let Some(region) = region {
                if req.get_cmd_type() == AdminCmdType::CompactLog {
                    // We already persist when fn_try_flush_data.
                } else {
                    write_to_db_data(
                        &mut (*self.engine_store_server),
                        region,
                        format!("admin {:?}", req),
                    );
                }
            }
        }
        res
    }

    unsafe fn handle_write_raft_cmd(
        &mut self,
        cmds: interfaces_ffi::WriteCmdsView,
        header: interfaces_ffi::RaftCmdHeader,
    ) -> interfaces_ffi::EngineStoreApplyRes {
        let region_id = header.region_id;
        let server = &mut (*self.engine_store_server);
        let node_id = (*self.engine_store_server).id;
        let _kv = &mut (*self.engine_store_server).engines.as_mut().unwrap().kv;
        let proxy_compat = server.mock_cfg.proxy_compat;
        let mut do_handle_write_raft_cmd = move |region: &mut Box<MockRegion>| {
            if region.apply_state.get_applied_index() >= header.index {
                debug!("obsolete write index";
                "apply_state"=>?region.apply_state,
                "header"=>?header,
                "node_id"=>node_id,
                );
                panic!("observe obsolete write index");
                // return interfaces_ffi::EngineStoreApplyRes::None;
            }
            for i in 0..cmds.len {
                let key = &*cmds.keys.add(i as _);
                let val = &*cmds.vals.add(i as _);
                let k = &key.to_slice();
                let v = &val.to_slice();
                let tp = &*cmds.cmd_types.add(i as _);
                let cf = &*cmds.cmd_cf.add(i as _);
                let cf_index = (*cf) as u8;
                debug!(
                    "handle_write_raft_cmd with kv";
                    "k" => ?&k[..std::cmp::min(4usize, k.len())],
                    "v" => ?&v[..std::cmp::min(4usize, v.len())],
                    "region_id" => region_id,
                    "node_id" => server.id,
                    "header" => ?header,
                );
                let _data = &mut region.data[cf_index as usize];
                match tp {
                    interfaces_ffi::WriteCmdType::Put => {
                        write_kv_in_mem(region.as_mut(), cf_index as usize, k, v);
                    }
                    interfaces_ffi::WriteCmdType::Del => {
                        delete_kv_in_mem(region.as_mut(), cf_index as usize, k);
                    }
                }
            }
            // Advance apply index, but do not persist
            region.set_applied(header.index, header.term);
            if !proxy_compat {
                // If we don't support new proxy, we persist everytime.
                write_to_db_data(server, region, "write".to_string());
            }
            interfaces_ffi::EngineStoreApplyRes::None
        };

        match (*self.engine_store_server).kvstore.entry(region_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                do_handle_write_raft_cmd(o.get_mut())
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                warn!("region {} not found", region_id);
                do_handle_write_raft_cmd(v.insert(Default::default()))
            }
        }
    }
}
