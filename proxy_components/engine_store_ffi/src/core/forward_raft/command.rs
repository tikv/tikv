// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use encryption::DataKeyManager;
use proxy_ffi::snapshot_reader_impls::{sst_file_reader::SSTFileReader, LockCFFileReader};

use crate::core::{common::*, ProxyForwarder};

pub fn get_first_key(
    path: &str,
    cf: ColumnFamilyType,
    key_manager: Option<Arc<DataKeyManager>>,
) -> Vec<u8> {
    unsafe {
        if cf == ColumnFamilyType::Lock {
            let mut reader = LockCFFileReader::ffi_get_cf_file_reader(path, key_manager.as_ref());
            reader.as_mut_sst_lock().ffi_key().to_slice().to_vec()
        } else {
            let mut reader = SSTFileReader::ffi_get_cf_file_reader(path, key_manager);
            reader.as_mut_sst_other().ffi_key().to_slice().to_vec()
        }
    }
}

pub fn sort_sst_by_start_key(
    ssts: Vec<(PathBuf, ColumnFamilyType)>,
    key_manager: Option<Arc<DataKeyManager>>,
) -> Vec<(PathBuf, ColumnFamilyType)> {
    let mut sw: Vec<(PathBuf, ColumnFamilyType)> = vec![];
    let mut sd: Vec<(PathBuf, ColumnFamilyType)> = vec![];
    let mut sl: Vec<(PathBuf, ColumnFamilyType)> = vec![];

    for (p, c) in ssts.into_iter() {
        match c {
            ColumnFamilyType::Default => sd.push((p, c)),
            ColumnFamilyType::Write => sw.push((p, c)),
            ColumnFamilyType::Lock => sl.push((p, c)),
        };
    }

    sw.sort_by(|a, b| {
        let fk1 = get_first_key(
            a.0.to_str().unwrap(),
            ColumnFamilyType::Write,
            key_manager.clone(),
        );
        let fk2 = get_first_key(
            b.0.to_str().unwrap(),
            ColumnFamilyType::Write,
            key_manager.clone(),
        );
        fk1.cmp(&fk2)
    });
    sd.sort_by(|a, b| {
        let fk1 = get_first_key(
            a.0.to_str().unwrap(),
            ColumnFamilyType::Default,
            key_manager.clone(),
        );
        let fk2 = get_first_key(
            b.0.to_str().unwrap(),
            ColumnFamilyType::Default,
            key_manager.clone(),
        );
        fk1.cmp(&fk2)
    });
    sl.sort_by(|a, b| {
        let fk1 = get_first_key(
            a.0.to_str().unwrap(),
            ColumnFamilyType::Lock,
            key_manager.clone(),
        );
        let fk2 = get_first_key(
            b.0.to_str().unwrap(),
            ColumnFamilyType::Lock,
            key_manager.clone(),
        );
        fk1.cmp(&fk2)
    });

    sw.append(&mut sd);
    sw.append(&mut sl);
    sw
}

impl<T: Transport + 'static, ER: RaftEngine> ProxyForwarder<T, ER> {
    fn handle_ingest_sst_for_engine_store(
        &self,
        ob_region: &Region,
        ssts: &Vec<engine_traits::SstMetaInfo>,
        index: u64,
        term: u64,
    ) -> EngineStoreApplyRes {
        let mut ssts_wrap = vec![];
        let mut sst_views = vec![];

        info!("begin handle ingest sst";
            "region" => ?ob_region,
            "index" => index,
            "term" => term,
        );

        for sst in ssts {
            let sst = &sst.meta;
            if sst.get_cf_name() == engine_traits::CF_LOCK {
                panic!("should not ingest sst of lock cf");
            }

            // We still need this to filter error ssts.
            if let Err(e) = check_sst_for_ingestion(sst, ob_region) {
                error!(?e;
                 "proxy ingest fail";
                 "sst" => ?sst,
                 "region" => ?ob_region,
                );
                break;
            }

            ssts_wrap.push((
                self.sst_importer.get_path(sst),
                name_to_cf(sst.get_cf_name()),
            ));
        }

        let ssts_wrap = sort_sst_by_start_key(ssts_wrap, self.key_manager.clone());
        for (path, cf) in &ssts_wrap {
            sst_views.push((path.to_str().unwrap().as_bytes(), *cf));
        }

        self.engine_store_server_helper.handle_ingest_sst(
            sst_views,
            RaftCmdHeader::new(ob_region.get_id(), index, term),
        )
    }

    fn handle_error_apply(
        &self,
        ob_region: &Region,
        cmd: &Cmd,
        region_state: &RegionState,
    ) -> bool {
        // We still need to pass a dummy cmd, to forward updates.
        let cmd_dummy = WriteCmds::new();
        let flash_res = self.engine_store_server_helper.handle_write_raft_cmd(
            &cmd_dummy,
            RaftCmdHeader::new(ob_region.get_id(), cmd.index, cmd.term),
        );
        match flash_res {
            EngineStoreApplyRes::None => false,
            EngineStoreApplyRes::Persist => !region_state.pending_remove,
            EngineStoreApplyRes::NotFound => false,
        }
    }

    pub fn pre_exec_admin(
        &self,
        ob_region: &Region,
        req: &AdminRequest,
        index: u64,
        term: u64,
    ) -> bool {
        match req.get_cmd_type() {
            AdminCmdType::CompactLog => {
                if !self.engine_store_server_helper.try_flush_data(
                    ob_region.get_id(),
                    false,
                    false,
                    index,
                    term,
                ) {
                    info!("can't flush data, filter CompactLog";
                        "region_id" => ?ob_region.get_id(),
                        "region_epoch" => ?ob_region.get_region_epoch(),
                        "index" => index,
                        "term" => term,
                        "compact_index" => req.get_compact_log().get_compact_index(),
                        "compact_term" => req.get_compact_log().get_compact_term(),
                    );
                    return true;
                }
                // Otherwise, we can exec CompactLog, without later rolling
                // back.
            }
            AdminCmdType::ComputeHash | AdminCmdType::VerifyHash => {
                // We can't support.
                return true;
            }
            AdminCmdType::TransferLeader => {
                error!("transfer leader won't exec";
                        "region" => ?ob_region,
                        "req" => ?req,
                );
                return true;
            }
            _ => (),
        };
        false
    }

    pub fn post_exec_admin(
        &self,
        ob_region: &Region,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
        _: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        fail::fail_point!("on_post_exec_admin", |e| {
            e.unwrap().parse::<bool>().unwrap()
        });
        let region_id = ob_region.get_id();
        let request = cmd.request.get_admin_request();
        let response = &cmd.response;
        let admin_reponse = response.get_admin_response();
        let cmd_type = request.get_cmd_type();

        if response.get_header().has_error() {
            info!(
                "error occurs when apply_admin_cmd, {:?}",
                response.get_header().get_error()
            );
            return self.handle_error_apply(ob_region, cmd, region_state);
        }

        match cmd_type {
            AdminCmdType::CompactLog | AdminCmdType::ComputeHash | AdminCmdType::VerifyHash => {
                info!(
                    "observe useless admin command";
                    "region_id" => region_id,
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                    "type" => ?cmd_type,
                    "region_epoch" => ?ob_region.get_region_epoch(),
                );
            }
            _ => {
                info!(
                    "observe admin command";
                    "region_id" => region_id,
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                    "command" => ?request,
                    "region_epoch" => ?ob_region.get_region_epoch(),
                );
            }
        }

        // We wrap `modified_region` into `mut_split()`
        let mut new_response = None;
        match cmd_type {
            AdminCmdType::CommitMerge
            | AdminCmdType::PrepareMerge
            | AdminCmdType::RollbackMerge => {
                let mut r = AdminResponse::default();
                match region_state.modified_region.as_ref() {
                    Some(region) => r.mut_split().set_left(region.clone()),
                    None => {
                        error!("empty modified region";
                            "region_id" => region_id,
                            "peer_id" => region_state.peer_id,
                            "term" => cmd.term,
                            "index" => cmd.index,
                            "command" => ?request
                        );
                        panic!("empty modified region");
                    }
                }
                new_response = Some(r);
            }
            _ => (),
        }

        let flash_res = {
            match new_response {
                Some(r) => self.engine_store_server_helper.handle_admin_raft_cmd(
                    request,
                    &r,
                    RaftCmdHeader::new(region_id, cmd.index, cmd.term),
                ),
                None => self.engine_store_server_helper.handle_admin_raft_cmd(
                    request,
                    admin_reponse,
                    RaftCmdHeader::new(region_id, cmd.index, cmd.term),
                ),
            }
        };
        let persist = match flash_res {
            EngineStoreApplyRes::None => {
                if cmd_type == AdminCmdType::CompactLog {
                    // This could only happen in mock-engine-store when we perform some related
                    // tests. Formal code should never return None for
                    // CompactLog now. If CompactLog can't be done, the
                    // engine-store should return `false` in previous `try_flush_data`.
                    error!("applying CompactLog should not return None"; "region_id" => region_id,
                            "peer_id" => region_state.peer_id, "apply_state" => ?apply_state, "cmd" => ?cmd);
                }
                false
            }
            EngineStoreApplyRes::Persist => !region_state.pending_remove,
            EngineStoreApplyRes::NotFound => {
                error!(
                    "region not found in engine-store, maybe have exec `RemoveNode` first";
                    "region_id" => region_id,
                    "peer_id" => region_state.peer_id,
                    "term" => cmd.term,
                    "index" => cmd.index,
                );
                !region_state.pending_remove
            }
        };
        if persist {
            info!("should persist admin"; "region_id" => region_id, "peer_id" => region_state.peer_id, "state" => ?apply_state);
        }
        persist
    }

    pub fn on_empty_cmd(&self, ob_region: &Region, index: u64, term: u64) {
        let region_id = ob_region.get_id();
        fail::fail_point!("on_empty_cmd_normal", |_| {});
        debug!("encounter empty cmd, maybe due to leadership change";
            "region" => ?ob_region,
            "region_id" => region_id,
            "index" => index,
            "term" => term,
        );
        // We still need to pass a dummy cmd, to forward updates.
        let cmd_dummy = WriteCmds::new();
        self.engine_store_server_helper
            .handle_write_raft_cmd(&cmd_dummy, RaftCmdHeader::new(region_id, index, term));
    }

    pub fn post_exec_query(
        &self,
        ob_region: &Region,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
        apply_ctx_info: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        fail::fail_point!("on_post_exec_normal", |e| {
            e.unwrap().parse::<bool>().unwrap()
        });
        let region_id = ob_region.get_id();
        const NONE_STR: &str = "";
        let requests = cmd.request.get_requests();
        let response = &cmd.response;
        if response.get_header().has_error() {
            let proto_err = response.get_header().get_error();
            let mut ignorable_error = false;
            if proto_err.has_flashback_in_progress() {
                ignorable_error = true;
            }
            if cmd.request.has_admin_request()
                && cmd.request.get_admin_request().get_cmd_type() == AdminCmdType::UpdateGcPeer
            {
                // We are safe to treaat v2's admin cmd as an empty cmd.
                ignorable_error = true;
            }
            if ignorable_error {
                debug!(
                    "error occurs when apply_write_cmd, {:?}",
                    response.get_header().get_error()
                );
            } else {
                info!(
                    "error occurs when apply_write_cmd, {:?}",
                    response.get_header().get_error()
                );
            }
            return self.handle_error_apply(ob_region, cmd, region_state);
        }

        let mut ssts = vec![];
        let mut cmds = WriteCmds::with_capacity(requests.len());
        for req in requests {
            let cmd_type = req.get_cmd_type();
            match cmd_type {
                CmdType::Put => {
                    let put = req.get_put();
                    let cf = name_to_cf(put.get_cf());
                    let (key, value) = (put.get_key(), put.get_value());
                    cmds.push(key, value, WriteCmdType::Put, cf);
                }
                CmdType::Delete => {
                    let del = req.get_delete();
                    let cf = name_to_cf(del.get_cf());
                    let key = del.get_key();
                    cmds.push(key, NONE_STR.as_ref(), WriteCmdType::Del, cf);
                }
                CmdType::IngestSst => {
                    ssts.push(engine_traits::SstMetaInfo {
                        total_bytes: 0,
                        total_kvs: 0,
                        meta: req.get_ingest_sst().get_sst().clone(),
                    });
                }
                CmdType::Snap | CmdType::Get | CmdType::DeleteRange => {
                    // engine-store will drop table, no need DeleteRange
                    // We will filter delete range in engine_tiflash
                    continue;
                }
                CmdType::Prewrite | CmdType::Invalid | CmdType::ReadIndex => {
                    panic!("invalid cmd type, message maybe corrupted");
                }
            }
        }

        let persist = if !ssts.is_empty() {
            assert_eq!(cmds.len(), 0);
            match self.handle_ingest_sst_for_engine_store(ob_region, &ssts, cmd.index, cmd.term) {
                EngineStoreApplyRes::None => {
                    // Before, BR/Lightning may let ingest sst cmd contain only one cf,
                    // which may cause that TiFlash can not flush all region cache into column.
                    // so we have a optimization proxy@cee1f003.
                    // The optimization is to introduce a `pending_delete_ssts`,
                    // which holds ssts from being cleaned(by adding into `delete_ssts`),
                    // when engine-store returns None.
                    // Though this is fixed by br#1150 & tikv#10202, we still have to handle None,
                    // since TiKV's compaction filter can also cause mismatch between default and
                    // write. According to tiflash#1811.
                    // Since returning None will cause no persistence of advanced apply index,
                    // So in a recovery, we can replay ingestion in `pending_delete_ssts`,
                    // thus leaving no un-tracked sst files.

                    // We must hereby move all ssts to `pending_delete_ssts` for protection.
                    match apply_ctx_info.pending_handle_ssts {
                        None => (), // No ssts to handle, unlikely.
                        Some(v) => {
                            self.pending_delete_ssts
                                .write()
                                .expect("lock error")
                                .append(v);
                        }
                    };
                    info!(
                        "skip persist for ingest sst";
                        "region_id" => region_id,
                        "peer_id" => region_state.peer_id,
                        "term" => cmd.term,
                        "index" => cmd.index,
                        "ssts_to_clean" => ?ssts,
                    );
                    false
                }
                EngineStoreApplyRes::NotFound | EngineStoreApplyRes::Persist => {
                    info!(
                        "ingest sst success";
                        "region_id" => region_id,
                        "peer_id" => region_state.peer_id,
                        "term" => cmd.term,
                        "index" => cmd.index,
                        "ssts_to_clean" => ?ssts,
                    );
                    match apply_ctx_info.pending_handle_ssts {
                        None => (),
                        Some(v) => {
                            let mut sst_in_region: Vec<SstMetaInfo> = self
                                .pending_delete_ssts
                                .write()
                                .expect("lock error")
                                .extract_if(|e| e.meta.get_region_id() == region_id)
                                .collect();
                            apply_ctx_info.delete_ssts.append(&mut sst_in_region);
                            apply_ctx_info.delete_ssts.append(v);
                        }
                    }
                    !region_state.pending_remove
                }
            }
        } else {
            let flash_res = {
                self.engine_store_server_helper.handle_write_raft_cmd(
                    &cmds,
                    RaftCmdHeader::new(region_id, cmd.index, cmd.term),
                )
            };
            match flash_res {
                EngineStoreApplyRes::None => false,
                EngineStoreApplyRes::Persist => !region_state.pending_remove,
                EngineStoreApplyRes::NotFound => false,
            }
        };
        fail::fail_point!("on_post_exec_normal_end", |e| {
            e.unwrap().parse::<bool>().unwrap()
        });
        if persist {
            info!("should persist query"; "region_id" => region_id, "peer_id" => region_state.peer_id, "state" => ?apply_state);
        }
        persist
    }

    pub fn on_raft_message(&self, msg: &RaftMessage) -> bool {
        fail::fail_point!(
            "fap_on_msg_snapshot_1_3003",
            msg.get_message().get_msg_type() == raft::eraftpb::MessageType::MsgSnapshot
                && msg.get_to_peer().get_store_id() == 3
                && msg.get_to_peer().get_id() == 3003
                && msg.region_id == 1,
            |_| unreachable!()
        );
        !self.maybe_fast_path_tick(msg)
    }
}
