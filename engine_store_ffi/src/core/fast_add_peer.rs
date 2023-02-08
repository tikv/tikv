// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::{
    core::{common::*, ProxyForwarder},
    ffi::interfaces_ffi::FastAddPeerStatus,
};

pub fn get_region_local_state<EK: engine_traits::KvEngine>(
    engine: &EK,
    region_id: u64,
) -> Option<RegionLocalState> {
    let region_state_key = keys::region_state_key(region_id);
    engine
        .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
        .unwrap_or(None)
}

pub fn validate_remote_peer_region(
    new_region: &kvproto::metapb::Region,
    store_id: u64,
    new_peer_id: u64,
) -> bool {
    match find_peer(new_region, store_id) {
        Some(peer) => peer.get_id() == new_peer_id,
        None => false,
    }
}

impl<T: Transport + 'static, ER: RaftEngine> ProxyForwarder<T, ER> {
    pub fn is_initialized(&self, region_id: u64) -> bool {
        match get_region_local_state(&self.engine, region_id) {
            None => false,
            Some(r) => {
                raftstore::store::util::is_region_initialized(r.get_region())
                    && (r.get_state() != PeerState::Tombstone)
            }
        }
    }

    // Returns whether we need to ignore this message and run fast path instead.
    pub fn maybe_fast_path(&self, msg: &RaftMessage) -> bool {
        if !self.packed_envs.engine_store_cfg.enable_fast_add_peer {
            // fast path not enabled
            return false;
        }
        let inner_msg = msg.get_message();
        if inner_msg.get_commit() == 0 && inner_msg.get_msg_type() == MessageType::MsgHeartbeat {
            return false;
        } else if inner_msg.get_msg_type() == MessageType::MsgAppend {
        } else {
            return false;
        }
        // We don't need to recover all region infomation from restart,
        // since we have `has_already_inited`.
        let inner_msg = msg.get_message();
        if inner_msg.get_msg_type() != MessageType::MsgAppend {
            // we only handles the first MsgAppend
            return false;
        }
        let region_id = msg.get_region_id();
        let new_peer_id = msg.get_to_peer().get_id();
        let cached_manager = self.get_cached_manager();
        let mut is_first = false;
        let mut is_replicated = false;
        let mut has_already_inited = None;
        let mut early_skip = false;
        let f = |info: MapEntry<u64, Arc<CachedRegionInfo>>| {
            let current = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            match info {
                MapEntry::Occupied(mut o) => {
                    // Test if a fast path is timeout
                    let fast_path_start = o.get().fast_add_peer_start.load(Ordering::SeqCst);
                    if fast_path_start != 0 {
                        let elapsed = current.as_millis() - fast_path_start;
                        #[cfg(any(test, feature = "testexport"))]
                        const TRACE_SLOW_MILLIS: u128 = 0;
                        #[cfg(any(test, feature = "testexport"))]
                        const FALLBACK_MILLIS: u128 = 1000 * 2;
                        #[cfg(not(any(test, feature = "testexport")))]
                        const TRACE_SLOW_MILLIS: u128 = 1000 * 60 * 3;
                        #[cfg(not(any(test, feature = "testexport")))]
                        const FALLBACK_MILLIS: u128 = 1000 * 60 * 5;

                        #[allow(clippy::redundant_closure_call)]
                        let fallback_millis = (|| {
                            fail::fail_point!("fap_core_fallback_millis", |t| {
                                let t = t.unwrap().parse::<u128>().unwrap();
                                t
                            });
                            FALLBACK_MILLIS
                        })();
                        if elapsed >= TRACE_SLOW_MILLIS {
                            let need_fallback = elapsed > fallback_millis;
                            // TODO If snapshot is sent, we need fallback but can't do fallback?
                            let do_fallback = need_fallback;
                            info!("fast path: ongoing {}:{} {}, MsgAppend duplicated",
                                self.store_id, region_id, new_peer_id;
                                    "to_peer_id" => msg.get_to_peer().get_id(),
                                    "from_peer_id" => msg.get_from_peer().get_id(),
                                    "region_id" => region_id,
                                    "inner_msg" => ?inner_msg,
                                    "is_replicated" => is_replicated,
                                    "has_already_inited" => has_already_inited,
                                    "is_first" => is_first,
                                    "elapsed" => elapsed,
                                    "do_fallback" => do_fallback,
                            );
                            if do_fallback {
                                o.get_mut().inited_or_fallback.store(true, Ordering::SeqCst);
                                is_first = false;
                                early_skip = false;
                                return;
                            }
                        }
                    }
                    // If a snapshot is sent, we must skip further handling.
                    let last = o.get().snapshot_inflight.load(Ordering::SeqCst);
                    if last != 0 {
                        early_skip = true;
                        // We must return here to avoid changing `inited_or_fallback`.
                        // Otherwise will cause different value in pre/post_apply_snapshot.
                        return;
                    }
                    (is_first, has_already_inited) =
                        if !o.get().inited_or_fallback.load(Ordering::SeqCst) {
                            // If `has_already_inited` is true:
                            // 1. We recover from a restart,
                            // 2. The peer is created by TiKV like split;
                            // So we have data in disk, but not in memory.
                            // In these cases, we need to check everytime.

                            // TODO We can then remove logics in apply snapshot.
                            // This is because if the next maybe_fast_path after apply snapshot
                            // will have has_already_inited == true, which leads to normal
                            // MsgAppend.
                            let has_already_inited = self.is_initialized(region_id);
                            if has_already_inited {
                                o.get_mut().inited_or_fallback.store(true, Ordering::SeqCst);
                            }
                            (!has_already_inited, Some(has_already_inited))
                        } else {
                            (false, None)
                        };
                    if is_first {
                        // Don't care if the exchange succeeds.
                        let _ = o.get_mut().fast_add_peer_start.compare_exchange(
                            0,
                            current.as_millis(),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        );
                    }
                    // TODO include create
                    is_replicated = o.get().replicated_or_created.load(Ordering::SeqCst);
                }
                MapEntry::Vacant(v) => {
                    info!("fast path: ongoing {}:{} {}, first message", self.store_id, region_id, new_peer_id;
                        "to_peer_id" => msg.get_to_peer().get_id(),
                        "from_peer_id" => msg.get_from_peer().get_id(),
                        "region_id" => region_id,
                        "inner_msg" => ?inner_msg,
                    );
                    let c = CachedRegionInfo::default();
                    c.fast_add_peer_start
                        .store(current.as_millis(), Ordering::SeqCst);
                    v.insert(Arc::new(c));
                    is_first = true;
                }
            }
        };

        // Try not acquire write lock firstly.
        match cached_manager.get_inited_or_fallback(region_id) {
            Some(true) => {
                // Most cases, when the peer is already inited.
                is_first = false;
            }
            None | Some(false) => self
                .get_cached_manager()
                .access_cached_region_info_mut(region_id, f)
                .unwrap(),
        };

        #[cfg(any(test, feature = "testexport"))]
        {
            if is_first {
                info!("fast path: ongoing {}:{} {}, MsgAppend skipped",
                    self.store_id, region_id, new_peer_id;
                        "to_peer_id" => msg.get_to_peer().get_id(),
                        "from_peer_id" => msg.get_from_peer().get_id(),
                        "region_id" => region_id,
                        "inner_msg" => ?inner_msg,
                        "is_replicated" => is_replicated,
                        "has_already_inited" => has_already_inited,
                        "is_first" => is_first,
                );
            } else {
                info!("fast path: ongoing {}:{} {}, MsgAppend accepted",
                    self.store_id, region_id, new_peer_id;
                        "to_peer_id" => msg.get_to_peer().get_id(),
                        "from_peer_id" => msg.get_from_peer().get_id(),
                        "region_id" => region_id,
                        "inner_msg" => ?inner_msg,
                        "is_replicated" => is_replicated,
                        "has_already_inited" => has_already_inited,
                        "is_first" => is_first,
                );
            }
        }

        // If early_skip is true, we don't read the value of `is_first`.
        if early_skip {
            return true;
        }

        if !is_first {
            // Most cases, the region is already inited or fallback.
            // Skip fast add peer.
            return false;
        }

        // Peer is not created by Peer::replicate, will cause RegionNotRegistered error,
        // see `check_msg`.
        if !is_replicated {
            info!("fast path: ongoing {}:{} {}, wait replicating peer", self.store_id, region_id, new_peer_id;
                "to_peer_id" => msg.get_to_peer().get_id(),
                "from_peer_id" => msg.get_from_peer().get_id(),
                "region_id" => region_id,
                "inner_msg" => ?inner_msg,
            );
            return true;
        }

        info!("fast path: ongoing {}:{} {}, fetch data from remote peer", self.store_id, region_id, new_peer_id;
            "to_peer_id" => msg.get_to_peer().get_id(),
            "from_peer_id" => msg.get_from_peer().get_id(),
            "region_id" => region_id,
        );
        fail::fail_point!("fap_ffi_pause", |_| { return false });
        // Feed data
        let res = self
            .engine_store_server_helper
            .fast_add_peer(region_id, new_peer_id);
        match res.status {
            FastAddPeerStatus::Ok => (),
            FastAddPeerStatus::WaitForData => {
                info!(
                    "fast path: ongoing {}:{} {}. remote peer preparing data, wait",
                    self.store_id, region_id, new_peer_id;
                    "region_id" => region_id,
                );
                return true;
            }
            _ => {
                error!(
                    "fast path: ongoing {}:{} {} failed. fetch and replace error {:?}, fallback to normal",
                    self.store_id, region_id, new_peer_id, res;
                    "region_id" => region_id,
                );
                cached_manager.fallback_to_slow_path(region_id);
                return false;
            }
        };

        let apply_state_str = res.apply_state.view.to_slice();
        let region_str = res.region.view.to_slice();
        let mut apply_state = RaftApplyState::default();
        let mut new_region = kvproto::metapb::Region::default();
        if let Err(_e) = apply_state.merge_from_bytes(apply_state_str) {
            error!(
                "fast path: ongoing {}:{} {} failed. parse apply_state {:?}, fallback to normal",
                self.store_id, region_id, new_peer_id, res;
                "region_id" => region_id,
            );
            cached_manager.fallback_to_slow_path(region_id);
        }
        if let Err(_e) = new_region.merge_from_bytes(region_str) {
            error!(
                "fast path: ongoing {}:{} {} failed. parse region {:?}, fallback to normal",
                self.store_id, region_id, new_peer_id, res;
                "region_id" => region_id,
            );
            cached_manager.fallback_to_slow_path(region_id);
        }

        // Validate
        // check if the source already knows the know peer
        if !validate_remote_peer_region(&new_region, self.store_id, new_peer_id) {
            info!(
                "fast path: ongoing {}:{} {}. failed remote peer has not applied conf change",
                self.store_id, region_id, new_peer_id;
                "region_id" => region_id,
                "region" => ?new_region,
            );
            cached_manager.fallback_to_slow_path(region_id);
            return false;
        }

        info!("fast path: ongoing {}:{} {}, start build and send", self.store_id, region_id, new_peer_id;
            "to_peer_id" => msg.get_to_peer().get_id(),
            "from_peer_id" => msg.get_from_peer().get_id(),
            "region_id" => region_id,
            "new_region" => ?new_region,
            "apply_state" => ?apply_state,
        );
        match self.build_and_send_snapshot(region_id, new_peer_id, msg, apply_state, new_region) {
            Ok(s) => {
                match s {
                    FastAddPeerStatus::Ok => {
                        fail::fail_point!("fap_core_no_fast_path", |_| { return false });
                        info!("fast path: ongoing {}:{} {}, finish build and send", self.store_id, region_id, new_peer_id;
                            "to_peer_id" => msg.get_to_peer().get_id(),
                            "from_peer_id" => msg.get_from_peer().get_id(),
                            "region_id" => region_id,
                        );
                    }
                    FastAddPeerStatus::WaitForData => {
                        info!(
                            "fast path: ongoing {}:{} {}. remote peer preparing data, wait",
                            new_peer_id, self.store_id, region_id;
                            "region_id" => region_id,
                        );
                        return true;
                    }
                    _ => {
                        error!(
                            "fast path: ongoing {}:{} {} failed. build and sent snapshot code {:?}",
                            self.store_id, region_id, new_peer_id, s;
                            "region_id" => region_id,
                        );
                        cached_manager.fallback_to_slow_path(region_id);
                        return false;
                    }
                };
            }
            Err(e) => {
                error!(
                    "fast path: ongoing {}:{} {} failed. build and sent snapshot error {:?}",
                    self.store_id, region_id, new_peer_id, e;
                    "region_id" => region_id,
                );
                cached_manager.fallback_to_slow_path(region_id);
                return false;
            }
        };
        is_first
    }

    fn check_entry_at_index(
        &self,
        region_id: u64,
        index: u64,
        peer_id: u64,
        tag: &str,
    ) -> RaftStoreResult<u64> {
        match self.raft_engine.get_entry(region_id, index)? {
            Some(entry) => Ok(entry.get_term()),
            None => {
                return Err(box_err!(
                    "can't find entry for index {} of region {}, peer_id: {}, tag {}",
                    index,
                    region_id,
                    peer_id,
                    tag
                ));
            }
        }
    }

    fn build_and_send_snapshot(
        &self,
        region_id: u64,
        new_peer_id: u64,
        msg: &RaftMessage,
        apply_state: RaftApplyState,
        new_region: kvproto::metapb::Region,
    ) -> RaftStoreResult<FastAddPeerStatus> {
        let cached_manager = self.get_cached_manager();
        let inner_msg = msg.get_message();

        let current = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        #[cfg(any(test, feature = "testexport"))]
        {
            let fake_send: bool = (|| {
                fail::fail_point!("fap_core_fake_send", |t| {
                    let t = t.unwrap().parse::<u64>().unwrap();
                    t
                });
                0
            })() != 0;
            if fake_send {
                // A handling snapshot may block handling later MsgAppend.
                // So we fake send.
                cached_manager
                    .set_snapshot_inflight(region_id, current.as_millis())
                    .unwrap();
                return Ok(FastAddPeerStatus::Ok);
            }
        }

        // Get a snapshot object.
        let (mut snapshot, key) = {
            // Find term of entry at applied_index.
            let applied_index = apply_state.get_applied_index();
            let applied_term =
                self.check_entry_at_index(region_id, applied_index, new_peer_id, "applied_index")?;
            // Will otherwise cause "got message with lower index than committed" loop.
            self.check_entry_at_index(
                region_id,
                apply_state.get_commit_index(),
                new_peer_id,
                "commit_index",
            )?;

            let key = SnapKey::new(region_id, applied_term, applied_index);
            self.snap_mgr.register(key.clone(), SnapEntry::Generating);
            defer!(self.snap_mgr.deregister(&key, &SnapEntry::Generating));
            let snapshot = self.snap_mgr.get_snapshot_for_building(&key)?;
            // for cf in snapshot.cf_files().iter() {
            //     info!("!!!! snapshot cf_file of {} size {:?}", cf.cf, cf.size);
            // }

            (snapshot, key.clone())
        };

        // Build snapshot by do_snapshot
        let mut pb_snapshot: eraftpb::Snapshot = Default::default();
        let pb_snapshot_metadata: &mut eraftpb::SnapshotMetadata = pb_snapshot.mut_metadata();
        let mut pb_snapshot_data = kvproto::raft_serverpb::RaftSnapshotData::default();
        {
            // eraftpb::SnapshotMetadata
            for (_, cf) in raftstore::store::snap::SNAPSHOT_CFS_ENUM_PAIR {
                let cf_index: RaftStoreResult<usize> = snapshot
                    .cf_files()
                    .iter()
                    .position(|x| &x.cf == cf)
                    .ok_or(box_err!("can't find index for cf {}", cf));
                let cf_index = cf_index?;
                let cf_file = &snapshot.cf_files()[cf_index];
                // Create fake cf file.
                let mut path = cf_file.path.clone();
                path.push(cf_file.file_prefix.clone());
                path.set_extension("sst");
                let mut f = std::fs::File::create(path.as_path())?;
                f.flush()?;
                f.sync_all()?;
            }
            pb_snapshot_data.set_region(new_region.clone());
            pb_snapshot_data.set_file_size(0);
            const SNAPSHOT_VERSION: u64 = 2;
            pb_snapshot_data.set_version(SNAPSHOT_VERSION);

            // SnapshotMeta
            // Which is snap.meta_file.meta
            let snapshot_meta =
                raftstore::store::snap::gen_snapshot_meta(snapshot.cf_files(), true)?;

            // Write MetaFile
            {
                // let v = snapshot_meta.write_to_bytes()?;
                // let mut f = std::fs::File::create(snapshot.meta_path())?;
                // info!("!!!!! create snapshot meta file {:?}", snapshot.meta_path());
                // f.write_all(&v[..])?;
                // f.flush()?;
                // f.sync_all()?;
                // snapshot.mut_meta_file().meta.insert(snapshot_meta.clone());
                snapshot.set_snapshot_meta(snapshot_meta.clone())?;
                // snapshot.set_hold_tmp_files(false);
                snapshot.save_meta_file()?;
            }
            pb_snapshot_data.set_meta(snapshot_meta);
        }

        pb_snapshot_metadata
            .set_conf_state(raftstore::store::util::conf_state_from_region(&new_region));
        pb_snapshot_metadata.set_index(key.idx);
        pb_snapshot_metadata.set_term(key.term);

        pb_snapshot.set_data(pb_snapshot_data.write_to_bytes().unwrap().into());

        // Send reponse
        let mut response = RaftMessage::default();
        let epoch = new_region.get_region_epoch();
        response.set_region_epoch(epoch.clone());
        response.set_region_id(region_id);
        response.set_from_peer(msg.get_from_peer().clone());
        response.set_to_peer(msg.get_to_peer().clone());

        let message = response.mut_message();
        message.set_msg_type(MessageType::MsgSnapshot);
        message.set_term(inner_msg.get_term());
        message.set_snapshot(pb_snapshot);
        // If no set, will result in a MsgResponse to peer 0.
        message.set_from(msg.get_from_peer().get_id());
        message.set_to(msg.get_to_peer().get_id());
        // debug!(
        //     "!!!! send snapshot to {} key {} raft message {:?} snap data {:?}
        // apply_state {:?}",     msg.get_to_peer().get_id(),
        //     key,
        //     response,
        //     pb_snapshot_data,
        //     apply_state
        // );

        match self.trans.lock() {
            Ok(mut trans) => match trans.send(response) {
                Ok(_) => {
                    cached_manager
                        .set_snapshot_inflight(region_id, current.as_millis())
                        .unwrap();
                    // If we don't flush here, packet will lost.
                    trans.flush();
                }
                Err(RaftStoreError::RegionNotFound(_)) => (),
                _ => return Ok(FastAddPeerStatus::OtherError),
            },
            Err(e) => return Err(box_err!("send snapshot meets error {:?}", e)),
        }

        Ok(FastAddPeerStatus::Ok)
    }
}
