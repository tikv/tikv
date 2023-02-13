// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::{
    core::{common::*, PrehandleTask, ProxyForwarder, PtrWrapper},
    fatal,
};

fn retrieve_sst_files(snap: &store::Snapshot) -> Vec<(PathBuf, ColumnFamilyType)> {
    let mut sst_views: Vec<(PathBuf, ColumnFamilyType)> = vec![];
    let mut ssts = vec![];
    for cf_file in snap.cf_files() {
        // Skip empty cf file.
        // CfFile is changed by dynamic region.
        if cf_file.size.is_empty() {
            continue;
        }

        if cf_file.size[0] == 0 {
            continue;
        }

        if plain_file_used(cf_file.cf) {
            assert!(cf_file.cf == CF_LOCK);
        }
        let mut full_paths = cf_file.file_paths();
        assert!(!full_paths.is_empty());
        if full_paths.len() != 1 {
            // Multi sst files for one cf.
            tikv_util::info!("observe multi-file snapshot";
                "snap" => ?snap,
                "cf" => ?cf_file.cf,
                "total" => full_paths.len(),
            );
            for f in full_paths.into_iter() {
                ssts.push((f, name_to_cf(cf_file.cf)));
            }
        } else {
            // Old case, one file for one cf.
            ssts.push((full_paths.remove(0), name_to_cf(cf_file.cf)));
        }
    }
    for (s, cf) in ssts.iter() {
        sst_views.push((PathBuf::from_str(s).unwrap(), *cf));
    }
    sst_views
}

fn pre_handle_snapshot_impl(
    engine_store_server_helper: &'static EngineStoreServerHelper,
    peer_id: u64,
    ssts: Vec<(PathBuf, ColumnFamilyType)>,
    region: &Region,
    snap_key: &SnapKey,
) -> PtrWrapper {
    let idx = snap_key.idx;
    let term = snap_key.term;
    let ptr = {
        let sst_views = ssts
            .iter()
            .map(|(b, c)| (b.to_str().unwrap().as_bytes(), c.clone()))
            .collect();
        engine_store_server_helper.pre_handle_snapshot(region, peer_id, sst_views, idx, term)
    };
    PtrWrapper(ptr)
}

impl<T: Transport + 'static, ER: RaftEngine> ProxyForwarder<T, ER> {
    #[allow(clippy::single_match)]
    pub fn pre_apply_snapshot(
        &self,
        ob_region: &Region,
        peer_id: u64,
        snap_key: &store::SnapKey,
        snap: Option<&store::Snapshot>,
    ) {
        let region_id = ob_region.get_id();
        info!("pre apply snapshot";
            "peer_id" => peer_id,
            "region_id" => region_id,
            "snap_key" => ?snap_key,
            "has_snap" => snap.is_some(),
            "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
        );
        fail::fail_point!("on_ob_pre_handle_snapshot", |_| {});

        let snap = match snap {
            None => return,
            Some(s) => s,
        };

        fail::fail_point!("on_ob_pre_handle_snapshot_delete", |_| {
            let ssts = retrieve_sst_files(snap);
            for (pathbuf, _) in ssts.iter() {
                debug!("delete snapshot file"; "path" => ?pathbuf);
                std::fs::remove_file(pathbuf.as_path()).unwrap();
            }
            return;
        });

        let mut should_skip = false;
        #[allow(clippy::collapsible_if)]
        if self.packed_envs.engine_store_cfg.enable_fast_add_peer {
            if self.get_cached_manager().access_cached_region_info_mut(
                region_id,
                |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                    MapEntry::Occupied(o) => {
                        let is_first_snapsot = !o.get().inited_or_fallback.load(Ordering::SeqCst);
                        if is_first_snapsot {
                            info!("fast path: prehandle first snapshot {}:{} {}", self.store_id, region_id, peer_id;
                                "snap_key" => ?snap_key,
                                "region_id" => region_id,
                            );
                            should_skip = true;
                        }
                    }
                    MapEntry::Vacant(_) => {
                        // Compat no fast add peer logic
                        // panic!("unknown snapshot!");
                    }
                },
            ).is_err() {
                fatal!("post_apply_snapshot poisoned")
            };
        }

        if should_skip {
            return;
        }

        match self.apply_snap_pool.as_ref() {
            Some(p) => {
                let (sender, receiver) = mpsc::channel();
                let task = Arc::new(PrehandleTask::new(receiver, peer_id));
                {
                    let mut lock = match self.pre_handle_snapshot_ctx.lock() {
                        Ok(l) => l,
                        Err(_) => fatal!("pre_apply_snapshot poisoned"),
                    };
                    let ctx = lock.deref_mut();
                    ctx.tracer.insert(snap_key.clone(), task.clone());
                }

                let engine_store_server_helper = self.engine_store_server_helper;
                let region = ob_region.clone();
                let snap_key = snap_key.clone();
                let ssts = retrieve_sst_files(snap);

                // We use thread pool to do pre handling.
                self.engine
                    .proxy_ext
                    .pending_applies_count
                    .fetch_add(1, Ordering::SeqCst);
                p.spawn(async move {
                    // The original implementation is in `Snapshot`, so we don't need to care abort
                    // lifetime.
                    fail::fail_point!("before_actually_pre_handle", |_| {});
                    let res = pre_handle_snapshot_impl(
                        engine_store_server_helper,
                        task.peer_id,
                        ssts,
                        &region,
                        &snap_key,
                    );
                    match sender.send(res) {
                        Err(_e) => {
                            error!("pre apply snapshot err when send to receiver";
                                "region_id" => region.get_id(),
                                "peer_id" => task.peer_id,
                                "snap_key" => ?snap_key,
                            )
                        }
                        Ok(_) => (),
                    }
                });
            }
            None => {
                // quit background pre handling
                warn!("apply_snap_pool is not initialized";
                    "peer_id" => peer_id,
                    "region_id" => region_id
                );
            }
        }
    }

    pub fn post_apply_snapshot(
        &self,
        ob_region: &Region,
        peer_id: u64,
        snap_key: &store::SnapKey,
        snap: Option<&store::Snapshot>,
    ) {
        fail::fail_point!("on_ob_post_apply_snapshot", |_| {
            return;
        });
        let region_id = ob_region.get_id();
        info!("post apply snapshot";
            "peer_id" => ?peer_id,
            "snap_key" => ?snap_key,
            "region_id" => region_id,
            "region" => ?ob_region,
            "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
        );
        let mut should_skip = false;
        #[allow(clippy::collapsible_if)]
        if self.packed_envs.engine_store_cfg.enable_fast_add_peer {
            if self.get_cached_manager().access_cached_region_info_mut(
                region_id,
                |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                    MapEntry::Occupied(mut o) => {
                        let is_first_snapsot = !o.get().inited_or_fallback.load(Ordering::SeqCst);
                        if is_first_snapsot {
                            let last = o.get().snapshot_inflight.load(Ordering::SeqCst);
                            let total = o.get().fast_add_peer_start.load(Ordering::SeqCst);
                            let current = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap();
                            info!("fast path: applied first snapshot {}:{} {}, recover MsgAppend", self.store_id, region_id, peer_id;
                                "snap_key" => ?snap_key,
                                "region_id" => region_id,
                                "cost_snapshot" => current.as_millis() - last,
                                "cost_total" => current.as_millis() - total,
                            );
                            should_skip = true;
                            o.get_mut().snapshot_inflight.store(0, Ordering::SeqCst);
                            o.get_mut().fast_add_peer_start.store(0, Ordering::SeqCst);
                            o.get_mut().inited_or_fallback.store(true, Ordering::SeqCst);
                        }
                    }
                    MapEntry::Vacant(_) => {
                        // Compat no fast add peer logic
                        // panic!("unknown snapshot!");
                    }
                },
            ).is_err() {
                fatal!("post_apply_snapshot poisoned")
            };
        }

        if should_skip {
            return;
        }

        let snap = match snap {
            None => return,
            Some(s) => s,
        };
        let maybe_prehandle_task = {
            let mut lock = match self.pre_handle_snapshot_ctx.lock() {
                Ok(l) => l,
                Err(_) => fatal!("post_apply_snapshot poisoned"),
            };
            let ctx = lock.deref_mut();
            ctx.tracer.remove(snap_key)
        };

        let need_retry = match maybe_prehandle_task {
            Some(t) => {
                let neer_retry = match t.recv.recv() {
                    Ok(snap_ptr) => {
                        info!("get prehandled snapshot success";
                            "peer_id" => peer_id,
                            "snap_key" => ?snap_key,
                            "region_id" => ob_region.get_id(),
                            "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
                        );
                        if !should_skip {
                            self.engine_store_server_helper
                                .apply_pre_handled_snapshot(snap_ptr.0);
                        }
                        false
                    }
                    Err(_) => {
                        info!("background pre-handle snapshot get error";
                            "peer_id" => peer_id,
                            "snap_key" => ?snap_key,
                            "region_id" => ob_region.get_id(),
                            "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
                        );
                        true
                    }
                };
                // According to pre_apply_snapshot, if registered tracer,
                // then we must have put it into thread pool.
                let _prev = self
                    .engine
                    .proxy_ext
                    .pending_applies_count
                    .fetch_sub(1, Ordering::SeqCst);

                #[cfg(any(test, feature = "testexport"))]
                assert!(_prev > 0);

                info!("apply snapshot finished";
                    "peer_id" => peer_id,
                    "snap_key" => ?snap_key,
                    "region" => ?ob_region,
                    "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
                );
                neer_retry
            }
            None => {
                // We can't find background pre-handle task, maybe:
                // 1. we can't get snapshot from snap manager at that time.
                // 2. we disabled background pre handling.
                info!("pre-handled snapshot not found";
                    "peer_id" => peer_id,
                    "snap_key" => ?snap_key,
                    "region_id" => ob_region.get_id(),
                    "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
                );
                true
            }
        };

        if need_retry && !should_skip {
            // Blocking pre handle.
            let ssts = retrieve_sst_files(snap);
            let ptr = pre_handle_snapshot_impl(
                self.engine_store_server_helper,
                peer_id,
                ssts,
                ob_region,
                snap_key,
            );
            info!("re-gen pre-handled snapshot success";
                "peer_id" => peer_id,
                "snap_key" => ?snap_key,
                "region_id" => ob_region.get_id(),
            );
            self.engine_store_server_helper
                .apply_pre_handled_snapshot(ptr.0);
            info!("apply snapshot finished";
                "peer_id" => peer_id,
                "snap_key" => ?snap_key,
                "region" => ?ob_region,
                "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
            );
        }
    }

    pub fn should_pre_apply_snapshot(&self) -> bool {
        true
    }
}
