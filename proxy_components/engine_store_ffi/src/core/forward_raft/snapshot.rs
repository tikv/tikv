// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use proxy_ffi::interfaces_ffi::SSTReaderPtr;

use crate::{
    core::{common::*, PrehandleTask, ProxyForwarder, PtrWrapper},
    fatal,
};

type SSTInfo = (String, ColumnFamilyType);

pub fn retrieve_sst_files(peer_id: u64, snap: &store::Snapshot) -> Vec<SSTInfo> {
    let mut sst_views: Vec<SSTInfo> = vec![];
    let mut ssts = vec![];
    let v2_db_path = snap.snapshot_meta().as_ref().and_then(|m| {
        if m.get_tablet_snap_path().is_empty() {
            None
        } else {
            Some(m.get_tablet_snap_path().to_owned())
        }
    });
    let v2_format_snap = v2_db_path.is_some();

    if !v2_format_snap {
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
                tikv_util::debug!("observe multi-file snapshot";
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
    } else {
        let path = v2_db_path.unwrap();
        for cf in &[
            ColumnFamilyType::Default,
            ColumnFamilyType::Lock,
            ColumnFamilyType::Write,
        ] {
            ssts.push((path.clone(), *cf));
        }
    }

    for (s, cf) in ssts.into_iter() {
        if v2_format_snap {
            sst_views.push((SSTReaderPtr::encode_v2(s.as_str()), cf));
        } else {
            sst_views.push((s, cf));
        }
    }
    if sst_views.is_empty() {
        // This logic should not happen here.
        info!("meet a empty snapshot, maybe error or no data";
            "peer_id" => peer_id
        );
    }
    sst_views
}

fn pre_handle_snapshot_impl(
    engine_store_server_helper: &'static EngineStoreServerHelper,
    peer_id: u64,
    ssts: Vec<SSTInfo>,
    region: &Region,
    snap_key: &SnapKey,
) -> PtrWrapper {
    let idx = snap_key.idx;
    let term = snap_key.term;
    let ptr = {
        let sst_views = ssts
            .iter()
            .map(|(b, c)| (b.as_bytes(), c.clone()))
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
            "snapshot" => ?snap,
        );
        fail::fail_point!("on_ob_pre_handle_snapshot", |_| {});
        fail::fail_point!("on_ob_pre_handle_snapshot_s3", self.store_id == 3, |_| {});
        // TODO in fap, Snapshot could still have display_path even though it is a faked
        // empty one
        let snap = match snap {
            None => return,
            Some(s) => s,
        };

        // Simulate loss of sst files.
        fail::fail_point!("on_ob_pre_handle_snapshot_delete", |_| {
            let ssts = retrieve_sst_files(peer_id, snap);
            for (path, _) in ssts.iter() {
                debug!("delete snapshot file"; "path" => ?path);
                std::fs::remove_file(std::path::Path::new(path)).unwrap();
            }
            return;
        });

        defer!({
            fail::fail_point!("on_ob_cancel_after_pre_handle_snapshot", |_| {
                self.cancel_apply_snapshot(region_id, peer_id)
            });
        });

        if self.pre_apply_snapshot_for_fap_snapshot(ob_region, peer_id, snap_key) {
            return;
        }

        fail::fail_point!("fap_core_no_prehandle", |_| {
            return;
        });
        match self.apply_snap_pool.as_ref() {
            Some(p) => {
                let (sender, receiver) = mpsc::channel();
                let task = Arc::new(PrehandleTask::new(receiver, peer_id, snap_key.clone()));
                {
                    let mut lock = match self.pre_handle_snapshot_ctx.lock() {
                        Ok(l) => l,
                        Err(_) => fatal!("pre_apply_snapshot poisoned"),
                    };
                    let ctx = lock.deref_mut();
                    if let Some(o) = ctx.tracer.insert(snap_key.region_id, task.clone()) {
                        let _prev = self
                            .engine
                            .proxy_ext
                            .pending_applies_count
                            .fetch_sub(1, Ordering::SeqCst);
                        info!("replace apply snapshot";
                            "peer_id" => peer_id,
                            "region_id" => region_id,
                            "snap_key" => ?snap_key,
                            "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
                            "new_snapshot" => ?snap,
                            "old_snapshot_index" => o.snap_key.idx,
                        );
                        // TODO elegantly stop the previous task.
                        drop(o);
                    }
                }

                let engine_store_server_helper = self.engine_store_server_helper;
                let region = ob_region.clone();
                let snap_key = snap_key.clone();
                let ssts = retrieve_sst_files(peer_id, snap);

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
        if self.post_apply_snapshot_for_fap_snapshot(ob_region, peer_id, snap_key, snap) {
            // Already handled as an fap snapshot.
            return;
        }

        info!("post apply snapshot";
            "peer_id" => ?peer_id,
            "snap_key" => ?snap_key,
            "region_id" => region_id,
            "region" => ?ob_region,
            "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
        );

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
            ctx.tracer.remove(&region_id)
        };

        let post_apply_start = tikv_util::time::Instant::now();
        let need_retry = match maybe_prehandle_task {
            Some(t) => {
                if &t.snap_key != snap_key {
                    panic!(
                        "mismatched prehandled snapshot [region_id={}] [peer_id={}] [snap_key={:?}] [snap_key={:?}]",
                        ob_region.get_id(),
                        peer_id,
                        snap_key,
                        t.snap_key,
                    );
                }
                let need_retry = match t.recv.recv() {
                    Ok(snap_ptr) => {
                        info!("get prehandled snapshot success";
                            "peer_id" => peer_id,
                            "snap_key" => ?snap_key,
                            "region_id" => ob_region.get_id(),
                            "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
                        );
                        // We must fetch snapshot here, as a consumer.
                        self.engine_store_server_helper
                            .apply_pre_handled_snapshot(snap_ptr.0);
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

                if !need_retry {
                    info!("apply snapshot finished";
                        "mode" => "normal",
                        "peer_id" => peer_id,
                        "snap_key" => ?snap_key,
                        "region" => ?ob_region,
                        "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
                        "elapsed" => post_apply_start.saturating_elapsed().as_millis(),
                    );
                }
                need_retry
            }
            None => {
                // We can't find background pre-handle task, maybe:
                // 1. We can't get snapshot from snap manager at that time. This is abnormal
                //    case.
                // 2. We disabled background pre handling.
                info!("pre-handled snapshot not found";
                    "peer_id" => peer_id,
                    "snap_key" => ?snap_key,
                    "region_id" => ob_region.get_id(),
                    "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
                );
                true
            }
        };

        if need_retry {
            // Blocking pre handle.
            let ssts = retrieve_sst_files(peer_id, snap);
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
                "mode" => "blockgen",
                "peer_id" => peer_id,
                "snap_key" => ?snap_key,
                "region" => ?ob_region,
                "pending" => self.engine.proxy_ext.pending_applies_count.load(Ordering::SeqCst),
                "elapsed" => post_apply_start.saturating_elapsed().as_millis(),
            );
        }
    }

    pub fn should_pre_apply_snapshot(&self) -> bool {
        true
    }

    pub fn cancel_apply_snapshot(&self, region_id: u64, peer_id: u64) {
        info!("start cancel apply snapshot";
            "peer_id" => peer_id,
            "region_id" => region_id,
        );
        // Notify TiFlash to stop pre handling snapshot. No blocking wait.
        let maybe_prehandle_task = {
            let mut lock = match self.pre_handle_snapshot_ctx.lock() {
                Ok(l) => l,
                Err(_) => fatal!("cancel_apply_snapshot poisoned"),
            };
            let ctx = lock.deref_mut();
            ctx.tracer.remove(&region_id)
        };

        // We don't clear fap snapshot, because applying could be resumed after restart.
        // Once resumed, the fap snapshot could be reused.
        if let Some(t) = maybe_prehandle_task {
            // If `cancel_applying_snap` is called, applying snapshot is cancelled.
            // It will happen only if the peer is scheduled away.
            // However, if the region will somehow request for a prehandled snapshot,
            // We can still reply on regenerate snapshot though it will take much time.
            // See `test_apply_cancelled_pre_handle`.
            self.engine_store_server_helper
                .abort_pre_handle_snapshot(region_id, peer_id);

            let current_cnt = self
                .engine
                .proxy_ext
                .pending_applies_count
                .fetch_sub(1, Ordering::SeqCst)
                - 1;
            match t.recv.recv() {
                Ok(f) => {
                    info!("cancel apply snapshot start cancel pre handled snapshot";
                        "peer_id" => peer_id,
                        "region_id" => region_id,
                        "pending_applies_count" => current_cnt,
                    );
                    self.engine_store_server_helper
                        .release_pre_handled_snapshot(f.0);
                }
                Err(e) => {
                    info!("cancel apply snapshot find error in prehandle task {:?}", e;
                        "peer_id" => peer_id,
                        "region_id" => region_id,
                        "pending_applies_count" => current_cnt,
                    );
                }
            }
        } else {
            info!("cancel apply snapshot find no prehandle task";
                "peer_id" => peer_id,
                "region_id" => region_id,
            );
        }
    }
}
