// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use api_version::ApiV2;
use dashmap::DashMap;
use futures::{compat::Future01CompatExt, stream, StreamExt};
use kvproto::{keyspacepb::KeyspaceMeta, meta_storagepb::EventEventType};
use pd_client::{
    meta_storage::{Checked, Get, MetaStorageClient, Sourced, Watch},
    Error as PdError, PdClient, RpcClient,
};
use serde::{Deserialize, Serialize};
use tikv_util::{debug, error, info, timer::GLOBAL_TIMER_HANDLE};

// RETRY_INTERVAL is the retry interval time(in seconds), to consistent with
// pd_client.
const RETRY_INTERVAL: Duration = Duration::from_secs(1);
pub const KEYSPACE_CONFIG_KEY_GC_MGMT_TYPE: &str = "gc_management_type";
pub const GC_MGMT_TYPE_GLOBAL_GC: &str = "global_gc";
pub const GC_MGMT_TYPE_KEYSPACE_LEVEL_GC: &str = "keyspace_level_gc";

// KeyspaceLevelGCWatchService used to watch keyspace level GC change in etcd of
// PD, and update the mapping from keyspace id to keyspace level GC.
#[derive(Clone)]
pub struct KeyspaceLevelGCWatchService {
    pd_client: Arc<RpcClient>,
    // wrap for etcd client.
    meta_client: Checked<Sourced<Arc<RpcClient>>>,
    // record watch revision.
    revision: i64,

    keyspace_level_gc_map: Arc<DashMap<u32, u64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct KeyspaceLevelGCJson {
    keyspace_id: u32,
    safe_point: u64,
}

impl KeyspaceLevelGCWatchService {
    /// Constructs a new `Service` with `KeyspaceLevelGCWatchService` and a
    /// `RpcClient`.
    pub fn new(
        pd_client: Arc<RpcClient>,
        keyspace_level_gc: Arc<DashMap<u32, u64>>,
    ) -> KeyspaceLevelGCWatchService {
        KeyspaceLevelGCWatchService {
            revision: 0,
            meta_client: Checked::new(Sourced::new(
                Arc::clone(&pd_client.clone()),
                pd_client::meta_storage::Source::KeysapceLevelGC,
            )),
            pd_client,
            keyspace_level_gc_map: keyspace_level_gc,
        }
    }

    pub async fn watch_keyspace_level_gc(&mut self) {
        // Firstly, load all keyspace level GC safe point as of now.
        self.reload_all_keyspace_level_gc().await;
        let keyspace_level_gc_prefix = self.get_keyspcace_level_gc_etcd_path();

        'outer: loop {
            // Secondly, start watcher at loading revision.
            let (mut stream, cancel) = stream::abortable(
                self.meta_client.watch(
                    Watch::of(keyspace_level_gc_prefix.as_str())
                        .prefixed()
                        .from_rev(self.revision)
                        .with_prev_kv(),
                ),
            );
            info!("pd meta client creating watch stream."; "path" => keyspace_level_gc_prefix.clone(), "rev" => %self.revision);
            while let Some(grpc_response) = stream.next().await {
                match grpc_response {
                    Ok(resp) => {
                        self.revision = resp.get_header().get_revision();
                        let events = resp.get_events();
                        events.iter().for_each(|event| match event.get_type() {
                            EventEventType::Put => {
                                match serde_json::from_slice::<KeyspaceLevelGCJson>(
                                    event.get_kv().get_value(),
                                ) {
                                    Ok(keyspace_level_gc_json) => {
                                        self.keyspace_level_gc_map.insert(keyspace_level_gc_json.keyspace_id, keyspace_level_gc_json.safe_point);
                                        debug!("[keyspace level gc watch service] update keyspace_level_gc_map keyspace-id:{},keyspace-level-gc-safe-point{}",keyspace_level_gc_json.keyspace_id,keyspace_level_gc_json.safe_point);
                                    },
                                    Err(e) => error!("[keyspace level gc watch service] parse put keyspace level gc event failed";  "err" => ?e),
                                }
                            }
                            EventEventType::Delete => {
                                match serde_json::from_slice::<KeyspaceLevelGCJson>(
                                    event.get_kv().get_value(),
                                ) {
                                    Ok(keyspace_level_gc_json) => {
                                        self.keyspace_level_gc_map.remove(&keyspace_level_gc_json.keyspace_id);
                                        debug!("[keyspace level gc watch service] remove entry from  keyspace_level_gc_map keyspace-id:{},keyspace-level-gc-safe-point{}",keyspace_level_gc_json.keyspace_id,keyspace_level_gc_json.safe_point);
                                    },
                                    Err(e) => error!("[keyspace level gc watch service] parse delete keyspace level gc event failed"; "name" => ?event.get_kv().get_key(), "err" => ?e),
                                }
                            }});
                    }
                    Err(PdError::DataCompacted(msg)) => {
                        error!("[keyspace level gc watch service] required revision has been compacted"; "err" => ?msg);
                        self.reload_all_keyspace_level_gc().await;
                        cancel.abort();
                        continue 'outer;
                    }
                    Err(err) => {
                        error!("[keyspace level gc watch service] failed to watch keyspace level gc safe point"; "err" => ?err);
                        let _ = GLOBAL_TIMER_HANDLE
                            .delay(std::time::Instant::now() + RETRY_INTERVAL)
                            .compat()
                            .await;
                        cancel.abort();
                        continue 'outer;
                    }
                }
            }
        }
    }

    fn get_keyspcace_level_gc_etcd_path(&self) -> String {
        let cluster_id = self.pd_client.get_cluster_id().unwrap();
        let keyspace_level_gc_prefix = format!("/pd/{}/keyspaces/gc_safe_point", cluster_id);
        keyspace_level_gc_prefix
    }

    async fn reload_all_keyspace_level_gc(&mut self) {
        let keyspace_level_gc_prefix = self.get_keyspcace_level_gc_etcd_path();
        loop {
            match self
                .meta_client
                .get(Get::of(keyspace_level_gc_prefix.as_str()).prefixed())
                .await
            {
                Ok(mut resp) => {
                    let kvs = resp.take_kvs().into_iter().collect::<Vec<_>>();
                    kvs.iter().for_each(|g| {
                        match serde_json::from_slice::<KeyspaceLevelGCJson>(
                            g.get_value(),
                        ) {
                            Ok(keyspace_level_gc_json) => {
                                self.keyspace_level_gc_map.insert(keyspace_level_gc_json.keyspace_id, keyspace_level_gc_json.safe_point);
                                debug!("[keyspace level gc watch service] update keyspace_level_gc_map keyspace-id:{},keyspace-level-gc-safe-point{}",keyspace_level_gc_json.keyspace_id,keyspace_level_gc_json.safe_point);
                            },
                            Err(e) => error!("[keyspace level gc watch service] parse put keyspace level gc event failed"; "name" => ?g.get_key(), "err" => ?e),
                        }
                    });

                    self.revision = resp.get_header().get_revision();
                    return;
                }
                Err(err) => {
                    error!("[keyspace level gc watch service] failed to get meta storage's keyspace level gc"; "err" => ?err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                        .compat()
                        .await;
                }
            }
        }
    }
}

// KeyspaceMetaWatchService used to watch keyspace meta change in etcd of PD,
// and update the mapping from keyspace id to keyspace meta.
#[derive(Clone)]
pub struct KeyspaceMetaWatchService {
    pd_client: Arc<RpcClient>,
    // wrap for etcd client.
    meta_client: Checked<Sourced<Arc<RpcClient>>>,
    // record watch revision.
    revision: i64,

    keyspace_id_meta_map: Arc<DashMap<u32, KeyspaceMeta>>,
}

impl KeyspaceMetaWatchService {
    pub fn new(
        pd_client: Arc<RpcClient>,
        keyspace_id_meta_map: Arc<DashMap<u32, KeyspaceMeta>>,
    ) -> KeyspaceMetaWatchService {
        KeyspaceMetaWatchService {
            revision: 0,
            meta_client: Checked::new(Sourced::new(
                Arc::clone(&pd_client.clone()),
                pd_client::meta_storage::Source::KeysapceMeta,
            )),
            pd_client,
            keyspace_id_meta_map,
        }
    }

    pub async fn watch_keyspace_meta(&mut self) {
        // Firstly, load all keyspace meta as of now.
        self.reload_all_keyspace_meta().await;
        let keyspace_meta_prefix = self.get_keyspace_meta_etcd_path();

        'outer: loop {
            // Secondly, start watcher at loading revision.
            let (mut stream, cancel) = stream::abortable(
                self.meta_client.watch(
                    Watch::of(keyspace_meta_prefix.as_str())
                        .prefixed()
                        .from_rev(self.revision)
                        .with_prev_kv(),
                ),
            );
            info!("[keyspace meta watch service] pd meta client creating watch stream."; "path" => keyspace_meta_prefix.clone(), "rev" => %self.revision);
            while let Some(grpc_response) = stream.next().await {
                match grpc_response {
                    Ok(resp) => {
                        self.revision = resp.get_header().get_revision();
                        let events = resp.get_events();
                        events.iter().for_each(|event| match event.get_type() {
                            EventEventType::Put => {
                                match protobuf::parse_from_bytes::<KeyspaceMeta>(event.get_kv().get_value()) {
                                    Ok(keyspace_meta) => {
                                        self.keyspace_id_meta_map.insert(keyspace_meta.id, keyspace_meta.clone());
                                        debug!("[keyspace meta watch service] update keyspace_id_meta_map keyspace-id:{},keyspace-meta{:?}",keyspace_meta.id,keyspace_meta);
                                    }
                                    Err(e) => error!("[keyspace meta watch service] parse put keyspace meta event failed"; "name" => ?event.get_kv().get_key(), "err" => ?e),
                                }
                            }
                            EventEventType::Delete => {
                                match protobuf::parse_from_bytes::<KeyspaceMeta>(event.get_kv().get_value()) {
                                    Ok(keyspace_meta) => {
                                        self.keyspace_id_meta_map.remove(&keyspace_meta.id);
                                        debug!("[keyspace meta watch service] remove entry from keyspace_id_meta_map cache keyspace-id:{},keyspace-meta{:?}",keyspace_meta.id,keyspace_meta);
                                    }
                                    Err(e) => error!("[keyspace meta service] parse delete keyspace meta event failed"; "name" => ?event.get_kv().get_key(), "err" => ?e),
                                }
                            }});
                    }
                    Err(PdError::DataCompacted(msg)) => {
                        error!("[keyspace meta watch service] required revision has been compacted"; "err" => ?msg);
                        self.reload_all_keyspace_meta().await;
                        cancel.abort();
                        continue 'outer;
                    }
                    Err(err) => {
                        error!("[keyspace meta watch service] failed to watch keyspace meta"; "err" => ?err);
                        let _ = GLOBAL_TIMER_HANDLE
                            .delay(std::time::Instant::now() + RETRY_INTERVAL)
                            .compat()
                            .await;
                        cancel.abort();
                        continue 'outer;
                    }
                }
            }
        }
    }

    fn get_keyspace_meta_etcd_path(&self) -> String {
        let cluster_id = self.pd_client.get_cluster_id().unwrap();
        let keyspace_meta_prefix = format!("/pd/{}/keyspaces/meta", cluster_id);
        keyspace_meta_prefix
    }

    // reload_all_keyspace_meta will load all keyspace meta from PD,
    // and update the mapping from keyspace id to keyspace meta.
    async fn reload_all_keyspace_meta(&mut self) {
        let keyspace_meta_path = self.get_keyspace_meta_etcd_path();
        loop {
            match self
                .meta_client
                .get(Get::of(keyspace_meta_path.as_str()).prefixed())
                .await
            {
                Ok(mut resp) => {
                    let kvs = resp.take_kvs().into_iter().collect::<Vec<_>>();
                    kvs.iter().for_each(|g| {
                        // Parse KeyspaceMeta from value.
                        match protobuf::parse_from_bytes::<KeyspaceMeta>(g.get_value()) {
                            Ok(keyspace_meta) => {
                                self.keyspace_id_meta_map.insert(keyspace_meta.id, keyspace_meta.clone());
                                info!("[keyspace meta watch service] update keyspace_id_meta_map keyspace-id:{},keyspace-meta{:?}",keyspace_meta.id,keyspace_meta);
                            }
                            Err(e) => error!("[keyspace meta watch service] parse keyspace meta failed"; "name" => ?g.get_key(), "err" => ?e),
                        }
                    });

                    self.revision = resp.get_header().get_revision();
                    return;
                }
                Err(err) => {
                    error!("[keyspace meta watch service] failed to get meta storage's keyspace meta"; "err" => ?err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                        .compat()
                        .await;
                }
            }
        }
    }
}

// KeyspaceLevelGCService is used to parse the user mvcc key,
// and through the keyspace id information in the key,
// combine the keyspace meta with the keyspace level GC cache
// to return the GC safe point for the keyspace.
#[derive(Clone)]
pub struct KeyspaceLevelGCService {
    keyspace_level_gc_map: Arc<DashMap<u32, u64>>,
    keyspace_id_meta_map: Arc<DashMap<u32, KeyspaceMeta>>,
}

impl KeyspaceLevelGCService {
    pub fn new(
        keyspace_level_gc_map: Arc<DashMap<u32, u64>>,
        keyspace_id_meta_map: Arc<DashMap<u32, KeyspaceMeta>>,
    ) -> KeyspaceLevelGCService {
        KeyspaceLevelGCService {
            keyspace_level_gc_map,
            keyspace_id_meta_map,
        }
    }

    // is_keyspace_use_global_gc_safe_point return true it means that keyspace use
    // global GC safe point.
    fn is_keyspace_use_global_gc_safe_point(&self, keyspace_id: u32) -> bool {
        let keyspace_meta_opt = self.keyspace_id_meta_map.get(&keyspace_id);
        match keyspace_meta_opt {
            None => {
                // We haven't got this keyspace meta yet, it will be updated
                // by KeyspaceMetaWatchService. So we can't use global GC safe point directly.
                // It should return false here.
                false
            }
            Some(keyspace_meta) => {
                let ks_gc_management_type =
                    keyspace_meta.config.get(KEYSPACE_CONFIG_KEY_GC_MGMT_TYPE);
                match ks_gc_management_type {
                    None => {
                        // Keyspace meta config doesn't set 'gc_management_type'.
                        // The default value of 'gc_management_type' is 'global_gc'.
                        // It should use global GC safe point directly.
                        true
                    }
                    Some(gc_management_type) => {
                        if gc_management_type == GC_MGMT_TYPE_GLOBAL_GC {
                            // The 'gc_management_type' is explicitly set to 'global_gc', return
                            // true.
                            return true;
                        }
                        // The 'gc_management_type' is explicitly set, but not equal to 'global_gc',
                        // returns false.
                        false
                    }
                }
            }
        }
    }

    // get_gc_safe_point_by_key is used to parse the user mvcc key,
    // and through the keyspace id information in the key,
    // combine the keyspace meta with the keyspace level GC cache
    // to return the GC safe point for the keyspace.
    pub fn get_gc_safe_point_by_key(&self, safe_point: u64, key: &[u8]) -> u64 {
        let keyspace_id_opt = ApiV2::get_u32_keyspace_id_by_key(key);
        match keyspace_id_opt {
            Some(keyspace_id) => {
                // API V2 with keyspace.
                let keyspace_gc_safe_point_opt = self.keyspace_level_gc_map.get(&keyspace_id);
                match keyspace_gc_safe_point_opt {
                    Some(keyspace_id_2_safe_point) => {
                        debug!(
                            "[keyspace level gc service] keyspace id:{}, can get keyspace level gc safe point:{}",
                            keyspace_id,
                            *keyspace_id_2_safe_point.value()
                        );
                        // If we can get the keyspace level GC safe point of the specified keyspace
                        // id, return this keyspace level GC safe point
                        // directly.
                        *keyspace_id_2_safe_point.value()
                    }
                    None => {
                        // Can't get keyspace level GC safe point from keyspace_level_gc_map.
                        let is_keyspace_use_global_gc_safe_point =
                            self.is_keyspace_use_global_gc_safe_point(keyspace_id);
                        if is_keyspace_use_global_gc_safe_point {
                            // keyspace use global GC.
                            debug!(
                                "[keyspace level gc service] keyspace id:{}, keyspace use global GC, return gc safe point:{}",
                                keyspace_id, safe_point
                            );
                            safe_point
                        } else {
                            // It is not certain to use global GC directly here,
                            // Maybe can not get keyspace meta,
                            // or can not get keyspace level GC safe point here,
                            // may be GC safe point of this keyspace hasn't been calculated or
                            // watched yet, so just return 0 here.
                            // Because we can't give an unsafe value greater than 0.
                            debug!(
                                "[keyspace level gc service] keyspace id:{}, keyspace can't get keyspace level GC, and don't use global GC, return gc safe point 0",
                                keyspace_id
                            );
                            0
                        }
                    }
                }
            }
            None => {
                // Api V1
                debug!(
                    "[keyspace level gc service] the key is in API V1 mode, use global GC directly, return global gc safe point:{}",
                    safe_point
                );
                safe_point
            }
        }
    }

    // is_all_keyspace_level_gc_have_not_initialized returns true
    // if there is no keyspace level gc > 0 in `keyspace_level_gc_map`.
    pub fn is_all_keyspace_level_gc_have_not_initialized(&self) -> bool {
        for kv in self.keyspace_level_gc_map.iter() {
            if *kv.value() > 0 {
                return false;
            }
        }
        true
    }

    // get_max_ts_of_all_ks_gc_safe_point is used to get the max ts
    // of all keyspace level GC safe point.
    // It only used in `check_need_gc` before create compaction filter now.
    pub fn get_max_ts_of_all_ks_gc_safe_point(&self) -> u64 {
        let mut max_ks_level_gc_sp = 0;
        for kv in self.keyspace_level_gc_map.iter() {
            let ks_level_gc = *kv.value();
            if ks_level_gc > max_ks_level_gc_sp {
                max_ks_level_gc_sp = ks_level_gc;
            }
        }
        debug!(
            "[keyspace meta service] max ts of all keyspace level GC safe point:{}",
            max_ks_level_gc_sp
        );
        max_ks_level_gc_sp
    }
}
