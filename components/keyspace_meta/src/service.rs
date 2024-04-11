// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use dashmap::DashMap;

use futures::{compat::Future01CompatExt, stream, StreamExt};
use kvproto::{keyspacepb, meta_storagepb::EventEventType, resource_manager::{ResourceGroup, TokenBucketRequest, TokenBucketsRequest}};
use kvproto::keyspacepb::KeyspaceMeta;
use pd_client::{
    meta_storage::{Checked, Get, MetaStorageClient, Sourced, Watch},
    Error as PdError, PdClient, RpcClient, RESOURCE_CONTROL_CONFIG_PATH,
    RESOURCE_CONTROL_CONTROLLER_CONFIG_PATH,
};
use serde::{Deserialize, Serialize};
use tikv_util::{error, info, timer::GLOBAL_TIMER_HANDLE};

const RETRY_INTERVAL: Duration = Duration::from_secs(1); // to consistent with pd_client

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
struct KeyspaceLevelGC {
    keyspace_id: u32,
    safe_point: u64,
}

impl KeyspaceLevelGCWatchService {
    /// Constructs a new `Service` with `ResourceGroupManager` and a
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
            keyspace_level_gc_map:keyspace_level_gc,
        }
    }

    pub async fn watch_keyspace_gc(&mut self) {
        info!("[test-yjy]watch_keyspace_gc");
        // Firstly, load all resource groups as of now.
        self.reload_all_keyspace_level_gc().await;
        let keyspace_level_gc_prefix=self.get_keyspcace_level_gc_prefix();

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
                                info!("[test-yjy]EventEventType::Put01");
                                let val=event.get_kv().get_value();
                                match serde_json::from_slice::<KeyspaceLevelGC>(
                                    event.get_kv().get_value(),
                                ) {
                                    Ok(keyspace_level_gc) => {
                                        self.keyspace_level_gc_map.insert(keyspace_level_gc.keyspace_id, keyspace_level_gc.safe_point);
                                        info!("[test-yjy]EventEventType::Put02-01 {},{}",keyspace_level_gc.keyspace_id,keyspace_level_gc.safe_point);
                                    },
                                    Err(e) => error!("parse put keyspace level gc event failed";  "err" => ?e),
                                }
                            }
                            EventEventType::Delete => {
                                info!("[test-yjy]EventEventType::Delete01");
                                match serde_json::from_slice::<KeyspaceLevelGC>(
                                    event.get_kv().get_value(),
                                ) {
                                    Ok(keyspace_level_gc) => {
                                        self.keyspace_level_gc_map.remove(&keyspace_level_gc.keyspace_id);
                                        info!("[test-yjy]EventEventType::Put02-01");
                                    },
                                    Err(e) => error!("parse delete keyspace level gc event failed"; "name" => ?event.get_kv().get_key(), "err" => ?e),
                                }
                            }});
                    }
                    Err(PdError::DataCompacted(msg)) => {
                        error!("required revision has been compacted"; "err" => ?msg);
                        //self.reload_all_resource_groups().await;
                        info!("[test-yjy] PdError::DataCompacted");
                        self.reload_all_keyspace_level_gc().await;
                        cancel.abort();
                        continue 'outer;
                    }
                    Err(err) => {
                        error!("failed to watch resource groups"; "err" => ?err);
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

    fn get_keyspcace_level_gc_prefix(&self) -> String {
        let cluster_id = self.pd_client
            .get_cluster_id()
            .unwrap();
        let keyspace_level_gc_prefix=format!("/pd/{}/keyspaces/gc_safe_point", cluster_id);
        return keyspace_level_gc_prefix;
    }

    async fn reload_all_keyspace_level_gc(&mut self) {
        let keyspace_level_gc_prefix=self.get_keyspcace_level_gc_prefix();
        loop {
            match self
                .meta_client
                .get(Get::of(keyspace_level_gc_prefix.as_str()).prefixed())
                .await
            {
                Ok(mut resp) => {
                    let kvs = resp.take_kvs().into_iter().collect::<Vec<_>>();
                    kvs.iter().for_each(|g| {
                        match serde_json::from_slice::<KeyspaceLevelGC>(
                            g.get_value(),
                        ) {
                            Ok(keyspace_level_gc) => {self.keyspace_level_gc_map.insert(keyspace_level_gc.keyspace_id, keyspace_level_gc.safe_point);info!("[test-yjy]EventEventType::Put02-01 {},{}",keyspace_level_gc.keyspace_id,keyspace_level_gc.safe_point);},
                            Err(e) => error!("parse put keyspace level gc event failed"; "name" => ?g.get_key(), "err" => ?e),
                        }
                    });

                    self.revision = resp.get_header().get_revision();
                    return;
                }
                Err(err) => {
                    error!("failed to get meta storage's keyspace level gc"; "err" => ?err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                        .compat()
                        .await;
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct KeyspaceMetaWatchService {
    pd_client: Arc<RpcClient>,
    // wrap for etcd client.
    meta_client: Checked<Sourced<Arc<RpcClient>>>,
    // record watch revision.
    revision: i64,

    keyspace_id_meta_map: Arc<DashMap<u32, keyspacepb::KeyspaceMeta>>,
}

impl KeyspaceMetaWatchService {
    pub fn new(
        pd_client: Arc<RpcClient>,
        keyspace_id_meta_map: Arc<DashMap<u32, keyspacepb::KeyspaceMeta>>,
    ) -> KeyspaceMetaWatchService {
        KeyspaceMetaWatchService {
            revision: 0,
            meta_client: Checked::new(Sourced::new(
                Arc::clone(&pd_client.clone()),
                pd_client::meta_storage::Source::KeysapceMeta,
            )),
            pd_client,
            keyspace_id_meta_map:keyspace_id_meta_map,
        }
    }

    pub async fn watch_keyspace_meta(&mut self) {
        info!("[test-yjy]watch_keyspace_meta");
        // Firstly, load all resource groups as of now.
        self.reload_all_keyspace_meta().await;
        let keyspace_meta_prefix=self.get_keyspace_meta_prefix();

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
            info!("pd meta client creating watch stream."; "path" => keyspace_meta_prefix.clone(), "rev" => %self.revision);
            while let Some(grpc_response) = stream.next().await {
                match grpc_response {
                    Ok(resp) => {
                        self.revision = resp.get_header().get_revision();
                        let events = resp.get_events();
                        events.iter().for_each(|event| match event.get_type() {
                            EventEventType::Put => {
                                info!("[test-yjy]EventEventType::Put01");
                                let val=event.get_kv().get_value();
                                match protobuf::parse_from_bytes::<KeyspaceMeta>(event.get_kv().get_value()) {
                                    Ok(keyspace_meta) => {
                                        self.keyspace_id_meta_map.insert(keyspace_meta.id, keyspace_meta.clone());
                                        info!("[test-yjy] watch_keyspace_meta EventEventType::Put02-01 {},{:?}",keyspace_meta.id,keyspace_meta);
                                    }
                                    Err(e) => error!("parse put resource group event failed"; "name" => ?event.get_kv().get_key(), "err" => ?e),
                                }

                            }
                            EventEventType::Delete => {
                                info!("[test-yjy]EventEventType::Delete01");
                                match protobuf::parse_from_bytes::<KeyspaceMeta>(event.get_kv().get_value()) {
                                    Ok(keyspace_meta) => {
                                        self.keyspace_id_meta_map.remove(&keyspace_meta.id);
                                        info!("[test-yjy] watch_keyspace_meta EventEventType::delete-01 {},{:?}",keyspace_meta.id,keyspace_meta);
                                    }
                                    Err(e) => error!("parse put resource group event failed"; "name" => ?event.get_kv().get_key(), "err" => ?e),
                                }
                            }});
                    }
                    Err(PdError::DataCompacted(msg)) => {
                        error!("required revision has been compacted"; "err" => ?msg);
                        //self.reload_all_resource_groups().await;
                        info!("[test-yjy] PdError::DataCompacted");
                        self.reload_all_keyspace_meta().await;
                        cancel.abort();
                        continue 'outer;
                    }
                    Err(err) => {
                        error!("failed to watch resource groups"; "err" => ?err);
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

    fn get_keyspace_meta_prefix(&self) -> String {
        let cluster_id = self.pd_client
            .get_cluster_id()
            .unwrap();
        let keyspace_meta_prefix=format!("/pd/{}/keyspaces/meta", cluster_id);
        return keyspace_meta_prefix;
    }

    async fn reload_all_keyspace_meta(&mut self) {
        let keyspace_meta_prefix=self.get_keyspace_meta_prefix();
        loop {
            match self
                .meta_client
                .get(Get::of(keyspace_meta_prefix.as_str()).prefixed())
                .await
            {
                Ok(mut resp) => {
                    let kvs = resp.take_kvs().into_iter().collect::<Vec<_>>();
                    kvs.iter().for_each(|g| {
                        match protobuf::parse_from_bytes::<KeyspaceMeta>(g.get_value()) {
                            Ok(keyspace_meta) => {
                                self.keyspace_id_meta_map.insert(keyspace_meta.id, keyspace_meta.clone());
                                info!("[test-yjy] watch_keyspace_meta EventEventType::Put02-01 {},{:?}",keyspace_meta.id,keyspace_meta);
                            }
                            Err(e) => error!("parse keyspace meta failed"; "name" => ?g.get_key(), "err" => ?e),
                        }
                    });

                    self.revision = resp.get_header().get_revision();
                    return;
                }
                Err(err) => {
                    error!("failed to get meta storage's keyspace meta"; "err" => ?err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                        .compat()
                        .await;
                }
            }
        }
    }
}