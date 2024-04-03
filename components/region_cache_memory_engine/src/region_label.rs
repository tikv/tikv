// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use futures::{
    compat::Future01CompatExt,
    stream::{self, StreamExt},
};
use kvproto::meta_storagepb::EventEventType;
use pd_client::{
    meta_storage::{Checked, Get, MetaStorageClient, Sourced, Watch},
    Error as PdError, PdClient, RpcClient, REGION_LABEL_PATH_PREFIX,
};
use serde::{Deserialize, Serialize};
use tikv_util::{error, info, timer::GLOBAL_TIMER_HANDLE};

/// RegionLabel is the label of a region. This struct is partially copied from
/// https://github.com/tikv/pd/blob/783d060861cef37c38cbdcab9777fe95c17907fe/server/schedule/labeler/rules.go#L31.
///
/// Convention: ranges that should always be cached by the in-memory engine
/// should be labeled with key "cache" set to value "always".
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegionLabel {
    pub key: String,
    pub value: String,
    pub ttl: Option<String>,
    pub start_at: Option<String>,
}

/// LabelRule is the rule to assign labels to a region. This struct is partially
/// copied from https://github.com/tikv/pd/blob/783d060861cef37c38cbdcab9777fe95c17907fe/server/schedule/labeler/rules.go#L41.
///
/// Note: `rule_type` should always be "key-range" for memory-engine use case.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LabelRule {
    pub id: String,
    pub labels: Vec<RegionLabel>,
    pub rule_type: String,
    pub data: Vec<KeyRangeRule>,
}

/// KeyRangeRule contains the start key and end key of the LabelRule. This
/// struct is partially copied from https://github.com/tikv/pd/blob/783d060861cef37c38cbdcab9777fe95c17907fe/server/schedule/labeler/rules.go#L62.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KeyRangeRule {
    pub start_key: String,
    pub end_key: String,
}

pub type RegionLabelAddedCb = Arc<dyn Fn(LabelRule) + Send + Sync>;
// Todo: more efficient way to do this for cache use case?
#[derive(Default)]
pub struct RegionLabelRulesManager {
    pub(crate) region_labels: DashMap<String, LabelRule>,
    pub(crate) region_label_added_cb: Option<RegionLabelAddedCb>,
}

impl RegionLabelRulesManager {
    pub fn add_region_label(&self, label_rule: LabelRule) {
        let _ = self
            .region_labels
            .insert(label_rule.id.clone(), label_rule.clone());
        if let Some(cb) = self.region_label_added_cb.as_ref() {
            cb(label_rule)
        }
    }

    pub fn region_labels(&self) -> Vec<LabelRule> {
        self.region_labels
            .iter()
            .map(|e| e.value().clone())
            .collect::<Vec<_>>()
    }

    pub fn remove_region_label(&self, label_rule_id: &String) {
        let _ = self.region_labels.remove(label_rule_id);
    }

    pub fn get_region_label(&self, label_rule_id: &str) -> Option<LabelRule> {
        self.region_labels
            .get(label_rule_id)
            .map(|r| r.value().clone())
    }
}

pub type RuleFilterFn = Arc<dyn Fn(&LabelRule) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct RegionLabelService {
    manager: Arc<RegionLabelRulesManager>,
    _pd_client: Arc<RpcClient>,
    meta_client: Checked<Sourced<Arc<RpcClient>>>,
    revision: i64,
    cluster_id: u64,
    path_suffix: Option<String>,
    rule_filter_fn: Option<RuleFilterFn>,
}

const RETRY_INTERVAL: Duration = Duration::from_secs(1); // to consistent with pd_client

pub struct RegionLabelServiceBuilder {
    manager: Arc<RegionLabelRulesManager>,
    pd_client: Arc<RpcClient>,
    path_suffix: Option<String>,
    rule_filter_fn: Option<RuleFilterFn>,
}

impl RegionLabelServiceBuilder {
    pub fn new(
        manager: Arc<RegionLabelRulesManager>,
        pd_client: Arc<RpcClient>,
    ) -> RegionLabelServiceBuilder {
        RegionLabelServiceBuilder {
            manager,
            pd_client,
            path_suffix: None,
            rule_filter_fn: None,
        }
    }

    pub fn path_suffix(mut self, suffix: String) -> Self {
        self.path_suffix = Some(suffix);
        self
    }

    pub fn rule_filter_fn(mut self, rule_filter_fn: RuleFilterFn) -> Self {
        self.rule_filter_fn = Some(rule_filter_fn);
        self
    }

    pub fn build(self) -> pd_client::Result<RegionLabelService> {
        let cluster_id = self.pd_client.get_cluster_id()?;
        Ok(RegionLabelService {
            cluster_id,
            manager: self.manager,
            revision: 0,
            meta_client: Checked::new(Sourced::new(
                Arc::clone(&self.pd_client.clone()),
                pd_client::meta_storage::Source::RegionLabel,
            )),
            _pd_client: self.pd_client,
            path_suffix: self.path_suffix,
            rule_filter_fn: self.rule_filter_fn,
        })
    }
}

impl RegionLabelService {
    fn region_label_path(&self) -> String {
        let path_suffix = self.path_suffix.clone();
        let path_suffix = path_suffix.unwrap_or_default();
        format!(
            "/pd/{}/{}{}",
            self.cluster_id, REGION_LABEL_PATH_PREFIX, path_suffix
        )
    }

    fn on_label_rule(&mut self, label_rule: &LabelRule) {
        let should_add_label = self
            .rule_filter_fn
            .as_ref()
            .map_or_else(|| true, |r_f_fn| r_f_fn(label_rule));
        if should_add_label {
            self.manager.add_region_label(label_rule.clone())
        }
    }
    pub async fn watch_region_labels(&mut self) {
        self.reload_all_region_labels().await;
        'outer: loop {
            let region_label_path = self.region_label_path();
            let (mut stream, cancel) = stream::abortable(
                self.meta_client.watch(
                    Watch::of(region_label_path.clone())
                        .prefixed()
                        .from_rev(self.revision)
                        .with_prev_kv(),
                ),
            );
            info!("pd meta client creating watch stream"; "path" => region_label_path, "rev" => %self.revision);
            while let Some(grpc_response) = stream.next().await {
                match grpc_response {
                    Ok(resp) => {
                        self.revision = resp.get_header().get_revision();
                        let events = resp.get_events();
                        events.iter().for_each(|event| match event.get_type() {
                            EventEventType::Put => {
                                match serde_json::from_slice::<LabelRule>(
                                    event.get_kv().get_value(),
                                ) {
                                    Ok(label_rule) => self.on_label_rule(&label_rule),
                                    Err(e) => error!("parse put region label event failed"; "name" => ?event.get_kv().get_key(), "err" => ?e),
                                }
                            }
                            EventEventType::Delete => {
                                match serde_json::from_slice::<LabelRule>(
                                    event.get_prev_kv().get_value()
                                ) {
                                    Ok(label_rule) => self.manager.remove_region_label(&label_rule.id),
                                    Err(e) => error!("parse delete region label event failed"; "name" => ?event.get_kv().get_key(), "err" => ?e),
                                }
                            }
                        });
                    }
                    Err(PdError::DataCompacted(msg)) => {
                        error!("required revision has been compacted"; "err" => ?msg);
                        self.reload_all_region_labels().await;
                        cancel.abort();
                        continue 'outer;
                    }
                    Err(err) => {
                        error!("failed to watch region labels"; "err" => ?err);
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

    async fn reload_all_region_labels(&mut self) {
        loop {
            match self
                .meta_client
                .get(Get::of(self.region_label_path()).prefixed())
                .await
            {
                Ok(mut resp) => {
                    let kvs = resp.take_kvs().into_iter().collect::<Vec<_>>();
                    for g in kvs.iter() {
                        match serde_json::from_slice::<LabelRule>(g.get_value()) {
                            Ok(label_rule) => self.on_label_rule(&label_rule),

                            Err(e) => {
                                error!("parse label rule failed"; "name" => ?g.get_key(), "err" => ?e);
                            }
                        }
                    }
                    return;
                }
                Err(err) => {
                    error!("failed to get meta storage's region label rules"; "err" => ?err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                        .compat()
                        .await;
                }
            }
        }
    }
}

#[cfg(test)]
pub mod tests {

    use futures::executor::block_on;
    use pd_client::meta_storage::{Delete, Put};
    use security::{SecurityConfig, SecurityManager};
    use test_pd::{mocker::MetaStorage, util::*, Server as MockServer};
    use tikv_util::{config::ReadableDuration, worker::Builder};

    use super::*;

    // Note: a test that runs against a local PD instance. This is for debugging
    // purposes only and is disabled by default. To run, remove `#[ignore]`
    // line below.
    #[ignore]
    #[test]
    fn local_crud_test() {
        let region_label_manager = RegionLabelRulesManager::default();
        let config = pd_client::Config {
            endpoints: vec!["127.0.0.1:2379".to_string()],
            ..Default::default()
        };
        let rpc_client = RpcClient::new(
            &config,
            None,
            Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap()),
        )
        .unwrap();
        let region_label_manager_arc = Arc::new(region_label_manager);

        let mut service = RegionLabelServiceBuilder::new(
            Arc::clone(&region_label_manager_arc),
            Arc::new(rpc_client),
        )
        .build()
        .unwrap();
        block_on(async move { service.reload_all_region_labels().await });
        let region_labels = region_label_manager_arc.region_labels();
        assert!(!region_labels.is_empty());
    }

    fn new_test_server_and_client(
        update_interval: ReadableDuration,
    ) -> (MockServer<MetaStorage>, RpcClient) {
        let server = MockServer::with_case(1, Arc::<MetaStorage>::default());
        let eps = server.bind_addrs();
        let client = new_client_with_update_interval(eps, None, update_interval);
        (server, client)
    }

    fn add_region_label_rule(
        meta_client: Checked<Sourced<Arc<RpcClient>>>,
        cluster_id: u64,
        label_rule: LabelRule,
    ) {
        let id = &label_rule.id;
        let key = format!("/pd/{}/{}/{}", cluster_id, REGION_LABEL_PATH_PREFIX, id);
        let buf = serde_json::to_vec::<LabelRule>(&label_rule).unwrap();
        block_on(async move { meta_client.put(Put::of(key, buf)).await }).unwrap();
    }

    fn delete_region_label_rule(
        meta_client: Checked<Sourced<Arc<RpcClient>>>,
        cluster_id: u64,
        id: &str,
    ) {
        let key = format!("/pd/{}/{}/{}", cluster_id, REGION_LABEL_PATH_PREFIX, id);
        block_on(async move { meta_client.delete(Delete::of(key)).await }).unwrap();
    }

    fn new_region_label_rule(id: &str, start_key: &str, end_key: &str) -> LabelRule {
        LabelRule {
            id: id.to_string(),
            labels: vec![RegionLabel {
                key: "cache".to_string(),
                value: "always".to_string(),
                ..RegionLabel::default()
            }],
            rule_type: "key-range".to_string(),
            data: vec![KeyRangeRule {
                start_key: start_key.to_string(),
                end_key: end_key.to_string(),
            }],
        }
    }

    #[test]
    fn crud_test() {
        let (mut server, client) = new_test_server_and_client(ReadableDuration::millis(100));
        let region_label_manager = RegionLabelRulesManager::default();
        let cluster_id = client.get_cluster_id().unwrap();
        let mut s =
            RegionLabelServiceBuilder::new(Arc::new(region_label_manager), Arc::new(client))
                .build()
                .unwrap();
        block_on(s.reload_all_region_labels());
        assert_eq!(s.manager.region_labels().len(), 0);
        add_region_label_rule(
            s.meta_client.clone(),
            cluster_id,
            new_region_label_rule("cache/0", "a", "b"),
        );
        block_on(s.reload_all_region_labels());
        assert_eq!(s.manager.region_labels().len(), 1);

        server.stop();
    }

    #[test]
    fn watch_test() {
        let (mut server, client) = new_test_server_and_client(ReadableDuration::millis(100));
        let region_label_manager = RegionLabelRulesManager::default();
        let cluster_id = client.get_cluster_id().unwrap();
        let mut s =
            RegionLabelServiceBuilder::new(Arc::new(region_label_manager), Arc::new(client))
                .build()
                .unwrap();
        block_on(s.reload_all_region_labels());
        assert_eq!(s.manager.region_labels().len(), 0);

        let wait_watch_ready = |s: &RegionLabelService, count: usize| {
            for _i in 0..100 {
                if s.manager.region_labels().len() == count {
                    return;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            panic!(
                "wait timed out, expected: {}, got: {}",
                count,
                s.manager.region_labels().len()
            );
        };

        let background_worker = Builder::new("background").thread_count(1).create();
        let mut s_clone = s.clone();
        background_worker.spawn_async_task(async move {
            s_clone.watch_region_labels().await;
        });

        add_region_label_rule(
            s.meta_client.clone(),
            cluster_id,
            new_region_label_rule("cache/0", "a", "b"),
        );
        add_region_label_rule(
            s.meta_client.clone(),
            cluster_id,
            new_region_label_rule("cache/1", "c", "d"),
        );
        add_region_label_rule(
            s.meta_client.clone(),
            cluster_id,
            new_region_label_rule("cache/2", "e", "f"),
        );

        wait_watch_ready(&s, 3);

        delete_region_label_rule(s.meta_client.clone(), cluster_id, "cache/0");

        wait_watch_ready(&s, 2);
        let labels = s.manager.region_labels();
        assert_eq!(labels.len(), 2);
        assert!(s.manager.get_region_label("cache/0").is_none());
        let label = s.manager.get_region_label("cache/1").unwrap();
        assert_eq!(label.data[0].start_key, "c".to_string());

        server.stop();
    }
}
