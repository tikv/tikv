// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use futures::compat::Future01CompatExt;
use pd_client::{
    meta_storage::{Checked, Get, MetaStorageClient, Sourced},
    PdClient, RpcClient, REGION_LABEL_PATH_PREFIX,
};
use serde::{Deserialize, Serialize};
use tikv_util::{error, timer::GLOBAL_TIMER_HANDLE};

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegionLabel {
    pub key: String,
    pub value: String,
    pub ttl: Option<String>,
    pub start_at: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct LabelRule {
    pub id: String,
    pub labels: Vec<RegionLabel>,
    pub rule_type: String,
    pub ttl: Option<String>,
    pub start_at: Option<String>,
    pub data: Vec<KeyRangeRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KeyRangeRule {
    pub start_key: String,
    pub end_key: String,
}

// Todo: more efficient way to do this for cache use case?
#[derive(Default)]
pub struct RegionLabelRulesManager {
    pub(crate) region_labels: DashMap<String, LabelRule>,
}

impl RegionLabelRulesManager {
    pub fn add_region_label(&self, label_rule: LabelRule) {
        let _ = self.region_labels.insert(label_rule.id.clone(), label_rule);
    }

    pub fn region_labels(&self) -> Vec<LabelRule> {
        self.region_labels
            .iter()
            .map(|e| e.value().clone())
            .collect::<Vec<_>>()
    }
}

pub type RuleFilterFn = Box<dyn Fn(&LabelRule) -> bool + Send + Sync>;

pub struct RegionLabelService {
    manager: Arc<RegionLabelRulesManager>,
    pd_client: Arc<RpcClient>,
    meta_client: Checked<Sourced<Arc<RpcClient>>>,
    revision: i64,
    path_suffix: Option<String>,
    rule_filter_fn: Option<RuleFilterFn>,
}

const RETRY_INTERVAL: Duration = Duration::from_secs(1); // to consistent with pd_client

// Todo: provide ways to restrict this.
impl RegionLabelService {
    pub fn new(
        manager: Arc<RegionLabelRulesManager>,
        pd_client: Arc<RpcClient>,
    ) -> RegionLabelService {
        RegionLabelService {
            manager,
            revision: 0,
            path_suffix: None,
            rule_filter_fn: None,
            meta_client: Checked::new(Sourced::new(
                Arc::clone(&pd_client.clone()),
                pd_client::meta_storage::Source::RegionLabel,
            )),
            pd_client,
        }
    }

    fn region_label_path(&self) -> String {
        let cluster_id = self.pd_client.get_cluster_id().unwrap();
        let path_suffix = self.path_suffix.clone();
        let path_suffix = path_suffix.unwrap_or_default();
        format!(
            "/pd/{}/{}{}",
            cluster_id, REGION_LABEL_PATH_PREFIX, path_suffix
        )
    }

    async fn reload_all_region_labels(&mut self) {
        loop {
            let cluster_id = self.pd_client.get_cluster_id().unwrap();
            match self
                .meta_client
                .get(Get::of(self.region_label_path()).prefixed())
                .await
            {
                Ok(mut resp) => {
                    let kvs = resp.take_kvs().into_iter().collect::<Vec<_>>();
                    for g in kvs.iter() {
                        match serde_json::from_slice::<LabelRule>(g.get_value()) {
                            Ok(label_rule) => {
                                let should_add_label = self
                                    .rule_filter_fn
                                    .as_ref()
                                    .map_or_else(|| true, |r_f_fn| r_f_fn(&label_rule));
                                if should_add_label {
                                    self.manager.add_region_label(label_rule)
                                }
                            }

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

    use security::{SecurityConfig, SecurityManager};

    use super::*;

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

        let mut service =
            RegionLabelService::new(Arc::clone(&region_label_manager_arc), Arc::new(rpc_client));
        futures::executor::block_on(async move { service.reload_all_region_labels().await });
        let region_labels = region_label_manager_arc.region_labels();
        assert!(!region_labels.is_empty());
    }
}
