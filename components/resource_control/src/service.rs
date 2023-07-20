// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    ops::Sub,
    sync::Arc,
    time::Duration,
};

use futures::{compat::Future01CompatExt, StreamExt};
use kvproto::{
    pdpb::EventType,
    resource_manager::{Consumption, ResourceGroup, TokenBucketRequest, TokenBucketsRequest},
};
use pd_client::{
    Error as PdError, PdClient, RpcClient, RESOURCE_CONTROL_CONFIG_PATH,
    RESOURCE_CONTROL_CONTROLLER_CONFIG_PATH,
};
use serde::{Deserialize, Serialize};
use tikv_util::{error, info, timer::GLOBAL_TIMER_HANDLE};

use crate::{
    resource_limiter::{GroupStatistics, ResourceType},
    worker::GroupStats,
    ResourceGroupManager,
};

#[derive(Clone)]
pub struct ResourceManagerService {
    manager: Arc<ResourceGroupManager>,
    pd_client: Arc<RpcClient>,
    // record watch revision
    revision: i64,
}

impl ResourceManagerService {
    /// Constructs a new `Service` with `ResourceGroupManager` and a `RpcClient`
    pub fn new(
        manager: Arc<ResourceGroupManager>,
        pd_client: Arc<RpcClient>,
    ) -> ResourceManagerService {
        ResourceManagerService {
            pd_client,
            manager,
            revision: 0,
        }
    }
}

const RETRY_INTERVAL: Duration = Duration::from_secs(1); // to consistent with pd_client
pub const BACKGROUND_RU_UPLOAD_DURATION: Duration = Duration::from_secs(5); // to consistent with pd_client

impl ResourceManagerService {
    pub async fn watch_resource_groups(&mut self) {
        'outer: loop {
            // Firstly, load all resource groups as of now.
            self.reload_all_resource_groups().await;
            // Secondly, start watcher at loading revision.
            loop {
                match self
                    .pd_client
                    .watch_global_config(RESOURCE_CONTROL_CONFIG_PATH.to_string(), self.revision)
                {
                    Ok(mut stream) => {
                        while let Some(grpc_response) = stream.next().await {
                            match grpc_response {
                                Ok(r) => {
                                    self.revision = r.get_revision();
                                    r.get_changes()
                                        .iter()
                                        .for_each(|item| match item.get_kind() {
                                            EventType::Put => {
                                                match protobuf::parse_from_bytes::<ResourceGroup>(
                                                    item.get_payload(),
                                                ) {
                                                    Ok(mut group) => {
                                                        // TODO: for now just for test
                                                        group.mut_background_settings().mut_job_types().push("lightning".to_owned());
                                                        self.manager.add_resource_group(group);
                                                    }
                                                    Err(e) => {
                                                        error!("parse put resource group event failed"; "name" => item.get_name(), "err" => ?e);
                                                    }
                                                }
                                            }
                                            EventType::Delete => {
                                                match protobuf::parse_from_bytes::<ResourceGroup>(
                                                    item.get_payload(),
                                                ) {
                                                    Ok(group) => {
                                                        self.manager.remove_resource_group(group.get_name());
                                                    }
                                                    Err(e) => {
                                                        error!("parse delete resource group event failed"; "name" => item.get_name(), "err" => ?e);
                                                    }
                                                }
                                            }
                                        });
                                }
                                Err(err) => {
                                    error!("failed to get stream"; "err" => ?err);
                                    let _ = GLOBAL_TIMER_HANDLE
                                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                                        .compat()
                                        .await;
                                }
                            }
                        }
                    }
                    Err(PdError::DataCompacted(msg)) => {
                        error!("required revision has been compacted"; "err" => ?msg);
                        continue 'outer;
                    }
                    Err(err) => {
                        error!("failed to watch resource groups"; "err" => ?err);
                        let _ = GLOBAL_TIMER_HANDLE
                            .delay(std::time::Instant::now() + RETRY_INTERVAL)
                            .compat()
                            .await;
                    }
                }
            }
        }
    }

    async fn reload_all_resource_groups(&mut self) {
        loop {
            match self
                .pd_client
                .load_global_config(RESOURCE_CONTROL_CONFIG_PATH.to_string())
                .await
            {
                Ok((items, revision)) => {
                    let mut vaild_groups = HashSet::with_capacity(items.len());
                    items.iter().for_each(|g| {
                        match protobuf::parse_from_bytes::<ResourceGroup>(g.get_payload()) {
                            Ok(mut rg) => {
                                // TODO: for now just for test
                                rg.mut_background_settings().mut_job_types().push("lightning".to_owned());
                                vaild_groups.insert(rg.get_name().to_ascii_lowercase());
                                self.manager.add_resource_group(rg);
                            }
                            Err(e) => {
                                error!("parse resource group failed"; "name" => g.get_name(), "err" => ?e);
                            }
                        }
                    });

                    self.manager.retain(|name, _g| vaild_groups.contains(name));
                    self.revision = revision;
                    return;
                }
                Err(err) => {
                    error!("failed to load global config"; "err" => ?err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                        .compat()
                        .await;
                }
            }
        }
    }

    async fn load_controller_config(&self) -> Result<RequestUnitConfig, ()> {
        loop {
            match self
                .pd_client
                .load_global_config(RESOURCE_CONTROL_CONTROLLER_CONFIG_PATH.to_string())
                .await
            {
                Ok((items, _)) => {
                    match serde_json::from_slice::<ControllerConfig>(items[0].get_payload()) {
                        Ok(c) => return Ok(c.request_unit),
                        Err(e) => {
                            error!("parse controller config failed"; "err" => ?e);
                            let _ = GLOBAL_TIMER_HANDLE
                                .delay(std::time::Instant::now() + RETRY_INTERVAL)
                                .compat()
                                .await;
                            continue;
                        }
                    }
                }
                Err(err) => {
                    error!("failed to controller config"; "err" => ?err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                        .compat()
                        .await;
                    continue;
                }
            }
        }
    }

    // upload ru metrics
    pub async fn upload_ru_metrics(&self) {
        let mut last_consumption_map: HashMap<String, Consumption> = HashMap::new();
        // load controller config firstly
        let config = self.load_controller_config().await.unwrap_or_default();

        loop {
            let background_groups: Vec<_> = self
                .manager
                .resource_groups
                .iter()
                .filter_map(|kv| {
                    let g = kv.value();
                    g.limiter.as_ref().map(|limiter| GroupStats {
                        name: g.group.name.clone(),
                        ru_quota: g.get_ru_quota() as f64,
                        limiter: limiter.clone(),
                        stats_per_sec: GroupStatistics::default(),
                        expect_cost_rate: 0.0,
                    })
                })
                .collect();
            if background_groups.is_empty() {
                let _ = GLOBAL_TIMER_HANDLE
                    .delay(std::time::Instant::now() + BACKGROUND_RU_UPLOAD_DURATION)
                    .compat()
                    .await;
                continue;
            }

            info!("[upload ru metrics] background_groups len is"; "background_groups" => background_groups.len());
            let mut req = TokenBucketsRequest::default();
            let all_reqs = req.mut_requests();
            background_groups.iter().for_each(|g| {
                let mut req = TokenBucketRequest::default();
                req.set_resource_group_name(g.name.clone());
                req.set_is_background(true);
                // update statistics
                let report_consumption = req.mut_consumption_since_last_request();
                if let Some(last_consumption) = last_consumption_map.get_mut(g.name.as_str()) {
                    let cpu_consumed = g
                        .limiter
                        .get_limiter(ResourceType::Cpu)
                        .get_statistics()
                        .total_consumed as f64;
                    let io_consumed = g.limiter.get_limiter(ResourceType::Io).get_statistics();
                    let read_bytes_consumed = io_consumed.read_consumed as f64;
                    let write_bytes_consumed = io_consumed.write_consumed as f64;

                    let read_total = config.cpu_ms_cost * cpu_consumed
                        + config.read_cost_per_byte * read_bytes_consumed
                        + config.read_per_batch_base_cost * DEFAULT_AVG_BATCH_PROPORTION;
                    let write_total = config.write_cost_per_byte * write_bytes_consumed
                        + config.write_per_batch_base_cost * DEFAULT_AVG_BATCH_PROPORTION;

                    let cur_rru = read_total.sub(last_consumption.get_r_r_u()).max(0.0);
                    let cur_wru = write_total.sub(last_consumption.get_w_r_u()).max(0.0);
                    let cur_read_bytes = read_bytes_consumed
                        .sub(last_consumption.get_read_bytes())
                        .max(0.0);
                    let cur_write_bytes = write_bytes_consumed
                        .sub(last_consumption.get_write_bytes())
                        .max(0.0);
                    let cur_time = cpu_consumed
                        .sub(last_consumption.get_total_cpu_time_ms())
                        .max(0.0);

                    report_consumption.set_r_r_u(cur_rru);
                    report_consumption.set_read_bytes(cur_read_bytes);
                    report_consumption.set_w_r_u(cur_wru);
                    report_consumption.set_write_bytes(cur_write_bytes);
                    report_consumption.set_total_cpu_time_ms(cur_time);

                    last_consumption.set_r_r_u(read_total);
                    last_consumption.set_read_bytes(read_bytes_consumed);
                    last_consumption.set_w_r_u(write_total);
                    last_consumption.set_write_bytes(write_bytes_consumed);
                    last_consumption.set_total_cpu_time_ms(cpu_consumed);
                } else {
                    last_consumption_map.insert(g.name.clone(), report_consumption.clone());
                }

                all_reqs.push(req);
            });

            info!("[upload ru metrics] all_reqs is"; "all_reqs" => ?all_reqs);
            if let Err(e) = self.pd_client.upload_ru_metrics(req).await {
                error!("upload ru metrics failed"; "err" => ?e);
            }

            let _ = GLOBAL_TIMER_HANDLE
                .delay(std::time::Instant::now() + BACKGROUND_RU_UPLOAD_DURATION)
                .compat()
                .await;
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(default)]
struct RequestUnitConfig {
    #[serde(rename = "read-base-cost")]
    read_base_cost: f64,
    #[serde(rename = "read-per-batch-base-cost")]
    read_per_batch_base_cost: f64,
    #[serde(rename = "read-cost-per-byte")]
    read_cost_per_byte: f64,
    #[serde(rename = "write-base-cost")]
    write_base_cost: f64,
    #[serde(rename = "write-per-batch-base-cost")]
    write_per_batch_base_cost: f64,
    #[serde(rename = "write-cost-per-byte")]
    write_cost_per_byte: f64,
    #[serde(rename = "read-cpu-ms-cost")]
    cpu_ms_cost: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ControllerConfig {
    #[serde(rename = "degraded-mode-wait-duration")]
    degraded_mode_wait_duration: String,
    #[serde(rename = "request-unit")]
    request_unit: RequestUnitConfig,
}

const DEFAULT_AVG_BATCH_PROPORTION: f64 = 0.7;
impl Default for RequestUnitConfig {
    fn default() -> Self {
        Self {
            // related on doc https://docs.pingcap.com/tidb/dev/tidb-resource-control#what-is-request-unit-ru.
            read_base_cost: 1. / 8.,
            read_per_batch_base_cost: 1. / 2.,
            read_cost_per_byte: 1. / (64. * 1024.),
            write_base_cost: 1.,
            write_per_batch_base_cost: 1.,
            write_cost_per_byte: 1. / 1024.,
            cpu_ms_cost: 1. / 3.,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use futures::executor::block_on;
    use kvproto::pdpb::GlobalConfigItem;
    use pd_client::RpcClient;
    use protobuf::Message;
    use test_pd::{mocker::Service, util::*, Server as MockServer};
    use tikv_util::{config::ReadableDuration, worker::Builder};

    use crate::resource_group::tests::{new_resource_group, new_resource_group_ru};

    fn new_test_server_and_client(
        update_interval: ReadableDuration,
    ) -> (MockServer<Service>, RpcClient) {
        let server = MockServer::new(1);
        let eps = server.bind_addrs();
        let client = new_client_with_update_interval(eps, None, update_interval);
        (server, client)
    }

    fn add_resource_group(pd_client: Arc<RpcClient>, group: ResourceGroup) {
        let mut item = GlobalConfigItem::default();
        item.set_kind(EventType::Put);
        item.set_name(group.get_name().to_string());
        let mut buf = Vec::new();
        group.write_to_vec(&mut buf).unwrap();
        item.set_payload(buf);

        futures::executor::block_on(async move {
            pd_client
                .store_global_config(RESOURCE_CONTROL_CONFIG_PATH.to_string(), vec![item])
                .await
        })
        .unwrap();
    }

    fn delete_resource_group(pd_client: Arc<RpcClient>, name: &str) {
        let mut item = GlobalConfigItem::default();
        item.set_kind(EventType::Delete);
        item.set_name(name.to_string());

        futures::executor::block_on(async move {
            pd_client
                .store_global_config(RESOURCE_CONTROL_CONFIG_PATH.to_string(), vec![item])
                .await
        })
        .unwrap();
    }

    use super::*;
    #[test]
    fn crud_config_test() {
        let (mut server, client) = new_test_server_and_client(ReadableDuration::millis(100));
        let resource_manager = ResourceGroupManager::default();

        let mut s = ResourceManagerService::new(Arc::new(resource_manager), Arc::new(client));
        assert_eq!(s.manager.get_all_resource_groups().len(), 1);
        let group = new_resource_group("TEST".into(), true, 100, 100, 0);
        add_resource_group(s.pd_client.clone(), group);
        block_on(s.reload_all_resource_groups());
        assert_eq!(s.manager.get_all_resource_groups().len(), 2);
        assert_eq!(s.revision, 1);

        delete_resource_group(s.pd_client.clone(), "TEST");
        block_on(s.reload_all_resource_groups());
        assert_eq!(s.manager.get_all_resource_groups().len(), 1);
        assert_eq!(s.revision, 2);

        server.stop();
    }

    #[test]
    fn watch_config_test() {
        let (mut server, client) = new_test_server_and_client(ReadableDuration::millis(100));
        let resource_manager = ResourceGroupManager::default();

        let mut s = ResourceManagerService::new(Arc::new(resource_manager), Arc::new(client));
        block_on(s.reload_all_resource_groups());
        assert_eq!(s.manager.get_all_resource_groups().len(), 1);
        assert_eq!(s.revision, 0);

        // TODO: find a better way to observe the watch is ready.
        let wait_watch_ready = |s: &ResourceManagerService, count: usize| {
            for _i in 0..100 {
                if s.manager.get_all_resource_groups().len() == count {
                    return;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            panic!(
                "wait time out, expectd: {}, got: {}",
                count,
                s.manager.get_all_resource_groups().len()
            );
        };

        let background_worker = Builder::new("background").thread_count(1).create();
        let mut s_clone = s.clone();
        background_worker.spawn_async_task(async move {
            s_clone.watch_resource_groups().await;
        });
        // Mock add
        let group1 = new_resource_group_ru("TEST1".into(), 100, 0);
        add_resource_group(s.pd_client.clone(), group1);
        let group2 = new_resource_group_ru("TEST2".into(), 100, 0);
        add_resource_group(s.pd_client.clone(), group2);
        // Mock modify
        let group2 = new_resource_group_ru("TEST2".into(), 50, 0);
        add_resource_group(s.pd_client.clone(), group2);
        wait_watch_ready(&s, 3);

        // Mock delete
        delete_resource_group(s.pd_client.clone(), "TEST1");

        // Wait for watcher
        wait_watch_ready(&s, 2);
        let groups = s.manager.get_all_resource_groups();
        assert_eq!(groups.len(), 2);
        assert!(s.manager.get_resource_group("TEST1").is_none());
        let group = s.manager.get_resource_group("TEST2").unwrap();
        assert_eq!(group.get_ru_quota(), 50);
        server.stop();
    }

    #[test]
    fn reboot_watch_server_test() {
        let (mut server, client) = new_test_server_and_client(ReadableDuration::millis(100));
        let resource_manager = ResourceGroupManager::default();

        let s = ResourceManagerService::new(Arc::new(resource_manager), Arc::new(client));
        let background_worker = Builder::new("background").thread_count(1).create();
        let mut s_clone = s.clone();
        background_worker.spawn_async_task(async move {
            s_clone.watch_resource_groups().await;
        });
        // Mock add
        let group1 = new_resource_group_ru("TEST1".into(), 100, 0);
        add_resource_group(s.pd_client.clone(), group1);
        // Mock reboot watch server
        let watch_global_config_fp = "watch_global_config_return";
        fail::cfg(watch_global_config_fp, "return").unwrap();
        std::thread::sleep(Duration::from_millis(100));
        fail::remove(watch_global_config_fp);
        // Mock add after rebooting will success
        let group2 = new_resource_group_ru("TEST2".into(), 100, 0);
        add_resource_group(s.pd_client.clone(), group2);
        // Wait watcher update
        std::thread::sleep(Duration::from_secs(1));
        let groups = s.manager.get_all_resource_groups();
        assert_eq!(groups.len(), 3);

        server.stop();
    }
}
