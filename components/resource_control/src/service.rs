// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use futures::{compat::Future01CompatExt, StreamExt};
use kvproto::{
    pdpb::EventType,
    resource_manager::{ResourceGroup, TokenBucketRequest, TokenBucketsRequest},
};
use pd_client::{
    Error as PdError, PdClient, RpcClient, RESOURCE_CONTROL_CONFIG_PATH,
    RESOURCE_CONTROL_CONTROLLER_CONFIG_PATH,
};
use serde::{Deserialize, Serialize};
use tikv_util::{error, info, timer::GLOBAL_TIMER_HANDLE};

use crate::{resource_limiter::ResourceType, ResourceGroupManager};

#[derive(Clone)]
pub struct ResourceManagerService {
    manager: Arc<ResourceGroupManager>,
    pd_client: Arc<RpcClient>,
    // record watch revision.
    revision: i64,
}

impl ResourceManagerService {
    /// Constructs a new `Service` with `ResourceGroupManager` and a
    /// `RpcClient`.
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
const BACKGROUND_RU_REPORT_DURATION: Duration = Duration::from_secs(5);

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
                                                    Ok(group) => {
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
                            Ok(rg) => {
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

    async fn load_controller_config(&self) -> RequestUnitConfig {
        loop {
            match self
                .pd_client
                .load_global_config(RESOURCE_CONTROL_CONTROLLER_CONFIG_PATH.to_string())
                .await
            {
                Ok((items, _)) => {
                    if items.is_empty() {
                        error!("server does not save config, load config failed.");
                        let _ = GLOBAL_TIMER_HANDLE
                            .delay(std::time::Instant::now() + RETRY_INTERVAL)
                            .compat()
                            .await;
                        continue;
                    }
                    match serde_json::from_slice::<ControllerConfig>(items[0].get_payload()) {
                        Ok(c) => return c.request_unit,
                        Err(err) => {
                            error!("parse controller config failed"; "err" => ?err);
                            let _ = GLOBAL_TIMER_HANDLE
                                .delay(std::time::Instant::now() + RETRY_INTERVAL)
                                .compat()
                                .await;
                            continue;
                        }
                    }
                }
                Err(err) => {
                    error!("failed to load controller config"; "err" => ?err);
                    let _ = GLOBAL_TIMER_HANDLE
                        .delay(std::time::Instant::now() + RETRY_INTERVAL)
                        .compat()
                        .await;
                    continue;
                }
            }
        }
    }

    // report ru metrics periodically.
    pub async fn report_ru_metrics(&self) {
        let mut last_group_statistics_map: HashMap<String, ReportStatistic> = HashMap::new();
        // load controller config firstly.
        let config = self.load_controller_config().await;
        info!("load controller config"; "config" => ?config);

        loop {
            let background_groups: Vec<_> = self
                .manager
                .resource_groups
                .iter()
                .filter_map(|kv| {
                    let g = kv.value();
                    g.limiter.clone().map(|limiter| {
                        let io_statistics = limiter.get_limit_statistics(ResourceType::Io);
                        let cpu_statistics = limiter.get_limit_statistics(ResourceType::Cpu);

                        (
                            g.group.name.clone(),
                            ReportStatistic {
                                // io statistics and cpu statistics should have the same version.
                                version: io_statistics.version,
                                read_bytes_consumed: io_statistics.read_consumed,
                                write_bytes_consumed: io_statistics.write_consumed,
                                cpu_consumed: cpu_statistics.total_consumed,
                            },
                        )
                    })
                })
                .collect();

            if background_groups.is_empty() {
                let _ = GLOBAL_TIMER_HANDLE
                    .delay(std::time::Instant::now() + BACKGROUND_RU_REPORT_DURATION)
                    .compat()
                    .await;
                continue;
            }

            let mut req = TokenBucketsRequest::default();
            let all_reqs = req.mut_requests();
            for (name, statistic) in background_groups.into_iter() {
                // Non-existence or version change means this is a brand new limiter, so no need
                // to sub the old statistics.
                let (cpu_consumed, io_consumed) = if let Some(last_stats) =
                    last_group_statistics_map
                        .get(&name)
                        .filter(|stats| statistic.version == stats.version)
                {
                    if statistic == *last_stats {
                        continue;
                    }
                    (
                        statistic.cpu_consumed - last_stats.cpu_consumed,
                        (
                            statistic.read_bytes_consumed - last_stats.read_bytes_consumed,
                            statistic.write_bytes_consumed - last_stats.write_bytes_consumed,
                        ),
                    )
                } else {
                    (
                        statistic.cpu_consumed,
                        (
                            statistic.read_bytes_consumed,
                            statistic.write_bytes_consumed,
                        ),
                    )
                };
                // replace the previous statistics.
                last_group_statistics_map.insert(name.clone(), statistic);
                // report ru statistics.
                let mut req = TokenBucketRequest::default();
                req.set_resource_group_name(name.clone());
                req.set_is_background(true);
                let report_consumption = req.mut_consumption_since_last_request();

                let read_total = config.read_cpu_ms_cost * cpu_consumed as f64
                    + config.read_cost_per_byte * io_consumed.0 as f64;
                let write_total = config.write_cost_per_byte * io_consumed.1 as f64;

                report_consumption.set_r_r_u(read_total);
                report_consumption.set_w_r_u(write_total);
                report_consumption.set_read_bytes(io_consumed.0 as f64);
                report_consumption.set_write_bytes(io_consumed.1 as f64);
                report_consumption.set_total_cpu_time_ms(cpu_consumed as f64);

                all_reqs.push(req);
            }

            if !all_reqs.is_empty() {
                if let Err(e) = self.pd_client.report_ru_metrics(req).await {
                    error!("report ru metrics failed"; "err" => ?e);
                }
            }

            let dur = if cfg!(feature = "failpoints") {
                (|| {
                    fail::fail_point!("set_report_duration", |v| {
                        let dur = v
                            .expect("should provide delay time (in ms)")
                            .parse::<u64>()
                            .expect("should be number (in ms)");
                        std::time::Duration::from_millis(dur)
                    });
                    std::time::Duration::from_millis(100)
                })()
            } else {
                BACKGROUND_RU_REPORT_DURATION
            };

            let _ = GLOBAL_TIMER_HANDLE
                .delay(std::time::Instant::now() + dur)
                .compat()
                .await;
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct RequestUnitConfig {
    read_base_cost: f64,
    read_cost_per_byte: f64,
    write_base_cost: f64,
    write_cost_per_byte: f64,
    read_cpu_ms_cost: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ControllerConfig {
    request_unit: RequestUnitConfig,
}

#[derive(PartialEq, Eq, Debug, Clone)]
struct ReportStatistic {
    version: u64,
    read_bytes_consumed: u64,
    write_bytes_consumed: u64,
    cpu_consumed: u64,
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use file_system::IoBytes;
    use futures::executor::block_on;
    use kvproto::pdpb::GlobalConfigItem;
    use pd_client::RpcClient;
    use protobuf::Message;
    use test_pd::{mocker::Service, util::*, Server as MockServer};
    use tikv_util::{config::ReadableDuration, worker::Builder};

    use crate::resource_group::tests::{
        new_background_resource_group_ru, new_resource_group, new_resource_group_ru,
    };

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

    fn store_controller_config(pd_client: Arc<RpcClient>, config: ControllerConfig) {
        let mut item = GlobalConfigItem::default();
        item.set_kind(EventType::Put);
        item.set_name("controller_config".to_string());
        let buf = serde_json::to_vec(&config).unwrap();
        item.set_payload(buf);

        futures::executor::block_on(async move {
            pd_client
                .store_global_config(
                    RESOURCE_CONTROL_CONTROLLER_CONFIG_PATH.to_string(),
                    vec![item],
                )
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
        // Mock add.
        let group1 = new_resource_group_ru("TEST1".into(), 100, 0);
        add_resource_group(s.pd_client.clone(), group1);
        let group2 = new_resource_group_ru("TEST2".into(), 100, 0);
        add_resource_group(s.pd_client.clone(), group2);
        // Mock modify
        let group2 = new_resource_group_ru("TEST2".into(), 50, 0);
        add_resource_group(s.pd_client.clone(), group2);
        wait_watch_ready(&s, 3);

        // Mock delete.
        delete_resource_group(s.pd_client.clone(), "TEST1");

        // Wait for watcher.
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
        // Mock add.
        let group1 = new_resource_group_ru("TEST1".into(), 100, 0);
        add_resource_group(s.pd_client.clone(), group1);
        // Mock reboot watch server.
        let watch_global_config_fp = "watch_global_config_return";
        fail::cfg(watch_global_config_fp, "return").unwrap();
        std::thread::sleep(Duration::from_millis(100));
        fail::remove(watch_global_config_fp);
        // Mock add after rebooting will success.
        let group2 = new_resource_group_ru("TEST2".into(), 100, 0);
        add_resource_group(s.pd_client.clone(), group2);
        // Wait watcher update.
        std::thread::sleep(Duration::from_secs(1));
        let groups = s.manager.get_all_resource_groups();
        assert_eq!(groups.len(), 3);

        server.stop();
    }

    #[test]
    fn load_controller_config_test() {
        let (mut server, client) = new_test_server_and_client(ReadableDuration::millis(100));
        let resource_manager = ResourceGroupManager::default();

        let s = ResourceManagerService::new(Arc::new(resource_manager), Arc::new(client));
        // Set controller config.
        let cfg = ControllerConfig {
            request_unit: RequestUnitConfig {
                read_base_cost: 1. / 8.,
                read_cost_per_byte: 1. / (64. * 1024.),
                write_base_cost: 1.,
                write_cost_per_byte: 1. / 1024.,
                read_cpu_ms_cost: 1. / 3.,
            },
        };
        store_controller_config(s.clone().pd_client, cfg);
        let config = block_on(s.load_controller_config());
        assert_eq!(config.read_base_cost, 1. / 8.);

        server.stop();
    }

    #[test]
    fn report_ru_metrics_test() {
        let (mut server, client) = new_test_server_and_client(ReadableDuration::millis(100));
        let resource_manager = ResourceGroupManager::default();

        let s = ResourceManagerService::new(Arc::new(resource_manager), Arc::new(client));
        let bg = new_background_resource_group_ru("background".into(), 1000, 15, vec!["br".into()]);
        s.manager.add_resource_group(bg);

        // Set controller config.
        let cfg = ControllerConfig {
            request_unit: RequestUnitConfig {
                read_base_cost: 1. / 8.,
                read_cost_per_byte: 1. / (64. * 1024.),
                write_base_cost: 1.,
                write_cost_per_byte: 1. / 1024.,
                read_cpu_ms_cost: 1. / 3.,
            },
        };
        store_controller_config(s.clone().pd_client, cfg);

        fail::cfg("set_report_duration", "return(10)").unwrap();
        let background_worker = Builder::new("background").thread_count(1).create();
        let s_clone = s.clone();
        background_worker.spawn_async_task(async move {
            s_clone.report_ru_metrics().await;
        });
        // Mock consume.
        let bg_limiter = s
            .manager
            .get_background_resource_limiter("background", "br")
            .unwrap();
        bg_limiter.consume(
            Duration::from_secs(2),
            IoBytes {
                read: 1000,
                write: 1000,
            },
            true,
        );
        // Wait for report ru metrics.
        std::thread::sleep(Duration::from_millis(100));
        // Mock update version.
        let bg = new_resource_group_ru("background".into(), 1000, 15);
        s.manager.add_resource_group(bg);

        let background_group =
            new_background_resource_group_ru("background".into(), 500, 8, vec!["lightning".into()]);
        s.manager.add_resource_group(background_group);
        let new_bg_limiter = s
            .manager
            .get_background_resource_limiter("background", "lightning")
            .unwrap();
        new_bg_limiter.consume(
            Duration::from_secs(5),
            IoBytes {
                read: 2000,
                write: 2000,
            },
            true,
        );
        // Wait for report ru metrics.
        std::thread::sleep(Duration::from_millis(100));
        fail::remove("set_report_duration");
        server.stop();
    }
}
