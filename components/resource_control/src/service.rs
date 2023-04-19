// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, sync::Arc, time::Duration};

use futures::{compat::Future01CompatExt, StreamExt};
use kvproto::{pdpb::EventType, resource_manager::ResourceGroup};
use pd_client::{Error as PdError, PdClient, RpcClient, RESOURCE_CONTROL_CONFIG_PATH};
use tikv_util::{error, timer::GLOBAL_TIMER_HANDLE};

use crate::ResourceGroupManager;

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
        let group = new_resource_group("TEST".into(), true, 100, 100, 0);
        add_resource_group(s.pd_client.clone(), group);
        block_on(s.reload_all_resource_groups());
        assert_eq!(s.manager.get_all_resource_groups().len(), 1);
        assert_eq!(s.revision, 1);

        delete_resource_group(s.pd_client.clone(), "TEST");
        block_on(s.reload_all_resource_groups());
        assert_eq!(s.manager.get_all_resource_groups().len(), 0);
        assert_eq!(s.revision, 2);

        server.stop();
    }

    #[test]
    fn watch_config_test() {
        let (mut server, client) = new_test_server_and_client(ReadableDuration::millis(100));
        let resource_manager = ResourceGroupManager::default();

        let mut s = ResourceManagerService::new(Arc::new(resource_manager), Arc::new(client));
        block_on(s.reload_all_resource_groups());
        assert_eq!(s.manager.get_all_resource_groups().len(), 0);
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
        wait_watch_ready(&s, 2);

        // Mock delete
        delete_resource_group(s.pd_client.clone(), "TEST1");

        // Wait for watcher
        wait_watch_ready(&s, 1);
        let groups = s.manager.get_all_resource_groups();
        assert_eq!(groups.len(), 1);
        assert!(s.manager.get_resource_group("TEST1").is_none());
        let group = s.manager.get_resource_group("TEST2").unwrap();
        assert_eq!(
            group
                .value()
                .get_r_u_settings()
                .get_r_u()
                .get_settings()
                .get_fill_rate(),
            50
        );
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
        let group1 = new_resource_group_ru("TEST2".into(), 100, 0);
        add_resource_group(s.pd_client.clone(), group1);
        // Wait watcher update
        std::thread::sleep(Duration::from_secs(1));
        let groups = s.manager.get_all_resource_groups();
        assert_eq!(groups.len(), 2);

        server.stop();
    }
}
