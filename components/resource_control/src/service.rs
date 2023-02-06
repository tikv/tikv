// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

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
        // Firstly, load all resource groups as of now.
        let (groups, revision) = self.list_resource_groups().await;
        self.revision = revision;
        groups
            .into_iter()
            .for_each(|rg| self.manager.add_resource_group(rg));
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
                                            if let Ok(group) =
                                                protobuf::parse_from_bytes::<ResourceGroup>(
                                                    item.get_payload(),
                                                )
                                            {
                                                self.manager.add_resource_group(group);
                                            }
                                        }
                                        EventType::Delete => {
                                            self.manager.remove_resource_group(item.get_name());
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
                    // If the etcd revision is compacted, we need to reload all resouce groups.
                    let (groups, revision) = self.list_resource_groups().await;
                    self.revision = revision;
                    groups
                        .into_iter()
                        .for_each(|rg| self.manager.add_resource_group(rg));
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

    async fn list_resource_groups(&mut self) -> (Vec<ResourceGroup>, i64) {
        loop {
            match self
                .pd_client
                .load_global_config(RESOURCE_CONTROL_CONFIG_PATH.to_string())
                .await
            {
                Ok((items, revision)) => {
                    let groups = items
                        .into_iter()
                        .filter_map(|g| protobuf::parse_from_bytes(g.get_payload()).ok())
                        .collect();
                    return (groups, revision);
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
        let group = new_resource_group("TEST".into(), true, 100, 100);
        add_resource_group(s.pd_client.clone(), group);
        let (res, revision) = block_on(s.list_resource_groups());
        assert_eq!(res.len(), 1);
        assert_eq!(revision, 1);

        delete_resource_group(s.pd_client.clone(), "TEST");
        let (res, revision) = block_on(s.list_resource_groups());
        assert_eq!(res.len(), 0);
        assert_eq!(revision, 2);

        server.stop();
    }

    #[test]
    fn watch_config_test() {
        let (mut server, client) = new_test_server_and_client(ReadableDuration::millis(100));
        let resource_manager = ResourceGroupManager::default();

        let mut s = ResourceManagerService::new(Arc::new(resource_manager), Arc::new(client));
        let (res, revision) = block_on(s.list_resource_groups());
        assert_eq!(res.len(), 0);
        assert_eq!(revision, 0);

        let background_worker = Builder::new("background").thread_count(1).create();
        let mut s_clone = s.clone();
        background_worker.spawn_async_task(async move {
            s_clone.watch_resource_groups().await;
        });
        // Mock add
        let group1 = new_resource_group_ru("TEST1".into(), 100);
        add_resource_group(s.pd_client.clone(), group1);
        let group2 = new_resource_group_ru("TEST2".into(), 100);
        add_resource_group(s.pd_client.clone(), group2);
        // Mock modify
        let group2 = new_resource_group_ru("TEST2".into(), 50);
        add_resource_group(s.pd_client.clone(), group2);
        let (res, revision) = block_on(s.list_resource_groups());
        assert_eq!(res.len(), 2);
        assert_eq!(revision, 3);
        // Mock delete
        delete_resource_group(s.pd_client.clone(), "TEST1");
        let (res, revision) = block_on(s.list_resource_groups());
        assert_eq!(res.len(), 1);
        assert_eq!(revision, 4);
        // Wait for watcher
        std::thread::sleep(Duration::from_millis(100));
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
        let group1 = new_resource_group_ru("TEST1".into(), 100);
        add_resource_group(s.pd_client.clone(), group1);
        // Mock reboot watch server
        let watch_global_config_fp = "watch_global_config_return";
        fail::cfg(watch_global_config_fp, "return").unwrap();
        std::thread::sleep(Duration::from_millis(100));
        fail::remove(watch_global_config_fp);
        // Mock add after rebooting will success
        let group1 = new_resource_group_ru("TEST2".into(), 100);
        add_resource_group(s.pd_client.clone(), group1);
        // Wait watcher update
        std::thread::sleep(Duration::from_secs(1));
        let groups = s.manager.get_all_resource_groups();
        assert_eq!(groups.len(), 2);

        server.stop();
    }
}
