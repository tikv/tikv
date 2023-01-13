// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::StreamExt;
use kvproto::{pdpb::ItemKind, resource_manager::ResourceGroup};
use pd_client::{PdClient, Result, RpcClient};
use tikv_util::{box_err, error};

use crate::ResourceGroupManager;

pub const CONFIG_PATH: &str = "resource_group/settings";

#[derive(Clone)]
pub struct ResourceManagerService {
    pub manager: Arc<ResourceGroupManager>,
    pd_client: Arc<RpcClient>,
    // record watch revision
    pub revision: i64,
}

impl ResourceManagerService {
    /// Constructs a new `Service` with `ResourceGroupManager` and a `Rpclient`
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

impl ResourceManagerService {
    pub async fn watch_resource_groups(&mut self) {
        // Firstly, load all resource groups as of now.
        match self.list_resource_groups() {
            Ok((groups, revision)) => {
                self.revision = revision;
                for group in groups {
                    self.manager.add_resource_group(group);
                }
            }
            Err(e) => {
                error!("failed to list resource groups, err: {:?}", e);
                return;
            }
        }
        // Secondly, start watcher at loading revision.
        match self
            .pd_client
            .watch_global_config(CONFIG_PATH.to_string(), self.revision)
            .await
        {
            Ok(mut stream) => {
                while let Some(grpc_response) = stream.next().await {
                    match grpc_response {
                        Ok(r) => {
                            self.revision = r.get_revision();
                            for item in r.get_changes() {
                                if let Ok(group) = protobuf::parse_from_bytes::<ResourceGroup>(
                                    item.get_value().as_bytes(),
                                ) {
                                    match item.get_kind() {
                                        ItemKind::Put => self.manager.add_resource_group(group),
                                        ItemKind::Delete => {
                                            self.manager.remove_resource_group(item.get_name())
                                        }
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            error!("failed to get stream, err: {:?}", err);
                            return;
                        }
                    }
                }
            }
            Err(e) => error!("failed to watch resource groups, err: {:?}", e),
        }
    }

    fn list_resource_groups(&mut self) -> Result<(Vec<ResourceGroup>, i64)> {
        match futures::executor::block_on(async move {
            self.pd_client
                .load_global_config(CONFIG_PATH.to_string())
                .await
        }) {
            Ok((items, revision)) => {
                let mut groups = Vec::default();
                for item in items {
                    if item.has_error() {
                        error!(
                            "failed to load global config with key {:?}",
                            item.get_error()
                        );
                        continue;
                    }
                    if let Ok(group) = protobuf::parse_from_bytes(item.get_value().as_bytes()) {
                        groups.push(group);
                    }
                }
                Ok((groups, revision))
            }
            Err(e) => return Err(box_err!("failed to load global config, err: {:?}", e)),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::time::Duration;

    use kvproto::pdpb::GlobalConfigItem;
    use pd_client::RpcClient;
    use protobuf::Message;
    use test_pd::{mocker::Service, util::*, Server as MockServer};
    use tikv_util::{config::ReadableDuration, worker::Builder};

    use crate::resource_group::tests::new_resource_group;

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
        item.set_kind(ItemKind::Put);
        item.set_name(group.get_name().to_string());
        let mut buf = Vec::new();
        group.write_to_vec(&mut buf).unwrap();
        item.set_value(String::from_utf8(buf).unwrap());

        futures::executor::block_on(async move {
            pd_client
                .store_global_config(CONFIG_PATH.to_string(), vec![item])
                .await
        })
        .unwrap();
    }

    fn delete_resource_group(pd_client: Arc<RpcClient>, name: &str) {
        let mut item = GlobalConfigItem::default();
        item.set_kind(ItemKind::Delete);
        item.set_name(name.to_string());

        futures::executor::block_on(async move {
            pd_client
                .store_global_config(CONFIG_PATH.to_string(), vec![item])
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
        let (res, revision) = s.list_resource_groups().unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(revision, 1);
        s.revision = revision;

        delete_resource_group(s.pd_client.clone(), "TEST");
        let (res, revision) = s.list_resource_groups().unwrap();
        assert_eq!(res.len(), 0);
        assert_eq!(revision, 2);

        server.stop();
    }

    #[test]
    fn watch_config_test() {
        let (mut server, client) = new_test_server_and_client(ReadableDuration::millis(100));
        let resource_manager = ResourceGroupManager::default();

        let mut s = ResourceManagerService::new(Arc::new(resource_manager), Arc::new(client));
        let (res, revision) = s.list_resource_groups().unwrap();
        assert_eq!(res.len(), 0);
        assert_eq!(revision, 0);

        let background_worker = Builder::new("background").thread_count(1).create();
        let mut s_clone = s.clone();
        background_worker.spawn_async_task(async move {
            s_clone.watch_resource_groups().await;
        });
        // Mock add
        let group1 = new_resource_group("TEST1".into(), true, 100, 100);
        add_resource_group(s.pd_client.clone(), group1);
        let group2 = new_resource_group("TEST2".into(), true, 100, 100);
        add_resource_group(s.pd_client.clone(), group2);
        // Mock modify
        let group2 = new_resource_group("TEST2".into(), true, 50, 50);
        add_resource_group(s.pd_client.clone(), group2);
        let (res, revision) = s.list_resource_groups().unwrap();
        s.revision = revision;
        assert_eq!(res.len(), 2);
        assert_eq!(revision, 3);
        // Mock delete
        delete_resource_group(s.pd_client.clone(), "TEST1");
        let (res, revision) = s.list_resource_groups().unwrap();
        s.revision = revision;
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
                .get_r_r_u()
                .get_settings()
                .get_fill_rate(),
            50
        );
        server.stop();
    }
}
