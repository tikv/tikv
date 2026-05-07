// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use backup::disk_snap::Env as BEnv;
use futures_executor::block_on;
use futures_util::{
    sink::SinkExt,
    stream::{Fuse, StreamExt},
};
use grpcio::{
    ChannelBuilder, ClientDuplexReceiver, Environment, Server, ServerBuilder, StreamingCallSink,
    WriteFlags,
};
use kvproto::{
    brpb::{
        self, PrepareSnapshotBackupEventType, PrepareSnapshotBackupRequest,
        PrepareSnapshotBackupRequestType, PrepareSnapshotBackupResponse,
    },
    metapb::Region,
    raft_cmdpb::RaftCmdResponse,
};
use raftstore::store::{snapshot_backup::PrepareDiskSnapObserver, Callback, WriteResponse};
use test_raftstore::*;
use tikv_util::{
    future::{block_on_timeout, paired_future_callback},
    worker::dummy_scheduler,
    HandyRwLock,
};

pub struct Node {
    service: Option<Server>,
    pub rejector: Arc<PrepareDiskSnapObserver>,
    pub backup_client: Option<brpb::BackupClient>,
}

pub struct Suite {
    pub cluster: Cluster<ServerCluster>,
    pub nodes: HashMap<u64, Node>,
    grpc_env: Arc<Environment>,
}

impl Suite {
    fn crate_node(&mut self, id: u64) {
        let rej = Arc::new(PrepareDiskSnapObserver::default());
        let rej2 = rej.clone();
        let mut w = self.cluster.sim.wl();
        w.coprocessor_hosts
            .entry(id)
            .or_default()
            .push(Box::new(move |host| {
                rej2.register_to(host);
            }));
        self.nodes.insert(
            id,
            Node {
                service: None,
                rejector: rej,
                backup_client: None,
            },
        );
    }

    fn start_backup(&mut self, id: u64) {
        let (sched, _) = dummy_scheduler();
        let w = self.cluster.sim.wl();
        let router = Arc::new(Mutex::new(w.get_router(id).unwrap()));
        let env = BEnv::new(router, self.nodes[&id].rejector.clone(), None);
        let service = backup::Service::new(sched, env);
        let builder = ServerBuilder::new(Arc::clone(&self.grpc_env))
            .register_service(brpb::create_backup(service));
        let mut server = builder.bind("127.0.0.1", 0).build().unwrap();
        server.start();
        let (_, port) = server.bind_addrs().next().unwrap();
        let addr = format!("127.0.0.1:{}", port);
        let channel = ChannelBuilder::new(self.grpc_env.clone()).connect(&addr);
        println!("connecting channel to {} for store {}", addr, id);
        let client = brpb::BackupClient::new(channel);
        let node = self.nodes.get_mut(&id).unwrap();
        node.service = Some(server);
        node.backup_client = Some(client);
    }

    pub fn try_split(&mut self, split_key: &[u8]) -> WriteResponse {
        let region = self.cluster.get_region(split_key);
        let (tx, rx) = paired_future_callback();
        self.cluster
            .split_region(&region, split_key, Callback::write(tx));
        block_on(rx).unwrap()
    }

    pub fn split(&mut self, split_key: &[u8]) {
        let region = self.cluster.get_region(split_key);
        self.try_split(split_key);
        self.cluster.wait_region_split(&region);
    }

    fn backup(&self, id: u64) -> &brpb::BackupClient {
        self.nodes[&id].backup_client.as_ref().unwrap()
    }

    pub fn prepare_backup(&self, node: u64) -> PrepareBackup {
        let cli = self.backup(node);
        let (tx, rx) = cli.prepare_snapshot_backup().unwrap();
        PrepareBackup {
            store_id: node,
            tx,
            rx: rx.fuse(),
        }
    }

    pub fn new(node_count: u64) -> Self {
        Self::new_with_cfg(node_count, |_| {})
    }

    pub fn new_with_cfg(node_count: u64, cfg: impl FnOnce(&mut Config)) -> Self {
        let cluster = new_server_cluster(42, node_count as usize);
        let grpc_env = Arc::new(Environment::new(1));
        let mut suite = Suite {
            cluster,
            nodes: HashMap::default(),
            grpc_env,
        };
        for id in 1..=node_count {
            suite.crate_node(id);
        }
        cfg(&mut suite.cluster.cfg);
        suite.cluster.run();
        for id in 1..=node_count {
            suite.start_backup(id);
        }
        suite
    }
}

pub struct PrepareBackup {
    tx: StreamingCallSink<PrepareSnapshotBackupRequest>,
    rx: Fuse<ClientDuplexReceiver<PrepareSnapshotBackupResponse>>,

    pub store_id: u64,
}

impl PrepareBackup {
    pub fn prepare(&mut self, lease_sec: u64) {
        let mut req = PrepareSnapshotBackupRequest::new();
        req.set_ty(PrepareSnapshotBackupRequestType::UpdateLease);
        req.set_lease_in_seconds(lease_sec);
        block_on(async {
            self.tx.send((req, WriteFlags::default())).await.unwrap();
            self.rx.next().await.unwrap().unwrap();
        });
    }

    pub fn wait_apply(&mut self, r: impl IntoIterator<Item = Region>) {
        let mut req = PrepareSnapshotBackupRequest::new();
        req.set_ty(PrepareSnapshotBackupRequestType::WaitApply);
        req.set_regions(r.into_iter().collect());
        let mut regions = req
            .get_regions()
            .iter()
            .map(|x| x.id)
            .collect::<HashSet<_>>();
        block_on(async {
            self.tx.send((req, WriteFlags::default())).await.unwrap();
            while !regions.is_empty() {
                let resp = self.rx.next().await.unwrap().unwrap();
                assert_eq!(resp.ty, PrepareSnapshotBackupEventType::WaitApplyDone);
                assert!(!resp.has_error(), "{resp:?}");
                assert!(regions.remove(&resp.get_region().id), "{regions:?}");
            }
        });
    }

    pub fn send_wait_apply(&mut self, r: impl IntoIterator<Item = Region>) {
        let mut req = PrepareSnapshotBackupRequest::new();
        req.set_ty(PrepareSnapshotBackupRequestType::WaitApply);
        req.set_regions(r.into_iter().collect());
        block_on(async {
            self.tx.send((req, WriteFlags::default())).await.unwrap();
        })
    }

    pub fn send_finalize(mut self) -> bool {
        if matches!(
            block_on(self.tx.send({
                let mut req = PrepareSnapshotBackupRequest::new();
                req.set_ty(PrepareSnapshotBackupRequestType::Finish);
                (req, WriteFlags::default())
            })),
            Ok(_) | Err(grpcio::Error::RpcFinished(_))
        ) {
            block_on_timeout(
                async {
                    while let Some(item) = self.rx.next().await {
                        let item = item.unwrap();
                        if item.ty == PrepareSnapshotBackupEventType::UpdateLeaseResult {
                            return item.last_lease_is_valid;
                        }
                    }
                    false
                },
                Duration::from_secs(2),
            )
            .expect("take too long to finalize the stream")
        } else {
            false
        }
    }

    pub fn next(&mut self) -> PrepareSnapshotBackupResponse {
        self.try_next().unwrap()
    }

    pub fn try_next(&mut self) -> grpcio::Result<PrepareSnapshotBackupResponse> {
        block_on(self.rx.next()).unwrap()
    }
}

#[track_caller]
pub fn must_wait_apply_success(res: &PrepareSnapshotBackupResponse) -> u64 {
    assert!(!res.has_error(), "{res:?}");
    assert_eq!(res.ty, PrepareSnapshotBackupEventType::WaitApplyDone);
    res.get_region().id
}

#[track_caller]
pub fn assert_success(resp: &RaftCmdResponse) {
    assert!(!resp.get_header().has_error(), "{:?}", resp);
}

#[track_caller]
pub fn assert_failure(resp: &RaftCmdResponse) {
    assert!(resp.get_header().has_error(), "{:?}", resp);
}

#[track_caller]
pub fn assert_failure_because(resp: &RaftCmdResponse, reason_contains: &str) {
    assert!(resp.get_header().has_error(), "{:?}", resp);
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains(reason_contains),
        "{:?}",
        resp
    );
}
