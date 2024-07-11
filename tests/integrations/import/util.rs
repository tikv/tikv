// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use engine_rocks::RocksEngine;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    import_sstpb::*, import_sstpb_grpc::import_s_s_t_client::ImportSSTClient, kvrpcpb::*,
    tikvpb::*, tikvpb_grpc::tikv_client::TikvClient,
};
use security::SecurityConfig;
use test_raftstore::*;
use test_raftstore_v2::{Cluster as ClusterV2, ServerCluster as ServerClusterV2};
use tikv::config::TikvConfig;
use tikv_util::HandyRwLock;
use tonic::transport::Channel;

const CLEANUP_SST_MILLIS: u64 = 10;

pub fn new_cluster(cfg: TikvConfig) -> (Cluster<RocksEngine, ServerCluster<RocksEngine>>, Context) {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    cluster.set_cfg(cfg);
    cluster.run();

    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader);
    ctx.set_region_epoch(epoch);

    (cluster, ctx)
}

pub fn new_cluster_v2(
    cfg: TikvConfig,
) -> (
    ClusterV2<ServerClusterV2<RocksEngine>, RocksEngine>,
    Context,
) {
    let count = 1;
    let mut cluster = test_raftstore_v2::new_server_cluster(0, count);
    cluster.set_cfg(cfg);
    cluster.run();

    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader);
    ctx.set_region_epoch(epoch);

    (cluster, ctx)
}

pub fn open_cluster_and_tikv_import_client(
    cfg: Option<TikvConfig>,
) -> (
    Cluster<RocksEngine, ServerCluster<RocksEngine>>,
    Context,
    TikvClient<Channel>,
    ImportSSTClient<Channel>,
) {
    let cfg = cfg.unwrap_or_else(|| {
        let mut config = TikvConfig::default();
        config.server.addr = "127.0.0.1:0".to_owned();
        let cleanup_interval = Duration::from_millis(CLEANUP_SST_MILLIS);
        config.raft_store.cleanup_import_sst_interval.0 = cleanup_interval;
        config.server.grpc_concurrency = 1;
        config
    });

    let (cluster, ctx) = new_cluster(cfg.clone());

    let ch = {
        let node = ctx.get_peer().get_store_id();
        let addr = cluster.sim.rl().get_addr(node);
        let enable_ssl = cfg.security != SecurityConfig::default();
        let builder = Channel::from_shared(tikv_util::format_url(&addr, enable_ssl))
            .unwrap()
            .http2_keep_alive_interval(cluster.cfg.server.grpc_keepalive_time.into())
            .keep_alive_timeout(cluster.cfg.server.grpc_keepalive_timeout.into());
        if enable_ssl {
            let creds = test_util::new_channel_cred();
            builder = builder.tls_config(creds).unwrap();
        }
        cluster.runtime.block_on(builder.connect()).unwrap()
    };
    let tikv = TikvClient::new(ch.clone());
    let import = ImportSstClient::new(ch);

    (cluster, ctx, tikv, import)
}

pub fn open_cluster_and_tikv_import_client_v2(
    cfg: Option<TikvConfig>,
) -> (
    ClusterV2<ServerClusterV2<RocksEngine>, RocksEngine>,
    Context,
    TikvClient<Channel>,
    ImportSSTClient<Channel>,
) {
    let cfg = cfg.unwrap_or_else(|| {
        let mut config = TikvConfig::default();
        config.server.addr = "127.0.0.1:0".to_owned();
        let cleanup_interval = Duration::from_millis(CLEANUP_SST_MILLIS);
        config.raft_store.cleanup_import_sst_interval.0 = cleanup_interval;
        config.server.grpc_concurrency = 1;
        config
    });

    let (cluster, ctx) = new_cluster_v2(cfg.clone());

    let ch = {
        let env = Arc::new(Environment::new(1));
        let node = ctx.get_peer().get_store_id();
        let builder = ChannelBuilder::new(env)
            .http2_max_ping_strikes(i32::MAX) // For pings without data from clients.
            .keepalive_time(cluster.cfg.server.grpc_keepalive_time.into())
            .keepalive_timeout(cluster.cfg.server.grpc_keepalive_timeout.into());

        if cfg.security != SecurityConfig::default() {
            let creds = test_util::new_channel_cred();
            builder.secure_connect(&cluster.sim.rl().get_addr(node), creds)
        } else {
            builder.connect(&cluster.sim.rl().get_addr(node))
        }
    };
    let tikv = TikvClient::new(ch.clone());
    let import = ImportSstClient::new(ch);

    (cluster, ctx, tikv, import)
}

pub fn new_cluster_and_tikv_import_client() -> (
    Cluster<RocksEngine, ServerCluster<RocksEngine>>,
    Context,
    TikvClient<Channel>,
    ImportSSTClient<Channel>,
) {
    open_cluster_and_tikv_import_client(None)
}

pub fn new_cluster_and_tikv_import_client_tde() -> (
    tempfile::TempDir,
    Cluster<RocksEngine, ServerCluster<RocksEngine>>,
    Context,
    TikvClient<Channel>,
    ImportSSTClient<Channel>,
) {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let encryption_cfg = test_util::new_file_security_config(tmp_dir.path());
    let mut security = test_util::new_security_cfg(None);
    security.encryption = encryption_cfg;
    let mut config = TikvConfig::default();
    config.server.addr = "127.0.0.1:0".to_owned();
    let cleanup_interval = Duration::from_millis(CLEANUP_SST_MILLIS);
    config.raft_store.cleanup_import_sst_interval.0 = cleanup_interval;
    config.server.grpc_concurrency = 1;
    config.security = security;
    let (cluster, ctx, tikv, import) = open_cluster_and_tikv_import_client(Some(config));
    (tmp_dir, cluster, ctx, tikv, import)
}
