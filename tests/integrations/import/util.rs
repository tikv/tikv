// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use engine_rocks::RocksEngine;
use futures::{executor::block_on, stream, SinkExt};
use grpcio::{ChannelBuilder, Environment, Result, WriteFlags};
use kvproto::{import_sstpb::*, kvrpcpb::*, tikvpb::*};
use security::SecurityConfig;
use test_raftstore::*;
use tikv::config::TikvConfig;
use tikv_util::HandyRwLock;
use uuid::Uuid;

const CLEANUP_SST_MILLIS: u64 = 10;

pub fn new_cluster(cfg: TikvConfig) -> (Cluster<ServerCluster>, Context) {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    cluster.cfg = Config {
        tikv: cfg,
        prefer_mem: true,
    };
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
) -> (Cluster<ServerCluster>, Context, TikvClient, ImportSstClient) {
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

#[allow(dead_code)]
pub fn open_cluster_and_tikv_import_client_v2(
    cfg: Option<TikvConfig>,
    cluster: &mut test_raftstore_v2::Cluster<
        test_raftstore_v2::ServerCluster<RocksEngine>,
        RocksEngine,
    >,
) -> (Context, TikvClient, ImportSstClient) {
    let cfg = cfg.unwrap_or_else(|| {
        let mut config = TikvConfig::default();
        config.server.addr = "127.0.0.1:0".to_owned();
        let cleanup_interval = Duration::from_millis(10);
        config.raft_store.cleanup_import_sst_interval.0 = cleanup_interval;
        config.server.grpc_concurrency = 1;
        config
    });
    cluster.cfg = Config {
        tikv: cfg.clone(),
        prefer_mem: true,
    };
    cluster.run();

    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader);
    ctx.set_region_epoch(epoch);

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

    (ctx, tikv, import)
}

pub fn new_cluster_and_tikv_import_client()
-> (Cluster<ServerCluster>, Context, TikvClient, ImportSstClient) {
    open_cluster_and_tikv_import_client(None)
}

pub fn new_cluster_and_tikv_import_client_tde() -> (
    tempfile::TempDir,
    Cluster<ServerCluster>,
    Context,
    TikvClient,
    ImportSstClient,
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

pub fn new_sst_meta(crc32: u32, length: u64) -> SstMeta {
    let mut m = SstMeta::default();
    m.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    m.set_crc32(crc32);
    m.set_length(length);
    m
}

pub fn send_upload_sst(
    client: &ImportSstClient,
    meta: &SstMeta,
    data: &[u8],
) -> Result<UploadResponse> {
    let mut r1 = UploadRequest::default();
    r1.set_meta(meta.clone());
    let mut r2 = UploadRequest::default();
    r2.set_data(data.to_vec());
    let reqs: Vec<_> = vec![r1, r2]
        .into_iter()
        .map(|r| Result::Ok((r, WriteFlags::default())))
        .collect();
    let (mut tx, rx) = client.upload().unwrap();
    let mut stream = stream::iter(reqs);
    block_on(async move {
        tx.send_all(&mut stream).await?;
        tx.close().await?;
        rx.await
    })
}

pub fn send_write_sst(
    client: &ImportSstClient,
    meta: &SstMeta,
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    commit_ts: u64,
) -> Result<WriteResponse> {
    let mut r1 = WriteRequest::default();
    r1.set_meta(meta.clone());
    let mut r2 = WriteRequest::default();

    let mut batch = WriteBatch::default();
    let mut pairs = vec![];

    for (i, key) in keys.iter().enumerate() {
        let mut pair = Pair::default();
        pair.set_key(key.to_vec());
        pair.set_value(values[i].to_vec());
        pairs.push(pair);
    }
    batch.set_commit_ts(commit_ts);
    batch.set_pairs(pairs.into());
    r2.set_batch(batch);

    let reqs: Vec<_> = vec![r1, r2]
        .into_iter()
        .map(|r| Result::Ok((r, WriteFlags::default())))
        .collect();

    let (mut tx, rx) = client.write().unwrap();
    let mut stream = stream::iter(reqs);
    block_on(async move {
        tx.send_all(&mut stream).await?;
        tx.close().await?;
        rx.await
    })
}

pub fn check_ingested_kvs(tikv: &TikvClient, ctx: &Context, sst_range: (u8, u8)) {
    check_ingested_kvs_cf(tikv, ctx, "", sst_range);
}

pub fn check_ingested_kvs_cf(tikv: &TikvClient, ctx: &Context, cf: &str, sst_range: (u8, u8)) {
    for i in sst_range.0..sst_range.1 {
        let mut m = RawGetRequest::default();
        m.set_context(ctx.clone());
        m.set_key(vec![i]);
        m.set_cf(cf.to_owned());
        let resp = tikv.raw_get(&m).unwrap();
        assert!(resp.get_error().is_empty());
        assert!(!resp.has_region_error());
        assert_eq!(resp.get_value(), &[i]);
    }
}

pub fn check_ingested_txn_kvs(
    tikv: &TikvClient,
    ctx: &Context,
    sst_range: (u8, u8),
    start_ts: u64,
) {
    for i in sst_range.0..sst_range.1 {
        let mut m = GetRequest::default();
        m.set_context(ctx.clone());
        m.set_key(vec![i]);
        m.set_version(start_ts);
        let resp = tikv.kv_get(&m).unwrap();
        assert!(!resp.has_region_error());
        assert_eq!(resp.get_value(), &[i]);
    }
}

pub fn check_sst_deleted(client: &ImportSstClient, meta: &SstMeta, data: &[u8]) {
    for _ in 0..10 {
        if send_upload_sst(client, meta, data).is_ok() {
            // If we can upload the file, it means the previous file has been deleted.
            return;
        }
        thread::sleep(Duration::from_millis(CLEANUP_SST_MILLIS));
    }
    send_upload_sst(client, meta, data).unwrap();
}
