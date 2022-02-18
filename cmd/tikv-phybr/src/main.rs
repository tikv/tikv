// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Duration;

use ::phybr::phybr::RegionMeta;
use ::phybr::phybr_grpc::PhybrClient;
use encryption_export::data_key_manager_from_config;
use engine_rocks::raw_util::new_engine_opt;
use engine_rocks::RocksEngine;
use engine_traits::Iterable;
use engine_traits::Peekable;
use engine_traits::RaftEngine;
use engine_traits::RaftLogBatch;
use engine_traits::{Engines, CF_RAFT};
use futures::executor::block_on;
use futures::future::ready;
use futures::sink::SinkExt;
use futures::stream::{self, Stream, StreamExt};
use grpcio::{ChannelBuilder, Environment, Error as GrpcError, WriteFlags};
use kvproto::raft_serverpb::{PeerState, RegionLocalState};
use protobuf::Message;
use raftstore::store::peer_storage::{init_apply_state, init_raft_state, last_index};
use structopt::StructOpt;
use tikv::config::TiKvConfig;

#[derive(StructOpt)]
#[structopt()]
struct Opts {
    adviser: String,
    config: String,
    data_dir: String,
}

fn init_engines(config: &TiKvConfig) -> Result<Engines<RocksEngine, RocksEngine>, String> {
    let key_manager =
        data_key_manager_from_config(&config.security.encryption, &config.storage.data_dir)
            .map_err(|e| format!("init encryption manager: {}", e))?
            .map(Arc::new);
    let env = config
        .build_shared_rocks_env(key_manager, None)
        .map_err(|e| format!("build shared rocks env: {}", e))?;
    let block_cache = config.storage.block_cache.build_shared_cache();

    let mut db_opts = config.raftdb.build_opt();
    db_opts.set_env(env.clone());
    let cf_opts = config.raftdb.build_cf_opts(&block_cache);
    let db_path = config
        .infer_raft_db_path(None)
        .map_err(|e| format!("infer raftdb path: {}", e))?;
    println!("raftdb path: {:?}", db_path);
    let raft_engine =
        new_engine_opt(&db_path, db_opts, cf_opts).map_err(|e| format!("create raftdb: {}", e))?;

    let mut db_opts = config.rocksdb.build_opt();
    db_opts.set_env(env);
    let cf_opts = config
        .rocksdb
        .build_cf_opts(&block_cache, None, config.storage.api_version());
    let db_path = config
        .infer_kv_engine_path(None)
        .map_err(|e| format!("infer kvdb path: {}", e))?;
    println!("kvdb path: {:?}", db_path);
    let kv_engine =
        new_engine_opt(&db_path, db_opts, cf_opts).map_err(|e| format!("create kvdb: {}", e))?;

    let mut kv_engine = RocksEngine::from_db(Arc::new(kv_engine));
    let mut raft_engine = RocksEngine::from_db(Arc::new(raft_engine));
    let shared_block_cache = block_cache.is_some();
    kv_engine.set_shared_block_cache(shared_block_cache);
    raft_engine.set_shared_block_cache(shared_block_cache);
    let engines = Engines::new(kv_engine, raft_engine);
    Ok(engines)
}

fn scan_regions(engines: &Engines<RocksEngine, RocksEngine>) -> Result<Vec<RegionMeta>, String> {
    let mut region_metas = Vec::with_capacity(1024 * 1024);
    let mut raft_wb = engines.raft.log_batch(4 * 1024);
    engines.kv.scan_cf(
        CF_RAFT,
        keys::REGION_META_MIN_KEY,
        keys::REGION_META_MAX_KEY,
        false,
        |key, value| {
            let (region_id, suffix) = keys::decode_region_meta_key(key).unwrap();
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }

            let mut region_state = RegionLocalState::default();
            region_state.merge_from_bytes(value).unwrap();

            let mut raft_state = init_raft_state(engines, region_state.get_region()).unwrap();
            if region_state.get_state() == PeerState::Applying {
                if let Some(snapshot_raft_state) = engines
                    .kv
                    .get_msg_cf(CF_RAFT, &keys::snapshot_raft_state_key(region_id))
                    .unwrap()
                {
                    if last_index(&snapshot_raft_state) > last_index(&raft_state) {
                        raft_state = snapshot_raft_state;
                        raft_wb.put_raft_state(region_id, &raft_state)?;
                    }
                }
            }

            let apply_state = init_apply_state(engines, region_state.get_region()).unwrap();

            let mut region_meta = RegionMeta::default();
            region_meta.region_id = region_id;
            region_meta.applied_index = apply_state.applied_index;
            region_meta.term = raft_state.get_hard_state().term;
            region_meta.version = region_state.get_region().get_region_epoch().version;
            region_meta.tombstone = region_state.state == PeerState::Tombstone;
            region_meta.start_key = region_state.get_region().get_start_key().to_owned();
            region_meta.end_key = region_state.get_region().get_end_key().to_owned();
            region_metas.push(region_meta);
            Ok(true)
        },
    )?;
    engines.raft.consume(&mut raft_wb, true)?;
    Ok(region_metas)
}

fn main() {
    let opts = Opts::from_args();
    let mut config: TiKvConfig = std::fs::read_to_string(&opts.config)
        .map_err(|e| format!("read config file: {}", e))
        .and_then(|s| toml::from_str(&s).map_err(|e| format!("parse config file: {}", e)))
        .unwrap();
    config.storage.data_dir = opts.data_dir;

    let engines = init_engines(&config).unwrap();
    let mut region_metas = stream::iter(
        scan_regions(&engines)
            .unwrap()
            .into_iter()
            .map(|x| Ok((x, WriteFlags::default()))),
    );

    let channel = ChannelBuilder::new(Arc::new(Environment::new(1)))
        .keepalive_time(Duration::from_secs(10))
        .keepalive_timeout(Duration::from_secs(3))
        .connect(&opts.adviser);
    let client = PhybrClient::new(channel);
    let (mut sink, receiver) = match client.recover_regions() {
        Ok((x, y)) => (x, y),
        Err(e) => {
            eprintln!("rpc fail: {}", e);
            return;
        }
    };

    let res = block_on(sink.send_all(&mut region_metas));
    println!("send: {:?}", res);
    let res = block_on(sink.close());
    println!("send close: {:?}", res);
    let res = block_on(receiver.for_each(|x| ready(println!("receive: {:?}", x))));
    println!("recv res: {:?}", res);
}

#[allow(dead_code)]
fn gen_region_metas() -> impl Stream<Item = Result<(RegionMeta, WriteFlags), GrpcError>> {
    let mut r1 = RegionMeta::default();
    r1.region_id = 1;
    r1.term = 2;
    r1.applied_index = 100;
    r1.version = 2;
    r1.start_key = b"".to_vec();
    r1.end_key = b"k".to_vec();
    let mut r2 = RegionMeta::default();
    r2.region_id = 2;
    r2.term = 2;
    r2.applied_index = 100;
    r2.version = 2;
    r2.start_key = b"k".to_vec();
    r2.end_key = b"".to_vec();
    stream::iter(vec![
        Ok((r1, WriteFlags::default())),
        Ok((r2, WriteFlags::default())),
    ])
}
