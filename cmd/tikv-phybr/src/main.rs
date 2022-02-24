// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::time::Duration;

use ::phybr::phybr::{RegionMeta, RegionRecover};
use ::phybr::phybr_grpc::PhybrClient;
use encryption_export::data_key_manager_from_config;
use engine_rocks::raw_util::new_engine_opt;
use engine_rocks::RocksEngine;
use engine_traits::{
    Engines, Iterable, MiscExt, Mutable, Peekable, RaftEngine, RaftEngineReadOnly, RaftLogBatch,
    WriteBatch, WriteBatchExt, CF_DEFAULT, CF_RAFT,
};
use futures::executor::block_on;
use futures::sink::SinkExt;
use futures::stream::{self, Stream, StreamExt};
use grpcio::{ChannelBuilder, Environment, Error as GrpcError, WriteFlags};
use kvproto::raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState};
use protobuf::Message;
use raftstore::store::peer_storage::{init_apply_state, init_raft_state, last_index};
use raftstore::store::{Callback, SignificantMsg, SignificantRouter};
use structopt::StructOpt;
use tikv::config::TiKvConfig;

fn main() {
    let opts = Opts::from_args();
    let config: TiKvConfig = std::fs::read_to_string(&opts.config)
        .map_err(|e| format!("read config file: {}", e))
        .and_then(|s| toml::from_str(&s).map_err(|e| format!("parse config file: {}", e)))
        .unwrap();

    let engines = init_engines(&config).unwrap();
    let mut region_meta_details = scan_regions(&engines).unwrap();

    let region_metas = region_meta_details
        .iter()
        .map(|x| x.to_region_meta())
        .collect::<Vec<_>>();
    println!("region metas to report:");
    for meta in &region_metas {
        println!("\t{:?}", meta)
    }
    let mut region_metas = stream::iter(
        region_metas
            .into_iter()
            .map(|x| Ok((x, WriteFlags::default()))),
    );

    let channel = ChannelBuilder::new(Arc::new(Environment::new(1)))
        .keepalive_time(Duration::from_secs(10))
        .keepalive_timeout(Duration::from_secs(3))
        .connect(&opts.adviser);
    let client = PhybrClient::new(channel);
    let (mut sink, mut receiver) = match client.recover_regions() {
        Ok((x, y)) => (x, y),
        Err(e) => {
            eprintln!("rpc send fail: {}", e);
            return;
        }
    };

    let res = block_on(sink.send_all(&mut region_metas));
    println!("send: {:?}", res);
    let res = block_on(sink.close());
    println!("send close: {:?}", res);

    let mut commands = HashMap::<u64, RegionRecover>::default();
    let res: Result<(), ()> = block_on(async {
        while let Some(command) = receiver.next().await {
            let command = command.map_err(|e| eprintln!("rpc recv fail: {}", e))?;
            assert!(commands.insert(command.region_id, command).is_none());
        }
        Ok(())
    });
    println!("recv res: {:?}", res);

    let mut raft_wb = engines.raft.log_batch(4 * 1024);
    let mut kv_wb = engines.kv.write_batch();
    let mut force_leaders = Vec::new();
    for md in &mut region_meta_details {
        let rid = md.region_state.get_region().id;
        let applied_index = md.apply_state.applied_index;
        let mut raft_changed = false;
        let mut region_changed = false;
        let mut apply_changed = false;
        let mut silence = false;

        if md.raft_state.last_index != applied_index {
            raft_wb.cut_logs(rid, applied_index + 1, md.raft_state.last_index + 1);
            md.raft_state.last_index = applied_index;
            raft_changed = true;
        }
        if md.raft_state.get_hard_state().commit != applied_index {
            md.raft_state.mut_hard_state().commit = applied_index;
            raft_changed = true;
        }

        if md.apply_state.commit_index != applied_index {
            md.apply_state.commit_index = applied_index;
            let trunc_state = md.apply_state.get_truncated_state();
            md.apply_state.commit_term = if applied_index == trunc_state.index {
                trunc_state.term
            } else {
                let e = engines.raft.get_entry(rid, applied_index).unwrap().unwrap();
                e.term
            };
            apply_changed = true;
        }

        if let Some(command) = commands.get(&rid) {
            if command.tombstone {
                md.region_state.set_state(PeerState::Tombstone);
                region_changed = true;
                silence = true;
                raft_changed = false;
                apply_changed = false;
            } else {
                if command.term != md.raft_state.get_hard_state().term {
                    md.raft_state.mut_hard_state().term = command.term;
                    raft_changed = true;
                }
                silence = command.silence;
            }
        }

        if raft_changed {
            let key = keys::raft_state_key(rid);
            raft_wb
                .put_msg_cf(CF_DEFAULT, &key, &md.raft_state)
                .unwrap();
        }
        if region_changed {
            let key = keys::region_state_key(rid);
            kv_wb.put_msg_cf(CF_RAFT, &key, &md.region_state).unwrap();
        }
        if apply_changed {
            let key = keys::apply_state_key(rid);
            kv_wb.put_msg_cf(CF_RAFT, &key, &md.apply_state).unwrap();
        }

        if !silence {
            force_leaders.push(rid);
        }

        println!(
            "region {} raft changed: {}, region changed: {}, apply changed: {}",
            rid, raft_changed, region_changed, apply_changed
        );
    }
    kv_wb.write().unwrap();
    engines.kv.sync_wal().unwrap();
    engines.raft.consume(&mut raft_wb, true).unwrap();

    // Start tikv nodes.
    drop(engines);
    drop(kv_wb);
    drop(raft_wb);

    std::thread::sleep(Duration::from_secs(3));
    println!("starting tikv node...");
    let (tx, rx) = sync_channel::<Box<dyn SignificantRouter<RocksEngine>>>(1);
    let th = std::thread::spawn(move || {
        let router = match rx.recv() {
            Ok(x) => x,
            Err(_) => {
                println!("starting tikv fail");
                return;
            }
        };
        println!("starting tikv success");

        let mut rxs = Vec::with_capacity(force_leaders.len());
        for &rid in &force_leaders {
            if let Err(e) = router.significant_send(rid, SignificantMsg::Campaign) {
                println!("region {} fails to campaign: {}", rid, e);
                continue;
            } else {
                println!("region {} starts to campaign", rid);
            }

            let (tx, rx) = sync_channel(1);
            let cb = Callback::Read(Box::new(move |_| tx.send(1).unwrap()));
            router
                .significant_send(rid, SignificantMsg::LeaderCallback(cb))
                .unwrap();
            rxs.push(Some(rx));
        }
        for (&rid, rx) in force_leaders.iter().zip(rxs) {
            if let Some(rx) = rx {
                let _ = rx.recv().unwrap();
                println!("region {} has applied to its current term", rid);
            }
        }

        println!("all region state adjustments are done");
    });

    server::server::run_phybr(config, tx);
    th.join().unwrap();
}

#[derive(StructOpt)]
#[structopt()]
struct Opts {
    adviser: String,
    config: String,
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

#[derive(Debug)]
struct RegionMetaDetail {
    raft_state: RaftLocalState,
    region_state: RegionLocalState,
    apply_state: RaftApplyState,
}

impl RegionMetaDetail {
    fn to_region_meta(&self) -> RegionMeta {
        let mut region_meta = RegionMeta::default();
        region_meta.region_id = self.region_state.get_region().id;
        region_meta.applied_index = self.apply_state.applied_index;
        region_meta.term = self.raft_state.get_hard_state().term;
        region_meta.version = self.region_state.get_region().get_region_epoch().version;
        region_meta.tombstone = self.region_state.state == PeerState::Tombstone;
        region_meta.start_key = self.region_state.get_region().get_start_key().to_owned();
        region_meta.end_key = self.region_state.get_region().get_end_key().to_owned();
        region_meta
    }
}

fn scan_regions(
    engines: &Engines<RocksEngine, RocksEngine>,
) -> Result<Vec<RegionMetaDetail>, String> {
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

            if region_state.get_state() == PeerState::Tombstone {
                // TODO: it's better to report tombstone regions to the adviser.
                return Ok(true);
            }

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
            region_metas.push(RegionMetaDetail {
                raft_state,
                region_state,
                apply_state,
            });
            Ok(true)
        },
    )?;
    engines.raft.consume(&mut raft_wb, true)?;
    Ok(region_metas)
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
