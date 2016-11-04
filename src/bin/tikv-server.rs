// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(plugin)]
#![cfg_attr(feature = "dev", plugin(clippy))]

extern crate tikv;
extern crate getopts;
#[macro_use]
extern crate log;
extern crate rocksdb;
extern crate mio;
extern crate toml;
extern crate libc;
extern crate fs2;
#[cfg(unix)]
extern crate signal;
#[cfg(unix)]
extern crate nix;
extern crate prometheus;

use std::{env, thread};
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use std::io::Read;
use std::time::Duration;

use getopts::{Options, Matches};
use rocksdb::{DB, Options as RocksdbOptions, BlockBasedOptions};
use mio::tcp::TcpListener;
use mio::EventLoop;
use fs2::FileExt;
use prometheus::{Encoder, TextEncoder};

use tikv::storage::{Storage, TEMP_DIR, ALL_CFS};
use tikv::util::{self, logger, file_log, panic_hook, rocksdb as rocksdb_util};
use tikv::util::transport::SendCh;
use tikv::server::{DEFAULT_LISTENING_ADDR, DEFAULT_CLUSTER_ID, Server, Node, Config, bind,
                   create_event_loop, create_raft_storage, Msg};
use tikv::server::{ServerTransport, ServerRaftStoreRouter};
use tikv::server::transport::RaftStoreRouter;
use tikv::server::{PdStoreAddrResolver, StoreAddrResolver};
use tikv::raftstore::store::{self, SnapManager};
use tikv::pd::RpcClient;
use tikv::util::time_monitor::TimeMonitor;

const ROCKSDB_STATS_KEY: &'static str = "rocksdb.stats";

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn get_string_value<F>(short: &str,
                       long: &str,
                       matches: &Matches,
                       config: &toml::Value,
                       default: Option<String>,
                       f: F)
                       -> String
    where F: Fn(&toml::Value) -> Option<String>
{
    // avoid panic if short is not defined.
    let s = if matches.opt_defined(short) {
        matches.opt_str(short)
    } else {
        None
    };

    s.or_else(|| {
            config.lookup(long).and_then(|v| f(v)).or_else(|| {
                info!("{}, use default {:?}", long, default);
                default
            })
        })
        .expect(&format!("please specify {}", long))
}

fn get_boolean_value<F>(short: &str,
                        long: &str,
                        matches: &Matches,
                        config: &toml::Value,
                        default: Option<bool>,
                        f: F)
                        -> bool
    where F: Fn(&toml::Value) -> Option<bool>
{
    let b = if matches.opt_defined(short) {
        matches.opt_str(short).map(|x| x.parse::<bool>().unwrap())
    } else {
        None
    };

    b.or_else(|| {
            config.lookup(long).and_then(|v| f(v)).or_else(|| {
                info!("{}, use default {:?}", long, default);
                default
            })
        })
        .expect(&format!("please specify {}", long))
}

fn get_integer_value<F>(short: &str,
                        long: &str,
                        matches: &Matches,
                        config: &toml::Value,
                        default: Option<i64>,
                        f: F)
                        -> i64
    where F: Fn(&toml::Value) -> Option<i64>
{
    let mut i = None;
    // avoid panic if short is not defined.
    if matches.opt_defined(short) {
        i = matches.opt_str(short).map(|x| {
            x.parse::<i64>()
                .or_else(|_| util::config::parse_readable_int(&x))
                .unwrap()
        });
    };

    i.or_else(|| {
            config.lookup(long)
                .and_then(|v| {
                    if let toml::Value::String(ref s) = *v {
                        Some(util::config::parse_readable_int(s)
                            .expect(&format!("malformed {}", long)))
                    } else {
                        f(v)
                    }
                })
                .or_else(|| {
                    info!("{}, use default {:?}", long, default);
                    default
                })
        })
        .expect(&format!("please specify {}", long))
}

fn initial_log(matches: &Matches, config: &toml::Value) {
    let level = get_string_value("L",
                                 "server.log-level",
                                 matches,
                                 config,
                                 Some("info".to_owned()),
                                 |v| v.as_str().map(|s| s.to_owned()));
    let log_file_path = get_string_value("f",
                                         "server.log-file",
                                         matches,
                                         config,
                                         Some("".to_owned()),
                                         |v| v.as_str().map(|s| s.to_owned()));
    if log_file_path.is_empty() {
        util::init_log(logger::get_level_by_string(&level)).unwrap();
    } else {
        file_log::init(&level, &log_file_path).unwrap();
    }
}

fn initial_metric(matches: &Matches, config: &toml::Value, node_id: Option<u64>) {
    let push_interval = get_integer_value("",
                                          "metric.interval",
                                          matches,
                                          config,
                                          Some(0),
                                          |v| v.as_integer());
    if push_interval == 0 {
        return;
    }

    let push_address = get_string_value("",
                                        "metric.address",
                                        matches,
                                        config,
                                        None,
                                        |v| v.as_str().map(|s| s.to_owned()));
    if push_address.is_empty() {
        return;
    }

    let mut push_job = get_string_value("",
                                        "metric.job",
                                        matches,
                                        config,
                                        None,
                                        |v| v.as_str().map(|s| s.to_owned()));
    if let Some(id) = node_id {
        push_job.push_str(&format!("_{}", id));
    }

    info!("start prometheus client");

    util::run_prometheus(Duration::from_millis(push_interval as u64),
                         &push_address,
                         &push_job);
}

fn get_rocksdb_db_option(matches: &Matches, config: &toml::Value) -> RocksdbOptions {
    let mut opts = get_rocksdb_default_cf_option(matches, config);
    let rmode = get_integer_value("",
                                  "rocksdb.wal-recovery-mode",
                                  matches,
                                  config,
                                  Some(2),
                                  |v| v.as_integer());
    let wal_recovery_mode = util::config::parse_rocksdb_wal_recovery_mode(rmode).unwrap();
    opts.set_wal_recovery_mode(wal_recovery_mode);

    let max_background_compactions = get_integer_value("",
                                                       "rocksdb.max-background-compactions",
                                                       matches,
                                                       config,
                                                       Some(6),
                                                       |v| v.as_integer());
    opts.set_max_background_compactions(max_background_compactions as i32);
    opts.set_max_background_flushes(2);

    let max_manifest_file_size = get_integer_value("",
                                                   "rocksdb.max-manifest-file-size",
                                                   matches,
                                                   config,
                                                   Some(20 * 1024 * 1024),
                                                   |v| v.as_integer());
    opts.set_max_manifest_file_size(max_manifest_file_size as u64);

    let create_if_missing = config.lookup("rocksdb.create-if-missing")
        .unwrap_or(&toml::Value::Boolean(true))
        .as_bool()
        .unwrap_or(true);
    opts.create_if_missing(create_if_missing);

    let max_open_files = get_integer_value("",
                                           "rocksdb.max-open-files",
                                           matches,
                                           config,
                                           Some(40960),
                                           |v| v.as_integer());
    opts.set_max_open_files(max_open_files as i32);

    let enable_statistics = get_boolean_value("",
                                              "rocksdb.enable-statistics",
                                              matches,
                                              config,
                                              Some(false),
                                              |v| v.as_bool());
    if enable_statistics {
        opts.enable_statistics();
    }
    let stats_dump_period_sec = get_integer_value("",
                                                  "rocksdb.stats-dump-period-sec",
                                                  matches,
                                                  config,
                                                  Some(600),
                                                  |v| v.as_integer());
    opts.set_stats_dump_period_sec(stats_dump_period_sec as usize);

    opts
}

fn get_rocksdb_cf_option(matches: &Matches,
                         config: &toml::Value,
                         cf: &str,
                         block_cache_default: i64,
                         use_bloom: bool)
                         -> RocksdbOptions {
    let prefix = String::from("rocksdb.") + cf + ".";

    let mut opts = RocksdbOptions::new();
    let mut block_base_opts = BlockBasedOptions::new();
    let block_size = get_integer_value("",
                                       (prefix.clone() + "block-size").as_str(),
                                       matches,
                                       config,
                                       Some(16 * 1024),
                                       |v| v.as_integer());
    block_base_opts.set_block_size(block_size as usize);
    let block_cache_size = get_integer_value("",
                                             (prefix.clone() + "block-cache-size").as_str(),
                                             matches,
                                             config,
                                             Some(block_cache_default),
                                             |v| v.as_integer());
    block_base_opts.set_lru_cache(block_cache_size as usize);

    if use_bloom {
        let bloom_bits_per_key = get_integer_value("",
                                                   (prefix.clone() + "bloom-filter-bits-per-key")
                                                       .as_str(),
                                                   matches,
                                                   config,
                                                   Some(10),
                                                   |v| v.as_integer());
        let block_based_filter =
            config.lookup((prefix.clone() + "block-based-bloom-filter").as_str())
                .unwrap_or(&toml::Value::Boolean(false))
                .as_bool()
                .unwrap_or(false);
        block_base_opts.set_bloom_filter(bloom_bits_per_key as i32, block_based_filter);
        opts.set_block_based_table_factory(&block_base_opts);
    }

    let cpl = get_string_value("",
                               (prefix.clone() + "compression-per-level").as_str(),
                               matches,
                               config,
                               Some("lz4:lz4:lz4:lz4:lz4:lz4:lz4".to_owned()),
                               |v| v.as_str().map(|s| s.to_owned()));
    let per_level_compression = util::config::parse_rocksdb_per_level_compression(&cpl).unwrap();
    opts.compression_per_level(&per_level_compression);

    let write_buffer_size = get_integer_value("",
                                              (prefix.clone() + "write-buffer-size").as_str(),
                                              matches,
                                              config,
                                              Some(64 * 1024 * 1024),
                                              |v| v.as_integer());
    opts.set_write_buffer_size(write_buffer_size as u64);

    let max_write_buffer_number = {
        get_integer_value("",
                          (prefix.clone() + "max-write-buffer-number").as_str(),
                          matches,
                          config,
                          Some(5),
                          |v| v.as_integer())
    };
    opts.set_max_write_buffer_number(max_write_buffer_number as i32);

    let min_write_buffer_number_to_merge = {
        get_integer_value("",
                          (prefix.clone() + "min-write-buffer-number-to-merge").as_str(),
                          matches,
                          config,
                          Some(1),
                          |v| v.as_integer())
    };
    opts.set_min_write_buffer_number_to_merge(min_write_buffer_number_to_merge as i32);

    let max_bytes_for_level_base = get_integer_value("",
                                                     (prefix.clone() + "max-bytes-for-level-base")
                                                         .as_str(),
                                                     matches,
                                                     config,
                                                     Some(64 * 1024 * 1024),
                                                     |v| v.as_integer());
    opts.set_max_bytes_for_level_base(max_bytes_for_level_base as u64);

    let target_file_size_base = get_integer_value("",
                                                  (prefix.clone() + "target-file-size-base")
                                                      .as_str(),
                                                  matches,
                                                  config,
                                                  Some(16 * 1024 * 1024),
                                                  |v| v.as_integer());
    opts.set_target_file_size_base(target_file_size_base as u64);

    let level_zero_slowdown_writes_trigger = {
        get_integer_value("",
                          (prefix.clone() + "level0-slowdown-writes-trigger").as_str(),
                          matches,
                          config,
                          Some(12),
                          |v| v.as_integer())
    };
    opts.set_level_zero_slowdown_writes_trigger(level_zero_slowdown_writes_trigger as i32);

    let level_zero_stop_writes_trigger =
        get_integer_value("",
                          (prefix.clone() + "level0-stop-writes-trigger").as_str(),
                          matches,
                          config,
                          Some(16),
                          |v| v.as_integer());
    opts.set_level_zero_stop_writes_trigger(level_zero_stop_writes_trigger as i32);

    opts
}

fn get_rocksdb_default_cf_option(matches: &Matches, config: &toml::Value) -> RocksdbOptions {
    // Default column family use bloom filter.
    get_rocksdb_cf_option(matches,
                          config,
                          "defaultcf",
                          1024 * 1024 * 1024,
                          true /* bloom filter */)
}


fn get_rocksdb_write_cf_option(matches: &Matches, config: &toml::Value) -> RocksdbOptions {
    // Don't need set bloom filter for write cf, because we use seek to get the correct
    // version base on provided timestamp.
    get_rocksdb_cf_option(matches, config, "writecf", 256 * 1024 * 1024, false)
}

fn get_rocksdb_raftlog_cf_option(matches: &Matches, config: &toml::Value) -> RocksdbOptions {
    get_rocksdb_cf_option(matches, config, "raftcf", 256 * 1024 * 1024, false)
}

fn get_rocksdb_lock_cf_option() -> RocksdbOptions {
    let mut opts = RocksdbOptions::new();
    let mut block_base_opts = BlockBasedOptions::new();
    block_base_opts.set_block_size(16 * 1024);
    block_base_opts.set_lru_cache(32 * 1024 * 1024);
    block_base_opts.set_bloom_filter(10, false);
    opts.set_block_based_table_factory(&block_base_opts);

    let cpl = "no:no:no:no:no:no:no".to_owned();
    let per_level_compression = util::config::parse_rocksdb_per_level_compression(&cpl).unwrap();
    opts.compression_per_level(&per_level_compression);
    opts.set_write_buffer_size(32 * 1024 * 1024);
    opts.set_max_write_buffer_number(5);
    opts.set_max_bytes_for_level_base(32 * 1024 * 1024);
    opts.set_target_file_size_base(32 * 1024 * 1024);

    // set level0_file_num_compaction_trigger = 1 is very important,
    // this will result in fewer sst files in lock cf.
    opts.set_level_zero_file_num_compaction_trigger(1);

    opts
}


// TODO: merge this function with Config::new
// Currently, to add a new option, we will define three default value
// in config.rs, this file and config-template.toml respectively. It may be more
// maintainable to keep things in one place.
fn build_cfg(matches: &Matches, config: &toml::Value, cluster_id: u64, addr: &str) -> Config {
    let mut cfg = Config::new();
    cfg.cluster_id = cluster_id;
    cfg.addr = addr.to_owned();
    cfg.notify_capacity = get_integer_value("",
                                            "server.notify-capacity",
                                            matches,
                                            config,
                                            Some(40960),
                                            |v| v.as_integer()) as usize;
    cfg.end_point_concurrency =
        get_integer_value("",
                          "server.end-point-concurrency",
                          matches,
                          config,
                          Some(8),
                          |v| v.as_integer()) as usize;
    cfg.messages_per_tick =
        get_integer_value("",
                          "server.messages-per-tick",
                          matches,
                          config,
                          Some(4096),
                          |v| v.as_integer()) as usize;
    let capacity = get_integer_value("capacity",
                                     "server.capacity",
                                     matches,
                                     config,
                                     Some(0),
                                     |v| v.as_integer());
    assert!(capacity >= 0);
    if capacity > 0 {
        cfg.raft_store.capacity = capacity as u64;
    }

    // Set advertise address for outer node and client use.
    // If no advertise listening address set, use the associated listening address.
    cfg.advertise_addr = get_string_value("advertise-addr",
                                          "server.advertise-addr",
                                          matches,
                                          config,
                                          Some(addr.to_owned()),
                                          |v| v.as_str().map(|s| s.to_owned()));
    cfg.send_buffer_size =
        get_integer_value("",
                          "server.send-buffer-size",
                          matches,
                          config,
                          Some(128 * 1024),
                          |v| v.as_integer()) as usize;
    cfg.recv_buffer_size =
        get_integer_value("",
                          "server.recv-buffer-size",
                          matches,
                          config,
                          Some(128 * 1024),
                          |v| v.as_integer()) as usize;

    cfg.raft_store.notify_capacity =
        get_integer_value("",
                          "raftstore.notify-capacity",
                          matches,
                          config,
                          Some(40960),
                          |v| v.as_integer()) as usize;
    cfg.raft_store.messages_per_tick =
        get_integer_value("",
                          "raftstore.messages-per-tick",
                          matches,
                          config,
                          Some(4096),
                          |v| v.as_integer()) as usize;
    cfg.raft_store.region_split_size =
        get_integer_value("",
                          "raftstore.region-split-size",
                          matches,
                          config,
                          Some(64 * 1024 * 1024),
                          |v| v.as_integer()) as u64;
    cfg.raft_store.region_max_size =
        get_integer_value("",
                          "raftstore.region-max-size",
                          matches,
                          config,
                          Some(80 * 1024 * 1024),
                          |v| v.as_integer()) as u64;

    cfg.raft_store.region_check_size_diff =
        get_integer_value("",
                          "raftstore.region-split-check-diff",
                          matches,
                          config,
                          Some(8 * 1024 * 1024),
                          |v| v.as_integer()) as u64;

    cfg.raft_store.region_compact_check_tick_interval =
        get_integer_value("",
                          "raftstore.region-compact-check-tick-interval",
                          matches,
                          config,
                          Some(300_000),
                          |v| v.as_integer()) as u64;

    cfg.raft_store.region_compact_delete_keys_count =
        get_integer_value("",
                          "raftstore.region-compact-delete-keys-count",
                          matches,
                          config,
                          Some(200_000),
                          |v| v.as_integer()) as u64;

    let max_peer_down_millis =
        get_integer_value("",
                          "raftstore.max-peer-down-duration",
                          matches,
                          config,
                          Some(300_000),
                          |v| v.as_integer()) as u64;
    cfg.raft_store.max_peer_down_duration = Duration::from_millis(max_peer_down_millis);

    cfg.raft_store.pd_heartbeat_tick_interval =
        get_integer_value("",
                          "raftstore.pd-heartbeat-tick-interval",
                          matches,
                          config,
                          Some(60_000),
                          |v| v.as_integer()) as u64;

    cfg.raft_store.pd_store_heartbeat_tick_interval =
        get_integer_value("",
                          "raftstore.pd-store-heartbeat-tick-interval",
                          matches,
                          config,
                          Some(10_000),
                          |v| v.as_integer()) as u64;

    cfg.storage.sched_notify_capacity =
        get_integer_value("",
                          "storage.scheduler-notify-capacity",
                          matches,
                          config,
                          Some(10240),
                          |v| v.as_integer()) as usize;
    cfg.storage.sched_msg_per_tick =
        get_integer_value("",
                          "storage.scheduler-messages-per-tick",
                          matches,
                          config,
                          Some(1024),
                          |v| v.as_integer()) as usize;
    cfg.storage.sched_concurrency =
        get_integer_value("",
                          "storage.scheduler-concurrency",
                          matches,
                          config,
                          Some(10240),
                          |v| v.as_integer()) as usize;
    cfg.storage.sched_worker_pool_size =
        get_integer_value("",
                          "storage.scheduler-worker-pool-size",
                          matches,
                          config,
                          Some(4),
                          |v| v.as_integer()) as usize;
    cfg
}

fn build_raftkv(matches: &Matches,
                config: &toml::Value,
                ch: SendCh<Msg>,
                pd_client: Arc<RpcClient>,
                cfg: &Config)
                -> (Node<RpcClient>, Storage, ServerRaftStoreRouter, SnapManager, Arc<DB>) {
    let trans = ServerTransport::new(ch);
    let path = Path::new(&cfg.storage.path).to_path_buf();
    let opts = get_rocksdb_db_option(matches, config);
    let cfs_opts = vec![get_rocksdb_default_cf_option(matches, config),
                        get_rocksdb_lock_cf_option(),
                        get_rocksdb_write_cf_option(matches, config),
                        get_rocksdb_raftlog_cf_option(matches, config)];
    let mut db_path = path.clone();
    db_path.push("db");
    let engine =
        Arc::new(rocksdb_util::new_engine_opt(opts, db_path.to_str().unwrap(), ALL_CFS, cfs_opts)
            .unwrap());

    let mut event_loop = store::create_event_loop(&cfg.raft_store).unwrap();
    let mut node = Node::new(&mut event_loop, cfg, pd_client);

    let mut snap_path = path.clone();
    snap_path.push("snap");
    let snap_path = snap_path.to_str().unwrap().to_owned();
    let snap_mgr = store::new_snap_mgr(snap_path, Some(node.get_sendch()));

    node.start(event_loop, engine.clone(), trans, snap_mgr.clone()).unwrap();
    let router = ServerRaftStoreRouter::new(node.get_sendch(), node.id());

    (node,
     create_raft_storage(router.clone(), engine.clone(), cfg).unwrap(),
     router,
     snap_mgr,
     engine)
}

fn get_store_path(matches: &Matches, config: &toml::Value) -> String {
    let path = get_string_value("s",
                                "server.store",
                                matches,
                                config,
                                Some(TEMP_DIR.to_owned()),
                                |v| v.as_str().map(|s| s.to_owned()));
    if path == TEMP_DIR {
        return path;
    }

    let p = Path::new(&path);
    if p.exists() && p.is_file() {
        panic!("{} is not a directory!", path);
    }
    if !p.exists() {
        fs::create_dir_all(p).unwrap();
    }
    let absolute_path = p.canonicalize().unwrap();
    format!("{}", absolute_path.display())
}

fn start_server<T, S>(mut server: Server<T, S>, mut el: EventLoop<Server<T, S>>, engine: Arc<DB>)
    where T: RaftStoreRouter,
          S: StoreAddrResolver + Send + 'static
{
    let ch = server.get_sendch();
    let h = thread::Builder::new()
        .name("tikv-server".to_owned())
        .spawn(move || {
            server.run(&mut el).unwrap();
        })
        .unwrap();
    handle_signal(ch, engine);
    h.join().unwrap();
}

#[cfg(unix)]
fn handle_signal(ch: SendCh<Msg>, engine: Arc<DB>) {
    use signal::trap::Trap;
    use nix::sys::signal::{SIGTERM, SIGINT, SIGUSR1};
    let trap = Trap::trap(&[SIGTERM, SIGINT, SIGUSR1]);
    for sig in trap {
        match sig {
            SIGTERM | SIGINT => {
                info!("receive signal {}, stopping server...", sig);
                ch.send(Msg::Quit).unwrap();
                break;
            }
            SIGUSR1 => {
                // Use SIGUSR1 to log metrics.
                let mut buffer = vec![];
                let metric_familys = prometheus::gather();
                let encoder = TextEncoder::new();
                encoder.encode(&metric_familys, &mut buffer).unwrap();
                info!("{}", String::from_utf8(buffer).unwrap());

                // Log common rocksdb stats.
                if let Some(v) = engine.get_property_value(ROCKSDB_STATS_KEY) {
                    info!("{}", v)
                }

                // Log more stats if enable_statistics is true.
                if let Some(v) = engine.get_statistics() {
                    info!("{}", v)
                }
            }
            // TODO: handle more signal
            _ => unreachable!(),
        }
    }
}

#[cfg(not(unix))]
fn handle_signal(ch: SendCh<Msg>) {}

fn run_raft_server(listener: TcpListener,
                   pd_client: RpcClient,
                   matches: &Matches,
                   config: &toml::Value,
                   cfg: &Config) {
    let mut event_loop = create_event_loop(cfg).unwrap();
    let ch = SendCh::new(event_loop.channel(), "raft-server");
    let pd_client = Arc::new(pd_client);
    let resolver = PdStoreAddrResolver::new(pd_client.clone()).unwrap();

    let store_path = get_store_path(matches, config);
    let mut lock_path = Path::new(&store_path).to_path_buf();
    lock_path.push("LOCK");
    let f = File::create(lock_path).unwrap();
    if f.try_lock_exclusive().is_err() {
        panic!("lock {} failed, maybe another instance is using this directory.",
               store_path);
    }

    let (mut node, mut store, raft_router, snap_mgr, engine) =
        build_raftkv(matches, config, ch.clone(), pd_client, cfg);
    info!("tikv server config: {:?}", cfg);

    initial_metric(matches, config, Some(node.id()));

    info!("start storage");
    if let Err(e) = store.start(&cfg.storage) {
        panic!("failed to start storage, error = {:?}", e);
    }

    let svr = Server::new(&mut event_loop,
                          cfg,
                          listener,
                          store,
                          raft_router,
                          resolver,
                          snap_mgr)
        .unwrap();
    start_server(svr, event_loop, engine);
    node.stop().unwrap();
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optopt("A",
                "addr",
                "set listening address",
                "default is 127.0.0.1:20160");
    opts.optopt("",
                "advertise-addr",
                "set advertise listening address for client communication",
                "if not set, use ${addr} instead.");
    opts.optopt("L",
                "log",
                "set log level",
                "log level: trace, debug, info, warn, error, off");
    opts.optopt("f",
                "log-file",
                "set log file",
                "if not set, output log to stdout");
    opts.optflag("v", "version", "print version information");
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("C", "config", "set configuration file", "file path");
    opts.optopt("s",
                "store",
                "set the path to store directory",
                "/tmp/tikv/store");
    opts.optopt("",
                "capacity",
                "set the store capacity",
                "default: 0 (unlimited)");
    opts.optopt("S",
                "dsn",
                "[deprecated] set which dsn to use, warning: now only support raftkv",
                "dsn: raftkv");
    opts.optopt("", "pd", "pd endpoints", "127.0.0.1:2379,127.0.0.1:3379");

    let matches = opts.parse(&args[1..]).expect("opts parse failed");
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }
    if matches.opt_present("v") {
        let (hash, date) = util::build_info();
        println!("Git Commit Hash: {}", hash);
        println!("UTC Build Time:  {}", date);
        return;
    }
    let config = match matches.opt_str("C") {
        Some(path) => {
            let mut config_file = fs::File::open(&path).expect("config open failed");
            let mut s = String::new();
            config_file.read_to_string(&mut s).expect("config read failed");
            toml::Value::Table(toml::Parser::new(&s).parse().expect("malformed config file"))
        }
        // Empty value, lookup() always return `None`.
        None => toml::Value::Integer(0),
    };

    initial_log(&matches, &config);

    // Print version information.
    util::print_tikv_info();

    let addr = get_string_value("A",
                                "server.addr",
                                &matches,
                                &config,
                                Some(DEFAULT_LISTENING_ADDR.to_owned()),
                                |v| v.as_str().map(|s| s.to_owned()));
    info!("Start listening on {}...", addr);
    let listener = bind(&addr).unwrap();
    panic_hook::set_exit_hook();

    let pd_endpoints = get_string_value("pd",
                                        "pd.endpoints",
                                        &matches,
                                        &config,
                                        None,
                                        |v| v.as_str().map(|s| s.to_owned()));
    let pd_client = RpcClient::new(&pd_endpoints).unwrap();
    let cluster_id = pd_client.cluster_id;

    let mut cfg = build_cfg(&matches,
                            &config,
                            cluster_id,
                            &format!("{}", listener.local_addr().unwrap()));
    cfg.storage.path = get_store_path(&matches, &config);

    if cluster_id == DEFAULT_CLUSTER_ID {
        panic!("in raftkv, cluster_id must greater than 0");
    }
    let _m = TimeMonitor::default();
    run_raft_server(listener, pd_client, &matches, &config, &cfg);
}
