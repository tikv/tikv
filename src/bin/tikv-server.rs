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
use rocksdb::{Options as RocksdbOptions, BlockBasedOptions};
use mio::tcp::TcpListener;
use mio::EventLoop;
use fs2::FileExt;
use prometheus::{Encoder, TextEncoder};

use tikv::storage::{Storage, TEMP_DIR, ALL_CFS};
use tikv::util::{self, logger, file_log, panic_hook, rocksdb as rocksdb_util};
use tikv::util::transport::SendCh;
use tikv::server::{DEFAULT_LISTENING_ADDR, DEFAULT_CLUSTER_ID, Server, Node, Config, bind,
                   create_event_loop, create_raft_storage, Msg};
use tikv::server::{ServerTransport, ServerRaftStoreRouter, MockRaftStoreRouter};
use tikv::server::transport::RaftStoreRouter;
use tikv::server::{MockStoreAddrResolver, PdStoreAddrResolver, StoreAddrResolver};
use tikv::raftstore::store::{self, SnapManager};
use tikv::pd::RpcClient;
use tikv::util::time_monitor::TimeMonitor;

const ROCKSDB_DSN: &'static str = "rocksdb";
const RAFTKV_DSN: &'static str = "raftkv";

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

fn get_rocksdb_option(matches: &Matches, config: &toml::Value) -> RocksdbOptions {
    let mut opts = get_rocksdb_default_cf_option(matches, config);
    let rmode = get_integer_value("",
                                  "rocksdb.wal-recovery-mode",
                                  matches,
                                  config,
                                  Some(2),
                                  |v| v.as_integer());
    let wal_recovery_mode = util::config::parse_rocksdb_wal_recovery_mode(rmode).unwrap();
    opts.set_wal_recovery_mode(wal_recovery_mode);

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

fn get_rocksdb_default_cf_option(matches: &Matches, config: &toml::Value) -> RocksdbOptions {
    let mut opts = RocksdbOptions::new();
    let mut block_base_opts = BlockBasedOptions::new();
    let block_size = get_integer_value("",
                                       "rocksdb.block-based-table.block-size",
                                       matches,
                                       config,
                                       Some(64 * 1024),
                                       |v| v.as_integer());
    block_base_opts.set_block_size(block_size as usize);
    let block_cache_size = get_integer_value("",
                                             "rocksdb.block-based-table.block-cache-size",
                                             matches,
                                             config,
                                             Some(1024 * 1024 * 1024),
                                             |v| v.as_integer());
    block_base_opts.set_lru_cache(block_cache_size as usize);
    let bloom_bits_per_key = get_integer_value("",
                                               "rocksdb.block-based-table.\
                                                bloom-filter-bits-per-key",
                                               matches,
                                               config,
                                               Some(10),
                                               |v| v.as_integer());
    let block_based_filter = config.lookup("rocksdb.block-based-table.block-based-bloom-filter")
        .unwrap_or(&toml::Value::Boolean(false))
        .as_bool()
        .unwrap_or(false);
    block_base_opts.set_bloom_filter(bloom_bits_per_key as i32, block_based_filter);
    opts.set_block_based_table_factory(&block_base_opts);

    let cpl = get_string_value("",
                               "rocksdb.compression-per-level",
                               matches,
                               config,
                               Some("lz4:lz4:lz4:lz4:lz4:lz4:lz4".to_owned()),
                               |v| v.as_str().map(|s| s.to_owned()));
    let per_level_compression = util::config::parse_rocksdb_per_level_compression(&cpl).unwrap();
    opts.compression_per_level(&per_level_compression);

    let write_buffer_size = get_integer_value("",
                                              "rocksdb.write-buffer-size",
                                              matches,
                                              config,
                                              Some(64 * 1024 * 1024),
                                              |v| v.as_integer());
    opts.set_write_buffer_size(write_buffer_size as u64);

    let max_write_buffer_number = {
        get_integer_value("",
                          "rocksdb.max-write-buffer-number",
                          matches,
                          config,
                          Some(5),
                          |v| v.as_integer())
    };
    opts.set_max_write_buffer_number(max_write_buffer_number as i32);

    let min_write_buffer_number_to_merge = {
        get_integer_value("",
                          "rocksdb.min-write-buffer-number-to-merge",
                          matches,
                          config,
                          Some(1),
                          |v| v.as_integer())
    };
    opts.set_min_write_buffer_number_to_merge(min_write_buffer_number_to_merge as i32);

    let max_background_compactions = get_integer_value("",
                                                       "rocksdb.max-background-compactions",
                                                       matches,
                                                       config,
                                                       Some(6),
                                                       |v| v.as_integer());
    opts.set_max_background_compactions(max_background_compactions as i32);
    opts.set_max_background_flushes(2);

    let max_bytes_for_level_base = get_integer_value("",
                                                     "rocksdb.max-bytes-for-level-base",
                                                     matches,
                                                     config,
                                                     Some(64 * 1024 * 1024),
                                                     |v| v.as_integer());
    opts.set_max_bytes_for_level_base(max_bytes_for_level_base as u64);

    let max_manifest_file_size = get_integer_value("",
                                                   "rocksdb.max-manifest-file-size",
                                                   matches,
                                                   config,
                                                   Some(20 * 1024 * 1024),
                                                   |v| v.as_integer());
    opts.set_max_manifest_file_size(max_manifest_file_size as u64);


    let target_file_size_base = get_integer_value("",
                                                  "rocksdb.target-file-size-base",
                                                  matches,
                                                  config,
                                                  Some(16 * 1024 * 1024),
                                                  |v| v.as_integer());
    opts.set_target_file_size_base(target_file_size_base as u64);

    let create_if_missing = config.lookup("rocksdb.create-if-missing")
        .unwrap_or(&toml::Value::Boolean(true))
        .as_bool()
        .unwrap_or(true);
    opts.create_if_missing(create_if_missing);

    let level_zero_slowdown_writes_trigger = {
        get_integer_value("",
                          "rocksdb.level0-slowdown-writes-trigger",
                          matches,
                          config,
                          Some(12),
                          |v| v.as_integer())
    };
    opts.set_level_zero_slowdown_writes_trigger(level_zero_slowdown_writes_trigger as i32);

    let level_zero_stop_writes_trigger = get_integer_value("",
                                                           "rocksdb.level0-stop-writes-trigger",
                                                           matches,
                                                           config,
                                                           Some(16),
                                                           |v| v.as_integer());
    opts.set_level_zero_stop_writes_trigger(level_zero_stop_writes_trigger as i32);

    opts
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

fn get_rocksdb_write_cf_option(matches: &Matches, config: &toml::Value) -> RocksdbOptions {
    let mut opts = RocksdbOptions::new();
    let mut block_base_opts = BlockBasedOptions::new();
    block_base_opts.set_block_size(16 * 1024);

    // Don't need set bloom filter for write cf, because we use seek to get the correct
    // version base on provided timestamp.
    let block_cache_size = get_integer_value("",
                                             "rocksdb.writecf.block-cache-size",
                                             matches,
                                             config,
                                             Some(256 * 1024 * 1024),
                                             |v| v.as_integer());
    block_base_opts.set_lru_cache(block_cache_size as usize);
    opts.set_block_based_table_factory(&block_base_opts);

    let cpl = get_string_value("",
                               "rocksdb.writecf.compression-per-level",
                               matches,
                               config,
                               Some("lz4:lz4:lz4:lz4:lz4:lz4:lz4".to_owned()),
                               |v| v.as_str().map(|s| s.to_owned()));
    let per_level_compression = util::config::parse_rocksdb_per_level_compression(&cpl).unwrap();
    opts.compression_per_level(&per_level_compression);

    let write_buffer_size = get_integer_value("",
                                              "rocksdb.writecf.write-buffer-size",
                                              matches,
                                              config,
                                              Some(64 * 1024 * 1024),
                                              |v| v.as_integer());
    opts.set_write_buffer_size(write_buffer_size as u64);

    let max_write_buffer_number = {
        get_integer_value("",
                          "rocksdb.writecf.max-write-buffer-number",
                          matches,
                          config,
                          Some(5),
                          |v| v.as_integer())
    };
    opts.set_max_write_buffer_number(max_write_buffer_number as i32);

    let min_write_buffer_number_to_merge = {
        get_integer_value("",
                          "rocksdb.writecf.min-write-buffer-number-to-merge",
                          matches,
                          config,
                          Some(1),
                          |v| v.as_integer())
    };
    opts.set_min_write_buffer_number_to_merge(min_write_buffer_number_to_merge as i32);

    let max_bytes_for_level_base = get_integer_value("",
                                                     "rocksdb.writecf.max-bytes-for-level-base",
                                                     matches,
                                                     config,
                                                     Some(64 * 1024 * 1024),
                                                     |v| v.as_integer());
    opts.set_max_bytes_for_level_base(max_bytes_for_level_base as u64);

    let target_file_size_base = get_integer_value("",
                                                  "rocksdb.writecf.target-file-size-base",
                                                  matches,
                                                  config,
                                                  Some(16 * 1024 * 1024),
                                                  |v| v.as_integer());
    opts.set_target_file_size_base(target_file_size_base as u64);

    opts
}

fn get_rocksdb_raftlog_cf_option(matches: &Matches, config: &toml::Value) -> RocksdbOptions {
    let mut opts = RocksdbOptions::new();
    let mut block_base_opts = BlockBasedOptions::new();
    block_base_opts.set_block_size(16 * 1024);

    let block_cache_size = get_integer_value("",
                                             "rocksdb.raftcf.block-cache-size",
                                             matches,
                                             config,
                                             Some(256 * 1024 * 1024),
                                             |v| v.as_integer());
    block_base_opts.set_lru_cache(block_cache_size as usize);
    block_base_opts.set_bloom_filter(10, false);
    opts.set_block_based_table_factory(&block_base_opts);

    let cpl = get_string_value("",
                               "rocksdb.raftcf.compression-per-level",
                               matches,
                               config,
                               Some("lz4:lz4:lz4:lz4:lz4:lz4:lz4".to_owned()),
                               |v| v.as_str().map(|s| s.to_owned()));
    let per_level_compression = util::config::parse_rocksdb_per_level_compression(&cpl).unwrap();
    opts.compression_per_level(&per_level_compression);
    let write_buffer_size = get_integer_value("",
                                              "rocksdb.raftcf.write-buffer-size",
                                              matches,
                                              config,
                                              Some(64 * 1024 * 1024),
                                              |v| v.as_integer());
    opts.set_write_buffer_size(write_buffer_size as u64);
    let max_write_buffer_number = {
        get_integer_value("",
                          "rocksdb.raftcf.max-write-buffer-number",
                          matches,
                          config,
                          Some(5),
                          |v| v.as_integer())
    };
    opts.set_max_write_buffer_number(max_write_buffer_number as i32);
    let min_write_buffer_number_to_merge = {
        get_integer_value("",
                          "rocksdb.raftcf.min-write-buffer-number-to-merge",
                          matches,
                          config,
                          Some(1),
                          |v| v.as_integer())
    };
    opts.set_min_write_buffer_number_to_merge(min_write_buffer_number_to_merge as i32);
    let max_bytes_for_level_base = get_integer_value("",
                                                     "rocksdb.raftcf.max-bytes-for-level-base",
                                                     matches,
                                                     config,
                                                     Some(64 * 1024 * 1024),
                                                     |v| v.as_integer());
    opts.set_max_bytes_for_level_base(max_bytes_for_level_base as u64);
    let target_file_size_base = get_integer_value("",
                                                  "rocksdb.raftcf.target-file-size-base",
                                                  matches,
                                                  config,
                                                  Some(16 * 1024 * 1024),
                                                  |v| v.as_integer());
    opts.set_target_file_size_base(target_file_size_base as u64);

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
                          Some(5000),
                          |v| v.as_integer()) as u64;

    cfg.raft_store.pd_store_heartbeat_tick_interval =
        get_integer_value("",
                          "raftstore.pd-store-heartbeat-tick-interval",
                          matches,
                          config,
                          Some(10000),
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
                          Some(1024),
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

fn build_raftkv
    (matches: &Matches,
     config: &toml::Value,
     ch: SendCh<Msg>,
     pd_client: Arc<RpcClient>,
     cfg: &Config)
     -> (Node<RpcClient>, Storage<ServerRaftStoreRouter>, ServerRaftStoreRouter, SnapManager) {
    let trans = ServerTransport::new(ch);
    let path = Path::new(&cfg.storage.path).to_path_buf();
    let opts = get_rocksdb_option(matches, config);
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

    node.start(event_loop, engine.clone(), trans, snap_mgr.clone())
        .unwrap();
    let router = ServerRaftStoreRouter::new(node.get_sendch(), node.id());

    (node, create_raft_storage(router.clone(), engine, cfg).unwrap(), router, snap_mgr)
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

fn start_server<T, S>(mut server: Server<T, S>, mut el: EventLoop<Server<T, S>>)
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
    handle_signal(ch);
    h.join().unwrap();
}

#[cfg(unix)]
fn handle_signal(ch: SendCh<Msg>) {
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
            }
            // TODO: handle more signal
            _ => unreachable!(),
        }
    }
}

#[cfg(not(unix))]
fn handle_signal(ch: SendCh<Msg>) {}

fn run_local_server(listener: TcpListener, config: &Config) {
    let mut event_loop = create_event_loop(config).unwrap();
    let snap_mgr = store::new_snap_mgr(TEMP_DIR, None);

    let mut store = Storage::new(&config.storage).unwrap();
    if let Err(e) = store.start(&config.storage, MockRaftStoreRouter) {
        panic!("failed to start storage, error = {:?}", e);
    }

    let svr = Server::new(&mut event_loop,
                          config,
                          listener,
                          store,
                          MockRaftStoreRouter,
                          MockStoreAddrResolver,
                          snap_mgr)
        .unwrap();
    start_server(svr, event_loop);
}

fn run_raft_server(listener: TcpListener, matches: &Matches, config: &toml::Value, cfg: &Config) {
    let mut event_loop = create_event_loop(cfg).unwrap();
    let ch = SendCh::new(event_loop.channel());
    let pd_endpoints = get_string_value("pd",
                                        "pd.endpoints",
                                        matches,
                                        config,
                                        None,
                                        |v| v.as_str().map(|s| s.to_owned()));
    let pd_client = Arc::new(RpcClient::new(&pd_endpoints, cfg.cluster_id).unwrap());
    let resolver = PdStoreAddrResolver::new(pd_client.clone()).unwrap();

    let store_path = get_store_path(matches, config);
    let mut lock_path = Path::new(&store_path).to_path_buf();
    lock_path.push("LOCK");
    let f = File::create(lock_path).unwrap();
    if f.try_lock_exclusive().is_err() {
        panic!("lock {} failed, maybe another instance is using this directory.",
               store_path);
    }

    let (mut node, mut store, raft_router, snap_mgr) =
        build_raftkv(matches, config, ch.clone(), pd_client, cfg);
    info!("tikv server config: {:?}", cfg);

    initial_metric(matches, config, Some(node.id()));

    info!("start storage");
    if let Err(e) = store.start(&cfg.storage, raft_router.clone()) {
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
    start_server(svr, event_loop);
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
                "set which dsn to use, warning: default is rocksdb without persistent",
                "dsn: rocksdb, raftkv");
    opts.optopt("I",
                "cluster-id",
                "set cluster id",
                "in raftkv, must greater than 0; in rocksdb, it will be ignored.");
    opts.optopt("", "pd", "pd endpoints", "127.0.0.1:2379,127.0.0.1:3379");

    let matches = opts.parse(&args[1..]).expect("opts parse failed");
    if matches.opt_present("h") {
        print_usage(&program, opts);
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
    let dsn_name = get_string_value("S",
                                    "server.dsn",
                                    &matches,
                                    &config,
                                    Some(ROCKSDB_DSN.to_owned()),
                                    |v| v.as_str().map(|s| s.to_owned()));
    panic_hook::set_exit_hook();
    let cluster_id = get_integer_value("I",
                                       "raft.cluster-id",
                                       &matches,
                                       &config,
                                       Some(DEFAULT_CLUSTER_ID as i64),
                                       |v| v.as_integer()) as u64;
    let mut cfg = build_cfg(&matches,
                            &config,
                            cluster_id,
                            &format!("{}", listener.local_addr().unwrap()));
    cfg.storage.path = get_store_path(&matches, &config);
    match dsn_name.as_ref() {
        ROCKSDB_DSN => {
            initial_metric(&matches, &config, None);
            run_local_server(listener, &cfg);
        }
        RAFTKV_DSN => {
            if cluster_id == DEFAULT_CLUSTER_ID {
                panic!("in raftkv, cluster_id must greater than 0");
            }
            let _m = TimeMonitor::default();
            run_raft_server(listener, &matches, &config, &cfg);
        }
        n => panic!("unrecognized dns name: {}", n),
    };
}
