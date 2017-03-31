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
#![cfg_attr(not(feature = "dev"), allow(unknown_lints))]

// TODO: deny it once Manishearth/rust-clippy#1586 is fixed.
#![allow(never_loop)]
#![allow(needless_pass_by_value)]

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
extern crate sys_info;

mod signal_handler;

use std::process;
use std::{env, thread};
use std::fs::{self, File};
use std::usize;
use std::path::Path;
use std::sync::Arc;
use std::io::Read;
use std::time::Duration;

use getopts::{Options, Matches};
use rocksdb::{DB, Options as RocksdbOptions, BlockBasedOptions};
use mio::EventLoop;
use fs2::FileExt;
use sys_info::{cpu_num, mem_info};

use tikv::storage::{Storage, TEMP_DIR, CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT};
use tikv::util::{self, panic_hook, rocksdb as rocksdb_util, HashMap};
use tikv::util::logger::{self, StderrLogger};
use tikv::util::file_log::RotatingFileLogger;
use tikv::util::transport::SendCh;
use tikv::server::{DEFAULT_LISTENING_ADDR, DEFAULT_CLUSTER_ID, ServerChannel, Server, Node,
                   Config, bind, create_event_loop, create_raft_storage, Msg};
use tikv::server::{ServerTransport, ServerRaftStoreRouter};
use tikv::server::transport::RaftStoreRouter;
use tikv::server::{PdStoreAddrResolver, StoreAddrResolver};
use tikv::raftstore::store::{self, SnapManager};
use tikv::pd::{RpcClient, PdClient};
use tikv::raftstore::store::keys::region_raft_prefix_len;
use tikv::util::time_monitor::TimeMonitor;

const RAFTCF_MIN_MEM: u64 = 256 * 1024 * 1024;
const RAFTCF_MAX_MEM: u64 = 2 * 1024 * 1024 * 1024;
const KB: u64 = 1024;
const MB: u64 = KB * 1024;
const DEFAULT_BLOCK_CACHE_RATIO: &'static [f64] = &[0.4, 0.15, 0.01];

fn sanitize_memory_usage() -> bool {
    let mut ratio = 0.0;
    for v in DEFAULT_BLOCK_CACHE_RATIO {
        ratio = ratio + v;
    }
    if ratio > 1.0 {
        return false;
    }
    true
}

fn align_to_mb(n: u64) -> u64 {
    n & 0xFFFFFFFFFFF00000
}

fn print_usage(program: &str, opts: &Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn exit_with_err(msg: String) -> ! {
    error!("{}", msg);
    process::exit(1)
}

fn get_flag_string(matches: &Matches, name: &str) -> Option<String> {
    let s = matches.opt_str(name);
    info!("flag {}: {:?}", name, s);

    s
}

fn get_flag_int(matches: &Matches, name: &str) -> Option<i64> {
    let i = matches.opt_str(name).map(|x| {
        x.parse::<i64>()
            .or_else(|_| util::config::parse_readable_int(&x))
            .unwrap_or_else(|e| exit_with_err(format!("parse {} failed: {:?}", name, e)))
    });
    info!("flag {}: {:?}", name, i);

    i
}

fn get_toml_boolean(config: &toml::Value, name: &str, default: Option<bool>) -> bool {
    let b = match config.lookup(name) {
        Some(&toml::Value::Boolean(b)) => b,
        None => {
            info!("{} use default {:?}", name, default);
            default.unwrap_or_else(|| exit_with_err(format!("please specify {}", name)))
        }
        _ => exit_with_err(format!("{} boolean is excepted", name)),
    };
    info!("toml value {}: {:?}", name, b);

    b
}

fn get_toml_string(config: &toml::Value, name: &str, default: Option<String>) -> String {
    let s = match config.lookup(name) {
        Some(&toml::Value::String(ref s)) => s.clone(),
        None => {
            info!("{} use default {:?}", name, default);
            default.unwrap_or_else(|| exit_with_err(format!("please specify {}", name)))
        }
        _ => exit_with_err(format!("{} string is excepted", name)),
    };
    info!("toml value {}: {:?}", name, s);

    s
}

fn get_toml_int_opt(config: &toml::Value, name: &str) -> Option<i64> {
    let res = match config.lookup(name) {
        Some(&toml::Value::Integer(i)) => Some(i),
        Some(&toml::Value::String(ref s)) => {
            Some(util::config::parse_readable_int(s)
                .unwrap_or_else(|e| exit_with_err(format!("{} parse failed {:?}", name, e))))
        }
        None => None,
        _ => exit_with_err(format!("{} int or readable int is excepted", name)),
    };
    if let Some(i) = res {
        info!("toml value {} : {:?}", name, i);
    }
    res
}

fn get_toml_int(config: &toml::Value, name: &str, default: Option<i64>) -> i64 {
    get_toml_int_opt(config, name).unwrap_or_else(|| {
        let i = default.unwrap_or_else(|| exit_with_err(format!("please specify {}", name)));
        info!("{} use default {:?}", name, default);
        i
    })
}

fn cfg_usize(target: &mut usize, config: &toml::Value, name: &str) -> bool {
    match get_toml_int_opt(config, name) {
        Some(i) => {
            assert!(i >= 0 && i as u64 <= usize::MAX as u64,
                    "{}: {} is invalid.",
                    name,
                    i);
            *target = i as usize;
            true
        }
        None => {
            info!("{} keep default {}", name, *target);
            false
        }
    }
}

fn cfg_u64(target: &mut u64, config: &toml::Value, name: &str) {
    match get_toml_int_opt(config, name) {
        Some(i) => {
            assert!(i >= 0, "{}: {} is invalid", name, i);
            *target = i as u64;
        }
        None => info!("{} keep default {}", name, *target),
    }
}

fn cfg_duration(target: &mut Duration, config: &toml::Value, name: &str) {
    match get_toml_int_opt(config, name) {
        Some(i) => {
            assert!(i >= 0);
            *target = Duration::from_millis(i as u64);
        }
        None => info!("{} keep default {:?}", name, *target),
    }
}

fn initial_log(matches: &Matches, config: &toml::Value) {
    let level = get_flag_string(matches, "L")
        .unwrap_or_else(|| get_toml_string(config, "server.log-level", Some("info".to_owned())));

    let log_file_path = get_flag_string(matches, "f")
        .unwrap_or_else(|| get_toml_string(config, "server.log-file", Some("".to_owned())));

    let level_filter = logger::get_level_by_string(&level);
    if log_file_path.is_empty() {
        let w = StderrLogger;
        logger::init_log(w, level_filter).unwrap();
    } else {
        let w = RotatingFileLogger::new(&log_file_path).unwrap();
        logger::init_log(w, level_filter).unwrap();
    }
}

fn initial_metric(config: &toml::Value, node_id: Option<u64>) {
    let push_interval = get_toml_int(config, "metric.interval", Some(0));
    if push_interval == 0 {
        return;
    }

    let push_address = get_toml_string(config, "metric.address", Some("".to_owned()));
    if push_address.is_empty() {
        return;
    }

    let mut push_job = get_toml_string(config, "metric.job", Some("tikv".to_owned()));
    if let Some(id) = node_id {
        push_job.push_str(&format!("_{}", id));
    }

    info!("start prometheus client");

    util::monitor_threads("tikv").unwrap();

    util::run_prometheus(Duration::from_millis(push_interval as u64),
                         &push_address,
                         &push_job);
}

fn check_system_config(config: &toml::Value) {
    let max_open_files = get_toml_int(config, "rocksdb.max-open-files", Some(40960));
    if let Err(e) = util::config::check_max_open_fds(max_open_files as u64) {
        exit_with_err(format!("{:?}", e));
    }

    for e in util::config::check_kernel() {
        warn!("{:?}", e);
    }
}

fn check_advertise_address(addr: &str) {
    if let Err(e) = util::config::check_addr(addr) {
        exit_with_err(format!("{:?}", e));
    }

    // FIXME: Forbidden addresses ending in 0 or 255? Those are not always invalid.
    // See more: https://en.wikipedia.org/wiki/IPv4#Addresses_ending_in_0_or_255
    let invalid_patterns = [("0.", 0)]; // Current network is not allowed.

    for &(pat, pos) in &invalid_patterns {
        if let Some(idx) = addr.find(pat) {
            if pos == idx {
                exit_with_err(format!("invalid advertise-addr: {:?}", addr));
            }
        }
    }
}

fn get_rocksdb_db_option(config: &toml::Value) -> RocksdbOptions {
    let mut opts = RocksdbOptions::new();
    let rmode = get_toml_int(config, "rocksdb.wal-recovery-mode", Some(2));
    let wal_recovery_mode = util::config::parse_rocksdb_wal_recovery_mode(rmode).unwrap();
    opts.set_wal_recovery_mode(wal_recovery_mode);

    let wal_dir = get_toml_string(config, "rocksdb.wal-dir", Some("".to_owned()));
    if !wal_dir.is_empty() {
        opts.set_wal_dir(&wal_dir)
    };

    let wal_ttl_seconds = get_toml_int(config, "rocksdb.wal-ttl-seconds", Some(0));
    opts.set_wal_ttl_seconds(wal_ttl_seconds as u64);

    let wal_size_limit = get_toml_int(config, "rocksdb.wal-size-limit", Some(0));
    // return size in MB
    let wal_size_limit_mb = align_to_mb(wal_size_limit as u64) / MB;
    opts.set_wal_size_limit_mb(wal_size_limit_mb as u64);

    let max_total_wal_size = get_toml_int(config,
                                          "rocksdb.max-total-wal-size",
                                          Some(4 * 1024 * 1024 * 1024));
    opts.set_max_total_wal_size(max_total_wal_size as u64);

    let max_background_compactions =
        get_toml_int(config, "rocksdb.max-background-compactions", Some(6));
    opts.set_max_background_compactions(max_background_compactions as i32);

    let max_background_flushes = get_toml_int(config, "rocksdb.max-background-flushes", Some(2));
    opts.set_max_background_flushes(max_background_flushes as i32);

    let max_manifest_file_size = get_toml_int(config,
                                              "rocksdb.max-manifest-file-size",
                                              Some(20 * 1024 * 1024));
    opts.set_max_manifest_file_size(max_manifest_file_size as u64);

    let create_if_missing = get_toml_boolean(config, "rocksdb.create-if-missing", Some(true));
    opts.create_if_missing(create_if_missing);

    let max_open_files = get_toml_int(config, "rocksdb.max-open-files", Some(40960));
    opts.set_max_open_files(max_open_files as i32);

    let enable_statistics = get_toml_boolean(config, "rocksdb.enable-statistics", Some(true));
    if enable_statistics {
        opts.enable_statistics();
        let stats_dump_period_sec =
            get_toml_int(config, "rocksdb.stats-dump-period-sec", Some(600));
        opts.set_stats_dump_period_sec(stats_dump_period_sec as usize);
    }

    let compaction_readahead_size =
        get_toml_int(config, "rocksdb.compaction-readahead-size", Some(0));
    opts.set_compaction_readahead_size(compaction_readahead_size as u64);

    opts
}

fn get_rocksdb_cf_option(config: &toml::Value,
                         cf: &str,
                         block_cache_default: u64,
                         use_bloom_filter: bool,
                         whole_key_filtering: bool)
                         -> RocksdbOptions {
    let prefix = String::from("rocksdb.") + cf + ".";
    let mut opts = RocksdbOptions::new();
    let mut block_base_opts = BlockBasedOptions::new();
    let block_size = get_toml_int(config,
                                  (prefix.clone() + "block-size").as_str(),
                                  Some(64 * 1024));
    block_base_opts.set_block_size(block_size as usize);
    let block_cache_size = get_toml_int(config,
                                        (prefix.clone() + "block-cache-size").as_str(),
                                        Some(block_cache_default as i64));
    block_base_opts.set_lru_cache(block_cache_size as usize);

    let cache_index_and_filter =
        get_toml_boolean(config,
                         (prefix.clone() + "cache-index-and-filter-blocks").as_str(),
                         Some(true));
    block_base_opts.set_cache_index_and_filter_blocks(cache_index_and_filter);

    if use_bloom_filter {
        let bloom_bits_per_key = get_toml_int(config,
                                              (prefix.clone() + "bloom-filter-bits-per-key")
                                                  .as_str(),
                                              Some(10));
        let block_based_filter = get_toml_boolean(config,
                                                  (prefix.clone() + "block-based-bloom-filter")
                                                      .as_str(),
                                                  Some(false));
        block_base_opts.set_bloom_filter(bloom_bits_per_key as i32, block_based_filter);

        block_base_opts.set_whole_key_filtering(whole_key_filtering);
    }
    opts.set_block_based_table_factory(&block_base_opts);

    let cpl = get_toml_string(config,
                              (prefix.clone() + "compression-per-level").as_str(),
                              Some("lz4:lz4:lz4:lz4:lz4:lz4:lz4".to_owned()));
    let per_level_compression = util::config::parse_rocksdb_per_level_compression(&cpl).unwrap();
    opts.compression_per_level(&per_level_compression);

    let write_buffer_size = get_toml_int(config,
                                         (prefix.clone() + "write-buffer-size").as_str(),
                                         Some(128 * 1024 * 1024));
    opts.set_write_buffer_size(write_buffer_size as u64);

    let max_write_buffer_number = get_toml_int(config,
                                               (prefix.clone() + "max-write-buffer-number")
                                                   .as_str(),
                                               Some(5));
    opts.set_max_write_buffer_number(max_write_buffer_number as i32);

    let min_write_buffer_number_to_merge =
        get_toml_int(config,
                     (prefix.clone() + "min-write-buffer-number-to-merge").as_str(),
                     Some(1));
    opts.set_min_write_buffer_number_to_merge(min_write_buffer_number_to_merge as i32);

    let max_bytes_for_level_base = get_toml_int(config,
                                                (prefix.clone() + "max-bytes-for-level-base")
                                                    .as_str(),
                                                Some(128 * 1024 * 1024));
    opts.set_max_bytes_for_level_base(max_bytes_for_level_base as u64);

    let target_file_size_base = get_toml_int(config,
                                             (prefix.clone() + "target-file-size-base").as_str(),
                                             Some(32 * 1024 * 1024));
    opts.set_target_file_size_base(target_file_size_base as u64);

    let level_zero_slowdown_writes_trigger =
        get_toml_int(config,
                     (prefix.clone() + "level0-slowdown-writes-trigger").as_str(),
                     Some(20));
    opts.set_level_zero_slowdown_writes_trigger(level_zero_slowdown_writes_trigger as i32);

    let level_zero_stop_writes_trigger =
        get_toml_int(config,
                     (prefix.clone() + "level0-stop-writes-trigger").as_str(),
                     Some(36));
    opts.set_level_zero_stop_writes_trigger(level_zero_stop_writes_trigger as i32);

    opts
}

fn get_rocksdb_default_cf_option(config: &toml::Value, total_mem: u64) -> RocksdbOptions {
    // Default column family uses bloom filter.
    let default_block_cache_size =
        align_to_mb((total_mem as f64 * DEFAULT_BLOCK_CACHE_RATIO[0]) as u64);
    get_rocksdb_cf_option(config,
                          "defaultcf",
                          default_block_cache_size,
                          true, // bloom filter
                          true /* whole key filtering */)
}

fn get_rocksdb_write_cf_option(config: &toml::Value, total_mem: u64) -> RocksdbOptions {
    let default_block_cache_size =
        align_to_mb((total_mem as f64 * DEFAULT_BLOCK_CACHE_RATIO[1]) as u64);
    let mut opt = get_rocksdb_cf_option(config, "writecf", default_block_cache_size, true, false);
    // prefix extractor(trim the timestamp at tail) for write cf.
    opt.set_prefix_extractor("FixedSuffixSliceTransform",
                              Box::new(rocksdb_util::FixedSuffixSliceTransform::new(8)))
        .unwrap();
    // create prefix bloom for memtable.
    opt.set_memtable_prefix_bloom_size_ratio(0.1 as f64);
    opt
}

fn get_rocksdb_raftlog_cf_option(config: &toml::Value, total_mem: u64) -> RocksdbOptions {
    let mut default_block_cache_size =
        align_to_mb((total_mem as f64 * DEFAULT_BLOCK_CACHE_RATIO[2]) as u64);
    if default_block_cache_size < RAFTCF_MIN_MEM {
        default_block_cache_size = RAFTCF_MIN_MEM;
    }
    if default_block_cache_size > RAFTCF_MAX_MEM {
        default_block_cache_size = RAFTCF_MAX_MEM;
    }
    let mut opt = get_rocksdb_cf_option(config, "raftcf", default_block_cache_size, false, false);
    opt.set_memtable_insert_hint_prefix_extractor("RaftPrefixSliceTransform",
            Box::new(rocksdb_util::FixedPrefixSliceTransform::new(region_raft_prefix_len())))
        .unwrap();
    opt
}

fn get_rocksdb_lock_cf_option(config: &toml::Value) -> RocksdbOptions {
    let mut opts = RocksdbOptions::new();
    let mut block_base_opts = BlockBasedOptions::new();
    block_base_opts.set_block_size(16 * 1024);

    let block_cache_size = get_toml_int(config,
                                        "rocksdb.lockcf.block-cache-size",
                                        Some(64 * 1024 * 1024));
    block_base_opts.set_lru_cache(block_cache_size as usize);

    block_base_opts.set_bloom_filter(10, false);
    opts.set_block_based_table_factory(&block_base_opts);

    let cpl = "no:no:no:no:no:no:no".to_owned();
    let per_level_compression = util::config::parse_rocksdb_per_level_compression(&cpl).unwrap();
    opts.compression_per_level(&per_level_compression);

    let write_buffer_size = get_toml_int(config,
                                         "rocksdb.lockcf.write-buffer-size",
                                         Some(64 * 1024 * 1024));
    opts.set_write_buffer_size(write_buffer_size as u64);

    let max_write_buffer_number =
        get_toml_int(config, "rocksdb.lockcf.max-write-buffer-number", Some(5));
    opts.set_max_write_buffer_number(max_write_buffer_number as i32);

    let max_bytes_for_level_base = get_toml_int(config,
                                                "rocksdb.lockcf.max-bytes-for-level-base",
                                                Some(64 * 1024 * 1024));
    opts.set_max_bytes_for_level_base(max_bytes_for_level_base as u64);
    opts.set_target_file_size_base(32 * 1024 * 1024);

    // set level0_file_num_compaction_trigger = 1 is very important,
    // this will result in fewer sst files in lock cf.
    opts.set_level_zero_file_num_compaction_trigger(1);

    let level_zero_slowdown_writes_trigger = get_toml_int(config,
                                                          "rocksdb.lockcf.\
                                                           level0-slowdown-writes-trigger",
                                                          Some(20));
    opts.set_level_zero_slowdown_writes_trigger(level_zero_slowdown_writes_trigger as i32);

    let level_zero_stop_writes_trigger = get_toml_int(config,
                                                      "rocksdb.lockcf.level0-stop-writes-trigger",
                                                      Some(36));
    opts.set_level_zero_stop_writes_trigger(level_zero_stop_writes_trigger as i32);

    opts
}

fn adjust_end_points_by_cpu_num(total_cpu_num: usize) -> usize {
    if total_cpu_num >= 8 {
        (total_cpu_num as f32 * 0.8) as usize
    } else {
        4
    }
}

fn adjust_sched_workers_by_cpu_num(total_cpu_num: usize) -> usize {
    if total_cpu_num >= 16 { 8 } else { 4 }
}

// TODO: merge this function with Config::new
// Currently, to add a new option, we will define three default value
// in config.rs, this file and config-template.toml respectively. It may be more
// maintainable to keep things in one place.
fn build_cfg(matches: &Matches,
             config: &toml::Value,
             cluster_id: u64,
             addr: String,
             total_cpu_num: usize)
             -> Config {
    let mut cfg = Config::new();
    cfg.cluster_id = cluster_id;
    cfg.addr = addr.to_owned();
    cfg_usize(&mut cfg.notify_capacity, config, "server.notify-capacity");
    if !cfg_usize(&mut cfg.end_point_concurrency,
                  config,
                  "server.end-point-concurrency") {
        cfg.end_point_concurrency = adjust_end_points_by_cpu_num(total_cpu_num);
    }
    cfg_usize(&mut cfg.messages_per_tick,
              config,
              "server.messages-per-tick");
    let capacity = get_flag_int(matches, "capacity")
        .or_else(|| get_toml_int_opt(config, "server.capacity"));
    if let Some(cap) = capacity {
        assert!(cap >= 0);
        cfg.raft_store.capacity = cap as u64;
    }

    // Set advertise address for outer node and client use.
    // If no advertise listening address set, use the associated listening address.
    cfg.advertise_addr = get_flag_string(matches, "advertise-addr")
        .unwrap_or_else(|| get_toml_string(config, "server.advertise-addr", Some(addr.to_owned())));
    check_advertise_address(&cfg.advertise_addr);

    cfg_usize(&mut cfg.send_buffer_size, config, "server.send-buffer-size");
    cfg_usize(&mut cfg.recv_buffer_size, config, "server.recv-buffer-size");

    cfg_usize(&mut cfg.raft_store.notify_capacity,
              config,
              "raftstore.notify-capacity");
    cfg_usize(&mut cfg.raft_store.messages_per_tick,
              config,
              "raftstore.messages-per-tick");
    cfg_u64(&mut cfg.raft_store.raft_base_tick_interval,
            config,
            "raftstore.raft-base-tick-interval");
    cfg_usize(&mut cfg.raft_store.raft_heartbeat_ticks,
              config,
              "raftstore.raft-heartbeat-ticks");
    if cfg_usize(&mut cfg.raft_store.raft_election_timeout_ticks,
                 config,
                 "raftstore.raft-election-timeout-ticks") {
        warn!("Election timeout ticks needs to be same across all the cluster, otherwise it may \
               lead to inconsistency.");
    }
    cfg_u64(&mut cfg.raft_store.split_region_check_tick_interval,
            config,
            "raftstore.split-region-check-tick-interval");
    cfg_u64(&mut cfg.raft_store.region_split_size,
            config,
            "raftstore.region-split-size");
    cfg_u64(&mut cfg.raft_store.region_max_size,
            config,
            "raftstore.region-max-size");
    cfg_u64(&mut cfg.raft_store.region_check_size_diff,
            config,
            "raftstore.region-split-check-diff");
    cfg_u64(&mut cfg.raft_store.raft_log_gc_tick_interval,
            config,
            "raftstore.raft-log-gc-tick-interval");
    cfg_u64(&mut cfg.raft_store.raft_log_gc_threshold,
            config,
            "raftstore.raft-log-gc-threshold");
    cfg_u64(&mut cfg.raft_store.raft_log_gc_count_limit,
            config,
            "raftstore.raft-log-gc-count-limit");
    cfg_u64(&mut cfg.raft_store.raft_log_gc_size_limit,
            config,
            "raftstore.raft-log-gc-size-limit");
    cfg_u64(&mut cfg.raft_store.region_compact_check_interval,
            config,
            "raftstore.region-compact-check-interval");
    cfg_u64(&mut cfg.raft_store.region_compact_delete_keys_count,
            config,
            "raftstore.region-compact-delete-keys-count");
    cfg_u64(&mut cfg.raft_store.lock_cf_compact_interval,
            config,
            "raftstore.lock-cf-compact-interval");
    cfg_u64(&mut cfg.raft_store.lock_cf_compact_threshold,
            config,
            "raftstore.lock-cf-compact-threshold");
    cfg_u64(&mut cfg.raft_store.raft_entry_max_size,
            config,
            "raftstore.raft-entry-max-size");
    cfg_duration(&mut cfg.raft_store.max_peer_down_duration,
                 config,
                 "raftstore.max-peer-down-duration");
    cfg_u64(&mut cfg.raft_store.pd_heartbeat_tick_interval,
            config,
            "raftstore.pd-heartbeat-tick-interval");
    cfg_u64(&mut cfg.raft_store.pd_store_heartbeat_tick_interval,
            config,
            "raftstore.pd-store-heartbeat-tick-interval");
    cfg_u64(&mut cfg.raft_store.consistency_check_tick_interval,
            config,
            "raftstore.consistency-check-interval");
    cfg.raft_store.use_sst_file_snapshot =
        get_toml_boolean(config, "raftstore.use-sst-file-snapshot", Some(false));
    cfg_usize(&mut cfg.storage.sched_notify_capacity,
              config,
              "storage.scheduler-notify-capacity");
    cfg_usize(&mut cfg.storage.sched_msg_per_tick,
              config,
              "storage.scheduler-messages-per-tick");
    cfg_usize(&mut cfg.storage.sched_concurrency,
              config,
              "storage.scheduler-concurrency");
    if !cfg_usize(&mut cfg.storage.sched_worker_pool_size,
                  config,
                  "storage.scheduler-worker-pool-size") {
        cfg.storage.sched_worker_pool_size = adjust_sched_workers_by_cpu_num(total_cpu_num);
    }
    cfg_usize(&mut cfg.storage.sched_too_busy_threshold,
              config,
              "storage.scheduler-too-busy-threshold");

    cfg
}

fn build_raftkv(config: &toml::Value,
                ch: SendCh<Msg>,
                pd_client: Arc<RpcClient>,
                cfg: &Config,
                total_mem: u64)
                -> (Node<RpcClient>, Storage, ServerRaftStoreRouter, SnapManager, Arc<DB>) {
    let trans = ServerTransport::new(ch);
    let path = Path::new(&cfg.storage.path).to_path_buf();
    let db_opts = get_rocksdb_db_option(config);
    let mut cfs_opts = HashMap::default();
    cfs_opts.insert(CF_DEFAULT, get_rocksdb_default_cf_option(config, total_mem));
    cfs_opts.insert(CF_LOCK, get_rocksdb_lock_cf_option(config));
    cfs_opts.insert(CF_WRITE, get_rocksdb_write_cf_option(config, total_mem));
    cfs_opts.insert(CF_RAFT, get_rocksdb_raftlog_cf_option(config, total_mem));
    let mut db_path = path.clone();
    db_path.push("db");
    let engine =
        Arc::new(rocksdb_util::new_engine_opt(db_path.to_str().unwrap(), db_opts, cfs_opts)
            .unwrap());

    let mut event_loop = store::create_event_loop(&cfg.raft_store).unwrap();
    let mut node = Node::new(&mut event_loop, cfg, pd_client);

    let mut snap_path = path.clone();
    snap_path.push("snap");
    let snap_path = snap_path.to_str().unwrap().to_owned();
    let snap_mgr = SnapManager::new(snap_path,
                                    Some(node.get_sendch()),
                                    cfg.raft_store.use_sst_file_snapshot);

    node.start(event_loop, engine.clone(), trans, snap_mgr.clone()).unwrap();
    let router = ServerRaftStoreRouter::new(node.get_sendch(), node.id());

    (node,
     create_raft_storage(router.clone(), engine.clone(), cfg).unwrap(),
     router,
     snap_mgr,
     engine)
}

fn canonicalize_path(path: &str) -> String {
    let p = Path::new(path);
    if p.exists() && p.is_file() {
        exit_with_err(format!("{} is not a directory!", path));
    }
    if !p.exists() {
        fs::create_dir_all(p).unwrap();
    }
    format!("{}", p.canonicalize().unwrap().display())
}

fn get_store_and_backup_path(matches: &Matches, config: &toml::Value) -> (String, String) {
    // Store path
    let store_path = get_flag_string(matches, "s")
        .unwrap_or_else(|| get_toml_string(config, "server.store", Some(TEMP_DIR.to_owned())));
    let store_abs_path = if store_path == TEMP_DIR {
        TEMP_DIR.to_owned()
    } else {
        canonicalize_path(&store_path)
    };

    // Backup path
    let mut backup_path = get_toml_string(config, "server.backup", Some(String::new()));
    if backup_path.is_empty() && store_abs_path != TEMP_DIR {
        backup_path = format!("{}", Path::new(&store_abs_path).join("backup").display())
    }

    if backup_path.is_empty() {
        info!("empty backup path, backup is disabled");
        (store_abs_path, backup_path)
    } else {
        let backup_abs_path = canonicalize_path(&backup_path);
        info!("backup path: {}", backup_abs_path);
        (store_abs_path, backup_abs_path)
    }
}

fn get_store_labels(matches: &Matches, config: &toml::Value) -> HashMap<String, String> {
    let labels = get_flag_string(matches, "labels")
        .unwrap_or_else(|| get_toml_string(config, "server.labels", Some("".to_owned())));
    util::config::parse_store_labels(&labels).unwrap()
}

fn start_server<T, S>(mut server: Server<T, S>,
                      mut el: EventLoop<Server<T, S>>,
                      engine: Arc<DB>,
                      backup_path: &str)
    where T: RaftStoreRouter,
          S: StoreAddrResolver + Send + 'static
{
    let ch = server.get_sendch();
    let h = thread::Builder::new()
        .name("tikv-eventloop".to_owned())
        .spawn(move || {
            server.run(&mut el).unwrap();
        })
        .unwrap();
    signal_handler::handle_signal(ch, engine, backup_path);
    h.join().unwrap();
}

fn run_raft_server(pd_client: RpcClient,
                   cfg: Config,
                   backup_path: &str,
                   config: &toml::Value,
                   total_mem: u64) {
    let mut event_loop = create_event_loop(&cfg).unwrap();
    let ch = SendCh::new(event_loop.channel(), "raft-server");
    let pd_client = Arc::new(pd_client);
    let resolver = PdStoreAddrResolver::new(pd_client.clone()).unwrap();

    let store_path = &cfg.storage.path;
    let mut lock_path = Path::new(store_path).to_path_buf();
    lock_path.push("LOCK");
    let f = File::create(lock_path).unwrap();
    if f.try_lock_exclusive().is_err() {
        panic!("lock {} failed, maybe another instance is using this directory.",
               store_path);
    }

    let (mut node, mut store, raft_router, snap_mgr, engine) =
        build_raftkv(config, ch.clone(), pd_client, &cfg, total_mem);
    info!("tikv server config: {:?}", cfg);

    initial_metric(config, Some(node.id()));

    info!("start storage");
    if let Err(e) = store.start(&cfg.storage) {
        panic!("failed to start storage, error = {:?}", e);
    }

    info!("Start listening on {}...", cfg.addr);
    let listener = bind(&cfg.addr).unwrap();

    let server_chan = ServerChannel {
        raft_router: raft_router,
        snapshot_status_sender: node.get_snapshot_status_sender(),
    };
    let svr = Server::new(&mut event_loop,
                          &cfg,
                          listener,
                          store,
                          server_chan,
                          resolver,
                          snap_mgr)
        .unwrap();
    start_server(svr, event_loop, engine, backup_path);
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
    opts.optflag("V", "version", "print version information");
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("C", "config", "set configuration file", "file path");
    opts.optopt("s",
                "store",
                "set the path to store directory",
                "/tmp/tikv/store");
    opts.optopt("",
                "capacity",
                "set the store capacity",
                "default: 0 (disk capacity)");
    opts.optopt("", "pd", "pd endpoints", "127.0.0.1:2379,127.0.0.1:3379");
    opts.optopt("",
                "labels",
                "attributes about this server",
                "zone=example-zone,disk=example-disk");

    let matches = opts.parse(&args[1..]).unwrap_or_else(|e| {
        println!("opts parse failed, {:?}", e);
        print_usage(&program, &opts);
        process::exit(1);
    });
    if matches.opt_present("h") {
        print_usage(&program, &opts);
        return;
    }
    if matches.opt_present("V") {
        let (hash, date, rustc) = util::build_info();
        println!("Git Commit Hash: {}", hash);
        println!("UTC Build Time:  {}", date);
        println!("Rustc Version:   {}", rustc);
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

    panic_hook::set_exit_hook();

    // Before any startup, check system configuration.
    check_system_config(&config);

    let addr = get_flag_string(&matches, "A").unwrap_or_else(|| {
        let addr = get_toml_string(&config,
                                   "server.addr",
                                   Some(DEFAULT_LISTENING_ADDR.to_owned()));
        if let Err(e) = util::config::check_addr(&addr) {
            exit_with_err(format!("{:?}", e));
        }
        addr
    });

    let pd_endpoints = get_flag_string(&matches, "pd")
        .unwrap_or_else(|| get_toml_string(&config, "pd.endpoints", None));
    for addr in pd_endpoints.split(',')
        .map(|s| s.trim())
        .filter_map(|s| if s.is_empty() {
            None
        } else if s.starts_with("http://") {
            Some(&s[7..])
        } else {
            Some(s)
        }) {
        if let Err(e) = util::config::check_addr(addr) {
            panic!("{:?}", e);
        }
    }

    let pd_client = RpcClient::new(&pd_endpoints)
        .unwrap_or_else(|err| exit_with_err(format!("{:?}", err)));
    let cluster_id = pd_client.get_cluster_id()
        .unwrap_or_else(|err| exit_with_err(format!("{:?}", err)));

    let total_cpu_num = cpu_num().unwrap();
    // return  memory in KB.
    let mem = mem_info().unwrap();
    let total_mem = mem.total * KB;
    if !sanitize_memory_usage() {
        panic!("default block cache size over total memory.");
    }

    let mut cfg = build_cfg(&matches, &config, cluster_id, addr, total_cpu_num as usize);
    cfg.labels = get_store_labels(&matches, &config);
    let (store_path, backup_path) = get_store_and_backup_path(&matches, &config);
    cfg.storage.path = store_path;

    if cluster_id == DEFAULT_CLUSTER_ID {
        panic!("in raftkv, cluster_id must greater than 0");
    }
    let _m = TimeMonitor::default();
    run_raft_server(pd_client, cfg, &backup_path, &config, total_mem);
}
