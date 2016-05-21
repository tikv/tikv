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

use std::env;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::io::Read;

use getopts::{Options, Matches};
use rocksdb::{DB, Options as RocksdbOptions, BlockBasedOptions, DBCompressionType};
use mio::tcp::TcpListener;

use tikv::storage::{Storage, Dsn, TEMP_DIR};
use tikv::util::{self, logger, panic_hook};
use tikv::server::{DEFAULT_LISTENING_ADDR, SendCh, Server, Node, Config, bind, create_event_loop,
                   create_raft_storage};
use tikv::server::{ServerTransport, ServerRaftStoreRouter, MockRaftStoreRouter};
use tikv::server::{MockStoreAddrResolver, PdStoreAddrResolver};
use tikv::raftstore::store;
use tikv::pd::{new_rpc_client, RpcClient};

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
    matches.opt_str(short)
        .or_else(|| {
            config.lookup(long).and_then(|v| f(v)).or_else(|| {
                info!("malformed or missing {}, use default", long);
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
    util::init_log(logger::get_level_by_string(&level)).unwrap();
}

fn build_raftkv(matches: &Matches,
                config: &toml::Value,
                ch: SendCh,
                cluster_id: u64,
                addr: String,
                pd_client: Arc<RpcClient>)
                -> (Storage, Arc<RwLock<ServerRaftStoreRouter>>) {
    let trans = Arc::new(RwLock::new(ServerTransport::new(ch)));

    let path = get_store_path(matches, config);
    let mut opts = RocksdbOptions::new();
    let mut block_base_opts = BlockBasedOptions::new();
    block_base_opts.set_block_size(64 * 1024);
    opts.set_block_based_table_factory(&block_base_opts);
    opts.compression(DBCompressionType::DBNo);
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(5);
    opts.set_min_write_buffer_number_to_merge(2);
    opts.set_max_background_compactions(3);
    opts.set_max_bytes_for_level_base(64 * 1024 * 1024);
    opts.set_target_file_size_base(64 * 1024 * 1024);
    opts.create_if_missing(true);
    opts.set_level_zero_slowdown_writes_trigger(12);
    opts.set_level_zero_stop_writes_trigger(24);
    // TODO: enable lz4 compression

    let engine = Arc::new(DB::open(&opts, &path).unwrap());
    let mut cfg = Config::new();
    cfg.cluster_id = cluster_id;

    cfg.addr = addr.clone();

    // Set advertise address for outer node and client use.
    // If no advertise listening address set, use the associated listening address.
    cfg.advertise_addr = get_string_value("advertise-addr",
                                          "server.advertise-addr",
                                          matches,
                                          config,
                                          Some(addr),
                                          |v| v.as_str().map(|s| s.to_owned()));

    let mut event_loop = store::create_event_loop(&cfg.store_cfg).unwrap();
    let mut node = Node::new(&mut event_loop, &cfg, pd_client);
    node.start(event_loop, engine.clone(), trans).unwrap();
    let raft_router = node.raft_store_router();

    (create_raft_storage(node, engine).unwrap(), raft_router)
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

fn run_local_server(listener: TcpListener, store: Storage) {
    let mut event_loop = create_event_loop().unwrap();
    let router = Arc::new(RwLock::new(MockRaftStoreRouter));
    let mut svr = Server::new(&mut event_loop,
                              listener,
                              store,
                              router,
                              MockStoreAddrResolver)
        .unwrap();
    svr.run(&mut event_loop).unwrap();
}

fn run_raft_server(listener: TcpListener, matches: &Matches, config: &toml::Value) {
    let mut event_loop = create_event_loop().unwrap();
    let ch = SendCh::new(event_loop.channel());

    let id = get_string_value("I",
                              "raft.cluster-id",
                              matches,
                              config,
                              None,
                              |v| v.as_integer().map(|i| i.to_string()));
    let cluster_id = u64::from_str_radix(&id, 10).expect("invalid cluster id");

    let pd_addr = get_string_value("pd",
                                   "raft.pd",
                                   matches,
                                   config,
                                   None,
                                   |v| v.as_str().map(|s| s.to_owned()));
    let pd_client = Arc::new(new_rpc_client(&pd_addr, cluster_id).unwrap());
    let resolver = PdStoreAddrResolver::new(pd_client.clone()).unwrap();

    let (store, raft_router) = build_raftkv(matches,
                                            config,
                                            ch,
                                            cluster_id,
                                            format!("{}", listener.local_addr().unwrap()),
                                            pd_client);

    let mut svr = Server::new(&mut event_loop, listener, store, raft_router, resolver).unwrap();
    svr.run(&mut event_loop).unwrap();
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
                "127.0.0.1:20160, if not set, use addr instead.");
    opts.optopt("L",
                "log",
                "set log level",
                "log level: trace, debug, info, warn, error, off");
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("C", "config", "set configuration file", "file path");
    opts.optopt("s",
                "store",
                "set the path to rocksdb directory",
                "/tmp/tikv/store");
    opts.optopt("S",
                "dsn",
                "set which dsn to use, warning: default is rocksdb without persistent",
                "dsn: rocksdb, raftkv");
    opts.optopt("I", "cluster-id", "set cluster id", "must greater than 0.");
    opts.optopt("", "pd", "set pd address", "host:port");
    let matches = opts.parse(&args[1..]).expect("opts parse failed");
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let config = match matches.opt_str("C") {
        Some(path) => {
            let mut config_file = fs::File::open(&path).expect("config open filed");
            let mut s = String::new();
            config_file.read_to_string(&mut s).expect("config read filed");
            toml::Value::Table(toml::Parser::new(&s).parse().expect("malformed config file"))
        }
        // Empty value, lookup() always return `None`.
        None => toml::Value::Integer(0),
    };

    initial_log(&matches, &config);

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

    match dsn_name.as_ref() {
        ROCKSDB_DSN => {
            let path = get_store_path(&matches, &config);
            let store = Storage::new(Dsn::RocksDBPath(&path)).unwrap();
            run_local_server(listener, store);
        }
        RAFTKV_DSN => {
            run_raft_server(listener, &matches, &config);
        }
        n => panic!("unrecognized dns name: {}", n),
    };
}
