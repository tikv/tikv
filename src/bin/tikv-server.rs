#![feature(plugin)]
#![plugin(clippy)]

extern crate tikv;
extern crate getopts;
#[macro_use]
extern crate log;
extern crate rocksdb;
extern crate mio;

use std::env;
use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};

use getopts::{Options, Matches};
use log::LogLevelFilter;
use rocksdb::DB;
use mio::tcp::TcpListener;

use tikv::storage::{Storage, Dsn};
use tikv::util::{self, logger};
use tikv::server::{DEFAULT_LISTENING_ADDR, SendCh, Server, Node, Config, bind, create_event_loop,
                   create_raft_storage};
use tikv::server::{ServerTransport, ServerRaftStoreRouter, MockRaftStoreRouter};
use tikv::pd::new_rpc_client;

const MEM_DSN: &'static str = "mem";
const ROCKSDB_DSN: &'static str = "rocksdb";
const RAFTKV_DSN: &'static str = "raftkv";

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn initial_log(matches: &Matches) {
    let log_filter = match matches.opt_str("L") {
        Some(level) => logger::get_level_by_string(&level),
        None => LogLevelFilter::Info,
    };
    util::init_log(log_filter).unwrap();
}


fn build_raftkv(matches: &Matches,
                ch: SendCh,
                addr: String)
                -> (Storage, Arc<RwLock<ServerRaftStoreRouter>>) {
    let pd_addr = matches.opt_str("pd").expect("raftkv needs pd client");
    let pd_client = Arc::new(RwLock::new(new_rpc_client(&pd_addr).unwrap()));
    let id = matches.opt_str("I").expect("raftkv requires cluster id");
    let cluster_id = u64::from_str_radix(&id, 10).expect("invalid cluster id");

    let trans = Arc::new(RwLock::new(ServerTransport::new(cluster_id, ch, pd_client.clone())));

    let path = get_store_path(matches);
    let engine = Arc::new(DB::open_default(&path).unwrap());
    let mut cfg = Config::new();
    cfg.cluster_id = cluster_id;

    cfg.addr = addr.clone();

    // Set advertise address for outer node and client use.
    // If no advertise listening address set, use the associated listening address.
    cfg.advertise_addr = matches.opt_str("advertise-addr")
                                .unwrap_or_else(|| addr);

    let mut node = Node::new(&cfg, pd_client, trans.clone());
    node.start(engine).unwrap();
    let raft_router = node.raft_store_router();

    (create_raft_storage(node).unwrap(), raft_router)
}

fn get_store_path(matches: &Matches) -> String {
    let path = matches.opt_str("s").expect("need store path, but none is specified!");

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
    let mut svr = Server::new(&mut event_loop, listener, store, router).unwrap();
    svr.run(&mut event_loop).unwrap();
}

fn run_raft_server(listener: TcpListener, matches: &Matches) {
    let mut event_loop = create_event_loop().unwrap();
    let ch = SendCh::new(event_loop.channel());

    let (store, raft_router) = build_raftkv(&matches,
                                            ch,
                                            format!("{}", listener.local_addr().unwrap()));
    let mut svr = Server::new(&mut event_loop, listener, store, raft_router).unwrap();
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
    // TODO: support loading config file
    // opts.optopt("C", "config", "set configuration file", "file path");
    opts.optopt("s",
                "store",
                "set the path to rocksdb directory",
                "/tmp/tikv/store");
    opts.optopt("S",
                "dsn",
                "set which dsn to use, default is mem",
                "dsn: mem, rocksdb, raftkv");
    opts.optopt("I", "cluster-id", "set cluster id", "must greater than 0.");
    opts.optopt("", "pd", "set pd address", "host:port");
    let matches = opts.parse(&args[1..]).expect("opts parse failed");
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }
    initial_log(&matches);

    let addr = matches.opt_str("A").unwrap_or_else(|| DEFAULT_LISTENING_ADDR.to_owned());
    let listener = bind(&addr).unwrap();

    info!("Start listening on {}...", addr);

    let dsn_name = matches.opt_str("S").unwrap_or_else(|| MEM_DSN.to_owned());

    match dsn_name.as_ref() {
        MEM_DSN => {
            let store = Storage::new(Dsn::Memory).unwrap();
            run_local_server(listener, store);
        }
        ROCKSDB_DSN => {
            let path = get_store_path(&matches);
            let store = Storage::new(Dsn::RocksDBPath(&path)).unwrap();
            run_local_server(listener, store);
        }
        RAFTKV_DSN => {
            run_raft_server(listener, &matches);
        }
        n => panic!("unrecognized dns name: {}", n),
    };
}
