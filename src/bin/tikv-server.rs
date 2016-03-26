#![feature(plugin)]
#![plugin(clippy)]

extern crate tikv;
extern crate getopts;
#[macro_use]
extern crate log;

use tikv::storage::{Storage, Dsn};
use tikv::kvserver::server::run::run;
use tikv::util::{self, logger};
use tikv::storage::RaftKvConfig;
use getopts::{Options, Matches};
use std::env;
use std::fs;
use std::path::Path;
use std::collections::HashSet;
use log::LogLevelFilter;

const DEFAULT_ADDR: &'static str = "127.0.0.1:6102";
const DEFAULT_RAFT_ADDR: &'static str = "127.0.0.1:20160";
const DEFAULT_DSN: &'static str = "mem";

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

fn build_rocksdb_dsn(dirs: &[String]) -> Dsn {
    if dirs.is_empty() {
        panic!("use rocksdb dsn but no rocksdb directory is specified!");
    }
    Dsn::RocksDBPath(dirs[0].as_ref())
}

fn build_raftkv_dsn<'a>(matches: &Matches,
                        cfg: &'a mut RaftKvConfig,
                        dirs: &'a [String],
                        pd_addr: &'a str)
                        -> Dsn<'a> {
    assert!(dirs.len() > 0,
            "use raftkv dsn but no rocksdb directory is specified!");
    cfg.store_pathes = dirs.to_vec();
    if pd_addr.len() == 0 {
        panic!("pd_addr is required when using raftkv.");
    }
    let raftserver_addr = matches.opt_str("R").unwrap_or_else(|| DEFAULT_RAFT_ADDR.to_owned());
    cfg.server_cfg.addr = raftserver_addr.clone();

    // Set advertise address for outer node and client use.
    // If no advertise listening address set, use the associated listening address.
    cfg.server_cfg.advertise_addr = matches.opt_str("advertise-raft")
                                           .unwrap_or_else(|| raftserver_addr);

    // Maybe we can find another place to set the advertise client address later.
    let kv_addr = matches.opt_str("A").unwrap_or_else(|| DEFAULT_ADDR.to_owned());
    cfg.server_cfg.advertise_client_addr = matches.opt_str("advertise-addr")
                                                  .unwrap_or_else(|| kv_addr);

    let cluster_id = matches.opt_str("I").expect("raftkv dsn require cluster id");
    cfg.server_cfg.cluster_id = u64::from_str_radix(&cluster_id, 10).expect("invalid cluster id");
    if cfg.server_cfg.cluster_id == 0 {
        panic!("cluster should not be 0!");
    }
    Dsn::RaftKv(cfg, pd_addr)
}

fn build_store(matches: &Matches, dsn_name: &str, paths: &[String], pd_addr: &str) -> Storage {
    let mut cfg = RaftKvConfig::default();
    let dsn = match dsn_name {
        "mem" => Dsn::Memory,
        "rocksdb" => build_rocksdb_dsn(paths),
        "raftkv" => build_raftkv_dsn(matches, &mut cfg, paths, pd_addr),
        n => panic!("unrecognized dns name: {}", n),
    };
    Storage::new(dsn).unwrap()
}

/// Only directory is accepted. Same directoy can not be specified twice.
fn parse_directory(mut path: Vec<String>) -> Vec<String> {
    let mut parsed = HashSet::with_capacity(path.len());
    for origin_path in path.drain(..) {
        let p = Path::new(&origin_path);
        if p.exists() && p.is_file() {
            panic!("{} is not a directory!", origin_path);
        }
        if !p.exists() {
            fs::create_dir_all(p).unwrap();
        }
        let absolute_path = p.canonicalize().unwrap();
        let final_path = format!("{}", absolute_path.display());
        if parsed.contains(&final_path) {
            panic!("{} has been specified twice.", origin_path);
        }
        parsed.insert(final_path);
    }
    let mut res = Vec::with_capacity(parsed.len());
    res.extend(parsed.drain());
    res
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optopt("A",
                "addr",
                "set listening address",
                "default is 127.0.0.1:6102");
    opts.optopt("",
                "advertise-addr",
                "set advertise listening address for client communication",
                "host:port, if not set, use addr instead.");
    opts.optopt("R",
                "raft",
                "set raft server listening address",
                "default is 127.0.0.1:20160");
    opts.optopt("",
                "advertise-raft",
                "set advertise raft server listening address",
                "host:port, if not set, use raft addr instead.");

    opts.optopt("L",
                "log",
                "set log level",
                "log level: trace, debug, info, warn, error, off");
    opts.optflag("h", "help", "print this help menu");
    // TODO: support loading config file
    // opts.optopt("C", "config", "set configuration file", "file path");
    opts.optmulti("s",
                  "store",
                  "set the path to rocksdb directory",
                  "when specified multiple times, will use each as a rocksdb storage.");
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

    let dsn_name = matches.opt_str("S").unwrap_or_else(|| DEFAULT_DSN.to_owned());
    let pathes = parse_directory(matches.opt_strs("s"));
    let pd_addr = matches.opt_str("pd").unwrap_or("".to_owned());

    let kv_addr = matches.opt_str("A").unwrap_or_else(|| DEFAULT_ADDR.to_owned());

    let store = build_store(&matches, dsn_name.as_ref(), &pathes, pd_addr.as_ref());

    info!("Start listening on {}...", kv_addr);
    run(&kv_addr, store);
}
