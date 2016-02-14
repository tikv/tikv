extern crate tikv;
extern crate getopts;
#[macro_use]
extern crate log;

use tikv::storage::{Storage, Dsn};
use tikv::kvserver::server::run::run;
use tikv::util::{self, logger};
use getopts::Options;
use std::env;
use log::LogLevelFilter;

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optopt("H", "host", "set host:port", "host:port");
    opts.optopt("L",
                "log",
                "set log level",
                "log level: trace, debug, info, warn, error, off");
    opts.optflag("h", "help", "print this help menu");
    let matches = opts.parse(&args[1..]).expect("opts parse failed");
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }
    let log_filter = match matches.opt_str("L") {
        Some(level) => logger::get_level_by_string(&level),
        None => LogLevelFilter::Info,
    };
    util::init_log(log_filter).unwrap();
    let default_host: String = "127.0.0.1:61234".to_string();
    let host: String = matches.opt_str("H").unwrap_or(default_host);
    let store: Storage = Storage::new(Dsn::Memory).unwrap();
    info!("Start listenning on {}...", host);
    run(&host, store);
}
