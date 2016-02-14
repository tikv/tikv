extern crate tikv;
extern crate getopts;
#[macro_use]
extern crate log;

use tikv::storage::{Storage, Dsn};
use tikv::kvserver::server::run::run;
use tikv::util::{self, LogLevelFilter};
use getopts::Options;
use std::env;

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

fn main() {
    util::init_log(LogLevelFilter::Debug).unwrap();

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();
    let mut opts = Options::new();
    opts.optopt("H", "host", "set host:port", "host:port");
    opts.optflag("h", "help", "print this help menu");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(why) => {
            panic!(why.to_string());
        }
    };
    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }
    let default_host: String = "127.0.0.1:61234".to_string();
    let host: String = match matches.opt_str("H") {
        Some(x) => x,
        None => default_host,
    };
    let store: Storage = Storage::new(Dsn::Memory).unwrap();
    info!("Start listenning on {}...", host);
    run(&host, store);
}
