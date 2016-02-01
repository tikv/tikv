extern crate tikv;
#[macro_use]
extern crate log;

use tikv::storage::{Storage, Dsn};
use tikv::kvserver::server::run::run;

fn main() {
    let default_host: &str = "127.0.0.1:61234";
    let store: Storage = Storage::new(Dsn::Memory).unwrap();
    info!("Start listenning on port 61234...");
    run(default_host, store);
}
