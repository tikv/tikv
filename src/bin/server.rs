extern crate tikv;
#[macro_use]
extern crate log;

use tikv::util::{self, LogLevelFilter};

fn main() {
    util::init_log(LogLevelFilter::Debug).unwrap();
    info!("bye.");
}
