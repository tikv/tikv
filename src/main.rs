#[macro_use]
extern crate log;
use util::LogLevelFilter;
mod util;

fn main() {
    util::init_log(LogLevelFilter::Debug).unwrap();
    info!("bye.");
}
