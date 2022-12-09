// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use external_storage_export::new_service;
use grpcio::{self};
use slog::{self};
use slog_global::{info, warn};
use tikv_util::logger::{self};

fn build_logger<D>(drainer: D, log_level: slog::Level)
where
    D: slog::Drain + Send + 'static,
    <D as slog::Drain>::Err: std::fmt::Display,
{
    // use async drainer and init std log.
    logger::init_log(drainer, log_level, true, true, vec![], 100).unwrap_or_else(|e| {
        println!("failed to initialize log: {}", e);
    });
}

fn main() {
    println!("starting GRPC cloud-storage service");
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build();
    build_logger(drain, slog::Level::Debug);
    warn!("redirect grpcio logging");
    grpcio::redirect_log();
    info!("slog logging");
    let service = new_service().expect("GRPC service creation for tikv-cloud-storage");
    wait::for_signal();
    info!("service {:?}", service);
}

#[cfg(unix)]
mod wait {
    use libc::c_int;
    use signal_hook::{
        consts::{SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2},
        iterator::Signals,
        Signals,
    };
    use slog_global::info;

    pub fn for_signal() {
        let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGHUP]).unwrap();
        for signal in &mut signals {
            match signal {
                SIGTERM | SIGINT | SIGHUP => {
                    info!("receive signal {}, stopping server...", signal);
                    break;
                }
                // TODO: handle more signals
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(not(unix))]
mod wait {
    pub fn for_signal() {}
}
