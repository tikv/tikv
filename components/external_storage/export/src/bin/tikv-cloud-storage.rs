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
    use nix::sys::signal::{SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2};
    use signal::trap::Trap;
    use slog_global::info;

    pub fn for_signal() {
        let trap = Trap::trap(&[SIGTERM, SIGINT, SIGHUP, SIGUSR1, SIGUSR2]);
        for sig in trap {
            match sig {
                SIGUSR1 | SIGTERM | SIGINT | SIGHUP => {
                    info!("receive signal {}, stopping server...", sig as c_int);
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
