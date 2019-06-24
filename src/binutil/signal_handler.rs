// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

#[cfg(unix)]
mod imp {
    use libc::c_int;

    use engine::rocks::util::stats as rocksdb_stats;
    use engine::Engines;
    use tikv_util::metrics;

    #[allow(dead_code)]
    pub fn handle_signal(engines: Option<Engines>) {
        use nix::sys::signal::{SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2};
        use signal::trap::Trap;
        let trap = Trap::trap(&[SIGTERM, SIGINT, SIGHUP, SIGUSR1, SIGUSR2]);
        for sig in trap {
            match sig {
                SIGTERM | SIGINT | SIGHUP => {
                    info!("receive signal {}, stopping server...", sig as c_int);
                    break;
                }
                SIGUSR1 => {
                    // Use SIGUSR1 to log metrics.
                    info!("{}", metrics::dump());
                    if let Some(ref engines) = engines {
                        info!("{:?}", rocksdb_stats::dump(&engines.kv));
                        info!("{:?}", rocksdb_stats::dump(&engines.raft));
                    }
                }
                // TODO: handle more signal
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(not(unix))]
mod imp {
    use engine::Engines;

    pub fn handle_signal(_: Option<Engines>) {}
}

pub use self::imp::handle_signal;
