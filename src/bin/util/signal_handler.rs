// Copyright 2017 TiKV Project Authors.
#[cfg(unix)]
mod imp {
    use libc::c_int;

    use tikv_alloc;

    use tikv::raftstore::store::Engines;
    use tikv::util::{metrics, rocksdb_util::stats as rocksdb_stats};

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
                SIGUSR2 => tikv_alloc::dump_prof(None),
                // TODO: handle more signal
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(not(unix))]
mod imp {
    use tikv::raftstore::store::Engines;

    pub fn handle_signal(_: Option<Engines>) {}
}

pub use self::imp::handle_signal;
