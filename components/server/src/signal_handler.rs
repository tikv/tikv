// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

pub use self::imp::wait_for_signal;

#[cfg(unix)]
mod imp {
    use engine_traits::{Engines, KvEngine, MiscExt, RaftEngine};
    use signal_hook::{
        consts::{SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2},
        iterator::Signals,
    };
    use tikv_util::metrics;

    #[allow(dead_code)]
    pub fn wait_for_signal(engines: Option<Engines<impl KvEngine, impl RaftEngine>>) {
        let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGHUP, SIGUSR1, SIGUSR2]).unwrap();
        for signal in &mut signals {
            match signal {
                SIGTERM | SIGINT | SIGHUP => {
                    info!("receive signal {}, stopping server...", signal);
                    break;
                }
                SIGUSR1 => {
                    // Use SIGUSR1 to log metrics.
                    info!("{}", metrics::dump(false));
                    if let Some(ref engines) = engines {
                        info!("{:?}", MiscExt::dump_stats(&engines.kv));
                        info!("{:?}", RaftEngine::dump_stats(&engines.raft));
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
    use engine_traits::{Engines, KvEngine, RaftEngine};

    pub fn wait_for_signal(_: Option<Engines<impl KvEngine, impl RaftEngine>>) {}
}
