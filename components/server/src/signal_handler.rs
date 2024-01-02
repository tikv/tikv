// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_rocks::RocksStatistics;
use engine_traits::{Engines, KvEngine, RaftEngine};

pub use self::imp::wait_for_signal;

#[cfg(unix)]
mod imp {
    use engine_traits::MiscExt;
    use service::service_event::ServiceEvent;
    use signal_hook::{
        consts::{SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2},
        iterator::Signals,
    };
    use tikv_util::{metrics, mpsc as TikvMpsc};

    use super::*;

    #[allow(dead_code)]
    pub fn wait_for_signal(
        engines: Option<Engines<impl KvEngine, impl RaftEngine>>,
        kv_statistics: Option<Arc<RocksStatistics>>,
        raft_statistics: Option<Arc<RocksStatistics>>,
        service_event_tx: Option<TikvMpsc::Sender<ServiceEvent>>,
    ) {
        let mut signals = Signals::new([SIGTERM, SIGINT, SIGHUP, SIGUSR1, SIGUSR2]).unwrap();
        for signal in &mut signals {
            match signal {
                SIGTERM | SIGINT | SIGHUP => {
                    info!("receive signal {}, stopping server...", signal);
                    if let Some(tx) = service_event_tx {
                        if let Err(e) = tx.send(ServiceEvent::Exit) {
                            warn!("failed to notify grpc server exit, {:?}", e);
                        }
                    }
                    break;
                }
                SIGUSR1 => {
                    // Use SIGUSR1 to log metrics.
                    info!("{}", metrics::dump(false));
                    if let Some(ref engines) = engines {
                        info!("{:?}", MiscExt::dump_stats(&engines.kv));
                        if let Some(s) = kv_statistics.as_ref()
                            && let Some(s) = s.to_string()
                        {
                            info!("{:?}", s);
                        }
                        info!("{:?}", RaftEngine::dump_stats(&engines.raft));
                        if let Some(s) = raft_statistics.as_ref()
                            && let Some(s) = s.to_string()
                        {
                            info!("{:?}", s);
                        }
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
    use super::*;

    pub fn wait_for_signal(
        _: Option<Engines<impl KvEngine, impl RaftEngine>>,
        _: Option<Arc<RocksStatistics>>,
        _: Option<Arc<RocksStatistics>>,
    ) {
    }
}
