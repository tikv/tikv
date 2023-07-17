// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

pub use self::imp::wait_for_signal;

#[cfg(unix)]
mod imp {
<<<<<<< HEAD
    use engine_traits::{Engines, KvEngine, MiscExt, RaftEngine};
=======
    use engine_traits::MiscExt;
    use service::service_event::ServiceEvent;
>>>>>>> c27b43018c (raftstore & raftstore-v2:control grpc server according to slowness. (#15088))
    use signal_hook::{
        consts::{SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2},
        iterator::Signals,
    };
    use tikv_util::{metrics, mpsc as TikvMpsc};

    #[allow(dead_code)]
<<<<<<< HEAD
    pub fn wait_for_signal(engines: Option<Engines<impl KvEngine, impl RaftEngine>>) {
=======
    pub fn wait_for_signal(
        engines: Option<Engines<impl KvEngine, impl RaftEngine>>,
        kv_statistics: Option<Arc<RocksStatistics>>,
        raft_statistics: Option<Arc<RocksStatistics>>,
        service_event_tx: Option<TikvMpsc::Sender<ServiceEvent>>,
    ) {
>>>>>>> c27b43018c (raftstore & raftstore-v2:control grpc server according to slowness. (#15088))
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
