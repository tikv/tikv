// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


#[cfg(unix)]
mod imp {
    use std::sync::Arc;

    use rocksdb::DB;
    use libc;

    use tikv::server::Msg;
    use tikv::util::transport::SendCh;
    use prometheus::{self, Encoder, TextEncoder};

    // Real-time signals
    const TOGGLE_PROF_SIG: libc::c_int = 41;

    use profiling;

    const ROCKSDB_DB_STATS_KEY: &'static str = "rocksdb.dbstats";
    const ROCKSDB_CF_STATS_KEY: &'static str = "rocksdb.cfstats";

    // TODO: remove backup_path from configuration
    pub fn handle_signal(ch: SendCh<Msg>, engine: Arc<DB>, _: &str) {
        use signal::trap::Trap;
        use nix::sys::signal::{SIGTERM, SIGINT, SIGUSR1, SIGUSR2};
        let trap = Trap::trap(&[SIGTERM, SIGINT, SIGUSR1, SIGUSR2, TOGGLE_PROF_SIG]);
        for sig in trap {
            match sig {
                SIGTERM | SIGINT => {
                    info!("receive signal {}, stopping server...", sig);
                    ch.send(Msg::Quit).unwrap();
                    break;
                }
                SIGUSR1 => {
                    // Use SIGUSR1 to log metrics.
                    let mut buffer = vec![];
                    let metric_familys = prometheus::gather();
                    let encoder = TextEncoder::new();
                    encoder.encode(&metric_familys, &mut buffer).unwrap();
                    info!("{}", String::from_utf8(buffer).unwrap());

                    // Log common rocksdb stats.
                    for name in engine.cf_names() {
                        let handler = engine.cf_handle(name).unwrap();
                        if let Some(v) =
                               engine.get_property_value_cf(handler, ROCKSDB_CF_STATS_KEY) {
                            info!("{}", v)
                        }
                    }

                    if let Some(v) = engine.get_property_value(ROCKSDB_DB_STATS_KEY) {
                        info!("{}", v)
                    }

                    // Log more stats if enable_statistics is true.
                    if let Some(v) = engine.get_statistics() {
                        info!("{}", v)
                    }
                }
                SIGUSR2 => profiling::dump_prof(None),
                TOGGLE_PROF_SIG => {
                    if let Err(e) = profiling::toggle_prof() {
                        error!("failed to toggle memory profiling: {}", e);
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
    use std::sync::Arc;

    use rocksdb::DB;

    use tikv::server::Msg;
    use tikv::util::transport::SendCh;

    pub fn handle_signal(_: SendCh<Msg>, _: Arc<DB>, _: &str) {}
}

pub use self::imp::handle_signal;
