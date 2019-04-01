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
    use libc::c_int;

    use tikv_alloc;

    use tikv::engine::rocks::util::stats as rocksdb_stats;
    use tikv::engine::Engines;
    use tikv::util::metrics;

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
    use tikv::engine::Engines;

    pub fn handle_signal(_: Option<Engines>) {}
}

pub use self::imp::handle_signal;
