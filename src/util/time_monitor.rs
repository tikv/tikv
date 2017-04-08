// Copyright 2016 PingCAP, Inc.
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

use std::time::{SystemTime, Duration};
use std::thread::{self, JoinHandle, Builder};
use std::sync::mpsc::{self, Sender};

const DEFAULT_WAIT_MS: u64 = 100;

pub struct TimeMonitor {
    tx: Sender<bool>,
    handle: Option<JoinHandle<()>>,
}

impl TimeMonitor {
    pub fn new<D, N>(on_jumped: D, now: N) -> TimeMonitor
        where D: Fn() + Send + 'static,
              N: Fn() -> SystemTime + Send + 'static
    {
        let (tx, rx) = mpsc::channel();
        let h = Builder::new()
            .name(thd_name!("time-monitor-worker"))
            .spawn(move || while let Err(_) = rx.try_recv() {
                       let before = now();
                       thread::sleep(Duration::from_millis(DEFAULT_WAIT_MS));

                       let after = now();
                       if let Err(e) = after.duration_since(before) {
                           error!("system time jumped back, {:?} -> {:?}, err {:?}",
                                  before,
                                  after,
                                  e);
                           on_jumped()
                       }
                   })
            .unwrap();

        TimeMonitor {
            tx: tx,
            handle: Some(h),
        }
    }
}

impl Default for TimeMonitor {
    fn default() -> TimeMonitor {
        TimeMonitor::new(|| {}, SystemTime::now)
    }
}

impl Drop for TimeMonitor {
    fn drop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }

        if let Err(e) = self.tx.send(true) {
            error!("send quit message for time monitor worker failed {:?}", e);
            return;
        }

        if let Err(e) = h.unwrap().join() {
            error!("join time monitor worker failed {:?}", e);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, Duration};
    use std::thread;
    use std::ops::Sub;
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_time_monitor() {
        let jumped = Arc::new(AtomicBool::new(false));
        let triggered = AtomicBool::new(false);
        let now = move || if !triggered.load(Ordering::SeqCst) {
            triggered.store(true, Ordering::SeqCst);
            SystemTime::now()
        } else {
            SystemTime::now().sub(Duration::from_secs(2))
        };

        let jumped2 = jumped.clone();
        let on_jumped = move || { jumped2.store(true, Ordering::SeqCst); };

        let _m = TimeMonitor::new(on_jumped, now);
        thread::sleep(Duration::from_secs(1));

        assert_eq!(jumped.load(Ordering::SeqCst), true);
    }
}
