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

use std::thread::{self, JoinHandle};
use std::sync::mpsc;
use futures::sync::oneshot;
use tokio_core::reactor::{Core, Remote};

/// `CoreRunner` runs a Core on a standalone thread.
pub struct CoreRunner {
    thread_handle: JoinHandle<()>,
    remote: Remote,
    stop_sender: oneshot::Sender<()>,
}

impl CoreRunner {
    pub fn new(thread_name: String) -> CoreRunner {
        let (tx, rx) = mpsc::channel();
        let (sender, future) = oneshot::channel();
        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let mut core = Core::new().unwrap();
                tx.send(core.remote()).unwrap();
                core.run(future).unwrap();
            })
            .unwrap();
        let remote = rx.recv().unwrap();
        CoreRunner {
            thread_handle: handle,
            remote: remote,
            stop_sender: sender,
        }
    }

    pub fn remote(&self) -> Remote {
        self.remote.clone()
    }

    pub fn stop(self) {
        self.stop_sender.send(()).unwrap();
        self.thread_handle.join().unwrap();
    }
}
