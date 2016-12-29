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

use std::fmt::{self, Formatter, Display};

use kvproto::statpb::{Request as StatRequest, Response as StatResponse};

use super::{Result, Msg};
use util::worker::{Runnable, Worker, Scheduler};
use util::transport::SendCh;

// NetworkMonitor keep watch on the network status between servers.
pub trait NetworkMonitor {
    fn renew_network_stat(&self, store_id: u64, remote_store_ids: Vec<u64>) -> Result<()>;

    fn send_request(&self, req: StatRequest) -> Result<()>;

    fn send_response(&self, resp: StatResponse) -> Result<()>;
}

pub enum Event {
    RenewNetworkStat {
        store_id: u64,
        remote_store_ids: Vec<u64>,
    },
    Request { req: StatRequest },
    Response { resp: StatResponse },
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        // TODO add messages
        match *self {
            Event::RenewNetworkStat { .. } => write!(f, ""),
            Event::Request { .. } => write!(f, ""),
            Event::Response { .. } => write!(f, ""),
        }
    }
}

pub struct Runner {
    ch: SendCh<Msg>,
}

impl Runner {
    pub fn new(ch: SendCh<Msg>) -> Runner {
        Runner { ch: ch }
    }
}

impl Runnable<Event> for Runner {
    fn run(&mut self, event: Event) {
        match event {
            Event::RenewNetworkStat { .. } => {
                // TODO add impl
            }
            Event::Request { .. } => {
                // TODO add impl
            }
            Event::Response { .. } => {
                // TODO add impl
            }
        }
    }
}

pub struct SimpleNetworkMonitor {
    worker: Worker<Event>,
}

impl SimpleNetworkMonitor {
    pub fn new(ch: SendCh<Msg>) -> Result<SimpleNetworkMonitor> {
        let mut m = SimpleNetworkMonitor { worker: Worker::new("network-monitor") };
        let runner = Runner::new(ch);
        box_try!(m.worker.start(runner));
        Ok(m)
    }

    pub fn scheduler(&self) -> Scheduler<Event> {
        self.worker.scheduler()
    }
}

impl NetworkMonitor for SimpleNetworkMonitor {
    fn renew_network_stat(&self, store_id: u64, remote_store_ids: Vec<u64>) -> Result<()> {
        let event = Event::RenewNetworkStat {
            store_id: store_id,
            remote_store_ids: remote_store_ids,
        };
        box_try!(self.worker.schedule(event));
        Ok(())
    }

    fn send_request(&self, req: StatRequest) -> Result<()> {
        let event = Event::Request { req: req };
        box_try!(self.worker.schedule(event));
        Ok(())
    }

    fn send_response(&self, resp: StatResponse) -> Result<()> {
        let event = Event::Response { resp: resp };
        box_try!(self.worker.schedule(event));
        Ok(())
    }
}

impl Drop for SimpleNetworkMonitor {
    fn drop(&mut self) {
        if let Some(Err(e)) = self.worker.stop().map(|h| h.join()) {
            error!("failed to stop simple network monitor theard: {:?}!!!", e);
        }
    }
}
