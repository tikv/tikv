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
use std::collections::{HashMap, VecDeque};
use std::time::Duration as StdDuration;

use time::{Timespec, Duration, now, empty_tm};

use kvproto::msgpb::{MessageType, Message};
use kvproto::statpb::{MessageType as StatMessageType, Timespec as StatTimespec,
                      RequestHeader as StatRequestHeader, ResponseHeader as StatResponseHeader,
                      Request as StatRequest, Response as StatResponse, PingReq, PingResp};

use pd::INVALID_ID;
use util::worker::{Runnable, Worker, Scheduler};
use util::transport::SendCh;
use super::{Result, Msg, ConnData};

const PING_TIMEOUT_MAX_RETRY_NUMBER: usize = 200;

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
            Event::RenewNetworkStat { store_id, .. } => {
                write!(f, "renew network stat for current store id {}", store_id)
            }
            Event::Request { ref req } => write!(f, "request {:?}", req),
            Event::Response { ref resp } => write!(f, "response {:?}", resp),
        }
    }
}

#[derive(Debug)]
struct NetStat {
    seq_id: u64,
    last_active_ts: Option<Timespec>,
    last_ping_ts: Timespec,
}

impl NetStat {
    pub fn new() -> NetStat {
        NetStat {
            seq_id: 0,
            last_active_ts: None,
            last_ping_ts: empty_tm().to_timespec(),
        }
    }
}

struct Ping {
    pub ts: Timespec,
    pub store_id: u64,
}

fn timespec_elapsed(ts: Timespec) -> Duration {
    let now_ts = now().to_timespec();
    now_ts - ts
}

fn conv_timespec(ts: &StatTimespec) -> Timespec {
    Timespec::new(ts.get_sec(), ts.get_nsec())
}

fn conv_stat_timespec(ts: Timespec) -> StatTimespec {
    let mut sts = StatTimespec::new();
    sts.set_sec(ts.sec);
    sts.set_nsec(ts.nsec);
    sts
}

fn calc_duration(timeout: u64) -> Duration {
    Duration::from_std(StdDuration::from_secs(timeout)).unwrap()
}

pub struct Runner {
    store_id: u64,
    max_store_number: usize,
    keepalive_timeout: Duration,
    connection_timeout: Duration,
    ch: SendCh<Msg>,
    stats: HashMap<u64, NetStat>,
    ping_list: VecDeque<Ping>,
    msg_id: u64,
}

impl Runner {
    pub fn new(max_store_number: usize,
               keepalive_timeout: u64,
               connection_timeout: u64,
               ch: SendCh<Msg>)
               -> Runner {
        Runner {
            store_id: INVALID_ID,
            max_store_number: max_store_number,
            keepalive_timeout: calc_duration(keepalive_timeout),
            connection_timeout: calc_duration(connection_timeout),
            ch: ch,
            stats: HashMap::new(),
            ping_list: VecDeque::new(),
            msg_id: 0,
        }
    }

    fn send_ping_store(&mut self, store_id: u64, seq_id: u64) -> Timespec {
        let ts = now().to_timespec();

        let mut ping_req = PingReq::new();
        ping_req.set_ping_ts(conv_stat_timespec(ts));

        let mut req_header = StatRequestHeader::new();
        req_header.set_id(seq_id);
        req_header.set_from_store_id(self.store_id);
        req_header.set_to_store_id(store_id);

        let mut req = StatRequest::new();
        req.set_header(req_header);
        req.set_msg_type(StatMessageType::Ping);
        req.set_ping_req(ping_req);

        let mut message = Message::new();
        message.set_msg_type(MessageType::StatReq);
        message.set_stat_req(req);

        self.send_message(store_id, message);
        ts
    }

    fn ping_store(&mut self, stat: &mut NetStat, store_id: u64) -> Timespec {
        stat.seq_id += 1;
        let ts = self.send_ping_store(store_id, stat.seq_id);
        stat.last_ping_ts = ts;
        ts
    }

    fn handle_renew_network_stat(&mut self, store_id: u64, remote_store_ids: Vec<u64>) {
        if self.store_id == INVALID_ID {
            self.store_id = store_id;
        }

        for remote_store_id in remote_store_ids {
            // Ignore the store id that as already tracked.
            if self.stats.contains_key(&remote_store_id) {
                continue;
            }

            // Track new remote store.

            if self.stats.len() >= self.max_store_number {
                // Free one slot for the the new remote store id.
                let op = self.ping_list.pop_front().unwrap();
                let _ = self.stats.remove(&op.store_id);
            }
            // Ping the remote store and add its stat for tracking.
            let mut stat = NetStat::new();
            let ts = self.ping_store(&mut stat, remote_store_id);
            self.stats.insert(remote_store_id, stat);
            // Add new Ping for this remote store.
            let op = Ping {
                ts: ts,
                store_id: remote_store_id,
            };
            self.ping_list.push_back(op);
        }

        // Retry the timeout Pings.
        for _ in 0..PING_TIMEOUT_MAX_RETRY_NUMBER {
            if self.ping_list.front().is_none() {
                break;
            }
            {
                let front = self.ping_list.front().unwrap();
                if timespec_elapsed(front.ts) < self.keepalive_timeout {
                    break;
                }
            }
            // Get the first timeout Ping.
            let op = self.ping_list.pop_front().unwrap();
            let mut stat = self.stats.remove(&op.store_id).unwrap();
            // Log a message if the connection is considered lost.
            if (stat.last_active_ts.is_none() &&
                timespec_elapsed(stat.last_ping_ts) >= self.connection_timeout) ||
               (stat.last_active_ts.is_some() &&
                timespec_elapsed(stat.last_active_ts.unwrap()) >= self.connection_timeout) {
                info!("[store {}] lost connection with store {}",
                      self.store_id,
                      op.store_id);
                // Stop tracking the store whose connection is considered lost.
                continue;
            }
            // Retry to ping the store.
            let ts = self.ping_store(&mut stat, op.store_id);
            // Add the store stat for tracking.
            self.stats.insert(op.store_id, stat);
            // Add new Ping for this store.
            let op = Ping {
                ts: ts,
                store_id: op.store_id,
            };
            self.ping_list.push_back(op);
        }
    }

    fn alloc_msg_id(&mut self) -> u64 {
        self.msg_id += 1;
        self.msg_id
    }

    fn send_message(&mut self, to_store_id: u64, msg: Message) {
        let msg_id = self.alloc_msg_id();
        if let Err(e) = self.ch.try_send(Msg::SendStore {
            store_id: to_store_id,
            data: ConnData::new(msg_id, msg),
        }) {
            error!("[store {}] network monitor failed to send message {:?}",
                   self.store_id,
                   e);
        }
    }

    fn handle_request(&mut self, mut req: StatRequest) {
        let to_store_id = req.get_header().get_to_store_id();
        if self.store_id != INVALID_ID && to_store_id != self.store_id {
            error!("[store {}] receive invalid stat request with to_store_id {}",
                   self.store_id,
                   to_store_id);
            return;
        }

        match req.get_msg_type() {
            StatMessageType::Ping => {
                let mut ping_resp = PingResp::new();
                ping_resp.set_ping_ts(req.mut_ping_req().take_ping_ts());

                let mut resp_header = StatResponseHeader::new();
                resp_header.set_id(req.get_header().get_id());
                resp_header.set_from_store_id(to_store_id);
                resp_header.set_to_store_id(req.get_header().get_from_store_id());

                let mut resp = StatResponse::new();
                resp.set_header(resp_header);
                resp.set_msg_type(StatMessageType::Ping);
                resp.set_ping_resp(ping_resp);

                let mut message = Message::new();
                message.set_msg_type(MessageType::StatResp);
                message.set_stat_resp(resp);

                self.send_message(to_store_id, message)
            }
        }
    }

    fn handle_response(&mut self, resp: StatResponse) {
        let to_store_id = resp.get_header().get_to_store_id();
        if self.store_id != INVALID_ID && to_store_id != self.store_id {
            error!("[store {}] receive invalid stat response with to_store_id {}",
                   self.store_id,
                   to_store_id);
            return;
        }
        match resp.get_msg_type() {
            StatMessageType::Ping => {
                // Ensure that from_store_id is still tracked.
                let from_store_id = resp.get_header().get_from_store_id();
                if !self.stats.contains_key(&from_store_id) {
                    debug!("[store {}] receive obsolete ping response from store id {}",
                           self.store_id,
                           from_store_id);
                    return;
                }
                // Ensure the message is not stale.
                let stat = self.stats.get_mut(&from_store_id).unwrap();
                let seq_id = resp.get_header().get_id();
                if seq_id < stat.seq_id {
                    debug!("[store {}] receive stale ping response from store id {}",
                           self.store_id,
                           from_store_id);
                    return;
                }
                // Update the last active timespec.
                let ping_ts = conv_timespec(resp.get_ping_resp().get_ping_ts());
                stat.last_active_ts = Some(ping_ts);
            }
        }
    }
}

impl Runnable<Event> for Runner {
    fn run(&mut self, event: Event) {
        match event {
            Event::RenewNetworkStat { store_id, remote_store_ids } => {
                self.handle_renew_network_stat(store_id, remote_store_ids)
            }
            Event::Request { req } => self.handle_request(req),
            Event::Response { resp } => self.handle_response(resp),
        }
    }
}

pub struct SimpleNetworkMonitor {
    worker: Worker<Event>,
}

impl SimpleNetworkMonitor {
    pub fn new(max_store_number: usize,
               keepalive_timeout: u64,
               connection_timeout: u64,
               ch: SendCh<Msg>)
               -> Result<SimpleNetworkMonitor> {
        let mut m = SimpleNetworkMonitor { worker: Worker::new("network-monitor") };
        let runner = Runner::new(max_store_number, keepalive_timeout, connection_timeout, ch);
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
