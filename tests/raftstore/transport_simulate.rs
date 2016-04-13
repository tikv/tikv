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

use kvproto::raft_serverpb::RaftMessage;
use tikv::raftstore::store::Transport;
use tikv::raftstore::Result as RaftStoreResult;
// use tikv::raftstore::Error::Timeout;
use rand;
use std::sync::{Arc, RwLock};
use super::util::*;
use super::node::ChannelTransport;
use super::pd::TestPdClient;
use tikv::server::ServerTransport;

// SimulateTransport is the same as Transport, just to avoid name conflict
pub trait SimulateTransport: Send + Sync {
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()>;
}

// TransportHijack hijack the underlying Transport
pub trait TransportHijack {
    fn wrapper(&self, Box<SimulateTransport>) -> Box<SimulateTransport>;
}

impl SimulateTransport for Arc<RwLock<ChannelTransport>> {
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        self.read().unwrap().send(msg)
    }
}

impl SimulateTransport for Arc<RwLock<ServerTransport<TestPdClient>>> {
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        self.read().unwrap().send(msg)
    }
}

impl Transport for Box<SimulateTransport> {
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        self.as_ref().send(msg)
    }
}

pub struct IdentityTransportHijack;

impl TransportHijack for IdentityTransportHijack {
    fn wrapper(&self, input: Box<SimulateTransport>) -> Box<SimulateTransport> {
        input
    }
}

pub struct LossPacketHijack(pub u32);

impl TransportHijack for LossPacketHijack {
    fn wrapper(&self, input: Box<SimulateTransport>) -> Box<SimulateTransport> {
        Box::new(LossPacket {
            rate: self.0,
            transport: input,
        })
    }
}

struct LossPacket {
    // loss packet rate 100% means network unreachable
    rate: u32,
    transport: Box<SimulateTransport>,
}

impl SimulateTransport for LossPacket {
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        if rand::random::<u32>() % 100u32 < self.rate {
            // discard msg and return
            return Ok(());
        }
        self.transport.send(msg)
    }
}

struct SlowNetworkHijack(u32);

impl TransportHijack for SlowNetworkHijack {
    fn wrapper(&self, input: Box<SimulateTransport>) -> Box<SimulateTransport> {
        Box::new(SlowNetwork {
            latency: self.0,
            transport: input,
        })
    }
}

struct SlowNetwork {
    latency: u32,
    transport: Box<SimulateTransport>,
}

impl SimulateTransport for SlowNetwork {
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        sleep_ms(self.latency as u64);
        self.transport.send(msg)
    }
}

// struct OutOrder {
//     transport: Box<SimulateTransport>,
//     save_for_later: RaftMessage,
//     future: u32,
//     current: u32,
// }
//
// impl OutOrder {
//     fn new(latency: u32, transport: Box<SimulateTransport>) -> OutOrder {
//         OutOrder {
//             transport: transport,
//             save_for_later: RaftMessage::new(),
//             future: 0,
//             current: 0,
//         }
//     }
// }
//
// impl Transport for OutOrder<T> {
//     fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
//         if self.future == 0 {
//             self.future = 1 + rand::random::<u32>() % 10;
//             self.save_for_later = msg;
//             return Err(Timeout("transport simulator loss packet".to_string()));
//         }
//
//         let result;
//         if self.current == self.future {
//             self.future = 1 + rand::random::<u32>() % 10;
//             self.current = 0;
//             result = self.transport.send(self.save_for_later);
//             self.save_for_later = msg;
//         }
//         result
//     }
// }

// // use composited transports, which are get by a function of time
// struct TransportComposite<T: TimingTransportFunc> {
//     time: u32,
//     get_transport: T,
// }
//
// impl TransportComposite {
//     fn new<T: TimingTransportFunc, R: Transport>(func: T) -> TransportComposite<T> {
//         TransportComposite {
//             time: 0,
//             get_transport: func,
//         }
//     }
// }
//
// impl Transport for TransportComposite {
//     fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
//         self.time += 1;
//         self.get_transport(time).send(msg)
//     }
// }
//
// trait TransportTimingFunc {
//     fn get_transport_at_time(time: u32) -> Transport;
// }
