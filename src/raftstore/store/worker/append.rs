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

use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::fmt::{self, Display, Formatter};
use time::Timespec;

use rocksdb::{DB, WriteBatch};
use util::worker::Runnable;
use util::clocktime;
use raft::{Ready, INVALID_INDEX};
use raftstore::store::peer_storage::InvokeContext;
use raftstore::Result;
use kvproto::{metapb, eraftpb};
use kvproto::eraftpb::{MessageType, Message};
use kvproto::raft_serverpb::RaftMessage;

use super::super::transport::Transport;

pub struct Task {
    pub wb: Option<WriteBatch>,
    pub ready: Option<Ready>,
    pub ctx: Option<InvokeContext>,

    pub region: metapb::Region,
    pub peer: metapb::Peer,
    pub tag: String,
    pub is_leader: bool,
    pub is_initialized: bool,
}

pub struct TaskRes {
    pub ready: Option<Ready>,
    pub ctx: Option<InvokeContext>,

    pub send_to_quorum_ts: Option<Timespec>,
    pub msg_unreachable_peers: Option<Vec<u64>>,
    pub snap_unreachable_peers: Option<Vec<u64>>,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "[region {}] async append",
               self.ctx.as_ref().unwrap().region_id)
    }
}

pub struct Runner<T: 'static> {
    tag: String,
    db: Arc<DB>,
    notifier: Sender<TaskRes>,
    trans: T,
}

impl<T: Transport> Runner<T> {
    pub fn new(tag: String, db: Arc<DB>, notifier: Sender<TaskRes>, trans: T) -> Runner<T> {
        Runner {
            tag: tag,
            db: db,
            notifier: notifier,
            trans: trans,
        }
    }

    fn handle_append(&mut self, mut task: Task) {
        let wb = task.wb.take().unwrap();
        if wb.is_empty() {
            panic!("{} write batch should not empty", task.tag);
        }

        self.db.write(wb).unwrap_or_else(|e| {
            panic!("{} failed to save append result: {:?}", self.tag, e);
        });

        let mut task_res = TaskRes {
            ready: task.ready.take(),
            ctx: task.ctx.take(),
            send_to_quorum_ts: None,
            msg_unreachable_peers: None,
            snap_unreachable_peers: None,
        };

        // send msg after flush raft log to disk if role is follower
        if !task.is_leader {
            self.send_msgs(&task, &mut task_res).unwrap_or_else(|e| {
                warn!("{} follower send messages err {:?}", task.tag, e);
            });
        }

        // send to raftstore
        self.notifier.send(task_res).unwrap();

        // send to apply worker

    }

    fn send_msgs(&mut self, task: &Task, task_res: &mut TaskRes) -> Result<()> {
        let msgs: Vec<Message> = task_res.ready.as_mut().unwrap().messages.drain(..).collect();
        for msg in msgs {
            let msg_type = msg.get_msg_type();
            try!(self.send_raft_message(msg, task, task_res));
            if let MessageType::MsgTimeoutNow = msg_type {
                // After a leader transfer procedure is triggered, the lease for
                // the old leader may be expired earlier than usual, since a new leader
                // may be elected and the old leader doesn't step down due to
                // network partition from the new leader.
                // For lease safty during leader transfer, mark `leader_lease_expired_time`
                // to be unsafe until next_lease_expired_time from now
                task_res.send_to_quorum_ts = Some(clocktime::raw_now());
            }
        }
        Ok(())
    }

    fn send_raft_message(&mut self,
                         msg: eraftpb::Message,
                         task: &Task,
                         task_res: &mut TaskRes)
                         -> Result<()> {
        let mut send_msg = RaftMessage::new();
        send_msg.set_region_id(task.region.get_id());
        send_msg.set_region_epoch(task.region.get_region_epoch().clone());

        let from_peer = task.peer.clone();
        let to_peer = match get_peer_by_id(task.region.get_peers(), msg.get_to()) {
            Some(p) => p,
            None => {
                return Err(box_err!("failed to look up recipient peer {} in region {}",
                                    msg.get_to(),
                                    task.region.get_id()))
            }
        };

        let to_peer_id = to_peer.get_id();
        let to_store_id = to_peer.get_store_id();
        let msg_type = msg.get_msg_type();
        debug!("{} send raft msg {:?} from {} to {}",
               self.tag,
               msg_type,
               from_peer.get_id(),
               to_peer_id);

        send_msg.set_from_peer(from_peer);
        send_msg.set_to_peer(to_peer);

        // There could be two cases:
        // 1. Target peer already exists but has not established communication with leader yet
        // 2. Target peer is added newly due to member change or region split, but it's not
        //    created yet
        // For both cases the region start key and end key are attached in RequestVote and
        // Heartbeat message for the store of that peer to check whether to create a new peer
        // when receiving these messages, or just to wait for a pending region split to perform
        // later.
        if task.is_initialized &&
           (msg_type == MessageType::MsgRequestVote ||
        // the peer has not been known to this leader, it may exist or not.
        (msg_type == MessageType::MsgHeartbeat && msg.get_commit() == INVALID_INDEX)) {
            send_msg.set_start_key(task.region.get_start_key().to_vec());
            send_msg.set_end_key(task.region.get_end_key().to_vec());
        }

        send_msg.set_message(msg);

        if let Err(e) = self.trans.send(send_msg) {
            warn!("{} failed to send msg to {} in store {}, err: {:?}",
                  task.tag,
                  to_peer_id,
                  to_store_id,
                  e);

            // unreachable store
            if task_res.msg_unreachable_peers.is_none() {
                task_res.msg_unreachable_peers = Some(vec![]);
            }
            task_res.msg_unreachable_peers.as_mut().unwrap().push(to_peer_id);

            if msg_type == eraftpb::MessageType::MsgSnapshot {
                if task_res.snap_unreachable_peers.is_none() {
                    task_res.snap_unreachable_peers = Some(vec![]);
                }
                task_res.snap_unreachable_peers.as_mut().unwrap().push(to_peer_id);
            }
        }

        Ok(())
    }
}

fn get_peer_by_id(peers: &[metapb::Peer], peer_id: u64) -> Option<metapb::Peer> {
    for peer in peers {
        if peer.get_id() == peer_id {
            return Some(peer.clone());
        }
    }
    None
}

impl<T: Transport> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        self.handle_append(task);
    }
}
