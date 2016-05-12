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

use std::sync::{Arc, RwLock, Mutex};
use std::sync::mpsc::{self, Sender, Receiver};
use std::collections::HashMap;
use std::thread::{JoinHandle, Builder};
use std::collections::VecDeque;

use super::Result;
use util::{HandyRwLock, TryInsertWith};
use kvproto::msgpb::{Message, MessageType};
use super::http_client::{Client, Url};
use super::http_server::V1_MSG_PATH;
use super::http::OnResponseResult;
use super::transport::RaftStoreRouter;
use pd::PdClient;
use raftstore::store::Transport;
use raft::SnapshotStatus;
use kvproto::raftpb::MessageType as RaftMessageType;
use kvproto::raft_serverpb::RaftMessage;
use raftstore::Result as RaftStoreResult;

// TODO, after pd supports HTTP, this interface will be changed.
pub trait StoreAddrPeeker: Send {
    fn get_address(&mut self, store_id: u64) -> Result<&str>;
}

// TODO: use Http for pd later.
pub struct PdStoreAddrPeeker<T: PdClient> {
    cluster_id: u64,
    pd_client: Arc<RwLock<T>>,
    store_addrs: HashMap<u64, String>,
}

impl<T: PdClient> PdStoreAddrPeeker<T> {
    pub fn new(cluster_id: u64, pd_client: Arc<RwLock<T>>) -> PdStoreAddrPeeker<T> {
        PdStoreAddrPeeker {
            cluster_id: cluster_id,
            pd_client: pd_client,
            store_addrs: HashMap::new(),
        }
    }
}

impl<T: PdClient> StoreAddrPeeker for PdStoreAddrPeeker<T> {
    fn get_address(&mut self, store_id: u64) -> Result<&str> {
        // TODO: do we need re-update the cache sometimes?
        // Store address may be changed?
        let pd_client = self.pd_client.clone();
        let cluster_id = self.cluster_id;
        let s = try!(self.store_addrs.entry(store_id).or_try_insert_with(|| {
            pd_client.rl()
                     .get_store(cluster_id, store_id)
                     .and_then(|s| {
                         let addr = s.get_address().to_owned();
                         // In some tests, we use empty address for store first,
                         // so we should ignore here.
                         // TODO: we may remove this check after we refactor the test.
                         if addr.len() == 0 {
                             return Err(box_err!("invalid empty address for store {}", store_id));
                         }
                         Ok(addr)
                     })
        }));

        Ok(s)
    }
}

enum Task {
    Msg {
        msg: Message,
    },
    OnMsgResp {
        store_id: u64,
    },
    Snapshot {
        msg: Message,
    },

    Quit,
}

// Only for sending raft messages.
struct StorePending {
    pub url: Url,
    pub is_posting: bool,
    pub msgs: VecDeque<Message>,
}

impl StorePending {
    pub fn new(url: Url) -> StorePending {
        StorePending {
            url: url,
            is_posting: false,
            msgs: VecDeque::new(),
        }
    }
}

struct Worker<A: StoreAddrPeeker, R: RaftStoreRouter + 'static> {
    client: Client,

    raft_router: Arc<RwLock<R>>,
    store_peeker: A,

    pub tx: Sender<Task>,
    rx: Receiver<Task>,

    store_pendings: HashMap<u64, StorePending>,
}

impl<A, R> Worker<A, R>
    where A: StoreAddrPeeker,
          R: RaftStoreRouter
{
    pub fn new(client: Client, peeker: A, router: Arc<RwLock<R>>) -> Worker<A, R> {
        let (tx, rx) = mpsc::channel();
        Worker {
            client: client,
            store_peeker: peeker,
            tx: tx,
            rx: rx,

            store_pendings: HashMap::new(),
            raft_router: router,
        }
    }

    pub fn run(&mut self) {
        loop {
            let task = match self.rx.recv() {
                Err(e) => {
                    info!("receive transport task err {:?}", e);
                    return;
                }
                Ok(task) => task,
            };

            match task {
                Task::Msg { msg } => self.handle_msg(msg),
                Task::OnMsgResp { store_id } => self.handle_on_msg_resp(store_id),
                Task::Snapshot { msg } => self.handle_snapshot(msg),
                Task::Quit => return,
            }
        }
    }

    #[allow(map_entry)]
    fn handle_msg(&mut self, msg: Message) {
        let to_store_id = msg.get_raft().get_message().get_to();
        let region_id = msg.get_raft().get_region_id();

        if !self.store_pendings.contains_key(&to_store_id) {
            // TODO: Now we get store address from pd synchronously, later
            // we will refactor it with Http.
            let addr = match self.store_peeker.get_address(to_store_id) {
                Err(e) => {
                    error!("get store {} address failed {:?}", to_store_id, e);
                    self.raft_router.rl().report_unreachable(region_id, to_store_id);
                    return;
                }
                Ok(addr) => addr,
            };

            // Must be valid URL format, if not, the raft can't work, so panic directly.
            // Of course, we have already checked this before saving to pd.
            let url = format!("{}{}", addr, V1_MSG_PATH).parse().unwrap();

            self.store_pendings.insert(to_store_id, StorePending::new(url));
        }

        {
            let mut store_pending = self.store_pendings.get_mut(&to_store_id).unwrap();
            if !store_pending.msgs.is_empty() || store_pending.is_posting {
                store_pending.msgs.push_back(msg);
                return;
            }
        }

        // no pending messages and no posting message, we can send this message directly.
        self.post_msg(to_store_id, msg);
    }

    fn handle_snapshot(&mut self, msg: Message) {
        let to_store_id = msg.get_raft().get_message().get_to();
        let region_id = msg.get_raft().get_region_id();
        let router = self.raft_router.clone();

        // TODO: Now we get store address from pd synchronously, later
        // we will refactor it with Http.
        let addr = match self.store_peeker.get_address(to_store_id) {
            Err(e) => {
                error!("get store {} address failed {:?}", to_store_id, e);
                router.rl().report_unreachable(region_id, to_store_id);
                return;
            }
            Ok(addr) => addr,
        };

        // Now we use the same url for sending snapshot, we will change it later.
        let url = format!("{}{}", addr, V1_MSG_PATH).parse().unwrap();

        self.client
            .post_message(url,
                          msg,
                          box move |resp: OnResponseResult| {
                              let status = if resp.is_ok() {
                                  SnapshotStatus::Finish
                              } else {
                                  SnapshotStatus::Failure
                              };

                              router.rl().report_snapshot(region_id, to_store_id, status);
                          })
            .unwrap();
    }

    fn handle_on_msg_resp(&mut self, store_id: u64) {
        let pending = {
            let mut store_pending = self.store_pendings.get_mut(&store_id).unwrap();
            assert!(store_pending.is_posting);
            store_pending.is_posting = false;
            store_pending.msgs.pop_front()
        };

        if pending.is_none() {
            return;
        }

        // If we have other pending messages, send again.
        let msg = pending.unwrap();
        self.post_msg(store_id, msg);
    }


    fn post_msg(&mut self, store_id: u64, msg: Message) {
        // TODO: batch posting multi messages for optimization.
        let url = {
            let mut store_pending = self.store_pendings.get_mut(&store_id).unwrap();
            store_pending.is_posting = true;
            store_pending.url.clone()
        };

        let region_id = msg.get_raft().get_region_id();
        let tx = self.tx.clone();
        let router = self.raft_router.clone();
        // post_message can only fail with http event loop is closed, raft can't work, so panic.
        self.client
            .post_message(url,
                          msg,
                          box move |resp: OnResponseResult| {
                              if resp.is_err() {
                                  router.rl().report_unreachable(region_id, store_id);
                              }

                              if let Err(e) = tx.send(Task::OnMsgResp { store_id: store_id }) {
                                  error!("send msg response failed {:?}", e);
                              }
                          })
            .unwrap();
    }
}


pub struct HttpTransport {
    tx: Mutex<Sender<Task>>,

    handle: Option<JoinHandle<()>>,
}

impl HttpTransport {
    pub fn new<A, R>(client: Client, peeker: A, router: Arc<RwLock<R>>) -> Result<HttpTransport>
        where A: StoreAddrPeeker + 'static,
              R: RaftStoreRouter + 'static
    {
        let mut worker = Worker::new(client, peeker, router);

        let tx = worker.tx.clone();

        let builder = Builder::new().name("http transport worker".to_owned());
        let h = try!(builder.spawn(move || {
            worker.run();
            info!("quit http transport worker");
        }));

        Ok(HttpTransport {
            tx: Mutex::new(tx),
            handle: Some(h),
        })
    }
}

impl Transport for HttpTransport {
    fn send(&self, req: RaftMessage) -> RaftStoreResult<()> {
        let is_snapshot = req.get_message().get_msg_type() == RaftMessageType::MsgSnapshot;
        let mut msg = Message::new();
        msg.set_msg_type(MessageType::Raft);
        msg.set_raft(req);

        let tx = self.tx.lock().unwrap();
        if let Err(e) = {
            if is_snapshot {
                tx.send(Task::Snapshot { msg: msg })
            } else {
                tx.send(Task::Msg { msg: msg })
            }
        } {
            return Err(box_err!("send message error {:?}", e));
        }

        Ok(())
    }
}

impl Drop for HttpTransport {
    fn drop(&mut self) {
        if let Err(e) = self.tx.lock().unwrap().send(Task::Quit) {
            error!("send quit message error {:?}", e);
            return;
        }

        let h = self.handle.take().unwrap();
        if let Err(e) = h.join() {
            error!("join http transport thread failed {:?}", e);
            return;
        }
    }
}
