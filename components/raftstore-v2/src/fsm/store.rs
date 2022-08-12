// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use batch_system::Fsm;
use collections::HashMap;
use crossbeam::channel::TryRecvError;
use engine_traits::KvEngine;
use kvproto::metapb::Store;
use raftstore::store::{Config, ReadDelegate};
use tikv_util::mpsc::{self, LooseBoundedSender, Receiver};

use crate::{batch::StoreContext, tablet::CachedTablet, StoreMsg};

pub struct StoreMeta<E>
where
    E: KvEngine,
{
    pub store_id: Option<u64>,
    /// region_id -> reader
    pub readers: HashMap<u64, ReadDelegate>,
    /// region_id -> tablet cache
    pub tablet_caches: HashMap<u64, CachedTablet<E>>,
}

impl<E> StoreMeta<E>
where
    E: KvEngine,
{
    pub fn new() -> StoreMeta<E> {
        StoreMeta {
            store_id: None,
            readers: HashMap::default(),
            tablet_caches: HashMap::default(),
        }
    }
}

pub struct StoreFsm {
    store: Store,
    receiver: Receiver<StoreMsg>,
}

impl StoreFsm {
    pub fn new(cfg: &Config, store: Store) -> (LooseBoundedSender<StoreMsg>, Box<Self>) {
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        let fsm = Box::new(StoreFsm {
            store,
            receiver: rx,
        });
        (tx, fsm)
    }

    /// Fetches messages to `store_msg_buf`. It will stop when the buffer
    /// capacity is reached or there is no more pending messages.
    ///
    /// Returns how many messages are fetched.
    pub fn recv(&self, store_msg_buf: &mut Vec<StoreMsg>) -> usize {
        let l = store_msg_buf.len();
        for i in l..store_msg_buf.capacity() {
            match self.receiver.try_recv() {
                Ok(msg) => store_msg_buf.push(msg),
                Err(_) => return i - l,
            }
        }
        store_msg_buf.capacity() - l
    }
}

impl Fsm for StoreFsm {
    type Message = StoreMsg;

    #[inline]
    fn is_stopped(&self) -> bool {
        false
    }
}

pub struct StoreFsmDelegate<'a, T> {
    fsm: &'a mut StoreFsm,
    store_ctx: &'a mut StoreContext<T>,
}

impl<'a, T> StoreFsmDelegate<'a, T> {
    pub fn new(fsm: &'a mut StoreFsm, store_ctx: &'a mut StoreContext<T>) -> Self {
        Self { fsm, store_ctx }
    }

    pub fn handle_msgs(&self, store_msg_buf: &mut Vec<StoreMsg>) {
        for msg in store_msg_buf.drain(..) {
            // TODO: handle the messages.
        }
    }
}
