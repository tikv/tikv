// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::SystemTime;

use batch_system::Fsm;
use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine};
use raftstore::store::{Config, ReadDelegate};
use slog::{o, Logger};
use tikv_util::mpsc::{self, LooseBoundedSender, Receiver};

use crate::{
    batch::StoreContext,
    router::{StoreMsg, StoreTick},
    tablet::CachedTablet,
};

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

impl<E: KvEngine> Default for StoreMeta<E> {
    fn default() -> Self {
        Self::new()
    }
}
pub struct Store {
    id: u64,
    // Unix time when it's started.
    start_time: Option<u64>,
    logger: Logger,
}

impl Store {
    pub fn new(id: u64, logger: Logger) -> Store {
        Store {
            id,
            start_time: None,
            logger: logger.new(o!("store_id" => id)),
        }
    }

    pub fn store_id(&self) -> u64 {
        self.id
    }

    pub fn start_time(&self) -> Option<u64> {
        self.start_time
    }

    pub fn logger(&self) -> &Logger {
        &self.logger
    }
}

pub struct StoreFsm {
    store: Store,
    receiver: Receiver<StoreMsg>,
}

impl StoreFsm {
    pub fn new(
        cfg: &Config,
        store_id: u64,
        logger: Logger,
    ) -> (LooseBoundedSender<StoreMsg>, Box<Self>) {
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        let fsm = Box::new(StoreFsm {
            store: Store::new(store_id, logger),
            receiver: rx,
        });
        (tx, fsm)
    }

    /// Fetches messages to `store_msg_buf`. It will stop when the buffer
    /// capacity is reached or there is no more pending messages.
    ///
    /// Returns how many messages are fetched.
    pub fn recv(&self, store_msg_buf: &mut Vec<StoreMsg>, batch_size: usize) -> usize {
        let l = store_msg_buf.len();
        for i in l..batch_size {
            match self.receiver.try_recv() {
                Ok(msg) => store_msg_buf.push(msg),
                Err(_) => return i - l,
            }
        }
        batch_size - l
    }
}

impl Fsm for StoreFsm {
    type Message = StoreMsg;

    #[inline]
    fn is_stopped(&self) -> bool {
        false
    }
}

pub struct StoreFsmDelegate<'a, EK: KvEngine, ER: RaftEngine, T> {
    fsm: &'a mut StoreFsm,
    store_ctx: &'a mut StoreContext<EK, ER, T>,
}

impl<'a, EK: KvEngine, ER: RaftEngine, T> StoreFsmDelegate<'a, EK, ER, T> {
    pub fn new(fsm: &'a mut StoreFsm, store_ctx: &'a mut StoreContext<EK, ER, T>) -> Self {
        Self { fsm, store_ctx }
    }

    fn on_start(&mut self) {
        if self.fsm.store.start_time.is_some() {
            panic!("{:?} unable to start again", self.fsm.store.logger.list(),);
        }

        self.fsm.store.start_time = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        );
    }

    fn on_tick(&mut self, tick: StoreTick) {
        unimplemented!()
    }

    pub fn handle_msgs(&mut self, store_msg_buf: &mut Vec<StoreMsg>) {
        for msg in store_msg_buf.drain(..) {
            match msg {
                StoreMsg::Start => self.on_start(),
                StoreMsg::Tick(tick) => self.on_tick(tick),
                StoreMsg::RaftMessage(msg) => self.fsm.store.on_raft_message(self.store_ctx, msg),
            }
        }
    }
}
