// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::{Duration, SystemTime};

use batch_system::Fsm;
use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine};
use futures::{compat::Future01CompatExt, FutureExt};
use kvproto::{metapb::Region, raft_serverpb::RaftMessage};
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{Config, ReadDelegate, RegionReadProgressRegistry},
};
use slog::{info, o, Logger};
use tikv_util::{
    future::poll_future_notify,
    is_zero_duration,
    mpsc::{self, LooseBoundedSender, Receiver},
};

use crate::{
    batch::StoreContext,
    raft::Peer,
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
    /// region_id -> `RegionReadProgress`
    pub region_read_progress: RegionReadProgressRegistry,
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
            region_read_progress: RegionReadProgressRegistry::new(),
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
    pub store: Store,
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
    pub fsm: &'a mut StoreFsm,
    pub store_ctx: &'a mut StoreContext<EK, ER, T>,
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

        self.on_pd_store_heartbeat();
    }

    pub fn schedule_tick(&mut self, tick: StoreTick, timeout: Duration) {
        if !is_zero_duration(&timeout) {
            let mb = self.store_ctx.router.control_mailbox();
            let logger = self.fsm.store.logger().clone();
            let delay = self.store_ctx.timer.delay(timeout).compat().map(move |_| {
                if let Err(e) = mb.force_send(StoreMsg::Tick(tick)) {
                    info!(
                        logger,
                        "failed to schedule store tick, are we shutting down?";
                        "tick" => ?tick,
                        "err" => ?e
                    );
                }
            });
            poll_future_notify(delay);
        }
    }

    fn on_tick(&mut self, tick: StoreTick) {
        match tick {
            StoreTick::PdStoreHeartbeat => self.on_pd_store_heartbeat(),
            _ => unimplemented!(),
        }
    }

    pub fn handle_msgs(&mut self, store_msg_buf: &mut Vec<StoreMsg>) {
        for msg in store_msg_buf.drain(..) {
            match msg {
                StoreMsg::Start => self.on_start(),
                StoreMsg::Tick(tick) => self.on_tick(tick),
                StoreMsg::RaftMessage(msg) => self.fsm.store.on_raft_message(self.store_ctx, msg),
                StoreMsg::SplitInit(msg) => self.fsm.store.on_split_init(self.store_ctx, msg),
            }
        }
    }
}
