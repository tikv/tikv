// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::BTreeMap,
    ops::Bound::{Excluded, Unbounded},
    time::{Duration, SystemTime},
};

use batch_system::Fsm;
use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine};
use futures::{compat::Future01CompatExt, FutureExt};
use keys::{data_end_key, data_key};
use kvproto::metapb::Region;
use raftstore::store::{
    fsm::store::StoreRegionMeta, Config, ReadDelegate, RegionReadProgressRegistry, Transport,
};
use slog::{info, o, Logger};
use tikv_util::{
    future::poll_future_notify,
    is_zero_duration,
    log::SlogFormat,
    mpsc::{self, LooseBoundedSender, Receiver},
    slog_panic,
};

use crate::{
    batch::StoreContext,
    operation::ReadDelegatePair,
    router::{StoreMsg, StoreTick},
};

pub struct StoreMeta<EK> {
    pub store_id: u64,
    /// region_id -> reader
    pub readers: HashMap<u64, ReadDelegatePair<EK>>,
    /// region_id -> `RegionReadProgress`
    pub region_read_progress: RegionReadProgressRegistry,
    /// (region_end_key, epoch.version) -> region_id
    ///
    /// Unlinke v1, ranges in v2 may be overlapped. So we use version
    /// to avoid end key conflict.
    pub(crate) region_ranges: BTreeMap<(Vec<u8>, u64), u64>,
    /// region_id -> (region, initialized)
    pub(crate) regions: HashMap<u64, (Region, bool)>,
}

impl<EK> StoreMeta<EK> {
    pub fn new(store_id: u64) -> Self {
        Self {
            store_id,
            readers: HashMap::default(),
            region_read_progress: RegionReadProgressRegistry::default(),
            region_ranges: BTreeMap::default(),
            regions: HashMap::default(),
        }
    }

    pub fn set_region(&mut self, region: &Region, initialized: bool, logger: &Logger) {
        let region_id = region.get_id();
        let version = region.get_region_epoch().get_version();
        let prev = self
            .regions
            .insert(region_id, (region.clone(), initialized));
        // `prev` only makes sense when it's initialized.
        if let Some((prev, prev_init)) = prev && prev_init {
            assert!(initialized, "{} region corrupted", SlogFormat(logger));
            if prev.get_region_epoch().get_version() != version {
                let prev_id = self.region_ranges.remove(&(data_end_key(prev.get_end_key()), prev.get_region_epoch().get_version()));
                assert_eq!(prev_id, Some(region_id), "{} region corrupted", SlogFormat(logger));
            } else {
                assert!(self.region_ranges.get(&(data_end_key(prev.get_end_key()), version)).is_some(), "{} region corrupted", SlogFormat(logger));
                return;
            }
        }
        if initialized {
            assert!(
                self.region_ranges
                    .insert((data_end_key(region.get_end_key()), version), region_id)
                    .is_none(),
                "{} region corrupted",
                SlogFormat(logger)
            );
        }
    }

    pub fn remove_region(&mut self, region_id: u64) {
        let prev = self.regions.remove(&region_id);
        if let Some((prev, initialized)) = prev {
            if initialized {
                let key = (
                    data_end_key(prev.get_end_key()),
                    prev.get_region_epoch().get_version(),
                );
                let prev_id = self.region_ranges.remove(&key);
                assert_eq!(prev_id, Some(prev.get_id()));
            }
        }
    }
}

impl<EK: Send> StoreRegionMeta for StoreMeta<EK> {
    #[inline]
    fn store_id(&self) -> u64 {
        self.store_id
    }

    #[inline]
    fn region_read_progress(&self) -> &RegionReadProgressRegistry {
        &self.region_read_progress
    }

    #[inline]
    fn search_region(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        mut visitor: impl FnMut(&kvproto::metapb::Region),
    ) {
        let start_key = data_key(start_key);
        for (_, id) in self
            .region_ranges
            .range((Excluded((start_key, 0)), Unbounded::<(Vec<u8>, u64)>))
        {
            let (region, initialized) = &self.regions[id];
            if !initialized {
                continue;
            }
            if end_key.is_empty() || end_key > region.get_start_key() {
                visitor(region);
            } else {
                break;
            }
        }
    }

    #[inline]
    fn reader(&self, region_id: u64) -> Option<&ReadDelegate> {
        self.readers.get(&region_id).map(|e| &e.0)
    }
}

pub struct Store {
    id: u64,
    last_compact_checked_key: Vec<u8>,
    // Unix time when it's started.
    start_time: Option<u64>,
    logger: Logger,
}

impl Store {
    pub fn new(id: u64, logger: Logger) -> Store {
        Store {
            id,
            last_compact_checked_key: keys::DATA_MIN_KEY.to_vec(),
            start_time: None,
            logger: logger.new(o!("store_id" => id)),
        }
    }

    pub fn store_id(&self) -> u64 {
        self.id
    }

    pub fn last_compact_checked_key(&self) -> &Vec<u8> {
        &self.last_compact_checked_key
    }

    pub fn set_last_compact_checked_key(&mut self, key: Vec<u8>) {
        self.last_compact_checked_key = key;
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
            slog_panic!(self.fsm.store.logger, "store is already started");
        }

        self.fsm.store.start_time = Some(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        );

        self.on_pd_store_heartbeat();
        self.schedule_tick(
            StoreTick::CleanupImportSst,
            self.store_ctx.cfg.cleanup_import_sst_interval.0,
        );
        self.register_compact_check_tick();
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
            StoreTick::CleanupImportSst => self.on_cleanup_import_sst(),
            StoreTick::CompactCheck => self.on_compact_check_tick(),
            _ => unimplemented!(),
        }
    }

    pub fn handle_msgs(&mut self, store_msg_buf: &mut Vec<StoreMsg>)
    where
        T: Transport,
    {
        for msg in store_msg_buf.drain(..) {
            match msg {
                StoreMsg::Start => self.on_start(),
                StoreMsg::Tick(tick) => self.on_tick(tick),
                StoreMsg::RaftMessage(msg) => self.fsm.store.on_raft_message(self.store_ctx, msg),
                StoreMsg::SplitInit(msg) => self.fsm.store.on_split_init(self.store_ctx, msg),
                StoreMsg::StoreUnreachable { to_store_id } => self
                    .fsm
                    .store
                    .on_store_unreachable(self.store_ctx, to_store_id),
                StoreMsg::AskCommitMerge(req) => {
                    self.fsm.store.on_ask_commit_merge(self.store_ctx, req)
                }
                #[cfg(feature = "testexport")]
                StoreMsg::WaitFlush { region_id, ch } => {
                    self.fsm.store.on_wait_flush(self.store_ctx, region_id, ch)
                }
            }
        }
    }
}
