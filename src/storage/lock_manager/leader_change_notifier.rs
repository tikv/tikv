// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use super::metrics::*;
use super::{Error, Result};
use crate::pd::{RpcClient, INVALID_ID};
use crate::raftstore::coprocessor::{
    Coprocessor, CoprocessorHost, ObserverContext, RegionChangeEvent, RegionChangeObserver,
    RoleObserver,
};
use crate::server::resolve::StoreAddrResolver;
use crate::tikv_util::worker::{FutureRunnable, FutureScheduler, FutureWorker};
use futures::{Future, Stream};
use kvproto::metapb::Region;
use raft::StateRole;
use std::cell::RefCell;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio_core::reactor::Handle;
use tokio_timer::Interval;

/// The leader of the region containing the LEADER_KEY is the leader of deadlock detector.
const LEADER_KEY: &[u8] = b"";

/// Returns true if the region containing the LEADER_KEY.
fn is_leader_region(region: &'_ Region) -> bool {
    region.get_start_key() <= LEADER_KEY && LEADER_KEY < region.get_end_key()
}

/// Get store id from region by peer id.
fn get_store_id(region: &Region, peer_id: u64) -> Option<u64> {
    // `peers` won't be very big, so traverse it to find.
    for peer in region.get_peers() {
        if peer.get_id() == peer_id {
            return Some(peer.get_store_id());
        }
    }
    None
}

/// The leader change notifier starts lazyily, but the observer has registered to
/// the CoprocessorHost. Only when the leader change notifier has started, region
/// change events will be sent to it.
pub static LEADER_CHANGE_NOTIFIER_STARTED: AtomicBool = AtomicBool::new(false);

fn leader_change_notifier_started() -> bool {
    LEADER_CHANGE_NOTIFIER_STARTED.load(Ordering::Relaxed)
}

pub type NotifyCallback = Box<dyn Fn(Result<(u64, String)>) + Send>;

pub trait LeaderChangeNotifier {
    /// Starts the leader change notifier. If the leader changes, the cb will be called
    /// with the new leader's id and address.
    fn start(&mut self, cb: NotifyCallback);

    /// Stops the leader change notifier.
    fn stop(&mut self);

    /// Gets the id and address of the leader synchronous.
    fn get_leader_info(&self, timeout: Duration) -> Result<(u64, String)>;
}

/// If the node has leader region, and the region has some updates, `LeaderChangeEvent`s will
/// be sent to the `Runner`.
enum LeaderChangeEvent {
    /// The leader region is destroyed in this node.
    LeaderRegionDestroyed,

    /// The role of this node in the leader region is changed.
    /// `leader_peer_id` is the peer id of the new leader.
    /// `region` is the leader region.
    /// `role` is the role of this node in the leader region.
    RoleChanged {
        leader_peer_id: u64,
        region: Region,
        role: StateRole,
    },

    /// Sends leader's id and address through `NotifyCallback`.
    GetLeaderInfo(NotifyCallback),
}

impl Display for LeaderChangeEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LeaderChangeEvent::LeaderRegionDestroyed => write!(f, "LeaderRegionDestroyed"),
            LeaderChangeEvent::RoleChanged {
                leader_peer_id,
                role,
                ..
            } => write!(
                f,
                "RoleChanged{{ leader_peer_id: {}, role: {:?}, .. }}",
                leader_peer_id, role
            ),
            LeaderChangeEvent::GetLeaderInfo(_) => write!(f, "GetLeaderInfo"),
        }
    }
}

#[derive(Default)]
struct RoleInfo {
    /// Leader's id and address if exists.
    leader_info: Option<(u64, String)>,

    /// The leader region if has.
    region: Option<Region>,
}

#[derive(Clone)]
struct Runner<S: StoreAddrResolver + 'static> {
    /// `pd_client` is used to get leader info of the leader region from PD.
    pd_client: Arc<RpcClient>,
    /// `resolver` resolves store address.
    resolver: S,
    /// `cb` will be called with the new leader's id and address if the leader changes.
    cb: Rc<Option<NotifyCallback>>,
    /// Save the leader info and the leader region info.
    role_info: Rc<RefCell<RoleInfo>>,
    /// The interval of getting leader info from pd.
    monitor_membership_interval: u64,

    is_initialized: bool,
}

unsafe impl<S: StoreAddrResolver + 'static> Send for Runner<S> {}

impl<S: StoreAddrResolver + 'static> Runner<S> {
    fn new(pd_client: Arc<RpcClient>, resolver: S, monitor_membership_interval: u64) -> Self {
        Self {
            pd_client,
            resolver,
            cb: Rc::new(None),
            role_info: Rc::new(RefCell::new(RoleInfo::default())),
            monitor_membership_interval,
            is_initialized: false,
        }
    }

    fn set_leader_region(&self, region: Option<Region>) {
        mem::replace(&mut self.role_info.borrow_mut().region, region);
    }

    fn has_leader_region(&self) -> bool {
        self.role_info.borrow().region.is_some()
    }

    fn initialize(&mut self, handle: &Handle) {
        self.schedule_leader_change_monitor(handle);
        self.is_initialized = true;
    }

    /// Schedules timer to get leader info from PD periodically.
    /// It is the backup plan if something goes wrong.
    fn schedule_leader_change_monitor(&mut self, handle: &Handle) {
        info!("schedule leader change monitor");
        let mut clone = self.clone();
        let timer = Interval::new(
            Instant::now(),
            Duration::from_millis(self.monitor_membership_interval),
        )
        .for_each(move |_| {
            if let Err(e) = clone.monitor_leader_change() {
                error!("monitor leader change failed"; "err" => ?e);
                ERROR_COUNTER_VEC.monitor_membership_change.inc();
            }
            Ok(())
        })
        .map_err(|e| panic!("unexpected err: {:?}", e));
        handle.spawn(timer);
    }

    /// Gets leader id from PD and resolves its address.
    /// If the node has leader region, the function does nothing.
    fn monitor_leader_change(&mut self) -> Result<()> {
        // If the node has leader region, gets leader info from raftstore.
        if self.has_leader_region() {
            return Ok(());
        }

        let _timer = DETECTOR_HISTOGRAM_VEC
            .monitor_membership_change
            .start_coarse_timer();
        let (_, leader) = self.pd_client.get_region_and_leader(LEADER_KEY)?;
        if let Some(leader) = leader {
            let leader_id = leader.get_store_id();
            let leader_addr = self.resolve_store_address(leader_id)?;
            self.update_leader_info(leader_id, leader_addr);
        } else {
            warn!("leader not found");
            ERROR_COUNTER_VEC.leader_not_found.inc();
        }
        Ok(())
    }

    /// Resolves store address.
    fn resolve_store_address(&self, store_id: u64) -> Result<String> {
        match wait_op!(|cb| self
            .resolver
            .resolve(store_id, cb)
            .map_err(|e| Error::Other(box_err!(e))))
        {
            Some(Ok(addr)) => Ok(addr),
            _ => Err(box_err!("failed to resolve store address")),
        }
    }

    /// If the leader changes, `NotifyCallback` will be called.
    fn update_leader_info(&mut self, leader_id: u64, leader_addr: String) {
        let leader_info = &mut self.role_info.borrow_mut().leader_info;
        match leader_info {
            Some((id, addr)) if *id == leader_id && *addr == leader_addr => {
                debug!("leader not change"; "leader_id" => leader_id, "leader_addr" => %leader_addr);
            }
            _ => {
                debug!("leader changed"; "leader_id" => leader_id, "leader_addr" => %leader_addr);
                leader_info.replace((leader_id, leader_addr.clone()));
                self.cb.as_ref().as_ref().unwrap()(Ok((leader_id, leader_addr)));
            }
        }
    }

    /// If the leader region is destroyed in this node, gets leader info from PD.
    fn handle_leader_region_destroyed(&mut self) {
        info!("leader region destroyed");
        self.set_leader_region(None);
    }

    /// If the leader changed, notifys deadlock detector.
    fn handle_role_changed(&mut self, leader_peer_id: u64, region: Region, role: StateRole) {
        info!("role changed"; "leader_peer_id" => leader_peer_id, "role" => ?role);
        // If the role of this node changes, it will receive a role change event with INVALID_ID
        // first, and then an event with the new leader peer id when the leader is elected.
        if leader_peer_id == INVALID_ID {
            return;
        }
        match get_store_id(&region, leader_peer_id) {
            Some(leader_store_id) => {
                match self.resolve_store_address(leader_store_id) {
                    Ok(addr) => {
                        // Updates the leader region in case peers are updated.
                        self.set_leader_region(Some(region));
                        self.update_leader_info(leader_store_id, addr);
                    }
                    Err(e) => {
                        error!("resolve leader address failed"; "err" => ?e);
                        // If resolve fails, the notifier will get leader info from PD to
                        // prevent leader info stale even if the node has leader region
                        self.set_leader_region(None);
                    }
                };
            }

            None => {
                warn!("leader's store id not found");
                // Same reason as 'resolve leader address failed'.
                self.set_leader_region(None);
            }
        };
    }

    fn handle_get_leader_info(&mut self, cb: NotifyCallback) {
        if let Err(e) = self.monitor_leader_change() {
            error!("request getting leader info failed, return previous info"; "err" => ?e);
        }
        match &self.role_info.borrow().leader_info {
            Some((id, addr)) => cb(Ok((*id, addr.to_owned()))),
            None => cb(Err(box_err!("no leader"))),
        }
    }
}

impl<S: StoreAddrResolver + 'static> FutureRunnable<LeaderChangeEvent> for Runner<S> {
    fn run(&mut self, event: LeaderChangeEvent, handle: &Handle) {
        if !self.is_initialized {
            self.initialize(handle);
        }

        match event {
            LeaderChangeEvent::LeaderRegionDestroyed => self.handle_leader_region_destroyed(),
            LeaderChangeEvent::RoleChanged {
                leader_peer_id,
                region,
                role,
            } => self.handle_role_changed(leader_peer_id, region, role),
            LeaderChangeEvent::GetLeaderInfo(cb) => self.handle_get_leader_info(cb),
        }
    }
}

/// `LeaderChangeObserver` implements observer traits. If the leader region has some updates
/// and the leader change notifier has started, these updates will be sent to the notifier.
#[derive(Clone)]
struct LeaderChangeObserver {
    scheduler: FutureScheduler<LeaderChangeEvent>,
}

impl Coprocessor for LeaderChangeObserver {}

impl RegionChangeObserver for LeaderChangeObserver {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        _: StateRole,
    ) {
        if leader_change_notifier_started() && is_leader_region(ctx.region()) {
            if let RegionChangeEvent::Destroy = event {
                self.scheduler
                    .schedule(LeaderChangeEvent::LeaderRegionDestroyed)
                    .unwrap();
            }
        }
    }
}

impl RoleObserver for LeaderChangeObserver {
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, leader_peer_id: u64, role: StateRole) {
        if leader_change_notifier_started() && is_leader_region(ctx.region()) {
            let region = ctx.region().clone();
            self.scheduler
                .schedule(LeaderChangeEvent::RoleChanged {
                    leader_peer_id,
                    region,
                    role,
                })
                .unwrap();
        }
    }
}

/// Registers leader change observer to CoprocessorHost.
fn register_leader_change_observer(
    host: &mut CoprocessorHost,
    scheduler: FutureScheduler<LeaderChangeEvent>,
) {
    let observer = LeaderChangeObserver { scheduler };
    host.registry
        .register_region_change_observer(100, Box::new(observer.clone()));
    host.registry
        .register_role_observer(100, Box::new(observer));
}

pub struct Notifier<S: StoreAddrResolver + 'static> {
    worker: FutureWorker<LeaderChangeEvent>,
    /// Worker's runner.
    runner: Option<Runner<S>>,
    /// `scheduler` is used to send `GetLeaderInfo` event to the worker.
    scheduler: FutureScheduler<LeaderChangeEvent>,
}

impl<S: StoreAddrResolver + 'static> Notifier<S> {
    /// Creates a new `Notifier` and register `LeaderChangeObserver` to CoprocessorHost.
    pub fn new(
        pd_client: Arc<RpcClient>,
        resolver: S,
        monitor_membership_interval: u64,
        host: &mut CoprocessorHost,
    ) -> Self {
        let worker = FutureWorker::new("leader-change-notifier");
        let scheduler = worker.scheduler();

        register_leader_change_observer(host, scheduler.clone());
        Self {
            worker,
            runner: Some(Runner::new(
                pd_client,
                resolver,
                monitor_membership_interval,
            )),
            scheduler,
        }
    }

    fn get_leader_info_async(&self, cb: NotifyCallback) -> Result<()> {
        self.scheduler
            .schedule(LeaderChangeEvent::GetLeaderInfo(cb))
            .map_err(|e| box_err!(e))
    }
}

impl<S: StoreAddrResolver + 'static> LeaderChangeNotifier for Notifier<S> {
    /// Starts worker lazily.
    fn start(&mut self, cb: NotifyCallback) {
        let mut runner = self.runner.take().unwrap();
        runner.cb = Rc::new(Some(cb));
        self.worker.start(runner).unwrap();
        // Set it to true, then observer will send events to the notifier.
        LEADER_CHANGE_NOTIFIER_STARTED.store(true, Ordering::Relaxed);
    }

    /// Stops the worker.
    fn stop(&mut self) {
        if let Some(Err(e)) = self.worker.stop().map(JoinHandle::join) {
            info!(
                "ignore failure when stopping leader change notifier";
                "err" => ?e
            );
        }
    }

    fn get_leader_info(&self, timeout: Duration) -> Result<(u64, String)> {
        wait_op!(|cb| self.get_leader_info_async(cb), timeout)
            .unwrap_or(Err(Error::Timeout(timeout)))
    }
}
