// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Debug, Formatter},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    vec::IntoIter,
};

use engine_traits::{CfName, SstMetaInfo};
use kvproto::{
    metapb::Region,
    pdpb::CheckPolicy,
    raft_cmdpb::{AdminRequest, AdminResponse, RaftCmdRequest, RaftCmdResponse, Request},
    raft_serverpb::RaftApplyState,
};
use raft::{eraftpb, StateRole};

pub mod config;
mod consistency_check;
pub mod dispatcher;
mod error;
mod metrics;
pub mod region_info_accessor;
mod split_check;
pub mod split_observer;
use kvproto::raft_serverpb::RaftMessage;

pub use self::{
    config::{Config, ConsistencyCheckMethod},
    consistency_check::{ConsistencyCheckObserver, Raw as RawConsistencyCheckObserver},
    dispatcher::{
        BoxAdminObserver, BoxApplySnapshotObserver, BoxCmdObserver, BoxConsistencyCheckObserver,
        BoxMessageObserver, BoxPdTaskObserver, BoxQueryObserver, BoxRegionChangeObserver,
        BoxRoleObserver, BoxSplitCheckObserver, BoxUpdateSafeTsObserver, CoprocessorHost, Registry,
        StoreHandle,
    },
    error::{Error, Result},
    region_info_accessor::{
        Callback as RegionInfoCallback, RangeKey, RegionCollector, RegionInfo, RegionInfoAccessor,
        RegionInfoProvider, SeekRegionCallback,
    },
    split_check::{
        get_region_approximate_keys, get_region_approximate_middle, get_region_approximate_size,
        HalfCheckObserver, Host as SplitCheckerHost, KeysCheckObserver, SizeCheckObserver,
        TableCheckObserver,
    },
};
pub use crate::store::{Bucket, KeyEntry};

/// Coprocessor is used to provide a convenient way to inject code to
/// KV processing.
pub trait Coprocessor: Send {
    fn start(&self) {}
    fn stop(&self) {}
}

/// Context of observer.
pub struct ObserverContext<'a> {
    region: &'a Region,
    /// Whether to bypass following observer hook.
    pub bypass: bool,
}

impl<'a> ObserverContext<'a> {
    pub fn new(region: &Region) -> ObserverContext<'_> {
        ObserverContext {
            region,
            bypass: false,
        }
    }

    pub fn region(&self) -> &Region {
        self.region
    }
}

/// Context of a region provided for observers.
#[derive(Default, Clone)]
pub struct RegionState {
    pub peer_id: u64,
    pub pending_remove: bool,
    pub modified_region: Option<Region>,
}

/// Context for exec observers of mutation to be applied to ApplyContext.
pub struct ApplyCtxInfo<'a> {
    pub pending_handle_ssts: &'a mut Option<Vec<SstMetaInfo>>,
    pub delete_ssts: &'a mut Vec<SstMetaInfo>,
    pub pending_delete_ssts: &'a mut Vec<SstMetaInfo>,
}

pub trait AdminObserver: Coprocessor {
    /// Hook to call before proposing admin request.
    fn pre_propose_admin(&self, _: &mut ObserverContext<'_>, _: &mut AdminRequest) -> Result<()> {
        Ok(())
    }

    /// Hook to call before applying admin request.
    fn pre_apply_admin(&self, _: &mut ObserverContext<'_>, _: &AdminRequest) {}

    /// Hook to call after applying admin request.
    /// For now, the `region` in `ObserverContext` is an empty region.
    fn post_apply_admin(&self, _: &mut ObserverContext<'_>, _: &AdminResponse) {}

    /// Hook before exec admin request, returns whether we should skip this
    /// admin.
    fn pre_exec_admin(
        &self,
        _: &mut ObserverContext<'_>,
        _: &AdminRequest,
        _: u64,
        _: u64,
    ) -> bool {
        false
    }

    /// Hook to call immediately after exec command
    /// Will be a special persistence after this exec if a observer returns
    /// true.
    fn post_exec_admin(
        &self,
        _: &mut ObserverContext<'_>,
        _: &Cmd,
        _: &RaftApplyState,
        _: &RegionState,
        _: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        false
    }
}

pub trait QueryObserver: Coprocessor {
    /// Hook when observe applying empty cmd, probably caused by leadership
    /// change.
    fn on_empty_cmd(&self, _: &mut ObserverContext<'_>, _index: u64, _term: u64) {}

    /// Hook to call before proposing write request.
    ///
    /// We don't propose read request, hence there is no hook for it yet.
    fn pre_propose_query(&self, _: &mut ObserverContext<'_>, _: &mut Vec<Request>) -> Result<()> {
        Ok(())
    }

    /// Hook to call before applying write request.
    fn pre_apply_query(&self, _: &mut ObserverContext<'_>, _: &[Request]) {}

    /// Hook to call after applying write request.
    /// For now, the `region` in `ObserverContext` is an empty region.
    fn post_apply_query(&self, _: &mut ObserverContext<'_>, _: &Cmd) {}

    /// Hook before exec write request, returns whether we should skip this
    /// write.
    fn pre_exec_query(&self, _: &mut ObserverContext<'_>, _: &[Request], _: u64, _: u64) -> bool {
        false
    }

    /// Hook to call immediately after exec command.
    /// Will be a special persistence after this exec if a observer returns
    /// true.
    fn post_exec_query(
        &self,
        _: &mut ObserverContext<'_>,
        _: &Cmd,
        _: &RaftApplyState,
        _: &RegionState,
        _: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        false
    }
}

pub trait ApplySnapshotObserver: Coprocessor {
    /// Hook to call after applying key from plain file.
    /// This may be invoked multiple times for each plain file, and each time a
    /// batch of key-value pairs will be passed to the function.
    fn apply_plain_kvs(&self, _: &mut ObserverContext<'_>, _: CfName, _: &[(Vec<u8>, Vec<u8>)]) {}

    /// Hook to call after applying sst file. Currently the content of the
    /// snapshot can't be passed to the observer.
    fn apply_sst(&self, _: &mut ObserverContext<'_>, _: CfName, _path: &str) {}

    /// Hook when receiving Task::Apply.
    /// Should pass valid snapshot, the option is only for testing.
    /// Notice that we can call `pre_apply_snapshot` to multiple snapshots at
    /// the same time.
    fn pre_apply_snapshot(
        &self,
        _: &mut ObserverContext<'_>,
        _peer_id: u64,
        _: &crate::store::SnapKey,
        _: Option<&crate::store::Snapshot>,
    ) {
    }

    /// Hook when the whole snapshot is applied.
    /// Should pass valid snapshot, the option is only for testing.
    fn post_apply_snapshot(
        &self,
        _: &mut ObserverContext<'_>,
        _: u64,
        _: &crate::store::SnapKey,
        _snapshot: Option<&crate::store::Snapshot>,
    ) {
    }

    /// We call pre_apply_snapshot only when one of the observer returns true.
    fn should_pre_apply_snapshot(&self) -> bool {
        false
    }
}

/// SplitChecker is invoked during a split check scan, and decides to use
/// which keys to split a region.
pub trait SplitChecker<E> {
    /// Hook to call for every kv scanned during split.
    ///
    /// Return true to abort scan early.
    fn on_kv(&mut self, _: &mut ObserverContext<'_>, _: &KeyEntry) -> bool {
        false
    }

    /// Get the desired split keys.
    fn split_keys(&mut self) -> Vec<Vec<u8>>;

    /// Get approximate split keys without scan.
    fn approximate_split_keys(&mut self, _: &Region, _: &E) -> Result<Vec<Vec<u8>>> {
        Ok(vec![])
    }

    /// Get split policy.
    fn policy(&self) -> CheckPolicy;
}

pub trait SplitCheckObserver<E>: Coprocessor {
    /// Add a checker for a split scan.
    fn add_checker(
        &self,
        _: &mut ObserverContext<'_>,
        _: &mut SplitCheckerHost<'_, E>,
        _: &E,
        policy: CheckPolicy,
    );
}

/// Describes size information about all stores.
/// There is guarantee that capacity >= used + avail.
/// since some space can be reserved.
#[derive(Debug, Default)]
pub struct StoreSizeInfo {
    /// The capacity of the store.
    pub capacity: u64,
    /// Size of actual data.
    pub used: u64,
    /// Available space that can be written with actual data.
    pub avail: u64,
}

pub trait PdTaskObserver: Coprocessor {
    /// Compute capacity/used/available size of this store.
    fn on_compute_engine_size(&self, _: &mut Option<StoreSizeInfo>) {}
}

pub struct RoleChange {
    pub state: StateRole,
    pub leader_id: u64,
    /// The previous `lead_transferee` if no leader currently.
    pub prev_lead_transferee: u64,
    /// Which peer is voted by itself.
    pub vote: u64,
    pub initialized: bool,
    pub peer_id: u64,
}

impl RoleChange {
    #[cfg(any(test, feature = "testexport"))]
    pub fn new(state: StateRole) -> Self {
        RoleChange {
            state,
            leader_id: raft::INVALID_ID,
            prev_lead_transferee: raft::INVALID_ID,
            vote: raft::INVALID_ID,
            initialized: true,
            peer_id: raft::INVALID_ID,
        }
    }
}

pub trait RoleObserver: Coprocessor {
    /// Hook to call when role of a peer changes.
    ///
    /// Please note that, this hook is not called at realtime. There maybe a
    /// situation that the hook is not called yet, however the role of some
    /// peers have changed.
    fn on_role_change(&self, _: &mut ObserverContext<'_>, _: &RoleChange) {}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RegionChangeReason {
    ChangePeer,
    Split,
    PrepareMerge,
    CommitMerge,
    RollbackMerge,
    SwitchWitness,
    Flashback,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RegionChangeEvent {
    Create,
    Update(RegionChangeReason),
    Destroy,
    UpdateBuckets(usize),
}

pub trait RegionChangeObserver: Coprocessor {
    /// Hook to call when a region changed on this TiKV
    fn on_region_changed(&self, _: &mut ObserverContext<'_>, _: RegionChangeEvent, _: StateRole) {}

    /// Should be called everytime before we write a WriteBatch into
    /// KvEngine. Returns false if we can't commit at this time.
    fn pre_persist(
        &self,
        _: &mut ObserverContext<'_>,
        _is_finished: bool,
        _cmd: Option<&RaftCmdRequest>,
    ) -> bool {
        true
    }

    /// Should be called everytime before we want to write apply state when
    /// applying. Return a bool which indicates whether we can actually do
    /// this write.
    fn pre_write_apply_state(&self, _: &mut ObserverContext<'_>) -> bool {
        true
    }
}

pub trait MessageObserver: Coprocessor {
    /// Returns false if the message should not be stepped later.
    fn on_raft_message(&self, _: &RaftMessage) -> bool {
        true
    }
}

#[derive(Clone, Debug, Default)]
pub struct Cmd {
    pub index: u64,
    pub term: u64,
    pub request: RaftCmdRequest,
    pub response: RaftCmdResponse,
}

impl Cmd {
    pub fn new(index: u64, term: u64, request: RaftCmdRequest, response: RaftCmdResponse) -> Cmd {
        Cmd {
            index,
            term,
            request,
            response,
        }
    }
}

static OBSERVE_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier for checking stale observed commands.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObserveId(usize);

impl ObserveId {
    pub fn new() -> ObserveId {
        ObserveId(OBSERVE_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

/// ObserveHandle is the status of a term of observing, it contains the
/// `ObserveId` and the `observing` flag indicate whether the observing is
/// ongoing
#[derive(Clone, Default, Debug)]
pub struct ObserveHandle {
    pub id: ObserveId,
    observing: Arc<AtomicBool>,
}

impl ObserveHandle {
    pub fn new() -> ObserveHandle {
        ObserveHandle {
            id: ObserveId::new(),
            observing: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn with_id(id: usize) -> ObserveHandle {
        ObserveHandle {
            id: ObserveId(id),
            observing: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn is_observing(&self) -> bool {
        self.observing.load(Ordering::Acquire)
    }

    pub fn stop_observing(&self) {
        self.observing.store(false, Ordering::Release)
    }
}

#[derive(Default)]
pub struct CmdObserveInfo {
    pub cdc_id: ObserveHandle,
    pub rts_id: ObserveHandle,
    pub pitr_id: ObserveHandle,
}

impl CmdObserveInfo {
    pub fn from_handle(
        cdc_id: ObserveHandle,
        rts_id: ObserveHandle,
        pitr_id: ObserveHandle,
    ) -> CmdObserveInfo {
        CmdObserveInfo {
            cdc_id,
            rts_id,
            pitr_id,
        }
    }

    /// Get the max observe level of the observer info by the observers
    /// currently registered. Currently, TiKV uses a static strategy for
    /// managing observers. There are a fixed number type of observer being
    /// registered in each TiKV node, and normally, observers are singleton.
    /// The types are:
    /// CDC: Observer supports the `ChangeData` service.
    /// PiTR: Observer supports the `backup-log` function.
    /// RTS: Observer supports the `resolved-ts` advancing (and follower read,
    /// etc.).
    pub fn observe_level(&self) -> ObserveLevel {
        let cdc = if self.cdc_id.is_observing() {
            // `cdc` observe all data
            ObserveLevel::All
        } else {
            ObserveLevel::None
        };
        let pitr = if self.pitr_id.is_observing() {
            // `pitr` observe all data.
            ObserveLevel::All
        } else {
            ObserveLevel::None
        };
        let rts = if self.rts_id.is_observing() {
            // `resolved-ts` observe lock related data
            ObserveLevel::LockRelated
        } else {
            ObserveLevel::None
        };
        cdc.max(rts).max(pitr)
    }
}

impl Debug for CmdObserveInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CmdObserveInfo")
            .field("cdc_id", &self.cdc_id.id)
            .field("rts_id", &self.rts_id.id)
            .field("pitr_id", &self.pitr_id.id)
            .finish()
    }
}

// `ObserveLevel` describe what data the observer want to observe
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObserveLevel {
    // Don't observe any data
    None,
    // Only observe lock related data (i.e `lock_cf`, `write_cf`)
    LockRelated,
    // Observe all data
    All,
}

#[derive(Clone, Debug)]
pub struct CmdBatch {
    pub level: ObserveLevel,
    pub cdc_id: ObserveId,
    pub rts_id: ObserveId,
    pub pitr_id: ObserveId,
    pub region_id: u64,
    pub cmds: Vec<Cmd>,
}

impl CmdBatch {
    pub fn new(observe_info: &CmdObserveInfo, region_id: u64) -> CmdBatch {
        CmdBatch {
            level: observe_info.observe_level(),
            cdc_id: observe_info.cdc_id.id,
            rts_id: observe_info.rts_id.id,
            pitr_id: observe_info.pitr_id.id,
            region_id,
            cmds: Vec::new(),
        }
    }

    pub fn push(&mut self, observe_info: &CmdObserveInfo, region_id: u64, cmd: Cmd) {
        assert_eq!(region_id, self.region_id);
        assert_eq!(observe_info.cdc_id.id, self.cdc_id);
        assert_eq!(observe_info.rts_id.id, self.rts_id);
        assert_eq!(observe_info.pitr_id.id, self.pitr_id);
        self.cmds.push(cmd)
    }

    pub fn extend<I: IntoIterator<Item = Cmd>>(
        &mut self,
        observe_info: &CmdObserveInfo,
        region_id: u64,
        cmds: I,
    ) {
        assert_eq!(region_id, self.region_id);
        assert_eq!(observe_info.cdc_id.id, self.cdc_id);
        assert_eq!(observe_info.rts_id.id, self.rts_id);
        assert_eq!(observe_info.pitr_id.id, self.pitr_id);
        self.cmds.extend(cmds)
    }

    pub fn into_iter(self, region_id: u64) -> IntoIter<Cmd> {
        assert_eq!(region_id, self.region_id);
        self.cmds.into_iter()
    }

    pub fn len(&self) -> usize {
        self.cmds.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cmds.is_empty()
    }

    pub fn size(&self) -> usize {
        let mut cmd_bytes = 0;
        for cmd in self.cmds.iter() {
            let Cmd {
                ref request,
                ref response,
                ..
            } = cmd;
            if !response.get_header().has_error() && !request.has_admin_request() {
                for req in request.requests.iter() {
                    let put = req.get_put();
                    cmd_bytes += put.get_key().len();
                    cmd_bytes += put.get_value().len();
                }
            }
        }
        cmd_bytes
    }
}

pub trait CmdObserver<E>: Coprocessor {
    /// Hook to call after flushing writes to db.
    fn on_flush_applied_cmd_batch(
        &self,
        max_level: ObserveLevel,
        cmd_batches: &mut Vec<CmdBatch>,
        engine: &E,
    );
    // TODO: maybe should move `on_applied_current_term` to a separated
    // `Coprocessor`
    /// Hook to call at the first time the leader applied on its term
    fn on_applied_current_term(&self, role: StateRole, region: &Region);
}

pub trait ReadIndexObserver: Coprocessor {
    // Hook to call when stepping in raft and the message is a read index message.
    fn on_step(&self, _msg: &mut eraftpb::Message, _role: StateRole) {}
}

pub trait UpdateSafeTsObserver: Coprocessor {
    /// Hook after update self safe_ts and received leader safe_ts.
    fn on_update_safe_ts(&self, _: u64, _: u64, _: u64) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observe_level() {
        // Both cdc and `resolved-ts` are observing
        let observe_info = CmdObserveInfo::from_handle(
            ObserveHandle::new(),
            ObserveHandle::new(),
            ObserveHandle::new(),
        );
        assert_eq!(observe_info.observe_level(), ObserveLevel::All);

        // No observer
        observe_info.cdc_id.stop_observing();
        observe_info.rts_id.stop_observing();
        observe_info.pitr_id.stop_observing();
        assert_eq!(observe_info.observe_level(), ObserveLevel::None);

        // Only cdc observing
        let observe_info = CmdObserveInfo::from_handle(
            ObserveHandle::new(),
            ObserveHandle::new(),
            ObserveHandle::new(),
        );
        observe_info.rts_id.stop_observing();
        observe_info.pitr_id.stop_observing();
        assert_eq!(observe_info.observe_level(), ObserveLevel::All);

        // Only `resolved-ts` observing
        let observe_info = CmdObserveInfo::from_handle(
            ObserveHandle::new(),
            ObserveHandle::new(),
            ObserveHandle::new(),
        );
        observe_info.cdc_id.stop_observing();
        observe_info.pitr_id.stop_observing();
        assert_eq!(observe_info.observe_level(), ObserveLevel::LockRelated);

        // Only `backup-stream(pitr)` observing
        let observe_info = CmdObserveInfo::from_handle(
            ObserveHandle::new(),
            ObserveHandle::new(),
            ObserveHandle::new(),
        );
        observe_info.cdc_id.stop_observing();
        observe_info.rts_id.stop_observing();
        assert_eq!(observe_info.observe_level(), ObserveLevel::All);
    }
}
