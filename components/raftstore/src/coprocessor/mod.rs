// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::vec::IntoIter;

use engine_traits::CfName;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{
    AdminRequest, AdminResponse, RaftCmdRequest, RaftCmdResponse, Request, Response,
};
use raft::StateRole;

pub mod config;
pub mod dispatcher;
mod error;
mod metrics;
pub mod region_info_accessor;
mod split_check;
pub mod split_observer;

pub use self::config::Config;
pub use self::dispatcher::{
    BoxAdminObserver, BoxApplySnapshotObserver, BoxCmdObserver, BoxQueryObserver,
    BoxRegionChangeObserver, BoxRoleObserver, BoxSplitCheckObserver, CoprocessorHost, Registry,
};
pub use self::error::{Error, Result};
pub use self::region_info_accessor::{
    Callback as RegionInfoCallback, RegionCollector, RegionInfo, RegionInfoAccessor,
    RegionInfoProvider, SeekRegionCallback,
};
pub use self::split_check::{
    get_region_approximate_keys, get_region_approximate_keys_cf, get_region_approximate_middle,
    get_region_approximate_size, get_region_approximate_size_cf, HalfCheckObserver,
    Host as SplitCheckerHost, KeysCheckObserver, SizeCheckObserver, TableCheckObserver,
};

pub use crate::store::KeyEntry;

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

pub trait AdminObserver: Coprocessor {
    /// Hook to call before proposing admin request.
    fn pre_propose_admin(&self, _: &mut ObserverContext<'_>, _: &mut AdminRequest) -> Result<()> {
        Ok(())
    }

    /// Hook to call before applying admin request.
    fn pre_apply_admin(&self, _: &mut ObserverContext<'_>, _: &AdminRequest) {}

    /// Hook to call after applying admin request.
    fn post_apply_admin(&self, _: &mut ObserverContext<'_>, _: &mut AdminResponse) {}
}

pub trait QueryObserver: Coprocessor {
    /// Hook to call before proposing write request.
    ///
    /// We don't propose read request, hence there is no hook for it yet.
    fn pre_propose_query(&self, _: &mut ObserverContext<'_>, _: &mut Vec<Request>) -> Result<()> {
        Ok(())
    }

    /// Hook to call before applying write request.
    fn pre_apply_query(&self, _: &mut ObserverContext<'_>, _: &[Request]) {}

    /// Hook to call after applying write request.
    fn post_apply_query(&self, _: &mut ObserverContext<'_>, _: &mut Vec<Response>) {}
}

pub trait ApplySnapshotObserver: Coprocessor {
    /// Hook to call before applying key from plain file.
    /// This may be invoked multiple times for each plain file, and each time a batch of key-value
    /// pairs will be passed to the function.
    fn pre_apply_plain_kvs(
        &self,
        _: &mut ObserverContext<'_>,
        _: CfName,
        _: &[(Vec<u8>, Vec<u8>)],
    ) {
    }

    /// Hook to call before applying sst file. Currently the content of the snapshot can't be
    /// passed to the observer.
    fn pre_apply_sst(&self, _: &mut ObserverContext<'_>, _: CfName, _path: &str) {}
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

pub trait RoleObserver: Coprocessor {
    /// Hook to call when role of a peer changes.
    ///
    /// Please note that, this hook is not called at realtime. There maybe a
    /// situation that the hook is not called yet, however the role of some peers
    /// have changed.
    fn on_role_change(&self, _: &mut ObserverContext<'_>, _: StateRole) {}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RegionChangeEvent {
    Create,
    Update,
    Destroy,
}

pub trait RegionChangeObserver: Coprocessor {
    /// Hook to call when a region changed on this TiKV
    fn on_region_changed(&self, _: &mut ObserverContext<'_>, _: RegionChangeEvent, _: StateRole) {}
}

#[derive(Clone, Debug)]
pub struct Cmd {
    pub index: u64,
    pub request: RaftCmdRequest,
    pub response: RaftCmdResponse,
}

impl Cmd {
    pub fn new(index: u64, request: RaftCmdRequest, response: RaftCmdResponse) -> Cmd {
        Cmd {
            index,
            request,
            response,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CmdBatch {
    pub region_id: u64,
    cmds: Vec<Cmd>,
}

impl CmdBatch {
    pub fn new(region_id: u64) -> CmdBatch {
        CmdBatch {
            region_id,
            cmds: Vec::new(),
        }
    }

    pub fn push(&mut self, region_id: u64, cmd: Cmd) {
        assert_eq!(region_id, self.region_id);
        self.cmds.push(cmd)
    }

    pub fn into_iter(self, region_id: u64) -> IntoIter<Cmd> {
        assert_eq!(self.region_id, region_id);
        self.cmds.into_iter()
    }

    pub fn len(&self) -> usize {
        self.cmds.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cmds.is_empty()
    }
}

pub trait CmdObserver: Coprocessor {
    /// Hook to call after preparing for applying write requests.
    fn on_prepare_for_apply(&self, region_id: u64);
    /// Hook to call after applying a write request.
    fn on_apply_cmd(&self, region_id: u64, cmd: Cmd);
    /// Hook to call after flushing writes to db.
    fn on_flush_apply(&self);
}
