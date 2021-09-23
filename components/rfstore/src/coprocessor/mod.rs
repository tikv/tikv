// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod config;
mod dispatcher;
mod error;

pub use config::*;
pub use dispatcher::*;
pub use error::*;

use kvproto::metapb::Region;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use raft::StateRole;
use raft_proto::eraftpb;
use std::cmp;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::vec::IntoIter;

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

#[derive(Clone, Debug, Default)]
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

static OBSERVE_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier for checking stale observed commands.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObserveID(usize);

impl ObserveID {
    pub fn new() -> ObserveID {
        ObserveID(OBSERVE_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

/// ObserveHandle is the status of a term of observing, it contains the `ObserveID`
/// and the `observing` flag indicate whether the observing is ongoing
#[derive(Clone, Default, Debug)]
pub struct ObserveHandle {
    pub id: ObserveID,
    observing: Arc<AtomicBool>,
}

impl ObserveHandle {
    pub fn new() -> ObserveHandle {
        ObserveHandle {
            id: ObserveID::new(),
            observing: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn with_id(id: usize) -> ObserveHandle {
        ObserveHandle {
            id: ObserveID(id),
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
}

impl CmdObserveInfo {
    pub fn from_handle(cdc_id: ObserveHandle, rts_id: ObserveHandle) -> CmdObserveInfo {
        CmdObserveInfo { cdc_id, rts_id }
    }

    fn observe_level(&self) -> ObserveLevel {
        let cdc = if self.cdc_id.is_observing() {
            // `cdc` observe all data
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
        cmp::max(cdc, rts)
    }
}

impl Debug for CmdObserveInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CmdObserveInfo")
            .field("cdc_id", &self.cdc_id.id)
            .field("rts_id", &self.rts_id.id)
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
    pub cdc_id: ObserveID,
    pub rts_id: ObserveID,
    pub region: Region,
    pub cmds: Vec<Cmd>,
}

impl CmdBatch {
    pub fn new(observe_info: &CmdObserveInfo, region: Region) -> CmdBatch {
        CmdBatch {
            level: observe_info.observe_level(),
            cdc_id: observe_info.cdc_id.id,
            rts_id: observe_info.rts_id.id,
            region,
            cmds: Vec::new(),
        }
    }

    pub fn push(&mut self, observe_info: &CmdObserveInfo, region_id: u64, cmd: Cmd) {
        assert_eq!(region_id, self.region.get_id());
        assert_eq!(observe_info.cdc_id.id, self.cdc_id);
        assert_eq!(observe_info.rts_id.id, self.rts_id);
        self.cmds.push(cmd)
    }

    pub fn into_iter(self, region_id: u64) -> IntoIter<Cmd> {
        assert_eq!(self.region.get_id(), region_id);
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
    // TODO: maybe shoulde move `on_applied_current_term` to a separated `Coprocessor`
    /// Hook to call at the first time the leader applied on its term
    fn on_applied_current_term(&self, role: StateRole, region: &Region);
}

pub trait ReadIndexObserver: Coprocessor {
    // Hook to call when stepping in raft and the message is a read index message.
    fn on_step(&self, _msg: &mut eraftpb::Message) {}
}
