use engine::rocks::DB;
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, Request, Response};
use protobuf::RepeatedField;
use raft::StateRole;

pub use error::{Error, Result};

pub mod config;
pub mod error;
pub mod metrics;
pub use tikv_misc::cop_props as properties;
pub mod split_observer;

/// Coprocessor is used to provide a convenient way to inject code to
/// KV processing.
pub trait Coprocessor {
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
    fn pre_propose_query(
        &self,
        _: &mut ObserverContext<'_>,
        _: &mut RepeatedField<Request>,
    ) -> Result<()> {
        Ok(())
    }

    /// Hook to call before applying write request.
    fn pre_apply_query(&self, _: &mut ObserverContext<'_>, _: &[Request]) {}

    /// Hook to call after applying write request.
    fn post_apply_query(&self, _: &mut ObserverContext<'_>, _: &mut RepeatedField<Response>) {}
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
