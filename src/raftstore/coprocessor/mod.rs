mod region_snapshot;
pub mod dispatcher;
pub mod split_observer;

pub use self::region_snapshot::RegionSnapshot;
pub use self::dispatcher::{CoprocessorHost, Registry};

use kvproto::raft_cmdpb::{AdminRequest, Request, AdminResponse, Response};
use protobuf::RepeatedField;
use raftstore::store::PeerStorage;

/// Coprocessor is used to provide a convient way to inject code to
/// KV processing.
pub trait Coprocessor {
    fn start(&mut self) -> ();
    fn stop(&mut self) -> ();
}

/// Context of request.
pub struct ObserverContext<'a> {
    /// A snapshot of requested region.
    pub snap: RegionSnapshot<'a>,
    /// Whether to bypass following observer hook.
    pub bypass: bool,
}

impl<'a> ObserverContext<'a> {
    pub fn new(peer: &'a PeerStorage) -> ObserverContext<'a> {
        ObserverContext {
            snap: RegionSnapshot::new(&peer),
            bypass: false,
        }
    }
}

/// Observer hook of region level.
pub trait RegionObserver: Coprocessor {
    /// Hook to call before execute admin request.
    fn pre_admin(&mut self, ctx: &mut ObserverContext, req: &mut AdminRequest) -> ();

    /// Hook to call before execute read/write request.
    fn pre_query(&mut self, ctx: &mut ObserverContext, req: &mut RepeatedField<Request>) -> ();

    /// Hook to call after admin request being executed.
    fn post_admin(&mut self,
                  ctx: &mut ObserverContext,
                  req: &AdminRequest,
                  resp: &mut AdminResponse)
                  -> ();

    /// Hook to call after read/write request being executed.
    fn post_query(&mut self,
                  ctx: &mut ObserverContext,
                  req: &[Request],
                  resp: &mut RepeatedField<Response>)
                  -> ();
}
