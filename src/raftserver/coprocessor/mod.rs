mod region_snapshot;
pub mod dispatcher;

pub use self::region_snapshot::RegionSnapshot;

use proto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse};

/// Coprocessor is used to provide a convient way to inject code to
/// KV processing.
pub trait Coprocessor {
    fn start(&mut self) -> ();
    fn stop(&mut self) -> ();
}

/// Context of request.
pub struct RequestContext<'a> {
    /// A snapshot of requested region.
    pub snap: RegionSnapshot<'a>,
    pub req: RaftCommandRequest,
    /// Whether to bypass following observer pre-hook.
    pub bypass: bool,
}

/// Context of response.
pub struct ResponseContext<'a> {
    snap: RegionSnapshot<'a>,
    req: RaftCommandRequest,
    pub res: RaftCommandResponse,
    /// Whether to bypass following observer post-hook.
    pub bypass: bool,
}

impl<'a> ResponseContext<'a> {
    /// Get a snapshot of requested snapshot.
    pub fn get_snapshot(&self) -> &RegionSnapshot {
        &self.snap
    }

    /// Get a readonly request object.
    pub fn get_request(&self) -> &RaftCommandRequest {
        &self.req
    }
}

/// Observer hook of region level.
pub trait RegionObserver: Coprocessor {
    /// Hook to call before execute request command.
    fn pre_propose(&mut self, ctx: &mut RequestContext) -> ();
    /// Hook to call after command being executed.
    fn post_apply(&mut self, ctx: &mut ResponseContext) -> ();
}
