mod region_snapshot;
pub mod dispatcher;

pub use self::region_snapshot::RegionSnapshot;
pub use self::dispatcher::{CoprocessorHost, Registry};

use proto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse};
use raftserver::store::PeerStorage;

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

impl<'a> RequestContext<'a> {
    pub fn new(peer: &'a PeerStorage, cmd: RaftCommandRequest) -> RequestContext<'a> {
        RequestContext {
            snap: RegionSnapshot::new(&peer),
            req: cmd,
            bypass: false,
        }
    }
}

/// Context of response.
pub struct ResponseContext<'a> {
    snap: RegionSnapshot<'a>,
    req: RaftCommandRequest,
    pub resp: RaftCommandResponse,
    /// Whether to bypass following observer post-hook.
    pub bypass: bool,
}

impl<'a> ResponseContext<'a> {
    pub fn new(peer: &'a PeerStorage,
               cmd: RaftCommandRequest,
               resp: RaftCommandResponse)
               -> ResponseContext<'a> {
        ResponseContext {
            snap: RegionSnapshot::new(peer),
            req: cmd,
            resp: resp,
            bypass: false,
        }
    }

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
