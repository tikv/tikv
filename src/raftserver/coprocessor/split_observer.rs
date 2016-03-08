use super::{Coprocessor, RegionObserver, ObserverContext};

use proto::raft_cmdpb::{SplitRequest, AdminRequest, Request, AdminResponse, Response,
                        AdminCommandType};
use protobuf::RepeatedField;

/// SplitObserver assign a split key to SplitRequest if it doesn't have one.
pub struct SplitObserver;

impl SplitObserver {
    // fn get_appromi

    fn handle_split(&mut self, _: &mut ObserverContext, split: &mut SplitRequest) {
        if split.has_split_key() {
            return;
        }
        // ctx.snap
    }
}

impl Coprocessor for SplitObserver {
    fn start(&mut self) {}
    fn stop(&mut self) {}
}

impl RegionObserver for SplitObserver {
    fn pre_admin(&mut self, ctx: &mut ObserverContext, req: &mut AdminRequest) {
        if req.get_cmd_type() != AdminCommandType::Split {
            return;
        }
        if !req.has_split() {
            error!("cmd_type is Split but it doesn't have split request, message maybe corrupted!");
            return;
        }
        self.handle_split(ctx, req.mut_split());
    }

    fn post_admin(&mut self, _: &mut ObserverContext, _: &AdminRequest, _: &mut AdminResponse) {}

    /// Hook to call before execute read/write request.
    fn pre_query(&mut self, _: &mut ObserverContext, _: &mut RepeatedField<Request>) -> () {}

    /// Hook to call after read/write request being executed.
    fn post_query(&mut self,
                  _: &mut ObserverContext,
                  _: &[Request],
                  _: &mut RepeatedField<Response>)
                  -> () {
    }
}
