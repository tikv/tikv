use super::{Coprocessor, RegionObserver, RequestContext, ResponseContext};

/// SplitObserver handles how to split a region.
pub struct SplitObserver;

impl SplitObserver {
    fn handle_split(&mut self, split: )
}

impl Coprocessor for SplitObserver {
    fn start(&mut self) {}
    fn stop(&mut self) {}
}

impl RegionObserver for SplitObserver {
    fn pre_propose(&mut self, ctx: &mut RequestContext) {
        if !ctx.req.has_admin_request() {
            return;
        }
        let admin_req = ctx.req.mut_admin_request();
        if admin_req.get_cmd_type() != AdminCommandType::Split {
            continue;
        }
        if !admin_req.has_split() {
            error!("cmd_type is Split but it doesn't have split request, message maybe corrupted!");
            continue;
        }
        self.handle_split(&mut )
    }
    
    fn post_apply(&mut self, ctx: &mut ResponseContext) {}
}
