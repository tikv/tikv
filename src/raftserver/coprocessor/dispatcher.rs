use super::{RegionObserver, ObserverContext};

use raftserver::store::PeerStorage;
use proto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse};

struct ObserverEntry {
    priority: u32,
    observer: Box<RegionObserver + Send>,
}

/// Registry contains all registered coprocessors.
#[derive(Default)]
pub struct Registry {
    observers: Vec<ObserverEntry>, // TODO: add endpoint
}

impl Registry {
    /// register an Observer to dispatcher.
    pub fn register_observer(&mut self, priority: u32, mut ro: Box<RegionObserver + Send>) {
        ro.start();
        let r = ObserverEntry {
            priority: priority,
            observer: ro,
        };
        self.observers.push(r);
        self.observers.sort_by(|l, r| l.priority.cmp(&r.priority));
    }
}

/// Admin and invoke all coprocessors.
#[derive(Default)]
pub struct CoprocessorHost {
    pub registry: Registry,
}

impl CoprocessorHost {
    // TODO load from configuration.

    pub fn new() -> CoprocessorHost {
        CoprocessorHost::default()
    }

    /// Call all prepose hook until bypass is set to true.
    pub fn pre_propose(&mut self, ps: &PeerStorage, req: &mut RaftCommandRequest) {
        let ctx = ObserverContext::new(ps);
        if req.has_admin_request() {
            self.execute_pre_hook(ctx, req.mut_admin_request(), |o, ctx, q| o.pre_admin(ctx, q));
        } else {
            self.execute_pre_hook(ctx, req.mut_requests(), |o, ctx, q| o.pre_query(ctx, q));
        }
    }
    
    fn execute_pre_hook<Q, H>(&mut self, mut ctx: ObserverContext, req: &mut Q, mut hook: H)
    where H: FnMut(&mut RegionObserver, &mut ObserverContext, &mut Q)
     {
        for entry in &mut self.registry.observers {
            hook(entry.observer.as_mut(), &mut ctx, req);
            if ctx.bypass {
                break;
            }
        }
    }
    
    fn execute_post_hook<Q, R, H>(&mut self, mut ctx: ObserverContext, req: Q, resp: &mut R, mut hook: H)
    where H: FnMut(&mut RegionObserver, &mut ObserverContext, &Q, &mut R)
     {
        for entry in &mut self.registry.observers {
            hook(entry.observer.as_mut(), &mut ctx, &req, resp);
            if ctx.bypass {
                break;
            }
        }
    }

    /// call all apply hook until bypass is set to true.
    pub fn post_apply(&mut self, ps: &PeerStorage, req: &RaftCommandRequest, resp: &mut RaftCommandResponse) {
        let ctx = ObserverContext::new(ps);
        if req.has_admin_request() {
            self.execute_post_hook(ctx, req.get_admin_request(), resp.mut_admin_response(), |o, ctx, q, r| o.post_admin(ctx, q, r));
        } else {
            self.execute_post_hook(ctx, req.get_requests(), resp.mut_responses(), |o, ctx, q, r| o.post_query(ctx, q, r));
        }
    }

    pub fn shutdown(&mut self) {
        for entry in &mut self.registry.observers {
            entry.observer.stop();
        }
    }
}

impl Drop for CoprocessorHost {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod test {
    use raftserver::coprocessor::*;
    use tempdir::TempDir;
    use raftserver::store::engine::*;
    use raftserver::store::PeerStorage;
    use std::sync::*;

    use proto::metapb::Region;
    use proto::raft_cmdpb::{RaftCommandRequest, RaftCommandResponse};

    struct TestCoprocessor {
        bypass_pre: Arc<RwLock<bool>>,
        bypass_post: Arc<RwLock<bool>>,
        called_pre: Arc<RwLock<bool>>,
        called_post: Arc<RwLock<bool>>,
    }

    impl TestCoprocessor {
        fn new(bypass_pre: Arc<RwLock<bool>>,
               bypass_post: Arc<RwLock<bool>>,
               called_pre: Arc<RwLock<bool>>,
               called_post: Arc<RwLock<bool>>)
               -> TestCoprocessor {
            TestCoprocessor {
                bypass_post: bypass_post,
                bypass_pre: bypass_pre,
                called_post: called_post,
                called_pre: called_pre,
            }
        }
    }

    impl Coprocessor for TestCoprocessor {
        fn start(&mut self) {}
        fn stop(&mut self) {}
    }

    impl RegionObserver for TestCoprocessor {
        fn pre_admin(&mut self, ctx: &mut ObserverContext, _: &mut AdminRequest) {
            *self.called_pre.write().unwrap() = true;
            ctx.bypass = *self.bypass_pre.read().unwrap();
        }
        
        fn pre_query(&mut self, ctx: &mut ObserverContext, _: &mut Request) {
            *self.called_pre.write().unwrap() = true;
            ctx.bypass = *self.bypass_pre.read().unwrap();
        }
        
        fn post_admin(&mut self, ctx: &mut ObserverContext, _: &AdminRequest, _: &mut AdminResponse) {
            *self.called_post.write().unwrap() = true;
            ctx.bypass = *self.bypass_post.read().unwrap();
        }

        fn post_query(&mut self, ctx: &mut ObserverContext, _: &Request, _: &mut Response) {
            *self.called_post.write().unwrap() = true;
            ctx.bypass = *self.bypass_post.read().unwrap();
        }
    }

    fn new_peer_storage() -> PeerStorage {
        let path = TempDir::new("test-raftserver").unwrap();
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();
        PeerStorage::new(Arc::new(engine), &Region::new()).unwrap()
    }

    fn share_bool() -> Arc<RwLock<bool>> {
        Arc::new(RwLock::new(false))
    }

    fn assert_all(conditions: &[&Arc<RwLock<bool>>], expect: &[bool]) {
        for (c, e) in conditions.iter().zip(expect) {
            assert_eq!(*c.write().unwrap(), *e);
        }
    }

    fn set_all(conditions: &[&Arc<RwLock<bool>>], b: bool) {
        for c in conditions {
            *c.write().unwrap() = b;
        }
    }

    #[test]
    fn test_coprocessor_host() {
        // bypass_pre, bypass_post, called_pre, called_post
        let (bpr1, bpt1, cpr1, cpt1) = (share_bool(), share_bool(), share_bool(), share_bool());
        let observer1 = TestCoprocessor::new(bpr1.clone(),
                                             bpt1.clone(),
                                             cpr1.clone(),
                                             cpt1.clone());
        let mut host = CoprocessorHost::default();
        host.registry.register_observer(3, Box::new(observer1));
        let ps = new_peer_storage();
        let mut admin_req = RaftCommandRequest::new();
        admin_req.set_admin(AdminRequest::new());
        let mut query_req = RaftCommandRequest::new();
        query.set_request(Request::new());
        let mut resp = RaftCommandResponse::new();

        assert!(!*cpr1.read().unwrap());
        host.pre_propose(&ps, &mut admin_req);
        assert!(*cpr1.read().unwrap());
        assert!(!req.bypass);

        assert!(!*cpt1.read().unwrap());
        host.post_apply(&mut res);
        assert!(*cpt1.read().unwrap());
        assert!(!res.bypass);

        // reset
        set_all(&[&cpt1, &cpr1], false);

        let (bpr2, bpt2, cpr2, cpt2) = (share_bool(), share_bool(), share_bool(), share_bool());
        let observer2 = TestCoprocessor::new(bpr2.clone(),
                                             bpt2.clone(),
                                             cpr2.clone(),
                                             cpt2.clone());
        host.registry.register_observer(2, Box::new(observer2));

        set_all(&[&bpr2, &bpt2], true);

        assert_all(&[&cpr1, &cpt1, &cpr2, &cpt2], &[false, false, false, false]);

        host.pre_propose(&mut req);
        host.post_apply(&mut res);

        assert_all(&[&cpr1, &cpt1, &cpr2, &cpt2], &[false, false, true, true]);

        set_all(&[&bpr2, &bpt2], false);
        set_all(&[&cpr2, &cpt2], false);

        assert_all(&[&cpr1, &cpt1, &cpr2, &cpt2], &[false, false, false, false]);

        host.pre_propose(&mut req);
        host.post_apply(&mut res);

        assert_all(&[&cpr1, &cpt1, &cpr2, &cpt2], &[true, true, true, true]);
    }
}
