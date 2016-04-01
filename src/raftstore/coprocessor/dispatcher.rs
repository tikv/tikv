use super::{RegionObserver, ObserverContext};

use raftstore::store::PeerStorage;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};

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
    pub fn pre_propose(&mut self, ps: &PeerStorage, req: &mut RaftCmdRequest) {
        let ctx = ObserverContext::new(ps);
        if req.has_admin_request() {
            self.execute_pre_hook(ctx,
                                  req.mut_admin_request(),
                                  |o, ctx, q| o.pre_admin(ctx, q));
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

    fn execute_post_hook<Q, R, H>(&mut self,
                                  mut ctx: ObserverContext,
                                  req: Q,
                                  resp: &mut R,
                                  mut hook: H)
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
    pub fn post_apply(&mut self,
                      ps: &PeerStorage,
                      req: &RaftCmdRequest,
                      resp: &mut RaftCmdResponse) {
        let ctx = ObserverContext::new(ps);
        if req.has_admin_request() {
            self.execute_post_hook(ctx,
                                   req.get_admin_request(),
                                   resp.mut_admin_response(),
                                   |o, ctx, q, r| o.post_admin(ctx, q, r));
        } else {
            self.execute_post_hook(ctx,
                                   req.get_requests(),
                                   resp.mut_responses(),
                                   |o, ctx, q, r| o.post_query(ctx, q, r));
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
    use raftstore::coprocessor::*;
    use tempdir::TempDir;
    use raftstore::store::engine::*;
    use raftstore::store::PeerStorage;
    use util::HandyRwLock;
    use std::sync::*;
    use std::fmt::Debug;
    use protobuf::RepeatedField;

    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::{AdminRequest, Request, AdminResponse, Response, RaftCmdRequest,
                              RaftCmdResponse};

    struct TestCoprocessor {
        bypass_pre: Arc<RwLock<bool>>,
        bypass_post: Arc<RwLock<bool>>,
        called_pre: Arc<RwLock<u8>>,
        called_post: Arc<RwLock<u8>>,
    }

    impl TestCoprocessor {
        fn new(bypass_pre: Arc<RwLock<bool>>,
               bypass_post: Arc<RwLock<bool>>,
               called_pre: Arc<RwLock<u8>>,
               called_post: Arc<RwLock<u8>>)
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
            *self.called_pre.wl() += 1;
            ctx.bypass = *self.bypass_pre.rl();
        }

        fn pre_query(&mut self, ctx: &mut ObserverContext, _: &mut RepeatedField<Request>) {
            *self.called_pre.wl() += 2;
            ctx.bypass = *self.bypass_pre.rl();
        }

        fn post_admin(&mut self,
                      ctx: &mut ObserverContext,
                      _: &AdminRequest,
                      _: &mut AdminResponse) {
            *self.called_post.wl() += 1;
            ctx.bypass = *self.bypass_post.rl();
        }

        fn post_query(&mut self,
                      ctx: &mut ObserverContext,
                      _: &[Request],
                      _: &mut RepeatedField<Response>) {
            *self.called_post.wl() += 2;
            ctx.bypass = *self.bypass_post.rl();
        }
    }

    fn new_peer_storage(path: &TempDir) -> PeerStorage {
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();
        PeerStorage::new(Arc::new(engine), &Region::new()).unwrap()
    }

    fn share<T>(t: T) -> Arc<RwLock<T>> {
        Arc::new(RwLock::new(t))
    }

    fn assert_all<T: PartialEq + Debug>(ts: &[&Arc<RwLock<T>>], expect: &[T]) {
        for (c, e) in ts.iter().zip(expect) {
            assert_eq!(*c.wl(), *e);
        }
    }

    fn set_all<T: Copy>(ts: &[&Arc<RwLock<T>>], b: T) {
        for c in ts {
            *c.wl() = b;
        }
    }

    #[test]
    fn test_coprocessor_host() {
        // bypass_pre, bypass_post, called_pre, called_post
        let (bpr1, bpt1, cpr1, cpt1) = (share(false), share(false), share(0), share(0));
        let observer1 = TestCoprocessor::new(bpr1.clone(),
                                             bpt1.clone(),
                                             cpr1.clone(),
                                             cpt1.clone());
        let mut host = CoprocessorHost::default();
        host.registry.register_observer(3, Box::new(observer1));
        let path = TempDir::new("test-raftstore").unwrap();
        let ps = new_peer_storage(&path);
        let mut admin_req = RaftCmdRequest::new();
        admin_req.set_admin_request(AdminRequest::new());
        let mut query_req = RaftCmdRequest::new();
        query_req.set_requests(RepeatedField::from_vec(vec![Request::new()]));
        let mut admin_resp = RaftCmdResponse::new();
        admin_resp.set_admin_response(AdminResponse::new());
        let mut query_resp = RaftCmdResponse::new();
        query_resp.set_responses(RepeatedField::from_vec(vec![Response::new()]));

        assert_eq!(*cpr1.rl(), 0);
        host.pre_propose(&ps, &mut admin_req);
        assert_eq!(*cpr1.rl(), 1);

        assert_eq!(*cpt1.rl(), 0);
        host.post_apply(&ps, &admin_req, &mut admin_resp);
        assert_eq!(*cpt1.rl(), 1);

        // reset
        set_all(&[&cpt1, &cpr1], 0);

        let (bpr2, bpt2, cpr2, cpt2) = (share(false), share(false), share(0), share(0));
        let observer2 = TestCoprocessor::new(bpr2.clone(),
                                             bpt2.clone(),
                                             cpr2.clone(),
                                             cpt2.clone());
        host.registry.register_observer(2, Box::new(observer2));

        set_all(&[&bpr2, &bpt2], true);

        assert_all(&[&cpr1, &cpt1, &cpr2, &cpt2], &[0, 0, 0, 0]);

        host.pre_propose(&ps, &mut query_req);
        host.post_apply(&ps, &query_req, &mut query_resp);

        assert_all(&[&cpr1, &cpt1, &cpr2, &cpt2], &[0, 0, 2, 2]);

        set_all(&[&bpr2, &bpt2], false);
        set_all(&[&cpr2, &cpt2], 0);

        assert_all(&[&cpr1, &cpt1, &cpr2, &cpt2], &[0, 0, 0, 0]);

        host.pre_propose(&ps, &mut admin_req);
        host.post_apply(&ps, &admin_req, &mut admin_resp);

        assert_all(&[&cpr1, &cpt1, &cpr2, &cpt2], &[1, 1, 1, 1]);
    }
}
