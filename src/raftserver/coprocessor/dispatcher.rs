use super::{RegionObserver, RequestContext, ResponseContext};

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
    pub fn pre_propose(&mut self, ctx: &mut RequestContext) {
        for entry in &mut self.registry.observers {
            entry.observer.pre_propose(ctx);
            if ctx.bypass {
                break;
            }
        }
    }

    /// call all apply hook until bypass is set to true.
    pub fn post_apply(&mut self, ctx: &mut ResponseContext) {
        for entry in &mut self.registry.observers {
            entry.observer.post_apply(ctx);
            if ctx.bypass {
                break;
            }
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
        fn pre_propose(&mut self, ctx: &mut RequestContext) {
            *self.called_pre.write().unwrap() = true;
            ctx.bypass = *self.bypass_pre.read().unwrap();
        }

        fn post_apply(&mut self, ctx: &mut ResponseContext) {
            *self.called_post.write().unwrap() = true;
            ctx.bypass = *self.bypass_post.read().unwrap();
        }
    }

    fn new_peer_storage() -> PeerStorage {
        let path = TempDir::new("test-raftserver").unwrap();
        let engine = new_engine(path.path().to_str().unwrap()).unwrap();
        PeerStorage::new(Arc::new(engine), &Region::new()).unwrap()
    }

    fn new_request_context(ps: &PeerStorage) -> RequestContext {
        RequestContext {
            snap: RegionSnapshot::new(ps),
            req: RaftCommandRequest::new(),
            bypass: false,
        }
    }

    fn new_response_context(ps: &PeerStorage) -> ResponseContext {
        ResponseContext {
            snap: RegionSnapshot::new(ps),
            req: RaftCommandRequest::new(),
            resp: RaftCommandResponse::new(),
            bypass: false,
        }
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
        let mut req = new_request_context(&ps);
        let mut res = new_response_context(&ps);

        assert!(!*cpr1.read().unwrap());
        host.pre_propose(&mut req);
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
