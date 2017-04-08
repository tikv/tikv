// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{RegionObserver, ObserverContext, Result};

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
    pub fn pre_propose(&mut self, ps: &PeerStorage, req: &mut RaftCmdRequest) -> Result<()> {
        let ctx = ObserverContext::new(ps);
        if req.has_admin_request() {
            self.execute_pre_hook(ctx,
                                  req.mut_admin_request(),
                                  |o, ctx, q| o.pre_admin(ctx, q))
        } else {
            self.execute_pre_hook(ctx, req.mut_requests(), |o, ctx, q| o.pre_query(ctx, q))
        }
    }

    fn execute_pre_hook<Q, H>(&mut self,
                              mut ctx: ObserverContext,
                              req: &mut Q,
                              mut hook: H)
                              -> Result<()>
        where H: FnMut(&mut RegionObserver, &mut ObserverContext, &mut Q) -> Result<()>
    {
        for entry in &mut self.registry.observers {
            try!(hook(entry.observer.as_mut(), &mut ctx, req));
            if ctx.bypass {
                break;
            }
        }
        Ok(())
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
        for mut entry in &mut self.registry.observers.drain(..) {
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
    use raftstore::store::PeerStorage;
    use util::HandyRwLock;
    use util::worker;
    use util::rocksdb;
    use std::sync::*;
    use std::fmt::Debug;
    use protobuf::RepeatedField;
    use storage::ALL_CFS;

    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::{AdminRequest, Request, AdminResponse, Response, RaftCmdRequest,
                              RaftCmdResponse};

    struct TestCoprocessor {
        bypass_pre: Arc<RwLock<bool>>,
        bypass_post: Arc<RwLock<bool>>,
        called_pre: Arc<RwLock<u8>>,
        called_post: Arc<RwLock<u8>>,
        return_err: Arc<RwLock<bool>>,
    }

    impl TestCoprocessor {
        fn new(bypass_pre: Arc<RwLock<bool>>,
               bypass_post: Arc<RwLock<bool>>,
               called_pre: Arc<RwLock<u8>>,
               called_post: Arc<RwLock<u8>>,
               return_err: Arc<RwLock<bool>>)
               -> TestCoprocessor {
            TestCoprocessor {
                bypass_post: bypass_post,
                bypass_pre: bypass_pre,
                called_post: called_post,
                called_pre: called_pre,
                return_err: return_err,
            }
        }
    }

    impl Coprocessor for TestCoprocessor {
        fn start(&mut self) {}
        fn stop(&mut self) {}
    }

    impl RegionObserver for TestCoprocessor {
        fn pre_admin(&mut self, ctx: &mut ObserverContext, _: &mut AdminRequest) -> Result<()> {
            *self.called_pre.wl() += 1;
            ctx.bypass = *self.bypass_pre.rl();
            if *self.return_err.rl() {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_query(&mut self,
                     ctx: &mut ObserverContext,
                     _: &mut RepeatedField<Request>)
                     -> Result<()> {
            *self.called_pre.wl() += 2;
            ctx.bypass = *self.bypass_pre.rl();
            if *self.return_err.rl() {
                return Err(box_err!("error"));
            }
            Ok(())
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
        let engine = Arc::new(rocksdb::new_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap());
        PeerStorage::new(engine,
                         &Region::new(),
                         worker::dummy_scheduler(),
                         "".to_owned())
                .unwrap()
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
        let (bypass_pre1, bypass_post1, called_pre1, called_post1, r1) =
            (share(false), share(false), share(0), share(0), share(false));
        let observer1 = TestCoprocessor::new(bypass_pre1.clone(),
                                             bypass_post1.clone(),
                                             called_pre1.clone(),
                                             called_post1.clone(),
                                             r1.clone());
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

        assert_eq!(*called_pre1.rl(), 0);
        assert!(host.pre_propose(&ps, &mut admin_req).is_ok());
        assert_eq!(*called_pre1.rl(), 1);

        assert_eq!(*called_post1.rl(), 0);
        host.post_apply(&ps, &admin_req, &mut admin_resp);
        assert_eq!(*called_post1.rl(), 1);

        // reset
        set_all(&[&called_post1, &called_pre1], 0);

        let (bypass_pre2, bypass_post2, called_pre2, called_post2, r2) =
            (share(false), share(false), share(0), share(0), share(false));
        let observer2 = TestCoprocessor::new(bypass_pre2.clone(),
                                             bypass_post2.clone(),
                                             called_pre2.clone(),
                                             called_post2.clone(),
                                             r2.clone());
        host.registry.register_observer(2, Box::new(observer2));

        set_all(&[&bypass_pre2, &bypass_post2], true);

        assert_all(&[&called_pre1, &called_post1, &called_pre2, &called_post2],
                   &[0, 0, 0, 0]);

        assert!(host.pre_propose(&ps, &mut query_req).is_ok());
        host.post_apply(&ps, &query_req, &mut query_resp);

        assert_all(&[&called_pre1, &called_post1, &called_pre2, &called_post2],
                   &[0, 0, 2, 2]);

        set_all(&[&bypass_pre2, &bypass_post2], false);
        set_all(&[&called_pre2, &called_post2], 0);

        assert_all(&[&called_pre1, &called_post1, &called_pre2, &called_post2],
                   &[0, 0, 0, 0]);

        assert!(host.pre_propose(&ps, &mut admin_req).is_ok());
        host.post_apply(&ps, &admin_req, &mut admin_resp);

        assert_all(&[&called_pre1, &called_post1, &called_pre2, &called_post2],
                   &[1, 1, 1, 1]);

        set_all(&[&bypass_pre2, &bypass_post2], false);
        set_all(&[&called_pre1, &called_post1, &called_pre2, &called_post2],
                0);
        assert_all(&[&called_pre1, &called_post1, &called_pre2, &called_post2],
                   &[0, 0, 0, 0]);
        // when return error, following coprocessor should not be run.
        *r2.wl() = true;
        assert!(host.pre_propose(&ps, &mut admin_req).is_err());
        assert_all(&[&called_pre1, &called_post1, &called_pre2, &called_post2],
                   &[0, 0, 1, 0]);
    }
}
