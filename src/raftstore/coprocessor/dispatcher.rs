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

use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::metapb::Region;

struct ObserverEntry {
    priority: u32,
    observer: Box<RegionObserver + Send + Sync>,
}

/// Registry contains all registered coprocessors.
#[derive(Default)]
pub struct Registry {
    observers: Vec<ObserverEntry>, // TODO: add endpoint
}

impl Registry {
    /// register an Observer to dispatcher.
    pub fn register_observer(&mut self, priority: u32, ro: Box<RegionObserver + Send + Sync>) {
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
    pub fn pre_propose(&self, region: &Region, req: &mut RaftCmdRequest) -> Result<()> {
        let ctx = ObserverContext::new(region);
        if req.has_admin_request() {
            self.execute_pre_hook(ctx,
                                  req.mut_admin_request(),
                                  |o, ctx, q| o.pre_admin(ctx, q))
        } else {
            self.execute_pre_hook(ctx, req, |o, ctx, q| o.pre_query(ctx, q))
        }
    }

    fn execute_pre_hook<Q, H>(&self,
                              mut ctx: ObserverContext,
                              req: &mut Q,
                              mut hook: H)
                              -> Result<()>
        where H: FnMut(&RegionObserver, &mut ObserverContext, &mut Q) -> Result<()>
    {
        for entry in &self.registry.observers {
            try!(hook(entry.observer.as_ref(), &mut ctx, req));
            if ctx.bypass {
                break;
            }
        }
        Ok(())
    }

    /// Call all pre apply hook until bypass is set to true.
    pub fn pre_apply(&self, region: &Region, req: &mut RaftCmdRequest) {
        let mut ctx = ObserverContext::new(region);
        if !req.has_admin_request() {
            for entry in &self.registry.observers {
                entry.observer.pre_apply_query(&mut ctx, req);
                if ctx.bypass {
                    break;
                }
            }
        }
    }

    pub fn shutdown(&self) {
        for entry in &self.registry.observers {
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
    use std::sync::*;
    use std::sync::atomic::*;
    use protobuf::RepeatedField;

    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::{AdminRequest, Request, RaftCmdRequest};

    struct TestCoprocessor {
        bypass: Arc<AtomicBool>,
        called: Arc<AtomicUsize>,
        return_err: Arc<AtomicBool>,
    }

    impl TestCoprocessor {
        fn new(bypass: Arc<AtomicBool>,
               called: Arc<AtomicUsize>,
               return_err: Arc<AtomicBool>)
               -> TestCoprocessor {
            TestCoprocessor {
                bypass: bypass,
                called: called,
                return_err: return_err,
            }
        }
    }

    impl Coprocessor for TestCoprocessor {}

    impl RegionObserver for TestCoprocessor {
        fn pre_admin(&self, ctx: &mut ObserverContext, _: &mut AdminRequest) -> Result<()> {
            self.called.fetch_add(1, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_query(&self,
                     ctx: &mut ObserverContext,
                     _: &mut RaftCmdRequest)
                     -> Result<()> {
            self.called.fetch_add(2, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }


        fn pre_apply_query(&self, ctx: &mut ObserverContext, _: &mut RepeatedField<Request>) {
            self.called.fetch_add(3, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    fn share_bool() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::default())
    }

    fn share_usize() -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::default())
    }

    macro_rules! assert_all {
        ($target:expr, $expect:expr) => ({
            for (c, e) in ($target).iter().zip($expect) {
                assert_eq!(c.load(Ordering::SeqCst), *e);
            }
        })
    }

    macro_rules! set_all {
        ($target:expr, $v:expr) => ({
            for v in $target {
                v.store($v, Ordering::SeqCst);
            }
        })
    }

    #[test]
    fn test_coprocessor_host() {
        let (bypass1, called1, r1) = (share_bool(), share_usize(), share_bool());
        let observer1 = TestCoprocessor::new(bypass1.clone(), called1.clone(), r1.clone());
        let mut host = CoprocessorHost::default();
        host.registry.register_observer(3, Box::new(observer1));
        let region = Region::new();
        let mut admin_req = RaftCmdRequest::new();
        admin_req.set_admin_request(AdminRequest::new());
        let mut query_req = RaftCmdRequest::new();
        query_req.set_requests(RepeatedField::from_vec(vec![Request::new()]));

        assert_eq!(called1.load(Ordering::SeqCst), 0);
        assert!(host.pre_propose(&region, &mut admin_req).is_ok());
        assert_eq!(called1.load(Ordering::SeqCst), 1);

        // pre_apply_request is ignored when handling admin request.
        host.pre_apply(&region, &mut admin_req);
        assert_eq!(called1.load(Ordering::SeqCst), 1);

        // reset
        set_all!(&[&called1], 0);

        let (bypass2, called2, r2) = (share_bool(), share_usize(), share_bool());
        let observer2 = TestCoprocessor::new(bypass2.clone(), called2.clone(), r2.clone());
        host.registry.register_observer(2, Box::new(observer2));

        set_all!(&[&bypass2, &bypass2], true);

        assert_all!(&[&called1, &called2], &[0, 0]);

        assert!(host.pre_propose(&region, &mut query_req).is_ok());

        assert_all!(&[&called1, &called2], &[0, 2]);

        host.pre_apply(&region, &mut query_req);
        assert_all!(&[&called1, &called2], &[0, 5]);

        set_all!(&[&bypass2], false);
        set_all!(&[&called2], 0);

        assert_all!(&[&called1, &called2], &[0, 0]);

        assert!(host.pre_propose(&region, &mut admin_req).is_ok());

        assert_all!(&[&called1, &called2], &[1, 1]);

        set_all!(&[&bypass2], false);
        set_all!(&[&called1, &called2], 0);
        assert_all!(&[&called1, &called2], &[0, 0]);

        // when return error, following coprocessor should not be run.
        set_all!(&[&r2], true);
        assert!(host.pre_propose(&region, &mut admin_req).is_err());
        assert_all!(&[&called1, &called2], &[0, 1]);
    }
}
