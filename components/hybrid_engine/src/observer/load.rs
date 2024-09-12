// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{CacheRegion, KvEngine, RangeCacheEngineExt, RegionEvent};
use raft::StateRole;
use raftstore::coprocessor::{
    BoxRoleObserver, Coprocessor, CoprocessorHost, ObserverContext, RoleObserver,
};

/// LoadObserver observes the role changes of regions and tries to load regions
/// cache they became leaders.
/// Currently, it is only used by the manual load.
#[derive(Clone)]
pub struct LoadObserver {
    cache_engine: Arc<dyn RangeCacheEngineExt + Send + Sync>,
}

impl LoadObserver {
    pub fn new(cache_engine: Arc<dyn RangeCacheEngineExt + Send + Sync>) -> Self {
        LoadObserver { cache_engine }
    }

    pub fn register_to(&self, coprocessor_host: &mut CoprocessorHost<impl KvEngine>) {
        // This observer does not need high priority, use the default 100.
        let priority = 100;
        // Try to load regions cache they became leaders.
        coprocessor_host
            .registry
            .register_role_observer(priority, BoxRoleObserver::new(self.clone()));
    }
}

impl Coprocessor for LoadObserver {}

impl RoleObserver for LoadObserver {
    fn on_role_change(
        &self,
        ctx: &mut ObserverContext<'_>,
        change: &raftstore::coprocessor::RoleChange,
    ) {
        if let StateRole::Leader = change.state {
            let cache_region = CacheRegion::from_region(ctx.region());
            self.cache_engine.on_region_event(RegionEvent::Load {
                region: cache_region,
            });
        }
    }
}
