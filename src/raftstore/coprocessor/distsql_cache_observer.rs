// Copyright 2017 PingCAP, Inc.
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

use kvproto::raft_cmdpb::{AdminRequest, AdminResponse, Request, Response};
use protobuf::RepeatedField;
use std::sync::Arc;

use super::{AdminObserver, Coprocessor, ObserverContext, QueryObserver};
use coprocessor::cache::SQLCache;

pub struct DistSQLObserver {
    cache: Arc<SQLCache>,
}

impl DistSQLObserver {
    pub fn new(cache: Arc<SQLCache>) -> Box<DistSQLObserver> {
        box DistSQLObserver { cache }
    }
    fn disable_cache(&self, ctx: &mut ObserverContext) {
        let region_id = ctx.region().get_id();
        self.cache.lock().disable_region_cache(region_id);
    }

    fn evict_region(&self, ctx: &mut ObserverContext) {
        let region_id = ctx.region().get_id();
        self.cache.lock().evict_region_and_enable(region_id);
    }
}

impl Coprocessor for DistSQLObserver {}

impl QueryObserver for DistSQLObserver {
    fn pre_apply_query(&self, ctx: &mut ObserverContext, _: &[Request]) {
        debug!("Disable cache at pre_apply_query");
        self.disable_cache(ctx);
    }

    fn post_apply_query(&self, ctx: &mut ObserverContext, _: &mut RepeatedField<Response>) {
        debug!("Evict region and enable cache at post_apply_query");
        self.evict_region(ctx);
    }
}

impl AdminObserver for DistSQLObserver {
    fn pre_apply_admin(&self, ctx: &mut ObserverContext, _: &AdminRequest) {
        debug!("Disable cache at pre_apply_admin");
        self.disable_cache(ctx);
    }

    fn post_apply_admin(&self, ctx: &mut ObserverContext, _: &mut AdminResponse) {
        debug!("Evict region and enable cache at post_apply_admin");
        self.evict_region(ctx);
    }
}
