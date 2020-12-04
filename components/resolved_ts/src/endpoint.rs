// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use raftstore::store::fsm::ObserveID;
use raftstore::store::util::RemoteLease;

use crate::resolver::Resolver;
use crate::scanner::{ScanMode, ScanTask, ScannerPool};

enum ResolverStatus {
    Pending(u64),
    Ready,
}

struct ObserveRegion {
    region_id: u64,
    observe_id: ObserveID,
    lease: Option<RemoteLease>,
    resolver: Resolver,
    resolver_status: ResolverStatus,
}

pub struct ResolvedTsEndpoint<T, E> {
    regions: HashMap<u64, ObserveRegion>,
    scanner_pool: Arc<ScannerPool<T, E>>,
    _phantom: PhantomData<(T, E)>,
}

impl<T, E> ResolvedTsEndpoint<T, E> {
    fn register_region() {}

    fn deregister_region() {}
}
