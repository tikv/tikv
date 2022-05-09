// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};

use collections::HashMap;
use fail::fail_point;
use kvproto::{
    metapb::{Peer, Region, Store, StoreState},
    pdpb::*,
};

use super::*;

#[derive(Debug)]
pub struct Service {
    id_allocator: AtomicUsize,
    members_resp: Mutex<Option<GetMembersResponse>>,
    is_bootstrapped: AtomicBool,
    stores: Mutex<HashMap<u64, Store>>,
    regions: Mutex<HashMap<u64, Region>>,
    leaders: Mutex<HashMap<u64, Peer>>,
    feature_gate: Mutex<String>,
}

impl Service {
    pub fn new() -> Service {
        Service {
            members_resp: Mutex::new(None),
            id_allocator: AtomicUsize::new(1), // start from 1.
            is_bootstrapped: AtomicBool::new(false),
            stores: Mutex::new(HashMap::default()),
            regions: Mutex::new(HashMap::default()),
            leaders: Mutex::new(HashMap::default()),
            feature_gate: Mutex::new(String::default()),
        }
    }

    pub fn header() -> ResponseHeader {
        let mut header = ResponseHeader::default();
        header.set_cluster_id(DEFAULT_CLUSTER_ID);
        header
    }

    /// Add an arbitrary store.
    pub fn add_store(&self, store: Store) {
        let store_id = store.get_id();
        self.stores.lock().unwrap().insert(store_id, store);
    }

    pub fn set_cluster_version(&self, version: String) {
        *self.feature_gate.lock().unwrap() = version;
    }
}

impl Default for Service {
    fn default() -> Self {
        Self::new()
    }
}

fn make_members_response(eps: Vec<String>) -> GetMembersResponse {
    let mut members = Vec::with_capacity(eps.len());
    for (i, ep) in eps.iter().enumerate() {
        let mut m = Member::default();
        m.set_name(format!("pd{}", i));
        m.set_member_id(100 + i as u64);
        m.set_client_urls(vec![ep.to_owned()].into());
        m.set_peer_urls(vec![ep.to_owned()].into());
        members.push(m);
    }

    let mut members_resp = GetMembersResponse::default();
    members_resp.set_members(members.clone().into());
    members_resp.set_leader(members.pop().unwrap());
    members_resp.set_header(Service::header());

    members_resp
}

// TODO: Check cluster ID.
// TODO: Support more rpc.
impl PdMocker for Service {
    fn get_members(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        Some(Ok(self.members_resp.lock().unwrap().clone().unwrap()))
    }

    fn bootstrap(&self, req: &BootstrapRequest) -> Option<Result<BootstrapResponse>> {
        let store = req.get_store();
        let region = req.get_region();

        let mut resp = BootstrapResponse::default();
        let mut header = Service::header();

        if self.is_bootstrapped.load(Ordering::SeqCst) {
            let mut err = Error::default();
            err.set_type(ErrorType::Unknown);
            err.set_message("cluster is already bootstrapped".to_owned());
            header.set_error(err);
            resp.set_header(header);
            return Some(Ok(resp));
        }

        self.is_bootstrapped.store(true, Ordering::SeqCst);
        self.stores
            .lock()
            .unwrap()
            .insert(store.get_id(), store.clone());
        self.regions
            .lock()
            .unwrap()
            .insert(region.get_id(), region.clone());
        Some(Ok(resp))
    }

    fn is_bootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        let mut resp = IsBootstrappedResponse::default();
        let header = Service::header();
        resp.set_header(header);
        resp.set_bootstrapped(self.is_bootstrapped.load(Ordering::SeqCst));
        Some(Ok(resp))
    }

    fn alloc_id(&self, _: &AllocIdRequest) -> Option<Result<AllocIdResponse>> {
        fail_point!("connect_leader", |_| None);
        let mut resp = AllocIdResponse::default();
        resp.set_header(Service::header());

        let id = self.id_allocator.fetch_add(1, Ordering::SeqCst);
        resp.set_id(id as u64);
        Some(Ok(resp))
    }

    // TODO: not bootstrapped error.
    fn get_store(&self, req: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        let mut resp = GetStoreResponse::default();
        let stores = self.stores.lock().unwrap();
        match stores.get(&req.get_store_id()) {
            Some(store) => {
                resp.set_header(Service::header());
                resp.set_store(store.clone());
                Some(Ok(resp))
            }
            None => {
                let mut header = Service::header();
                let mut err = Error::default();
                err.set_type(ErrorType::Unknown);
                err.set_message(format!("store not found {}", req.get_store_id()));
                header.set_error(err);
                resp.set_header(header);
                Some(Ok(resp))
            }
        }
    }

    fn get_all_stores(&self, req: &GetAllStoresRequest) -> Option<Result<GetAllStoresResponse>> {
        let mut resp = GetAllStoresResponse::default();
        resp.set_header(Service::header());
        let exclude_tombstone = req.get_exclude_tombstone_stores();
        let stores = self.stores.lock().unwrap();
        for store in stores.values() {
            if exclude_tombstone && store.get_state() == StoreState::Tombstone {
                continue;
            }
            resp.mut_stores().push(store.clone());
        }
        Some(Ok(resp))
    }

    fn get_region(&self, req: &GetRegionRequest) -> Option<Result<GetRegionResponse>> {
        let mut resp = GetRegionResponse::default();
        let key = req.get_region_key();
        let regions = self.regions.lock().unwrap();
        let leaders = self.leaders.lock().unwrap();

        for region in regions.values() {
            if key >= region.get_start_key()
                && (region.get_end_key().is_empty() || key < region.get_end_key())
            {
                resp.set_header(Service::header());
                resp.set_region(region.clone());
                if let Some(leader) = leaders.get(&region.get_id()) {
                    resp.set_leader(leader.clone());
                }
                return Some(Ok(resp));
            }
        }

        let mut header = Service::header();
        let mut err = Error::default();
        err.set_type(ErrorType::Unknown);
        err.set_message(format!("region not found {:?}", key));
        header.set_error(err);
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn get_region_by_id(&self, req: &GetRegionByIdRequest) -> Option<Result<GetRegionResponse>> {
        let mut resp = GetRegionResponse::default();
        let regions = self.regions.lock().unwrap();
        let leaders = self.leaders.lock().unwrap();

        match regions.get(&req.get_region_id()) {
            Some(region) => {
                resp.set_header(Service::header());
                resp.set_region(region.clone());
                if let Some(leader) = leaders.get(&region.get_id()) {
                    resp.set_leader(leader.clone());
                }
                Some(Ok(resp))
            }
            None => {
                let mut header = Service::header();
                let mut err = Error::default();
                err.set_type(ErrorType::Unknown);
                err.set_message(format!("region not found {}", req.region_id));
                header.set_error(err);
                resp.set_header(header);
                Some(Ok(resp))
            }
        }
    }

    fn region_heartbeat(
        &self,
        req: &RegionHeartbeatRequest,
    ) -> Option<Result<RegionHeartbeatResponse>> {
        let region_id = req.get_region().get_id();
        self.regions
            .lock()
            .unwrap()
            .insert(region_id, req.get_region().clone());
        self.leaders
            .lock()
            .unwrap()
            .insert(region_id, req.get_leader().clone());

        let mut resp = RegionHeartbeatResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn store_heartbeat(&self, _: &StoreHeartbeatRequest) -> Option<Result<StoreHeartbeatResponse>> {
        let mut resp = StoreHeartbeatResponse::default();
        let header = Service::header();
        resp.set_header(header);
        resp.set_cluster_version(self.feature_gate.lock().unwrap().to_owned());
        Some(Ok(resp))
    }

    fn ask_split(&self, _: &AskSplitRequest) -> Option<Result<AskSplitResponse>> {
        let mut resp = AskSplitResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn ask_batch_split(&self, _: &AskBatchSplitRequest) -> Option<Result<AskBatchSplitResponse>> {
        let mut resp = AskBatchSplitResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn report_batch_split(
        &self,
        _: &ReportBatchSplitRequest,
    ) -> Option<Result<ReportBatchSplitResponse>> {
        let mut resp = ReportBatchSplitResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn scatter_region(&self, _: &ScatterRegionRequest) -> Option<Result<ScatterRegionResponse>> {
        let mut resp = ScatterRegionResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn set_endpoints(&self, eps: Vec<String>) {
        let members_resp = make_members_response(eps);
        info!("[Service] members_resp {:?}", members_resp);
        let mut resp = self.members_resp.lock().unwrap();
        *resp = Some(members_resp);
    }

    fn get_operator(&self, _: &GetOperatorRequest) -> Option<Result<GetOperatorResponse>> {
        let mut resp = GetOperatorResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn put_store(&self, _: &PutStoreRequest) -> Option<Result<PutStoreResponse>> {
        let mut resp = PutStoreResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn get_cluster_config(
        &self,
        _: &GetClusterConfigRequest,
    ) -> Option<Result<GetClusterConfigResponse>> {
        let mut resp = GetClusterConfigResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }

    fn get_gc_safe_point(
        &self,
        _: &GetGcSafePointRequest,
    ) -> Option<Result<GetGcSafePointResponse>> {
        let mut resp = GetGcSafePointResponse::default();
        let header = Service::header();
        resp.set_header(header);
        Some(Ok(resp))
    }
}
