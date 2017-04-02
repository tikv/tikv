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

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use kvproto::metapb::{Store, Region};
use kvproto::pdpb::*;

use protobuf::{RepeatedField, Message};

use super::Mocker;
use super::Result;
use super::DEFAULT_CLUSTER_ID as CLUSTER_ID;

#[derive(Debug)]
pub struct Service {
    id_allocator: AtomicUsize,
    member_resp: GetMembersResponse,
    storage: Mutex<HashMap<String, Vec<u8>>>,
}

impl Service {
    pub fn new(eps: Vec<String>) -> Service {
        Service {
            member_resp: Self::get_members_response(eps),
            id_allocator: AtomicUsize::new(1), // start from 1.
            storage: Mutex::new(HashMap::new()),
        }
    }

    fn header() -> ResponseHeader {
        let mut header = ResponseHeader::new();
        header.set_cluster_id(CLUSTER_ID);
        header
    }

    fn get_members_response(eps: Vec<String>) -> GetMembersResponse {
        let mut members = Vec::with_capacity(eps.len());
        for (i, ep) in (&eps).into_iter().enumerate() {
            let mut m = Member::new();
            m.set_name(format!("pd{}", i));
            m.set_member_id(100 + i as u64);
            m.set_client_urls(RepeatedField::from_vec(vec![ep.to_owned()]));
            m.set_peer_urls(RepeatedField::from_vec(vec![ep.to_owned()]));
            members.push(m);
        }

        let mut member_resp = GetMembersResponse::new();
        member_resp.set_members(RepeatedField::from_vec(members.clone()));
        member_resp.set_leader(members.pop().unwrap());
        member_resp.set_header(Self::header());

        info!("[Service] member_resp {:?}", member_resp);
        member_resp
    }
}

// TODO: Check cluster ID.
// TODO: Support more rpc.
impl Mocker for Service {
    fn GetMembers(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        Some(Ok(self.member_resp.clone()))
    }

    fn Bootstrap(&self, req: &BootstrapRequest) -> Option<Result<BootstrapResponse>> {
        let store = req.get_store();
        let store_path = make_region_key(store.get_id());
        let store_value = store.write_to_bytes().unwrap();

        let region = req.get_region();
        let region_path = make_region_key(region.get_id());
        let region_value = region.write_to_bytes().unwrap();

        let mut resp = BootstrapResponse::new();
        let mut header = Service::header();

        let mut storage = self.storage.lock().unwrap();
        // try boot
        let boot_key = CLUSTER_ROOT_PATH.to_owned();
        if storage.contains_key(&boot_key) {
            let mut err = Error::new();
            err.field_type = ErrorType::UNKNOWN;
            err.set_message("cluster is already bootstrapped".to_owned());
            header.set_error(err);
            resp.set_header(header);
            return Some(Ok(resp));
        }

        storage.insert(boot_key, vec![1]);
        storage.insert(region_path, region_value);
        storage.insert(store_path, store_value);
        Some(Ok(resp))
    }

    fn IsBootstrapped(&self, _: &IsBootstrappedRequest) -> Option<Result<IsBootstrappedResponse>> {
        let mut resp = IsBootstrappedResponse::new();
        let header = Service::header();
        resp.set_header(header);

        let storage = self.storage.lock().unwrap();
        resp.set_bootstrapped(storage.len() != 0);
        Some(Ok(resp))
    }

    fn AllocID(&self, _: &AllocIDRequest) -> Option<Result<AllocIDResponse>> {
        let mut resp = AllocIDResponse::new();
        resp.set_header(Service::header());

        let id = self.id_allocator.fetch_add(1, Ordering::SeqCst);
        resp.set_id(id as u64);
        Some(Ok(resp))
    }

    // TODO: not bootstrapped error.
    fn GetStore(&self, req: &GetStoreRequest) -> Option<Result<GetStoreResponse>> {
        let mut resp = GetStoreResponse::new();
        let mut store = Store::new();
        let store_path = make_region_key(req.get_store_id());

        let storage = self.storage.lock().unwrap();
        match storage.get(&store_path) {
            Some(value) => {
                store.merge_from_bytes(value).unwrap();
                resp.set_header(Service::header());
                resp.set_store(store);
                Some(Ok(resp))
            }
            None => {
                let mut header = Service::header();
                let mut err = Error::new();
                err.field_type = ErrorType::UNKNOWN;
                err.set_message(format!("store not found {}", req.get_store_id()));
                header.set_error(err);
                resp.set_header(header);
                Some(Ok(resp))
            }
        }
    }

    fn GetRegionByID(&self, req: &GetRegionByIDRequest) -> Option<Result<GetRegionResponse>> {
        let mut resp = GetRegionResponse::new();
        let mut region = Region::new();
        let region_path = make_region_key(req.region_id);

        let storage = self.storage.lock().unwrap();
        match storage.get(&region_path) {
            Some(value) => {
                region.merge_from_bytes(value).unwrap();
                resp.set_header(Service::header());
                resp.set_region(region);
                Some(Ok(resp))
            }
            None => {
                let mut header = Service::header();
                let mut err = Error::new();
                err.field_type = ErrorType::UNKNOWN;
                err.set_message(format!("region not found {}", req.region_id));
                header.set_error(err);
                resp.set_header(header);
                Some(Ok(resp))
            }
        }
    }
}

const CLUSTER_ROOT_PATH: &'static str = "raft";

fn make_region_key(region_id: u64) -> String {
    return format!("{}/r/{}", CLUSTER_ROOT_PATH, region_id);
}
