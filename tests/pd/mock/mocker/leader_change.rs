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

use std::sync::Mutex;
use std::time::{Duration, Instant};

use protobuf::RepeatedField;

use grpc::error::GrpcError;

use kvproto::pdpb::*;

use super::Mocker;
use super::Result;

pub const LEADER_INTERVAL_SEC: u64 = 2;

#[derive(Debug)]
struct Roulette {
    ts: Instant,
    idx: usize,
}

#[derive(Debug)]
pub struct LeaderChange {
    resps: Vec<GetMembersResponse>,
    r: Mutex<Roulette>,
}

impl LeaderChange {
    pub fn new(eps: Vec<String>) -> LeaderChange {
        let mut members = Vec::with_capacity(eps.len());
        for (i, ep) in (&eps).into_iter().enumerate() {
            let mut m = Member::new();
            m.set_name(format!("pd{}", i));
            m.set_member_id(100 + i as u64);
            m.set_client_urls(RepeatedField::from_vec(vec![ep.to_owned()]));
            m.set_peer_urls(RepeatedField::from_vec(vec![ep.to_owned()]));
            members.push(m);
        }

        // A dead PD
        let mut m = Member::new();
        m.set_member_id(DEAD_ID);
        m.set_name(DEAD_NAME.to_owned());
        m.set_client_urls(RepeatedField::from_vec(vec![DEAD_URL.to_owned()]));
        m.set_peer_urls(RepeatedField::from_vec(vec![DEAD_URL.to_owned()]));
        members.push(m);

        let mut header = ResponseHeader::new();
        header.set_cluster_id(1);

        let mut resps = Vec::with_capacity(eps.len());
        for (i, _) in (&eps).into_iter().enumerate() {
            let mut resp = GetMembersResponse::new();
            resp.set_header(header.clone());
            resp.set_members(RepeatedField::from_vec(members.clone()));
            resp.set_leader(members[i].clone());
            resps.push(resp);
        }

        info!("[LeaerChange] resps {:?}", resps);

        LeaderChange {
            resps: resps,
            r: Mutex::new(Roulette {
                ts: Instant::now(),
                idx: 0,
            }),
        }
    }

    pub fn get_leader_interval() -> Duration {
        Duration::from_secs(LEADER_INTERVAL_SEC)
    }
}

const DEAD_ID: u64 = 1000;
const DEAD_NAME: &'static str = "walking_dead";
const DEAD_URL: &'static str = "http://127.0.0.1:65534";

impl Mocker for LeaderChange {
    fn get_member(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        let mut r = self.r.lock().unwrap();
        let now = Instant::now();
        if now.duration_since(r.ts) > LeaderChange::get_leader_interval() {
            r.idx += 1;
            r.ts = now;
            return Some(Err(GrpcError::Other("not leader")));
        }

        info!("[LeaderChange] GetMembers: {:?}",
              self.resps[r.idx % self.resps.len()]);
        Some(Ok(self.resps[r.idx % self.resps.len()].clone()))
    }

    fn get_region_by_id(&self, _: &GetRegionByIDRequest) -> Option<Result<GetRegionResponse>> {
        let mut r = self.r.lock().unwrap();
        let now = Instant::now();
        if now.duration_since(r.ts) > LeaderChange::get_leader_interval() {
            r.idx += 1;
            r.ts = now;
            debug!("[LeaderChange] change leader to {:?}",
                   self.resps[r.idx % self.resps.len()].get_leader());
        }

        Some(Err(GrpcError::Other("not leader")))
    }
}
