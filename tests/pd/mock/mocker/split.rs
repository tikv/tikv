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

use protobuf::RepeatedField;

use kvproto::pdpb::{Member, GetMembersRequest, GetMembersResponse, ResponseHeader};

use super::*;

#[derive(Debug)]
struct Inner {
    resps: Vec<GetMembersResponse>,
    idx: usize,
}

#[derive(Debug)]
pub struct Split {
    inner: Mutex<Option<Inner>>,
}

impl Split {
    pub fn new() -> Split {
        Split { inner: Mutex::new(None) }
    }
}

impl PdMocker for Split {
    fn get_members(&self, _: &GetMembersRequest) -> Option<Result<GetMembersResponse>> {
        let mut holder = self.inner.lock().unwrap();
        let mut inner = holder.as_mut().unwrap();
        inner.idx += 1;
        info!("[Split] get_member: {:?}",
              inner.resps[inner.idx % inner.resps.len()]);
        Some(Ok(inner.resps[inner.idx % inner.resps.len()].clone()))
    }

    fn set_endpoints(&self, eps: Vec<String>) {
        let mut members = Vec::with_capacity(eps.len());
        for (i, ep) in (&eps).into_iter().enumerate() {
            let mut m = Member::new();
            m.set_name(format!("pd{}", i));
            m.set_member_id(100 + i as u64);
            m.set_client_urls(RepeatedField::from_vec(vec![ep.to_owned()]));
            m.set_peer_urls(RepeatedField::from_vec(vec![ep.to_owned()]));
            members.push(m);
        }

        let mut resps = Vec::with_capacity(eps.len());
        for i in 0..eps.len() {
            let mut resp = GetMembersResponse::new();
            let mut header = ResponseHeader::new();
            header.set_cluster_id(i as u64 + 1); // start from 1.
            resp.set_header(header.clone());
            resp.set_members(RepeatedField::from_vec(members.clone()));
            resp.set_leader(members[0].clone());
            resps.push(resp);
        }

        info!("[Split] resps {:?}", resps);

        let mut inner = self.inner.lock().unwrap();
        *inner = Some(Inner {
            resps: resps,
            idx: 0,
        })
    }
}
