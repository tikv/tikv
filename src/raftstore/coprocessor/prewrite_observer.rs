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

use super::{Coprocessor, RegionObserver, ObserverContext};
use kvproto::raft_cmdpb::{RaftCmdRequest, CmdType, Request, PutRequest};
use storage::types::Key;
use storage::CF_LOCK;

use protobuf::RepeatedField;

pub struct PrewriteObserver;

impl Coprocessor for PrewriteObserver {}

impl RegionObserver for PrewriteObserver {
    fn pre_apply_query(&self, ctx: &mut ObserverContext, cmd_req: &mut RaftCmdRequest) {
        let mut new_reqs = Vec::<Request>::with_capacity(cmd_req.get_requests().len());
        for req in cmd_req.get_requests() {
            match req.get_cmd_type() {
                CmdType::Prewrite => {
                    let prewrite = req.get_prewrite();
                    let key = prewrite.get_key().to_vec();
                    let lock_key = Key::from_encoded(key.clone())
                        .truncate_ts()
                        .unwrap()
                        .encoded()
                        .to_owned();

                    let mut put = PutRequest::new();
                    put.set_cf(CF_LOCK.to_owned());
                    put.set_key(lock_key);
                    put.set_value(prewrite.get_lock().to_vec());
                    let mut new_req = Request::new();
                    new_req.set_cmd_type(CmdType::Put);
                    new_req.set_put(put);
                    new_reqs.push(req.to_owned());

                    let mut put = PutRequest::new();
                    put.set_key(key);
                    put.set_value(prewrite.get_value().to_vec());
                    let mut new_req = Request::new();
                    new_req.set_cmd_type(CmdType::Put);
                    new_req.set_put(put);
                    new_reqs.push(new_req.to_owned());
                }
                _ => {
                    new_reqs.push(req.to_owned());
                }
            }
        }
        cmd_req.set_requests(RepeatedField::from_vec(new_reqs));
        ctx.bypass = true;
    }
}
