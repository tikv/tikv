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

// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::test_raft::*;
use kvproto::eraftpb::*;

// test_msg_app_flow_control_full ensures:
// 1. msgApp can fill the sending window until full
// 2. when the window is full, no more msgApp can be sent.
#[test]
fn test_msg_app_flow_control_full() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, new_storage());
    r.become_candidate();
    r.become_leader();

    // force the progress to be in replicate state
    r.prs.get_mut(&2).unwrap().become_replicate();
    // fill in the inflights window
    for i in 0..r.max_inflight {
        r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");
        let ms = r.read_messages();
        if ms.len() != 1 {
            panic!("#{}: ms count = {}, want 1", i, ms.len());
        }
    }

    // ensure 1
    assert!(r.prs[&2].ins.full());

    // ensure 2
    for i in 0..10 {
        r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");
        let ms = r.read_messages();
        if !ms.is_empty() {
            panic!("#{}: ms count = {}, want 0", i, ms.len());
        }
    }
}

// test_msg_app_flow_control_move_forward ensures msgAppResp can move
// forward the sending window correctly:
// 1. valid msgAppResp.index moves the windows to pass all smaller or equal index.
// 2. out-of-dated msgAppResp has no effect on the sliding window.
#[test]
fn test_msg_app_flow_control_move_forward() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, new_storage());
    r.become_candidate();
    r.become_leader();

    // force the progress to be in replicate state
    r.prs.get_mut(&2).unwrap().become_replicate();
    // fill in the inflights window
    for _ in 0..r.max_inflight {
        r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");
        r.read_messages();
    }

    // 1 is noop, 2 is the first proposal we just sent.
    // so we start with 2.
    for tt in 2..r.max_inflight {
        // move forward the window
        let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
        m.set_index(tt as u64);
        r.step(m).expect("");
        r.read_messages();

        // fill in the inflights window again
        r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");
        let ms = r.read_messages();
        if ms.len() != 1 {
            panic!("#{}: ms count = {}, want 1", tt, ms.len());
        }

        // ensure 1
        assert!(r.prs[&2].ins.full());

        // ensure 2
        for i in 0..tt {
            let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
            m.set_index(i as u64);
            r.step(m).expect("");
            if !r.prs[&2].ins.full() {
                panic!("#{}: inflights.full = {}, want true",
                       tt,
                       r.prs[&2].ins.full());
            }
        }
    }
}

// test_msg_app_flow_control_recv_heartbeat ensures a heartbeat response
// frees one slot if the window is full.
#[test]
fn test_msg_app_flow_control_recv_heartbeat() {
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, new_storage());
    r.become_candidate();
    r.become_leader();

    // force the progress to be in replicate state
    r.prs.get_mut(&2).unwrap().become_replicate();
    // fill in the inflights window
    for _ in 0..r.max_inflight {
        r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");
        r.read_messages();
    }

    for tt in 1..5 {
        if !r.prs[&2].ins.full() {
            panic!("#{}: inflights.full = {}, want true",
                   tt,
                   r.prs[&2].ins.full());
        }

        // recv tt MsgHeartbeatResp and expect one free slot
        for i in 0..tt {
            r.step(new_message(2, 1, MessageType::MsgHeartbeatResponse, 0)).expect("");
            r.read_messages();
            if r.prs[&2].ins.full() {
                panic!("#{}.{}: inflights.full = {}, want false",
                       tt,
                       i,
                       r.prs[&2].ins.full());
            }
        }

        // one slot
        r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");
        let ms = r.read_messages();
        if ms.len() != 1 {
            panic!("#{}: free slot = 0, want 1", tt);
        }

        // and just one slot
        for i in 0..10 {
            r.step(new_message(1, 1, MessageType::MsgPropose, 1)).expect("");
            let ms1 = r.read_messages();
            if !ms1.is_empty() {
                panic!("#{}.{}, ms1 should be empty.", tt, i);
            }
        }

        // clear all pending messages
        r.step(new_message(2, 1, MessageType::MsgHeartbeatResponse, 0)).expect("");
        r.read_messages();
    }
}
