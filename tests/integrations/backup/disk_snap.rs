// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, time::Duration};

use futures::executor::block_on;
use kvproto::raft_cmdpb::{CmdType, PutRequest, RaftCmdRequest, Request};
use raft::prelude::MessageType;
use raftstore::store::Callback;
use test_backup::disk_snap::{
    assert_failure, assert_failure_because, assert_success, must_wait_apply_success, Suite,
};
use test_raftstore::{must_contains_error, Direction, RegionPacketFilter, Simulator};
use test_util::eventually;
use tikv_util::HandyRwLock;

#[test]
fn test_basic() {
    let mut suite = Suite::new(1);
    let mut call = suite.prepare_backup(1);
    call.prepare(60);
    let resp = suite.try_split(b"k");
    debug!("Failed to split"; "err" => ?resp.response.get_header().get_error());
    must_contains_error(&resp.response, "[Suspended] Preparing disk snapshot backup");
}

#[test]
fn test_conf_change() {
    let mut suite = Suite::new(4);
    let the_region = suite.cluster.get_region(b"");
    let last_peer = the_region.peers.last().unwrap();
    let res = block_on(
        suite
            .cluster
            .async_remove_peer(the_region.get_id(), last_peer.clone())
            .unwrap(),
    );
    assert_success(&res);
    eventually(Duration::from_millis(100), Duration::from_secs(2), || {
        let r = suite.cluster.get_region(b"");
        !r.peers.iter().any(|p| p.id == last_peer.id)
    });
    let mut calls = vec![];
    for i in 1..=4 {
        let mut call = suite.prepare_backup(i);
        call.prepare(60);
        calls.push(call);
    }

    // Make sure the change has been synchronized to all stores.
    std::thread::sleep(Duration::from_millis(500));
    let the_region = suite.cluster.get_region(b"");
    let res2 = block_on(
        suite
            .cluster
            .async_remove_peer(the_region.get_id(), last_peer.clone())
            .unwrap(),
    );
    assert_failure_because(&res2, "rejected by coprocessor");
    let last_peer = the_region.peers.last().unwrap();
    calls.into_iter().for_each(|c| assert!(c.send_finalize()));
    let res3 = block_on(
        suite
            .cluster
            .async_remove_peer(the_region.get_id(), last_peer.clone())
            .unwrap(),
    );
    assert_success(&res3);
    eventually(Duration::from_millis(100), Duration::from_secs(2), || {
        let r = suite.cluster.get_region(b"");
        !r.peers.iter().any(|p| p.id == last_peer.id)
    });
}

#[test]
fn test_transfer_leader() {
    let mut suite = Suite::new(3);
    let mut calls = vec![];
    for i in 1..=3 {
        let mut call = suite.prepare_backup(i);
        call.prepare(60);
        calls.push(call);
    }
    let region = suite.cluster.get_region(b"");
    let leader = suite.cluster.leader_of_region(region.get_id()).unwrap();
    let new_leader = region.peers.iter().find(|r| r.id != leader.id).unwrap();
    let res = suite
        .cluster
        .try_transfer_leader(region.id, new_leader.clone());
    assert_failure_because(&res, "[Suspended] Preparing disk snapshot backup");
    calls.into_iter().for_each(|c| assert!(c.send_finalize()));
    let res = suite
        .cluster
        .try_transfer_leader(region.id, new_leader.clone());
    assert_success(&res);
}

#[test]
fn test_prepare_merge() {
    let mut suite = Suite::new(1);
    suite.split(b"k");
    let source = suite.cluster.get_region(b"a");
    let target = suite.cluster.get_region(b"z");
    assert_ne!(source.id, target.id);
    let mut call = suite.prepare_backup(1);
    call.prepare(60);
    let resp = suite.cluster.try_merge(source.id, target.id);
    assert_failure(&resp);
}

#[test]
fn test_wait_apply() {
    let mut suite = Suite::new(3);
    for key in 'a'..'k' {
        suite.split(&[key as u8]);
    }
    let rc = suite.cluster.get_region(b"ca");
    suite.cluster.add_send_filter(|i| {
        RegionPacketFilter::new(rc.id, i)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Send)
    });
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let mut ld_sid = None;
    // Propose a simple write command to each region.
    for c in 'a'..'k' {
        let region = suite.cluster.get_region(&[c as u8]);
        let mut cmd = RaftCmdRequest::new();
        let mut put = PutRequest::new();
        put.set_key(vec![c as u8, b'a']);
        put.set_value(b"meow?".to_vec());
        let mut req = Request::new();
        req.set_put(put);
        req.set_cmd_type(CmdType::Put);
        cmd.mut_requests().push(req);
        cmd.mut_header().set_region_id(region.id);
        cmd.mut_header()
            .set_region_epoch(region.get_region_epoch().clone());
        let ld = suite.cluster.leader_of_region(region.id).unwrap();
        if let Some(lid) = ld_sid {
            assert_eq!(
                lid, ld.store_id,
                "not all leader are in the same store, this case cannot run"
            );
        }
        ld_sid = Some(ld.store_id);
        cmd.mut_header().set_peer(ld);
        let r = suite.cluster.sim.rl();
        r.async_command_on_node(
            ld_sid.unwrap(),
            cmd,
            Callback::write_ext(
                Box::new(|resp| assert_success(&resp.response)),
                Some(Box::new({
                    let tx = tx.clone();
                    move || drop(tx)
                })),
                None,
            ),
        )
        .unwrap();
    }
    let mut call = suite.prepare_backup(ld_sid.unwrap());
    call.prepare(60);

    drop(tx);
    rx.recv_timeout(Duration::from_secs(5)).unwrap_err();

    let v = ('a'..'k')
        .map(|c| suite.cluster.get_region(&[c as u8]))
        .collect::<Vec<_>>();
    let mut regions_ok = v
        .iter()
        .map(|r| r.id)
        .filter(|id| *id != rc.id)
        .collect::<HashSet<_>>();
    call.send_wait_apply(v);

    // The regions w/o network isolation must success to wait apply.
    while !regions_ok.is_empty() {
        let res = call.next();
        let removed = regions_ok.remove(&must_wait_apply_success(&res));
        let mut k = res.get_region().start_key.clone();
        k.push(b'a');
        let v = suite.cluster.must_get(&k);
        // Due to we have wait to it applied, this write result must be observable.
        assert_eq!(v.as_deref(), Some(b"meow?".as_slice()), "{res:?}");
        assert!(removed, "{regions_ok:?} {res:?}");
    }

    suite.cluster.clear_send_filters();
    // After the network partition restored, the item must be restored.
    let res = call.next();
    assert_eq!(must_wait_apply_success(&res), rc.id);
}
