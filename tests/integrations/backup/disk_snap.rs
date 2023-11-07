// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, time::Duration};

use kvproto::{
    brpb::PrepareSnapshotBackupEventType,
    raft_cmdpb::{CmdType, PutRequest, RaftCmdRequest, Request},
};
use raft::prelude::MessageType;
use raftstore::store::{Callback, RaftCommand};
use test_backup::disk_snap::{assert_success, must_wait_apply_success, Suite};
use test_raftstore::{
    must_contains_error, CloneFilterFactory, Direction, RegionPacketFilter, Simulator,
};
use tikv_util::HandyRwLock;

#[test]
fn test_basic() {
    let mut suite = Suite::new(1);
    let mut call = suite.prepare_backup(1);
    call.prepare(60);
    let resp = suite.split(b"k");
    println!("{:?}", resp.response.get_header().get_error());
    must_contains_error(
        &resp.response,
        "rejecting proposing admin commands while preparing snapshot backup",
    );
}

#[test]

fn test_wait_apply() {
    test_util::init_log_for_test();
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
        let _ = r
            .async_command_on_node(
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
    // Hacking: assume all leaders are in one store.
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
