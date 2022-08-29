// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::CF_DEFAULT;
use test_raftstore::*;
use txn_types::WriteBatchFlags;

#[test]
fn test_flahsback_for_parameters() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.call_prepare_flashback(region.get_id(), 1);
    sleep_ms(100);

    assert!(
        cluster.store_metas[&1]
            .lock()
            .unwrap()
            .readers
            .get(&region.get_id())
            .unwrap()
            .flashback_pending_ignore
    );

    cluster.call_finish_flashback(region.get_id(), 1);
    sleep_ms(100);

    assert!(
        !cluster.store_metas[&1]
            .lock()
            .unwrap()
            .readers
            .get(&region.get_id())
            .unwrap()
            .flashback_pending_ignore
    );
}

#[test]
fn test_flahsback_for_write() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    let reqs = vec![new_put_cf_cmd(CF_DEFAULT, b"k1", b"v1")];
    let resp = cluster.request(b"k1", reqs, false, Duration::from_secs(5));
    assert!(!resp.get_header().has_error());

    let mut region = cluster.get_region(b"k1");
    cluster.call_prepare_flashback(region.get_id(), 1);
    sleep_ms(1000);

    let reqs = vec![new_put_cf_cmd(CF_DEFAULT, b"k1", b"v1")];
    let resp = cluster.request(b"k1", reqs, false, Duration::from_secs(5));
    println!("{}", resp.get_header().get_error().get_message());
    assert!(resp.get_header().has_error());

    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![new_put_cmd(b"k1", b"v1")],
        false,
    );
    req.mut_header().set_peer(new_peer(1, 1));
    req.mut_header()
        .set_flags(WriteBatchFlags::FLASHBACK.bits());
    let resp = cluster.call_command(req, Duration::from_secs(5)).unwrap();
    assert!(!resp.get_header().has_error());

    cluster.call_finish_flashback(region.get_id(), 1);
    let reqs = vec![new_put_cf_cmd(CF_DEFAULT, b"k1", b"v1")];
    let resp = cluster.request(b"k1", reqs, false, Duration::from_secs(5));
    assert!(!resp.get_header().has_error());
}

#[test]
fn test_flahsback_for_read() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    let reqs = vec![new_put_cf_cmd(CF_DEFAULT, b"k1", b"v1")];
    let resp = cluster.request(b"k1", reqs, false, Duration::from_secs(5));
    assert!(!resp.get_header().has_error());

    let reqs = vec![new_get_cf_cmd(CF_DEFAULT, b"k1")];
    let resp = cluster.request(b"k1", reqs, false, Duration::from_secs(5));
    assert!(!resp.get_header().has_error());

    let mut region = cluster.get_region(b"k1");
    cluster.call_prepare_flashback(region.get_id(), 1);
    sleep_ms(1000);

    let reqs = vec![new_get_cf_cmd(CF_DEFAULT, b"k1")];
    let resp = cluster.request(b"k1", reqs, false, Duration::from_secs(5));
    println!("{}", resp.get_header().get_error().get_message());
    assert!(resp.get_header().has_error());

    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![new_get_cf_cmd(CF_DEFAULT, b"k1")],
        false,
    );
    req.mut_header().set_peer(new_peer(1, 1));
    req.mut_header()
        .set_flags(WriteBatchFlags::FLASHBACK.bits());
    let resp = cluster.call_command(req, Duration::from_secs(5)).unwrap();
    assert!(!resp.get_header().has_error());

    cluster.call_finish_flashback(region.get_id(), 1);
    let reqs = vec![new_get_cf_cmd(CF_DEFAULT, b"k1")];
    let resp = cluster.request(b"k1", reqs, false, Duration::from_secs(5));
    assert!(!resp.get_header().has_error());
}

#[test]
fn test_flashback_for_schedule() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // transfer leader to (2, 2) first to make address resolve happen early.
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let region = cluster.get_region(b"k1");
    cluster.call_prepare_flashback(region.get_id(), 1);
    sleep_ms(1000);

    let reqs = vec![new_put_cf_cmd(CF_DEFAULT, b"k1", b"v1")];
    let resp = cluster.request(b"k1", reqs, false, Duration::from_secs(5));
    println!("{}", resp.get_header().get_error().get_message());
    assert!(resp.get_header().has_error());

    let mut region = cluster.get_region(b"k3");
    let admin_req = new_transfer_leader_cmd(new_peer(2, 2));
    let mut transfer_leader =
        new_admin_request(region.get_id(), &region.take_region_epoch(), admin_req);
    transfer_leader.mut_header().set_peer(new_peer(1, 1));
    transfer_leader
        .mut_header()
        .set_flags(WriteBatchFlags::FLASHBACK.bits());
    let resp = cluster
        .call_command_on_leader(transfer_leader, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error());

    cluster.call_finish_flashback(region.get_id(), 1);
    // transfer leader to (1, 1)
    cluster.must_transfer_leader(1, new_peer(1, 1));
}

#[test]
fn test_flahsback_for_status_cmd_as_region_detail() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    cluster.call_prepare_flashback(1, 1);
    sleep_ms(1000);

    let leader = cluster.leader_of_region(1).unwrap();
    let region_detail = cluster.region_detail(1, 1);
    assert!(region_detail.has_region());
    let region = region_detail.get_region();
    assert_eq!(region.get_id(), 1);
    assert!(region.get_start_key().is_empty());
    assert!(region.get_end_key().is_empty());
    assert_eq!(region.get_peers().len(), 3);
    let epoch = region.get_region_epoch();
    assert_eq!(epoch.get_conf_ver(), 1);
    assert_eq!(epoch.get_version(), 1);

    assert!(region_detail.has_leader());
    assert_eq!(region_detail.get_leader(), &leader);
}

#[test]
fn test_flahsback_for_applied_index_with_last_index() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    let reqs = vec![new_put_cf_cmd(CF_DEFAULT, b"k1", b"v1")];
    let resp = cluster.request(b"k1", reqs, false, Duration::from_secs(5));
    assert!(!resp.get_header().has_error());

    let region = cluster.get_region(b"k1");
    cluster.call_prepare_flashback(region.get_id(), 1);
    sleep_ms(1000);

    let last_index = cluster.raft_local_state(1, 1).get_last_index();
    let appied_index = cluster.apply_state(1, 1).get_applied_index();

    assert_eq!(last_index, appied_index);
}
