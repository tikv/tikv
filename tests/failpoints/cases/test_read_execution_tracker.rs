// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::*;
use test_coprocessor::{init_with_data, DagSelect, ProductTable};
use test_raftstore::{
    configure_for_lease_read, kv_batch_read, kv_read, must_kv_commit, must_kv_prewrite,
};
use test_raftstore_macro::test_case;
use tikv_util::config::ReadableDuration;

#[test_case(test_raftstore::must_new_cluster_with_cfg_and_kv_client_mul)]
#[test_case(test_raftstore_v2::must_new_cluster_with_cfg_and_kv_client_mul)]
fn test_read_execution_tracking() {
    let (_cluster, client, ctx) = new_cluster(1, |c| {
        // set a small renew duration to avoid trigger pre-renew that can affact the
        // metrics.
        c.cfg.tikv.raft_store.renew_leader_lease_advance_duration = ReadableDuration::millis(1);
        configure_for_lease_read(&mut c.cfg, Some(50), Some(10_000));
    });
    let (k1, v1) = (b"k1".to_vec(), b"v1".to_vec());
    let (k2, v2) = (b"k2".to_vec(), b"v2".to_vec());

    // write entries
    let mut mutation1 = Mutation::default();
    mutation1.set_op(Op::Put);
    mutation1.set_key(k1.clone());
    mutation1.set_value(v1);

    let mut mutation2 = Mutation::default();
    mutation2.set_op(Op::Put);
    mutation2.set_key(k2.clone());
    mutation2.set_value(v2);

    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation1, mutation2],
        k1.clone(),
        10,
    );
    must_kv_commit(
        &client,
        ctx.clone(),
        vec![k1.clone(), k2.clone()],
        10,
        30,
        30,
    );

    let lease_read_checker = |scan_detail: &ScanDetailV2| {
        assert!(
            scan_detail.get_read_index_propose_wait_nanos() == 0,
            "resp lease read propose wait time={:?}",
            scan_detail.get_read_index_propose_wait_nanos()
        );

        assert!(
            scan_detail.get_read_index_confirm_wait_nanos() == 0,
            "resp lease read confirm wait time={:?}",
            scan_detail.get_read_index_confirm_wait_nanos()
        );

        assert!(
            scan_detail.get_read_pool_schedule_wait_nanos() > 0,
            "resp read pool scheduling wait time={:?}",
            scan_detail.get_read_pool_schedule_wait_nanos()
        );
    };

    fail::cfg("perform_read_local", "return()").unwrap();

    // should perform lease read
    let resp = kv_read(&client, ctx.clone(), k1.clone(), 100);

    lease_read_checker(resp.get_exec_details_v2().get_scan_detail_v2());

    // should perform lease read
    let resp = kv_batch_read(&client, ctx.clone(), vec![k1.clone(), k2.clone()], 100);

    lease_read_checker(resp.get_exec_details_v2().get_scan_detail_v2());

    let product = ProductTable::new();
    init_with_data(&product, &[(1, Some("name:0"), 2)]);
    let mut coprocessor_request = DagSelect::from(&product).build();
    coprocessor_request.set_context(ctx.clone());
    coprocessor_request.set_start_ts(100);

    // should perform lease read
    let resp = client.coprocessor(&coprocessor_request).unwrap();

    lease_read_checker(resp.get_exec_details_v2().get_scan_detail_v2());

    fail::remove("perform_read_local");

    let read_index_checker = |scan_detail: &ScanDetailV2| {
        assert!(
            scan_detail.get_read_index_propose_wait_nanos() > 0,
            "resp lease read propose wait time={:?}",
            scan_detail.get_read_index_propose_wait_nanos()
        );

        assert!(
            scan_detail.get_read_index_confirm_wait_nanos() > 0,
            "resp lease read confirm wait time={:?}",
            scan_detail.get_read_index_confirm_wait_nanos()
        );

        assert!(
            scan_detail.get_read_pool_schedule_wait_nanos() > 0,
            "resp read pool scheduling wait time={:?}",
            scan_detail.get_read_pool_schedule_wait_nanos()
        );
    };

    // return read_index twich: one for local reader and one for raftstore
    fail::cfg("perform_read_index", "2*return()").unwrap();

    // should perform read index
    let resp = kv_read(&client, ctx.clone(), k1.clone(), 100);

    read_index_checker(resp.get_exec_details_v2().get_scan_detail_v2());

    fail::cfg("perform_read_index", "2*return()").unwrap();
    // should perform read index
    let resp = kv_batch_read(&client, ctx, vec![k1, k2], 100);

    read_index_checker(resp.get_exec_details_v2().get_scan_detail_v2());

    fail::cfg("perform_read_index", "2*return()").unwrap();
    // should perform read index
    let resp = client.coprocessor(&coprocessor_request).unwrap();

    read_index_checker(resp.get_exec_details_v2().get_scan_detail_v2());

    fail::remove("perform_read_index");
}
