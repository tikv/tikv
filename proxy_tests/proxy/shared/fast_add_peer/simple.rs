// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::utils::v1::*;

#[derive(PartialEq, Eq)]
enum SourceType {
    Leader,
    Learner,
    // The learner coesn't catch up with Leader.
    DelayedLearner,
    InvalidSource,
}

#[derive(PartialEq, Eq, Debug)]
enum PauseType {
    None,
    Build,
    ApplySnapshot,
    SendFakeSnapshot,
}

// This test is covered in `simple_fast_add_peer`.
// It is here only as a demo for easy understanding the whole process.
#[test]
fn basic_fast_add_peer() {
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 2);
    fail::cfg("post_apply_snapshot_allow_no_unips", "return").unwrap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    // fail::cfg("on_pre_write_apply_state", "return").unwrap();
    fail::cfg("fap_mock_fake_snapshot", "return(1)").unwrap();
    // fail::cfg("before_tiflash_check_double_write", "return").unwrap();
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    cluster.must_put(b"k0", b"v0");
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1, 2]));

    cluster.shutdown();
    fail::remove("fap_core_no_fallback");
    fail::remove("fap_mock_fake_snapshot");
    // fail::remove("before_tiflash_check_double_write");
}

// `block_wait`: whether we block wait in a MsgAppend handling, or return with
// WaitForData. `pause`: pause in some core procedures.
// `check_timeout`: mock and check if FAP timeouts.
fn simple_fast_add_peer(
    source_type: SourceType,
    block_wait: bool,
    pause: PauseType,
    check_timeout: bool,
) {
    // The case in TiFlash is (DelayedPeer, false, Build)
    tikv_util::set_panic_hook(true, "./");
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    fail::cfg("post_apply_snapshot_allow_no_unips", "return").unwrap();
    cluster.cfg.proxy_cfg.engine_store.enable_fast_add_peer = true;
    if !check_timeout {
        fail::cfg("fap_core_fallback_millis", "return(1000000)").unwrap();
    } else {
        fail::cfg("fap_core_fallback_millis", "return(1500)").unwrap();
    }
    // fail::cfg("on_pre_write_apply_state", "return").unwrap();
    // fail::cfg("before_tiflash_check_double_write", "return").unwrap();
    if block_wait {
        fail::cfg("fap_mock_block_wait", "return(1)").unwrap();
    }
    match pause {
        PauseType::ApplySnapshot => {
            cluster.cfg.tikv.raft_store.region_worker_tick_interval = ReadableDuration::millis(500);
        }
        _ => (),
    }
    disable_auto_gen_compact_log(&mut cluster);
    // Disable auto generate peer.
    pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    // If we don't write here, we will have the first MsgAppend with (6,6), which
    // will cause "fast-forwarded commit to snapshot".
    cluster.must_put(b"k0", b"v0");

    debug!("=== add 2 by fap ===");
    // Add learner 2 from leader 1, this is FAP.
    pd_client.must_add_peer(1, new_learner_peer(2, 2));
    cluster.must_put(b"k1", b"v1");
    check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![1, 2]));

    // Got (k1,v1) not necessarily means peer 2 is ready.
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![2]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 2).is_some()
        },
    );

    // Add learner 3 according to source_type
    match source_type {
        SourceType::Learner | SourceType::DelayedLearner => {
            fail::cfg("fap_mock_add_peer_from_id", "return(2)").unwrap();
        }
        SourceType::InvalidSource => {
            fail::cfg("fap_mock_add_peer_from_id", "return(100)").unwrap();
        }
        _ => (),
    };

    match pause {
        PauseType::Build => fail::cfg("fap_ffi_pause", "pause").unwrap(),
        PauseType::ApplySnapshot => {
            assert!(
                cluster
                    .cfg
                    .proxy_cfg
                    .raft_store
                    .region_worker_tick_interval
                    .as_millis()
                    < 1000
            );
            assert!(
                cluster
                    .cfg
                    .tikv
                    .raft_store
                    .region_worker_tick_interval
                    .as_millis()
                    < 1000
            );
            fail::cfg("on_can_apply_snapshot", "return(false)").unwrap()
        }
        PauseType::SendFakeSnapshot => {
            fail::cfg("fap_core_fake_send", "return(1)").unwrap();
            // If we fake send snapshot, then fast path will certainly fail.
            // Then we will timeout in FALLBACK_MILLIS and go to slow path.
        }
        _ => (),
    }

    debug!("=== try add 3 by fap ===");
    // Add peer 3 by FAP
    pd_client.must_add_peer(1, new_learner_peer(3, 3));
    cluster.must_put(b"k2", b"v2");

    let need_fallback = check_timeout;

    // If we need to fallback to slow path,
    // we must make sure the data is persisted before Leader generated snapshot.
    // This is necessary, since we haven't adapt `handle_snapshot`,
    // which is a leader logic.
    if need_fallback {
        assert!(pause == PauseType::SendFakeSnapshot);
        check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![1]));
        iter_ffi_helpers(
            &cluster,
            Some(vec![1]),
            &mut |_, ffi: &mut FFIHelperSet| unsafe {
                let server = ffi.engine_store_server.as_mut();
                server.write_to_db_by_region_id(1, "persist for up-to-date snapshot".to_string());
            },
        );
    }

    match source_type {
        SourceType::DelayedLearner => {
            // Make sure conf change is applied in peer 2.
            check_key(&cluster, b"k2", b"v2", Some(true), None, Some(vec![1, 2]));
            cluster.add_send_filter(CloneFilterFactory(
                RegionPacketFilter::new(1, 2)
                    .msg_type(MessageType::MsgAppend)
                    .msg_type(MessageType::MsgSnapshot)
                    .direction(Direction::Recv),
            ));
            cluster.must_put(b"k3", b"v3");
        }
        _ => (),
    };

    // Wait some time and then recover.
    match pause {
        PauseType::Build => {
            std::thread::sleep(std::time::Duration::from_millis(3000));
            fail::remove("fap_ffi_pause");
        }
        PauseType::ApplySnapshot => {
            std::thread::sleep(std::time::Duration::from_millis(3000));
            check_key(&cluster, b"k2", b"v2", Some(false), None, Some(vec![3]));
            fail::remove("on_can_apply_snapshot");
            fail::cfg("on_can_apply_snapshot", "return(true)").unwrap();
            // Wait tick for region worker.
            std::thread::sleep(std::time::Duration::from_millis(2000));
        }
        PauseType::SendFakeSnapshot => {
            // Wait FALLBACK_MILLIS
            std::thread::sleep(std::time::Duration::from_millis(2000));
            fail::remove("fap_core_fake_send");
            std::thread::sleep(std::time::Duration::from_millis(2000));
        }
        _ => (),
    }

    debug!("=== check stage 1 ===");
    match source_type {
        SourceType::DelayedLearner => {
            check_key(&cluster, b"k3", b"v3", Some(true), None, Some(vec![1, 3]));
            check_key(&cluster, b"k3", b"v3", Some(false), None, Some(vec![2]));
        }
        SourceType::Learner => {
            check_key(
                &cluster,
                b"k2",
                b"v2",
                Some(true),
                None,
                Some(vec![1, 2, 3]),
            );
        }
        _ => {
            check_key(
                &cluster,
                b"k2",
                b"v2",
                Some(true),
                None,
                Some(vec![1, 2, 3]),
            );
        }
    };
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![3]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 3).is_some()
        },
    );

    iter_ffi_helpers(&cluster, Some(vec![3]), &mut |_, ffi: &mut FFIHelperSet| {
        assert_eq!(
            ffi.engine_store_server_helper
                .query_fap_snapshot_state(1, 3),
            proxy_ffi::interfaces_ffi::FapSnapshotState::NotFound
        );
    });

    match pause {
        PauseType::ApplySnapshot => {
            iter_ffi_helpers(
                &cluster,
                Some(vec![3]),
                &mut |_, _ffi: &mut FFIHelperSet| {
                    // Not actually the case, since we allow handling
                    // MsgAppend multiple times.
                    // So it fires when in
                    // (DelayedLearner, false, ApplySnapshot)
                    // when we want to assert `fast_add_peer_count` == 1.
                },
            );
        }
        _ => (),
    }

    match source_type {
        SourceType::DelayedLearner => {
            cluster.clear_send_filters();
        }
        _ => (),
    };

    debug!("=== remove peer ===");

    // Destroy peer, and then try re-add a new peer of the same region.
    pd_client.must_remove_peer(1, new_learner_peer(3, 3));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![1]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 3).is_none()
        },
    );
    std::thread::sleep(std::time::Duration::from_millis(1000));
    // Assert the peer removing succeeed.
    iter_ffi_helpers(&cluster, Some(vec![3]), &mut |_, ffi: &mut FFIHelperSet| {
        let server = &ffi.engine_store_server;
        assert!(!server.kvstore.contains_key(&1));
        (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
            e.fast_add_peer_count.store(0, Ordering::SeqCst);
        });
    });
    cluster.must_put(b"k5", b"v5");
    // These failpoints make sure we will cause again a fast path.
    if source_type == SourceType::InvalidSource {
        // If we still use InvalidSource, we still need to goto slow path.
    } else {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
    }
    // Re-add peer in store.
    pd_client.must_add_peer(1, new_learner_peer(3, 4));
    // Wait until Learner has applied ConfChange
    std::thread::sleep(std::time::Duration::from_millis(2000));
    must_wait_until_cond_node(
        &cluster.cluster_ext,
        1,
        Some(vec![3]),
        &|states: &States| -> bool {
            find_peer_by_id(states.in_disk_region_state.get_region(), 4).is_some()
        },
    );
    // If we re-add peer, we can still go fast path.
    iter_ffi_helpers(&cluster, Some(vec![3]), &mut |_, ffi: &mut FFIHelperSet| {
        (*ffi.engine_store_server).mutate_region_states(1, |e: &mut RegionStats| {
            assert!(e.fast_add_peer_count.load(Ordering::SeqCst) > 0);
        });
    });
    cluster.must_put(b"k6", b"v6");
    check_key(
        &cluster,
        b"k6",
        b"v6",
        Some(true),
        None,
        Some(vec![1, 2, 3]),
    );
    fail::remove("fap_core_no_fallback");
    fail::remove("fast_path_is_not_first");

    fail::remove("on_can_apply_snapshot");
    fail::remove("fap_mock_add_peer_from_id");
    fail::remove("on_pre_write_apply_state");
    fail::remove("fap_core_fallback_millis");
    fail::remove("fap_mock_block_wait");
    cluster.shutdown();
}

mod simple_normal {
    use super::*;
    #[test]
    fn test_simple_from_leader() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::Leader, false, PauseType::None, false);
        fail::remove("fap_core_no_fallback");
    }

    /// Fast path by learner snapshot.
    #[test]
    fn test_simple_from_learner() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::Learner, false, PauseType::None, false);
        fail::remove("fap_core_no_fallback");
    }

    /// If a learner is delayed, but already applied ConfChange.
    #[test]
    fn test_simple_from_delayed_learner() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::DelayedLearner, false, PauseType::None, false);
        fail::remove("fap_core_no_fallback");
    }

    /// If we select a wrong source, or we can't run fast path, we can fallback
    /// to normal.
    #[test]
    fn test_simple_from_invalid_source() {
        simple_fast_add_peer(SourceType::InvalidSource, false, PauseType::None, false);
    }
}

mod simple_blocked_nopause {}

mod simple_blocked_pause {
    use super::*;
    // Delay when fetch and build data

    #[test]
    fn test_simpleb_from_learner_paused_build() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        // Need to changed to pre_write_apply_state
        fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();
        simple_fast_add_peer(SourceType::Learner, true, PauseType::Build, false);
        fail::remove("on_pre_write_apply_state");
        fail::remove("fap_core_no_fallback");
    }

    #[test]
    fn test_simpleb_from_delayed_learner_paused_build() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        // Need to changed to pre_write_apply_state
        fail::cfg("on_pre_write_apply_state", "return(true)").unwrap();
        simple_fast_add_peer(SourceType::DelayedLearner, true, PauseType::Build, false);
        fail::remove("on_pre_write_apply_state");
        fail::remove("fap_core_no_fallback");
    }

    // Delay when applying snapshot
    // This test is origially aimed to test multiple MsgSnapshot.
    // However, we observed less repeated MsgAppend than in real cluster.
    #[test]
    fn test_simpleb_from_learner_paused_apply() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::Learner, true, PauseType::ApplySnapshot, false);
        fail::remove("fap_core_no_fallback");
    }

    #[test]
    fn test_simpleb_from_delayed_learner_paused_apply() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(
            SourceType::DelayedLearner,
            true,
            PauseType::ApplySnapshot,
            false,
        );
        fail::remove("fap_core_no_fallback");
    }
}

mod simple_non_blocked_non_pause {
    use super::*;
    #[test]
    fn test_simplenb_from_learner() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::Learner, false, PauseType::None, false);
        fail::remove("fap_core_no_fallback");
    }

    #[test]
    fn test_simplenb_from_delayed_learner() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::DelayedLearner, false, PauseType::None, false);
        fail::remove("fap_core_no_fallback");
    }
}

mod simple_non_blocked_pause {
    use super::*;
    #[test]
    fn test_simplenb_from_delayed_learner_paused_build() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(SourceType::DelayedLearner, false, PauseType::Build, false);
        fail::remove("fap_core_no_fallback");
    }

    #[test]
    fn test_simplenb_from_delayed_learner_paused_apply() {
        fail::cfg("fap_core_no_fallback", "panic").unwrap();
        simple_fast_add_peer(
            SourceType::DelayedLearner,
            false,
            PauseType::ApplySnapshot,
            false,
        );
        fail::remove("fap_core_no_fallback");
    }
}

#[test]
fn test_timeout_fallback() {
    fail::cfg("on_pre_write_apply_state", "return").unwrap();
    fail::cfg("apply_on_handle_snapshot_sync", "return(true)").unwrap();
    // By sending SendFakeSnapshot we can observe timeout.
    simple_fast_add_peer(
        SourceType::Learner,
        false,
        PauseType::SendFakeSnapshot,
        true,
    );
    fail::remove("on_pre_write_apply_state");
    fail::remove("apply_on_handle_snapshot_sync");
}
