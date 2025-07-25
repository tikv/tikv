// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{assert_matches::assert_matches, time::Duration};

use engine_traits::{
    CompactExt, DbOptionsExt, ManualCompactionOptions, MiscExt, Peekable, RaftEngineReadOnly,
    CF_DEFAULT, CF_RAFT, CF_WRITE,
};
use futures::executor::block_on;
use raftstore_v2::{router::PeerMsg, SimpleWriteEncoder};

use crate::cluster::Cluster;

/// Check if write batch is correctly maintained during apply.
#[test]
fn test_write_batch_rollback() {
    let mut cluster = Cluster::default();
    let router = &mut cluster.routers[0];
    let header = Box::new(router.new_request_for(2).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key", b"value");

    router.wait_applied_to_current_term(2, Duration::from_secs(3));
    // Make several entries to batch in apply thread.
    fail::cfg("APPLY_COMMITTED_ENTRIES", "pause").unwrap();

    // Good proposal should be committed.
    let (msg, mut sub0) = PeerMsg::simple_write(header.clone(), put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub0.wait_proposed()));
    assert!(block_on(sub0.wait_committed()));

    // If the write batch is correctly initialized, next write should not contain
    // last result.
    put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key1", b"value");
    let (msg, mut sub1) = PeerMsg::simple_write(header.clone(), put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub1.wait_proposed()));
    assert!(block_on(sub1.wait_committed()));

    fail::cfg("APPLY_PUT", "1*return()").unwrap();
    // Wake up and sleep in next committed entry.
    fail::remove("APPLY_COMMITTED_ENTRIES");
    // First apply will fail due to aborted. If write batch is initialized
    // correctly, correct response can be returned.
    let resp = block_on(sub0.result()).unwrap();
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("aborted"),
        "{:?}",
        resp
    );
    let resp = block_on(sub1.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    let snap = router.stale_snapshot(2);
    assert_matches!(snap.get_value(b"key"), Ok(None));
    assert_eq!(snap.get_value(b"key1").unwrap().unwrap(), b"value");

    fail::cfg("APPLY_COMMITTED_ENTRIES", "pause").unwrap();

    // Trigger error again, so an initialized write batch should be rolled back.
    put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key2", b"value");
    let (msg, mut sub0) = PeerMsg::simple_write(header.clone(), put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub0.wait_proposed()));
    assert!(block_on(sub0.wait_committed()));

    // If the write batch is correctly rollbacked, next write should not contain
    // last result.
    put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key3", b"value");
    let (msg, mut sub1) = PeerMsg::simple_write(header, put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub1.wait_proposed()));
    assert!(block_on(sub1.wait_committed()));

    fail::cfg("APPLY_PUT", "1*return()").unwrap();
    fail::remove("APPLY_COMMITTED_ENTRIES");
    let resp = block_on(sub0.result()).unwrap();
    assert!(
        resp.get_header()
            .get_error()
            .get_message()
            .contains("aborted"),
        "{:?}",
        resp
    );
    let resp = block_on(sub1.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let snap = router.stale_snapshot(2);
    assert_matches!(snap.get_value(b"key2"), Ok(None));
    assert_eq!(snap.get_value(b"key3").unwrap().unwrap(), b"value");
}

#[test]
fn test_delete_range() {
    let mut cluster = Cluster::default();
    let mut cached = cluster.node(0).tablet_registry().get(2).unwrap();
    let router = &mut cluster.routers[0];
    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    {
        let snap = router.stale_snapshot(2);
        assert!(snap.get_value(b"key1").unwrap().is_none());
    }
    // write to default and write cf.
    for i in 0..10 {
        let header = Box::new(router.new_request_for(2).take_header());
        let mut put = SimpleWriteEncoder::with_capacity(64);
        put.put(CF_DEFAULT, format!("k{i}").as_bytes(), b"value");
        put.put(CF_WRITE, format!("k{i}").as_bytes(), b"value");
        let (msg, mut sub) = PeerMsg::simple_write(header.clone(), put.encode());
        router.send(2, msg).unwrap();
        assert!(block_on(sub.wait_proposed()));
        assert!(block_on(sub.wait_committed()));
        let resp = block_on(sub.result()).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        let snap = router.stale_snapshot(2);
        assert_eq!(
            snap.get_value(format!("k{i}").as_bytes()).unwrap().unwrap(),
            b"value"
        );
        assert_eq!(
            snap.get_value_cf(CF_WRITE, format!("k{i}").as_bytes())
                .unwrap()
                .unwrap(),
            b"value"
        );
    }
    // flush all data.
    cached.latest().unwrap().flush_cfs(&[], true).unwrap();
    // delete some in default cf.
    let fp = fail::FailGuard::new("should_persist_apply_trace", "return");
    let header = Box::new(router.new_request_for(2).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    // it will write some tombstones.
    put.delete_range(CF_DEFAULT, b"k3", b"k6", false);
    let (msg, mut sub) = PeerMsg::simple_write(header, put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub.wait_proposed()));
    assert!(block_on(sub.wait_committed()));
    let resp = block_on(sub.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    {
        let snap = router.stale_snapshot(2);
        assert!(snap.get_value(b"k4").unwrap().is_none());
    }
    drop(fp);
    cached
        .latest()
        .unwrap()
        .set_db_options(&[("avoid_flush_during_shutdown", "true")])
        .unwrap();
    cached.release();
    drop(cached);
    cluster.node(0).tablet_registry().remove(2);
    // restart and check delete is re-applied.
    cluster.restart(0);
    cluster.routers[0].wait_applied_to_current_term(2, Duration::from_secs(3));
    let snap = cluster.routers[0].stale_snapshot(2);
    assert_eq!(snap.get_value(b"k2").unwrap().unwrap(), b"value");
    assert!(snap.get_value(b"k3").unwrap().is_none());
    assert!(snap.get_value(b"k4").unwrap().is_none());
    assert!(snap.get_value(b"k4").unwrap().is_none());
    assert_eq!(snap.get_value(b"k6").unwrap().unwrap(), b"value");
}

// It tests that delete range for an empty cf does not block the progress of
// persisted_applied. See the description of the PR #14905.
#[test]
fn test_delete_range_does_not_block_flushed_index() {
    let mut cluster = Cluster::default();
    let mut cached = cluster.node(0).tablet_registry().get(2).unwrap();
    let raft_engine = cluster.node(0).running_state().unwrap().raft_engine.clone();
    let router = &mut cluster.routers[0];
    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    let _fp = fail::FailGuard::new("should_persist_apply_trace", "return");
    {
        let snap = router.stale_snapshot(2);
        assert!(snap.get_value(b"key").unwrap().is_none());
    }
    // write to default cf and flush.
    let header = Box::new(router.new_request_for(2).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key", b"value");
    let (msg, mut sub) = PeerMsg::simple_write(header, put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub.wait_proposed()));
    assert!(block_on(sub.wait_committed()));
    let resp = block_on(sub.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let snap = router.stale_snapshot(2);
    assert_eq!(snap.get_value(b"key").unwrap().unwrap(), b"value");
    // Must compact to non-L0 level.
    cached
        .latest()
        .unwrap()
        .compact_range_cf(
            CF_DEFAULT,
            Some(b"A"),
            Some(b"{"),
            ManualCompactionOptions::new(false, 1, false),
        )
        .unwrap();
    // delete range by files.
    let header = Box::new(router.new_request_for(2).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.delete_range(CF_DEFAULT, b"k", b"z", false);
    let (msg, mut sub) = PeerMsg::simple_write(header, put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub.wait_proposed()));
    assert!(block_on(sub.wait_committed()));
    let resp = block_on(sub.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    {
        let snap = router.stale_snapshot(2);
        assert!(snap.get_value(b"key").unwrap().is_none());
        // Make sure memtable is empty.
        assert!(
            cached
                .latest()
                .unwrap()
                .get_active_memtable_stats_cf(CF_DEFAULT)
                .unwrap()
                .is_none()
        );
    }
    // record current admin flushed.
    let admin_flushed = raft_engine.get_flushed_index(2, CF_RAFT).unwrap().unwrap();
    // write to write cf and flush.
    let header = Box::new(router.new_request_for(2).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_WRITE, b"key", b"value");
    let (msg, mut sub) = PeerMsg::simple_write(header, put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub.wait_proposed()));
    assert!(block_on(sub.wait_committed()));
    let resp = block_on(sub.result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let snap = router.stale_snapshot(2);
    assert_eq!(
        snap.get_value_cf(CF_WRITE, b"key").unwrap().unwrap(),
        b"value"
    );
    cached.latest().unwrap().flush_cf(CF_WRITE, true).unwrap();

    let current_admin_flushed = raft_engine.get_flushed_index(2, CF_RAFT).unwrap().unwrap();
    assert!(current_admin_flushed > admin_flushed);
}
