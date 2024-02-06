use std::{sync::mpsc::sync_channel, time::Duration};

use engine_traits::{CacheRange, SnapshotContext, CF_WRITE};
use keys::{DATA_MAX_KEY, DATA_MIN_KEY};
use test_raftstore::new_node_cluster_with_hybrid_engine;
use txn_types::Key;

#[test]
fn test_basic_put_get() {
    let mut cluster = new_node_cluster_with_hybrid_engine(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 1;
    cluster.run();

    let range_cache_engine = cluster.get_range_cache_engine(1);
    // FIXME: load is not implemented, so we have to insert range manually
    {
        let mut core = range_cache_engine.core().write().unwrap();
        let cache_range = CacheRange::new(DATA_MIN_KEY.to_vec(), DATA_MAX_KEY.to_vec());
        core.mut_range_manager().new_range(cache_range.clone());
        core.mut_range_manager().set_safe_point(&cache_range, 1000);
        core.mut_range_manager()
            .set_range_readable(&cache_range, true);
    }

    cluster.put(b"k05", b"val").unwrap();
    let snap_ctx = SnapshotContext {
        read_ts: 1001,
        range: None,
    };
    let (tx, rx) = sync_channel(1);
    fail::cfg_callback("on_range_cache_get_value", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    let val = cluster.get_with_snap_ctx(b"k05", snap_ctx).unwrap();
    assert_eq!(&val, b"val");

    // verify it's read from range cache engine
    assert!(rx.try_recv().unwrap());
}

#[test]
fn test_load() {
    let mut cluster = new_node_cluster_with_hybrid_engine(0, 1);
    cluster.cfg.raft_store.apply_batch_system.pool_size = 1;
    cluster.run();

    for i in 0..10 {
        let key = format!("key-{:04}", i);
        let encoded_key = Key::from_raw(key.as_bytes())
            .append_ts(20.into())
            .into_encoded();
        cluster.must_put(&encoded_key, b"val-default");
        cluster.must_put_cf(CF_WRITE, &encoded_key, b"val-write");
    }

    {
        let range_cache_engine = cluster.get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write().unwrap();
        let cache_range = CacheRange::new(DATA_MIN_KEY.to_vec(), DATA_MAX_KEY.to_vec());
        core.mut_range_manager().load_range(cache_range);
    }

    for i in 10..20 {
        if i == 19 {
            std::thread::sleep(Duration::from_secs(1));
        }
        let key = format!("key-{:04}", i);
        let encoded_key = Key::from_raw(key.as_bytes())
            .append_ts(20.into())
            .into_encoded();
        cluster.must_put(&encoded_key, b"val-default");
        cluster.must_put_cf(CF_WRITE, &encoded_key, b"val-write");
    }

    {
        let range_cache_engine = cluster.get_range_cache_engine(1);
        let mut core = range_cache_engine.core().write().unwrap();
        let cache_range = CacheRange::new(DATA_MIN_KEY.to_vec(), DATA_MAX_KEY.to_vec());
        core.mut_range_manager().load_range(cache_range);
    }

    let (tx, rx) = sync_channel(1);
    fail::cfg_callback("on_range_cache_get_value", move || {
        tx.send(true).unwrap();
    })
    .unwrap();

    let snap_ctx = SnapshotContext {
        read_ts: 20,
        range: None,
    };

    for i in 0..20 {
        let key = format!("key-{:04}", i);
        let encoded_key = Key::from_raw(key.as_bytes())
            .append_ts(20.into())
            .into_encoded();
        let val = cluster
            .get_cf_with_snap_ctx(CF_WRITE, &encoded_key, snap_ctx.clone())
            .unwrap();
        assert_eq!(&val, b"val-write");
        println!("find {:?}", key);
        // verify it's read from range cache engine
        assert!(rx.try_recv().unwrap());

        let val = cluster
            .get_with_snap_ctx(&encoded_key, snap_ctx.clone())
            .unwrap();
        assert_eq!(&val, b"val-default");
        // verify it's read from range cache engine
        assert!(rx.try_recv().unwrap());
    }
}
