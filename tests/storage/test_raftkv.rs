use tikv::util::HandyRwLock;
use tikv::storage::engine::*;
use tikv::storage::{Key, CfName, CF_DEFAULT, CF_RAFT};
use tikv::util::codec::bytes;
use tikv::util::escape;
use kvproto::kvrpcpb::Context;
use raftstore::transport_simulate::Isolate;
use raftstore::server::new_server_cluster_with_cfs;

fn test_raftkv(read_quorum: bool) {
    let count = 1;
    let mut cluster = new_server_cluster_with_cfs(0, count, &["cf", CF_RAFT]);
    cluster.run();

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(b"k1"), None);

    let region = cluster.get_region(b"");
    let leader_id = cluster.leader_of_region(region.get_id()).unwrap();
    let storage = cluster.sim.rl().storages[&leader_id.get_id()].clone();

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(region.get_peers()[0].clone());
    ctx.set_read_quorum(read_quorum);

    get_put(&ctx, storage.as_ref());
    batch(&ctx, storage.as_ref());
    seek(&ctx, storage.as_ref());
    near_seek(&ctx, storage.as_ref());
    cf(&ctx, storage.as_ref());
    empty_write(&ctx, storage.as_ref());
    // TODO: test multiple node
}

#[test]
fn test_raftkv_with_read_local() {
    test_raftkv(false);
}

#[test]
fn test_raftkv_with_read_quorum() {
    test_raftkv(true);
}

#[test]
fn test_read_with_quorum() {
    test_read_leader_in_lease(true);
}

#[test]
fn test_read_with_lease() {
    test_read_leader_in_lease(false);
}

fn test_read_leader_in_lease(read_quorum: bool) {
    let count = 3;
    let mut cluster = new_server_cluster_with_cfs(0, count, &["cf", CF_RAFT]);
    cluster.run();

    let k1 = b"k1";
    let (k2, v2) = (b"k2", b"v2");

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(k1), None);

    let region = cluster.get_region(b"");
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let storage = cluster.sim.rl().storages[&leader.get_id()].clone();

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader.clone());
    ctx.set_read_quorum(read_quorum);

    // write some data
    assert_none(&ctx, storage.as_ref(), k2);
    must_put(&ctx, storage.as_ref(), k2, v2);

    // isolate leader
    cluster.add_send_filter(Isolate::new(leader.get_store_id()));

    // leader still in lease, check if can read on leader
    assert_eq!(can_read(&ctx, storage.as_ref(), k2, v2), !read_quorum);
}

pub fn make_key(k: &[u8]) -> Key {
    Key::from_raw(k)
}

fn must_put(ctx: &Context, engine: &Engine, key: &[u8], value: &[u8]) {
    engine.put(ctx, make_key(key), value.to_vec()).unwrap();
}

fn must_put_cf(ctx: &Context, engine: &Engine, cf: CfName, key: &[u8], value: &[u8]) {
    engine.put_cf(ctx, cf, make_key(key), value.to_vec()).unwrap();
}

fn must_delete(ctx: &Context, engine: &Engine, key: &[u8]) {
    engine.delete(ctx, make_key(key)).unwrap();
}

fn must_delete_cf(ctx: &Context, engine: &Engine, cf: CfName, key: &[u8]) {
    engine.delete_cf(ctx, cf, make_key(key)).unwrap();
}

fn assert_has(ctx: &Context, engine: &Engine, key: &[u8], value: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get(&make_key(key)).unwrap().unwrap(), value);
}

fn can_read(ctx: &Context, engine: &Engine, key: &[u8], value: &[u8]) -> bool {
    if let Ok(s) = engine.snapshot(ctx) {
        assert_eq!(s.get(&make_key(key)).unwrap().unwrap(), value);
        return true;
    }
    false
}

fn assert_has_cf(ctx: &Context, engine: &Engine, cf: CfName, key: &[u8], value: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get_cf(cf, &make_key(key)).unwrap().unwrap(), value);
}

fn assert_none(ctx: &Context, engine: &Engine, key: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get(&make_key(key)).unwrap(), None);
}

fn assert_none_cf(ctx: &Context, engine: &Engine, cf: CfName, key: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get_cf(cf, &make_key(key)).unwrap(), None);
}

fn assert_seek(ctx: &Context, engine: &Engine, key: &[u8], pair: (&[u8], &[u8])) {
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut iter = snapshot.iter(None).unwrap();
    iter.seek(&make_key(key)).unwrap();
    assert_eq!((iter.key(), iter.value()),
               (&*bytes::encode_bytes(pair.0), pair.1));
}

fn assert_near_seek(cursor: &mut Cursor, key: &[u8], pair: (&[u8], &[u8])) {
    assert!(cursor.near_seek(&make_key(key)).unwrap(), escape(key));
    assert_eq!((cursor.key(), cursor.value()),
               (&*bytes::encode_bytes(pair.0), pair.1));
}

fn assert_near_reverse_seek(cursor: &mut Cursor, key: &[u8], pair: (&[u8], &[u8])) {
    assert!(cursor.near_reverse_seek(&make_key(key)).unwrap(),
            escape(key));
    assert_eq!((cursor.key(), cursor.value()),
               (&*bytes::encode_bytes(pair.0), pair.1));
}

fn get_put(ctx: &Context, engine: &Engine) {
    assert_none(ctx, engine, b"x");
    must_put(ctx, engine, b"x", b"1");
    assert_has(ctx, engine, b"x", b"1");
    must_put(ctx, engine, b"x", b"2");
    assert_has(ctx, engine, b"x", b"2");
}

fn batch(ctx: &Context, engine: &Engine) {
    engine.write(ctx,
               vec![Modify::Put(CF_DEFAULT, make_key(b"x"), b"1".to_vec()),
                    Modify::Put(CF_DEFAULT, make_key(b"y"), b"2".to_vec())])
        .unwrap();
    assert_has(ctx, engine, b"x", b"1");
    assert_has(ctx, engine, b"y", b"2");

    engine.write(ctx,
               vec![Modify::Delete(CF_DEFAULT, make_key(b"x")),
                    Modify::Delete(CF_DEFAULT, make_key(b"y"))])
        .unwrap();
    assert_none(ctx, engine, b"y");
    assert_none(ctx, engine, b"y");
}

fn seek(ctx: &Context, engine: &Engine) {
    must_put(ctx, engine, b"x", b"1");
    assert_seek(ctx, engine, b"x", (b"x", b"1"));
    assert_seek(ctx, engine, b"a", (b"x", b"1"));
    must_put(ctx, engine, b"z", b"2");
    assert_seek(ctx, engine, b"y", (b"z", b"2"));
    assert_seek(ctx, engine, b"x\x00", (b"z", b"2"));
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut iter = snapshot.iter(None).unwrap();
    assert!(!iter.seek(&make_key(b"z\x00")).unwrap());
    must_delete(ctx, engine, b"x");
    must_delete(ctx, engine, b"z");
}

fn near_seek(ctx: &Context, engine: &Engine) {
    must_put(ctx, engine, b"x", b"1");
    must_put(ctx, engine, b"z", b"2");
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut cursor = snapshot.iter(None).unwrap();
    let cursor_mut = cursor.as_mut();
    assert_near_seek(cursor_mut, b"x", (b"x", b"1"));
    assert_near_seek(cursor_mut, b"a", (b"x", b"1"));
    assert_near_reverse_seek(cursor_mut, b"z1", (b"z", b"2"));
    assert_near_reverse_seek(cursor_mut, b"x1", (b"x", b"1"));
    assert_near_seek(cursor_mut, b"y", (b"z", b"2"));
    assert_near_seek(cursor_mut, b"x\x00", (b"z", b"2"));
    assert!(!cursor_mut.near_seek(&make_key(b"z\x00")).unwrap());
    must_delete(ctx, engine, b"x");
    must_delete(ctx, engine, b"z");
}

fn cf(ctx: &Context, engine: &Engine) {
    assert_none_cf(ctx, engine, "cf", b"key");
    must_put_cf(ctx, engine, "cf", b"key", b"value");
    assert_has_cf(ctx, engine, "cf", b"key", b"value");
    must_delete_cf(ctx, engine, "cf", b"key");
    assert_none_cf(ctx, engine, "cf", b"key");
}

fn empty_write(ctx: &Context, engine: &Engine) {
    engine.write(ctx, vec![]).unwrap();
}
