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

use kvproto::kvrpcpb::Context;

use test_raftstore::*;
use tikv::raftstore::store::engine::IterOption;
use tikv::storage::engine::*;
use tikv::storage::{CFStatistics, CfName, Key, CF_DEFAULT};
use tikv::util::codec::bytes;
use tikv::util::escape;
use tikv::util::HandyRwLock;

#[test]
fn test_raftkv() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
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

    get_put(&ctx, &storage);
    batch(&ctx, &storage);
    seek(&ctx, &storage);
    near_seek(&ctx, &storage);
    cf(&ctx, &storage);
    empty_write(&ctx, &storage);
    wrong_context(&ctx, &storage);
    // TODO: test multiple node
}

#[test]
fn test_read_leader_in_lease() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
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

    // write some data
    assert_none(&ctx, &storage, k2);
    must_put(&ctx, &storage, k2, v2);

    // isolate leader
    cluster.add_send_filter(IsolationFilterFactory::new(leader.get_store_id()));

    // leader still in lease, check if can read on leader
    assert_eq!(can_read(&ctx, &storage, k2, v2), true);
}

fn must_put<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) {
    engine.put(ctx, Key::from_raw(key), value.to_vec()).unwrap();
}

fn must_put_cf<E: Engine>(ctx: &Context, engine: &E, cf: CfName, key: &[u8], value: &[u8]) {
    engine
        .put_cf(ctx, cf, Key::from_raw(key), value.to_vec())
        .unwrap();
}

fn must_delete<E: Engine>(ctx: &Context, engine: &E, key: &[u8]) {
    engine.delete(ctx, Key::from_raw(key)).unwrap();
}

fn must_delete_cf<E: Engine>(ctx: &Context, engine: &E, cf: CfName, key: &[u8]) {
    engine.delete_cf(ctx, cf, Key::from_raw(key)).unwrap();
}

fn assert_has<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap().unwrap(), value);
}

fn can_read<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) -> bool {
    if let Ok(s) = engine.snapshot(ctx) {
        assert_eq!(s.get(&Key::from_raw(key)).unwrap().unwrap(), value);
        return true;
    }
    false
}

fn assert_has_cf<E: Engine>(ctx: &Context, engine: &E, cf: CfName, key: &[u8], value: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(
        snapshot.get_cf(cf, &Key::from_raw(key)).unwrap().unwrap(),
        value
    );
}

fn assert_none<E: Engine>(ctx: &Context, engine: &E, key: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get(&Key::from_raw(key)).unwrap(), None);
}

fn assert_none_cf<E: Engine>(ctx: &Context, engine: &E, cf: CfName, key: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(snapshot.get_cf(cf, &Key::from_raw(key)).unwrap(), None);
}

fn assert_seek<E: Engine>(ctx: &Context, engine: &E, key: &[u8], pair: (&[u8], &[u8])) {
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut cursor = snapshot
        .iter(IterOption::default(), ScanMode::Mixed)
        .unwrap();
    let mut statistics = CFStatistics::default();
    cursor.seek(&Key::from_raw(key), &mut statistics).unwrap();
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn assert_seek_cf<E: Engine>(
    ctx: &Context,
    engine: &E,
    cf: CfName,
    key: &[u8],
    pair: (&[u8], &[u8]),
) {
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut cursor = snapshot
        .iter_cf(cf, IterOption::default(), ScanMode::Mixed)
        .unwrap();
    let mut statistics = CFStatistics::default();
    cursor.seek(&Key::from_raw(key), &mut statistics).unwrap();
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn assert_near_seek<I: Iterator>(cursor: &mut Cursor<I>, key: &[u8], pair: (&[u8], &[u8])) {
    let mut statistics = CFStatistics::default();
    assert!(
        cursor
            .near_seek(&Key::from_raw(key), &mut statistics)
            .unwrap(),
        escape(key)
    );
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn assert_near_reverse_seek<I: Iterator>(cursor: &mut Cursor<I>, key: &[u8], pair: (&[u8], &[u8])) {
    let mut statistics = CFStatistics::default();
    assert!(
        cursor
            .near_reverse_seek(&Key::from_raw(key), &mut statistics)
            .unwrap(),
        escape(key)
    );
    assert_eq!(cursor.key(&mut statistics), &*bytes::encode_bytes(pair.0));
    assert_eq!(cursor.value(&mut statistics), pair.1);
}

fn get_put<E: Engine>(ctx: &Context, engine: &E) {
    assert_none(ctx, engine, b"x");
    must_put(ctx, engine, b"x", b"1");
    assert_has(ctx, engine, b"x", b"1");
    must_put(ctx, engine, b"x", b"2");
    assert_has(ctx, engine, b"x", b"2");
}

fn batch<E: Engine>(ctx: &Context, engine: &E) {
    engine
        .write(
            ctx,
            vec![
                Modify::Put(CF_DEFAULT, Key::from_raw(b"x"), b"1".to_vec()),
                Modify::Put(CF_DEFAULT, Key::from_raw(b"y"), b"2".to_vec()),
            ],
        )
        .unwrap();
    assert_has(ctx, engine, b"x", b"1");
    assert_has(ctx, engine, b"y", b"2");

    engine
        .write(
            ctx,
            vec![
                Modify::Delete(CF_DEFAULT, Key::from_raw(b"x")),
                Modify::Delete(CF_DEFAULT, Key::from_raw(b"y")),
            ],
        )
        .unwrap();
    assert_none(ctx, engine, b"y");
    assert_none(ctx, engine, b"y");
}

fn seek<E: Engine>(ctx: &Context, engine: &E) {
    must_put(ctx, engine, b"x", b"1");
    assert_seek(ctx, engine, b"x", (b"x", b"1"));
    assert_seek(ctx, engine, b"a", (b"x", b"1"));
    must_put(ctx, engine, b"z", b"2");
    assert_seek(ctx, engine, b"y", (b"z", b"2"));
    assert_seek(ctx, engine, b"x\x00", (b"z", b"2"));
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut iter = snapshot
        .iter(IterOption::default(), ScanMode::Mixed)
        .unwrap();
    let mut statistics = CFStatistics::default();
    assert!(
        !iter
            .seek(&Key::from_raw(b"z\x00"), &mut statistics)
            .unwrap()
    );
    must_delete(ctx, engine, b"x");
    must_delete(ctx, engine, b"z");
}

fn near_seek<E: Engine>(ctx: &Context, engine: &E) {
    must_put(ctx, engine, b"x", b"1");
    must_put(ctx, engine, b"z", b"2");
    let snapshot = engine.snapshot(ctx).unwrap();
    let mut cursor = snapshot
        .iter(IterOption::default(), ScanMode::Mixed)
        .unwrap();
    assert_near_seek(&mut cursor, b"x", (b"x", b"1"));
    assert_near_seek(&mut cursor, b"a", (b"x", b"1"));
    assert_near_reverse_seek(&mut cursor, b"z1", (b"z", b"2"));
    assert_near_reverse_seek(&mut cursor, b"x1", (b"x", b"1"));
    assert_near_seek(&mut cursor, b"y", (b"z", b"2"));
    assert_near_seek(&mut cursor, b"x\x00", (b"z", b"2"));
    let mut statistics = CFStatistics::default();
    assert!(
        !cursor
            .near_seek(&Key::from_raw(b"z\x00"), &mut statistics)
            .unwrap()
    );
    must_delete(ctx, engine, b"x");
    must_delete(ctx, engine, b"z");
}

fn cf<E: Engine>(ctx: &Context, engine: &E) {
    assert_none_cf(ctx, engine, "default", b"key");
    must_put_cf(ctx, engine, "default", b"key", b"value");
    assert_has_cf(ctx, engine, "default", b"key", b"value");
    assert_seek_cf(ctx, engine, "default", b"k", (b"key", b"value"));
    must_delete_cf(ctx, engine, "default", b"key");
    assert_none_cf(ctx, engine, "default", b"key");
}

fn empty_write<E: Engine>(ctx: &Context, engine: &E) {
    engine.write(ctx, vec![]).unwrap_err();
}

fn wrong_context<E: Engine>(ctx: &Context, engine: &E) {
    let region_id = ctx.get_region_id();
    let mut ctx = ctx.to_owned();
    ctx.set_region_id(region_id + 1);
    assert!(engine.write(&ctx, vec![]).is_err());
}
