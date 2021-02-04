// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::{default_engine};
use engine_traits::{Iterable, Iterator, KvEngine};
use engine_traits::SeekKey;
use std::panic::{self, AssertUnwindSafe};

fn iter_empty<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    let mut iter = i(e);

    assert_eq!(iter.valid().unwrap(), false);

    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = iter.prev();
    })).is_err());
    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = iter.next();
    })).is_err());
    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        iter.key();
    })).is_err());
    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        iter.value();
    })).is_err());

    assert_eq!(iter.seek(SeekKey::Start).unwrap(), false);
    assert_eq!(iter.seek(SeekKey::End).unwrap(), false);
    assert_eq!(iter.seek(SeekKey::Key(b"foo")).unwrap(), false);
    assert_eq!(iter.seek_for_prev(SeekKey::Start).unwrap(), false);
    assert_eq!(iter.seek_for_prev(SeekKey::End).unwrap(), false);
    assert_eq!(iter.seek_for_prev(SeekKey::Key(b"foo")).unwrap(), false);
}

#[test]
fn iter_empty_engine() {
    let db = default_engine();
    iter_empty(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn iter_empty_snapshot() {
    let db = default_engine();
    iter_empty(&db.engine, |e| e.snapshot().iterator().unwrap());
}

fn iter_forward<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    e.put(b"a", b"a").unwrap();
    e.put(b"b", b"b").unwrap();
    e.put(b"c", b"c").unwrap();

    let mut iter = i(e);

    assert!(!iter.valid().unwrap());

    assert!(iter.seek(SeekKey::Start).unwrap());

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"a");
    assert_eq!(iter.value(), b"a");

    assert_eq!(iter.next().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"b");
    assert_eq!(iter.value(), b"b");

    assert_eq!(iter.next().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"c");

    assert_eq!(iter.next().unwrap(), false);

    assert!(!iter.valid().unwrap());

    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        iter.key();
    })).is_err());
    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        iter.value();
    })).is_err());
}

#[test]
fn iter_forward_engine() {
    let db = default_engine();
    iter_forward(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn iter_forward_snapshot() {
    let db = default_engine();
    iter_forward(&db.engine, |e| e.snapshot().iterator().unwrap());
}

fn iter_reverse<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    e.put(b"a", b"a").unwrap();
    e.put(b"b", b"b").unwrap();
    e.put(b"c", b"c").unwrap();

    let mut iter = i(e);

    assert!(!iter.valid().unwrap());

    assert!(iter.seek(SeekKey::End).unwrap());

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"c");

    assert_eq!(iter.prev().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"b");
    assert_eq!(iter.value(), b"b");

    assert_eq!(iter.prev().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"a");
    assert_eq!(iter.value(), b"a");

    assert_eq!(iter.prev().unwrap(), false);

    assert!(!iter.valid().unwrap());

    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        iter.key();
    })).is_err());
    assert!(panic::catch_unwind(AssertUnwindSafe(|| {
        iter.value();
    })).is_err());
}

#[test]
fn iter_reverse_engine() {
    let db = default_engine();
    iter_reverse(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn iter_reverse_snapshot() {
    let db = default_engine();
    iter_reverse(&db.engine, |e| e.snapshot().iterator().unwrap());
}

fn seek_to_key_then_forward<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    e.put(b"a", b"a").unwrap();
    e.put(b"b", b"b").unwrap();
    e.put(b"c", b"c").unwrap();

    let mut iter = i(e);

    assert!(iter.seek(SeekKey::Key(b"b")).unwrap());

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"b");
    assert_eq!(iter.value(), b"b");

    assert_eq!(iter.next().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"c");

    assert_eq!(iter.next().unwrap(), false);

    assert!(!iter.valid().unwrap());
}

#[test]
fn seek_to_key_then_forward_engine() {
    let db = default_engine();
    seek_to_key_then_forward(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn seek_to_key_then_forward_snapshot() {
    let db = default_engine();
    seek_to_key_then_forward(&db.engine, |e| e.snapshot().iterator().unwrap());
}

fn seek_to_key_then_reverse<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    e.put(b"a", b"a").unwrap();
    e.put(b"b", b"b").unwrap();
    e.put(b"c", b"c").unwrap();

    let mut iter = i(e);

    assert!(iter.seek(SeekKey::Key(b"b")).unwrap());

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"b");
    assert_eq!(iter.value(), b"b");

    assert_eq!(iter.prev().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"a");
    assert_eq!(iter.value(), b"a");

    assert_eq!(iter.prev().unwrap(), false);

    assert!(!iter.valid().unwrap());
}

#[test]
fn seek_to_key_then_reverse_engine() {
    let db = default_engine();
    seek_to_key_then_reverse(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn seek_to_key_then_reverse_snapshot() {
    let db = default_engine();
    seek_to_key_then_reverse(&db.engine, |e| e.snapshot().iterator().unwrap());
}

fn iter_forward_then_reverse<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    e.put(b"a", b"a").unwrap();
    e.put(b"b", b"b").unwrap();
    e.put(b"c", b"c").unwrap();

    let mut iter = i(e);

    assert!(!iter.valid().unwrap());

    assert!(iter.seek(SeekKey::Start).unwrap());

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"a");
    assert_eq!(iter.value(), b"a");

    assert_eq!(iter.next().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"b");
    assert_eq!(iter.value(), b"b");

    assert_eq!(iter.next().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"c");

    assert_eq!(iter.prev().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"b");
    assert_eq!(iter.value(), b"b");

    assert_eq!(iter.prev().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"a");
    assert_eq!(iter.value(), b"a");

    assert_eq!(iter.prev().unwrap(), false);

    assert!(!iter.valid().unwrap());
}

#[test]
fn iter_forward_then_reverse_engine() {
    let db = default_engine();
    iter_forward_then_reverse(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn iter_forward_then_reverse_snapshot() {
    let db = default_engine();
    iter_forward_then_reverse(&db.engine, |e| e.snapshot().iterator().unwrap());
}

fn iter_reverse_then_forward<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    e.put(b"a", b"a").unwrap();
    e.put(b"b", b"b").unwrap();
    e.put(b"c", b"c").unwrap();

    let mut iter = i(e);

    assert!(!iter.valid().unwrap());

    assert!(iter.seek(SeekKey::End).unwrap());

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"c");

    assert_eq!(iter.prev().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"b");
    assert_eq!(iter.value(), b"b");

    assert_eq!(iter.prev().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"a");
    assert_eq!(iter.value(), b"a");

    assert_eq!(iter.next().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"b");
    assert_eq!(iter.value(), b"b");

    assert_eq!(iter.next().unwrap(), true);

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"c");

    assert_eq!(iter.next().unwrap(), false);

    assert!(!iter.valid().unwrap());
}

#[test]
fn iter_reverse_then_forward_engine() {
    let db = default_engine();
    iter_reverse_then_forward(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn iter_reverse_then_forward_snapshot() {
    let db = default_engine();
    iter_reverse_then_forward(&db.engine, |e| e.snapshot().iterator().unwrap());
}

// When seek finds an exact key then seek_for_prev behaves just like seek
fn seek_for_prev<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    e.put(b"a", b"a").unwrap();
    e.put(b"b", b"b").unwrap();
    e.put(b"c", b"c").unwrap();

    let mut iter = i(e);

    assert!(iter.seek_for_prev(SeekKey::Start).unwrap());

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"a");
    assert_eq!(iter.value(), b"a");

    assert!(iter.seek_for_prev(SeekKey::End).unwrap());

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"c");

    assert!(iter.seek_for_prev(SeekKey::Key(b"c")).unwrap());

    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"c");
    assert_eq!(iter.value(), b"c");
}

#[test]
fn seek_for_prev_engine() {
    let db = default_engine();
    seek_for_prev(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn seek_for_prev_snapshot() {
    let db = default_engine();
    seek_for_prev(&db.engine, |e| e.snapshot().iterator().unwrap());
}

// When Seek::Key doesn't find an exact match,
// it still might succeed, but its behavior differs
// based on whether `seek` or `seek_for_prev` is called.
fn seek_key_miss<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    e.put(b"c", b"c").unwrap();

    let mut iter = i(e);

    assert!(!iter.valid().unwrap());

    assert!(iter.seek(SeekKey::Key(b"b")).unwrap());
    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"c");

    assert!(!iter.seek(SeekKey::Key(b"d")).unwrap());
    assert!(!iter.valid().unwrap());
}

#[test]
fn seek_key_miss_engine() {
    let db = default_engine();
    seek_key_miss(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn seek_key_miss_snapshot() {
    let db = default_engine();
    seek_key_miss(&db.engine, |e| e.snapshot().iterator().unwrap());
}

fn seek_key_prev_miss<E, I, IF>(e: &E, i: IF)
where E: KvEngine,
      I: Iterator,
      IF: Fn(&E) -> I,
{
    e.put(b"c", b"c").unwrap();

    let mut iter = i(e);

    assert!(!iter.valid().unwrap());

    assert!(iter.seek_for_prev(SeekKey::Key(b"d")).unwrap());
    assert!(iter.valid().unwrap());
    assert_eq!(iter.key(), b"c");

    assert!(!iter.seek_for_prev(SeekKey::Key(b"b")).unwrap());
    assert!(!iter.valid().unwrap());
}

#[test]
fn seek_key_prev_miss_engine() {
    let db = default_engine();
    seek_key_prev_miss(&db.engine, |e| e.iterator().unwrap());
}

#[test]
fn seek_key_prev_miss_snapshot() {
    let db = default_engine();
    seek_key_prev_miss(&db.engine, |e| e.snapshot().iterator().unwrap());
}
