#![feature(test)]
#![allow(clippy::unit_arg)]

extern crate test;

use test::{black_box, Bencher};

use crossbeam_epoch as epoch;
use crossbeam_skiplist::SkipList;

#[bench]
fn insert(b: &mut Bencher) {
    let guard = &epoch::pin();

    b.iter(|| {
        let map = SkipList::new(epoch::default_collector().clone());

        let mut num = 0u64;
        for _ in 0..1_000 {
            num = num.wrapping_mul(17).wrapping_add(255);
            map.insert(num, !num, guard);
        }
    });
}

#[bench]
fn iter(b: &mut Bencher) {
    let guard = &epoch::pin();
    let map = SkipList::new(epoch::default_collector().clone());

    let mut num = 0u64;
    for _ in 0..1_000 {
        num = num.wrapping_mul(17).wrapping_add(255);
        map.insert(num, !num, guard).release(guard);
    }

    b.iter(|| {
        for x in map.iter(guard) {
            black_box(x.key());
        }
    });
}

#[bench]
fn rev_iter(b: &mut Bencher) {
    let guard = &epoch::pin();
    let map = SkipList::new(epoch::default_collector().clone());

    let mut num = 0u64;
    for _ in 0..1_000 {
        num = num.wrapping_mul(17).wrapping_add(255);
        map.insert(num, !num, guard).release(guard);
    }

    b.iter(|| {
        for x in map.iter(guard).rev() {
            black_box(x.key());
        }
    });
}

#[bench]
fn lookup(b: &mut Bencher) {
    let guard = &epoch::pin();
    let map = SkipList::new(epoch::default_collector().clone());

    let mut num = 0u64;
    for _ in 0..1_000 {
        num = num.wrapping_mul(17).wrapping_add(255);
        map.insert(num, !num, guard).release(guard);
    }

    b.iter(|| {
        let mut num = 0u64;
        for _ in 0..1_000 {
            num = num.wrapping_mul(17).wrapping_add(255);
            black_box(map.get(&num, guard));
        }
    });
}

#[bench]
fn insert_remove(b: &mut Bencher) {
    let guard = &epoch::pin();

    b.iter(|| {
        let map = SkipList::new(epoch::default_collector().clone());

        let mut num = 0u64;
        for _ in 0..1_000 {
            num = num.wrapping_mul(17).wrapping_add(255);
            map.insert(num, !num, guard).release(guard);
        }

        let mut num = 0u64;
        for _ in 0..1_000 {
            num = num.wrapping_mul(17).wrapping_add(255);
            black_box(map.remove(&num, guard).unwrap().release(guard));
        }
    });
}
