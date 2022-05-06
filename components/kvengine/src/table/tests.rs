// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use bytes::{Buf, Bytes};

use super::table::*;
use rand::Rng;
use std::cmp::Ordering::*;

#[derive(Debug)]
struct SimpleIterator {
    keys: Vec<Bytes>,
    vals: Vec<Vec<u8>>,
    idx: i32,
    reversed: bool,

    latest_offsets: Vec<usize>,
    ver_idx: usize,
}

#[allow(dead_code)]
impl SimpleIterator {
    fn new(keys: Vec<&'static str>, vals: Vec<&'static str>, reversed: bool) -> Self {
        let length = keys.len();
        let mut ks: Vec<Bytes> = vec![];
        let mut vs = vec![];
        let mut latest_off = vec![];
        for i in 0..length {
            ks.push(Bytes::from(keys[i]));
            let val = Value::encode_buf(0, &[], 0, vals[i].as_bytes());
            vs.push(val);
            latest_off.push(i);
        }
        Self {
            keys: ks,
            vals: vs,
            idx: 0,
            reversed,
            latest_offsets: latest_off,
            ver_idx: 0,
        }
    }

    fn new_multi_version(max_ver: u64, min_ver: u64, reversed: bool) -> Self {
        let mut last_offs = vec![];
        let mut keys = vec![];
        let mut vals = vec![];

        let mut rng = rand::thread_rng();
        for i in 0..100 {
            last_offs.push(keys.len());
            let key = Bytes::from(format!("key{:03}", i));
            for j in (min_ver..=max_ver).rev() {
                keys.push(key.clone());
                let val = Value::encode_buf(0, &[], j, key.chunk());
                vals.push(val);
                if rng.gen_range(0..4) == 0 {
                    break;
                }
            }
        }
        Self {
            keys,
            vals,
            idx: 0,
            reversed,
            latest_offsets: last_offs,
            ver_idx: 0,
        }
    }

    fn entry_idx(&self) -> usize {
        self.latest_offsets[self.idx as usize] + self.ver_idx
    }
}

impl Iterator for SimpleIterator {
    fn next(&mut self) {
        if !self.reversed {
            self.idx += 1;
        } else {
            self.idx -= 1;
        }
        self.ver_idx = 0;
    }

    fn next_version(&mut self) -> bool {
        let mut next_entry_off = self.latest_offsets.len();
        if self.idx + 1 < self.latest_offsets.len() as i32 {
            next_entry_off = self.latest_offsets[self.idx as usize + 1];
        }
        if self.entry_idx() + 1 < next_entry_off {
            self.ver_idx += 1;
            return true;
        }
        false
    }

    fn rewind(&mut self) {
        if !self.reversed {
            self.idx = 0;
            self.ver_idx = 0;
        } else {
            self.idx = self.latest_offsets.len() as i32 - 1;
            self.ver_idx = 0;
        }
    }

    fn seek(&mut self, key: &[u8]) {
        self.idx = search(self.latest_offsets.len(), |idx| {
            self.keys[self.latest_offsets[idx]].chunk().cmp(key) != Less
        }) as i32;
        self.ver_idx = 0;
        if self.reversed && (!self.valid() || self.key().cmp(key) != Equal) {
            self.idx -= 1;
        }
    }

    fn key(&self) -> &[u8] {
        self.keys[self.entry_idx()].as_ref()
    }

    fn value(&self) -> Value {
        let buf = self.vals[self.entry_idx()].as_slice();
        Value::decode(buf)
    }

    fn valid(&self) -> bool {
        self.idx >= 0 && self.idx < self.latest_offsets.len() as i32
    }
}

#[allow(dead_code)]
fn get_all(mut it: Box<dyn Iterator>) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = vec![];
    let mut vals = vec![];
    while it.valid() {
        let key_b = Bytes::copy_from_slice(it.key());
        keys.push(key_b);
        vals.push(Bytes::copy_from_slice(it.value().get_value()));
        it.next();
    }
    (keys, vals)
}

#[test]
fn test_simple_iterator() {
    let keys = vec!["1", "2", "3"];
    let vals = vec!["v1", "v2", "v3"];
    let mut it = Box::new(SimpleIterator::new(keys.clone(), vals.clone(), false));
    it.rewind();
    let (n_keys, n_vals) = get_all(it);
    for i in 0..keys.len() {
        assert_eq!(keys[i].as_bytes(), n_keys[i]);
        assert_eq!(vals[i].as_bytes(), n_vals[i]);
    }
}

#[test]
fn test_merge_single() {
    let keys = vec!["1", "2", "3"];
    let vals = vec!["v1", "v2", "v3"];
    let it = SimpleIterator::new(keys.clone(), vals.clone(), false);
    let mut merge_it = new_merge_iterator(vec![Box::new(it)], false);
    merge_it.rewind();
    let (n_keys, n_vals) = get_all(merge_it);
    for i in 0..keys.len() {
        assert_eq!(keys[i].as_bytes(), n_keys[i]);
        assert_eq!(vals[i].as_bytes(), n_vals[i]);
    }
}

#[test]
fn test_merge_single_resversed() {
    let keys = vec!["1", "2", "3"];
    let vals = vec!["v1", "v2", "v3"];
    let it = SimpleIterator::new(keys.clone(), vals.clone(), true);
    let mut merge_it = new_merge_iterator(vec![Box::new(it)], true);
    merge_it.rewind();
    let (n_keys, n_vals) = get_all(merge_it);
    for i in 0..keys.len() {
        let reverse_idx = keys.len() - 1 - i;
        assert_eq!(keys[reverse_idx].as_bytes(), n_keys[i]);
        assert_eq!(vals[reverse_idx].as_bytes(), n_vals[i]);
    }
}

#[test]
fn test_merge_more() {
    let it1 = Box::new(SimpleIterator::new(
        vec!["1", "3", "7"],
        vec!["a1", "a3", "a7"],
        false,
    ));
    let it2 = Box::new(SimpleIterator::new(
        vec!["2", "3", "5"],
        vec!["b2", "b3", "b5"],
        false,
    ));
    let it3 = Box::new(SimpleIterator::new(vec!["1"], vec!["c1"], false));
    let it4 = Box::new(SimpleIterator::new(
        vec!["1", "7", "9"],
        vec!["d1", "d7", "d9"],
        false,
    ));
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3, it4], false);
    let expected_keys = vec!["1", "2", "3", "5", "7", "9"];
    let expected_vals = vec!["a1", "b2", "a3", "b5", "a7", "d9"];
    merge_it.rewind();
    let (keys, vals) = get_all(merge_it);
    for i in 0..expected_keys.len() {
        assert_eq!(expected_keys[i].as_bytes(), keys[i]);
        assert_eq!(expected_vals[i].as_bytes(), vals[i]);
    }
}

#[test]
fn test_merge_iterator_nested() {
    let keys = vec!["1", "2", "3"];
    let vals = vec!["v1", "v2", "v3"];
    let it = Box::new(SimpleIterator::new(keys.clone(), vals.clone(), false));
    let merge1 = new_merge_iterator(vec![it], false);
    let mut merge2 = new_merge_iterator(vec![merge1], false);
    merge2.rewind();
    let (n_keys, n_vals) = get_all(merge2);
    for i in 0..keys.len() {
        assert_eq!(keys[i].as_bytes(), n_keys[i]);
        assert_eq!(vals[i].as_bytes(), n_vals[i])
    }
}

#[test]
fn test_merge_iterator_seek() {
    let it1 = Box::new(SimpleIterator::new(
        vec!["1", "3", "7"],
        vec!["a1", "a3", "a7"],
        false,
    ));
    let it2 = Box::new(SimpleIterator::new(
        vec!["2", "3", "5"],
        vec!["b2", "b3", "b5"],
        false,
    ));
    let it3 = Box::new(SimpleIterator::new(vec!["1"], vec!["c1"], false));
    let it4 = Box::new(SimpleIterator::new(
        vec!["1", "7", "9"],
        vec!["d1", "d7", "d9"],
        false,
    ));
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3, it4], false);
    merge_it.seek(&Bytes::from("4".as_bytes()));
    let (keys, vals) = get_all(merge_it);
    let expected_keys = vec!["5", "7", "9"];
    let expected_vals = vec!["b5", "a7", "d9"];
    for i in 0..expected_keys.len() {
        assert_eq!(expected_keys[i].as_bytes(), keys[i]);
        assert_eq!(expected_vals[i].as_bytes(), vals[i]);
    }
}

#[test]
fn test_merge_iterator_seek_reversed() {
    let it1 = Box::new(SimpleIterator::new(
        vec!["1", "3", "7"],
        vec!["a1", "a3", "a7"],
        true,
    ));
    let it2 = Box::new(SimpleIterator::new(
        vec!["2", "3", "5"],
        vec!["b2", "b3", "b5"],
        true,
    ));
    let it3 = Box::new(SimpleIterator::new(vec!["1"], vec!["c1"], true));
    let it4 = Box::new(SimpleIterator::new(
        vec!["1", "7", "9"],
        vec!["d1", "d7", "d9"],
        true,
    ));
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3, it4], true);
    merge_it.seek(&Bytes::from("5".as_bytes()));
    let (keys, vals) = get_all(merge_it);
    let expected_keys = vec!["5", "3", "2", "1"];
    let expected_vals = vec!["b5", "a3", "b2", "a1"];
    for i in 0..expected_keys.len() {
        assert_eq!(expected_keys[i].as_bytes(), keys[i]);
        assert_eq!(expected_vals[i].as_bytes(), vals[i]);
    }
}

#[test]
fn test_merge_iterator_seek_invalid() {
    let it1 = Box::new(SimpleIterator::new(
        vec!["1", "3", "7"],
        vec!["a1", "a3", "a7"],
        false,
    ));
    let it2 = Box::new(SimpleIterator::new(
        vec!["2", "3", "5"],
        vec!["b2", "b3", "b5"],
        false,
    ));
    let it3 = Box::new(SimpleIterator::new(vec!["1"], vec!["c1"], false));
    let it4 = Box::new(SimpleIterator::new(
        vec!["1", "7", "9"],
        vec!["d1", "d7", "d9"],
        false,
    ));
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3, it4], false);
    merge_it.seek(&Bytes::from("f".as_bytes()));
    assert!(!merge_it.valid());
}

#[test]
fn test_merge_iterator_seek_invalid_reversed() {
    let it1 = Box::new(SimpleIterator::new(
        vec!["1", "3", "7"],
        vec!["a1", "a3", "a7"],
        true,
    ));
    let it2 = Box::new(SimpleIterator::new(
        vec!["2", "3", "5"],
        vec!["b2", "b3", "b5"],
        true,
    ));
    let it3 = Box::new(SimpleIterator::new(vec!["1"], vec!["c1"], true));
    let it4 = Box::new(SimpleIterator::new(
        vec!["1", "7", "9"],
        vec!["d1", "d7", "d9"],
        true,
    ));
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3, it4], true);
    merge_it.seek(&Bytes::from("0".as_bytes()));
    assert!(!merge_it.valid());
}

#[test]
fn merge_iterator_duplicated() {
    let it1 = Box::new(SimpleIterator::new(
        vec!["0", "1", "2"],
        vec!["0", "1", "2"],
        false,
    ));
    let it2 = Box::new(SimpleIterator::new(vec!["1"], vec!["1"], false));
    let it3 = Box::new(SimpleIterator::new(vec!["2"], vec!["2"], false));
    let mut merge_it = new_merge_iterator(vec![it1, it2, it3], false);
    merge_it.rewind();
    let mut cnt = 0;
    while merge_it.valid() {
        assert_eq!(cnt + 48, merge_it.key()[0] as i32);
        cnt += 1;
        merge_it.next();
    }
    assert_eq!(cnt, 3);
}

#[test]
fn test_multi_version_merge_iterator() {
    let mut rnd = rand::thread_rng();
    for &reversed in &[false, true] {
        let it1 = Box::new(SimpleIterator::new_multi_version(100, 90, reversed));
        let it2 = Box::new(SimpleIterator::new_multi_version(90, 80, reversed));
        let it3 = Box::new(SimpleIterator::new_multi_version(80, 70, reversed));
        let it4 = Box::new(SimpleIterator::new_multi_version(70, 60, reversed));
        let mut it = new_merge_iterator(vec![it1, it2, it3, it4], reversed);
        it.rewind();
        let mut cur_key = Bytes::copy_from_slice(it.key());
        for _ in 1..100 {
            it.next();
            assert_eq!(it.valid(), true);
            assert_ne!(cur_key, it.key().to_owned());
            cur_key = Bytes::copy_from_slice(it.key());
            let cur_ver = it.value().version;
            while it.next_version() {
                assert_eq!(it.value().version < cur_ver, true);
            }
        }
        for _ in 0..100 {
            let key = Bytes::from(format!("key{:03}", rnd.gen_range::<u8, _>(0..100)));
            it.seek(&key);
            assert_eq!(it.valid(), true);
            assert_eq!(it.key(), key);
            let mut cur_ver = it.value().version;
            while it.next_version() {
                assert_eq!(it.value().version < cur_ver, true);
                cur_ver = it.value().version;
            }
            assert_eq!(cur_ver <= 70, true);
        }
    }
}
