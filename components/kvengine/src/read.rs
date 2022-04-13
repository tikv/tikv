// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes, BytesMut};

use crate::table::memtable::Hint;
use crate::table::table;
use crate::*;

pub struct Item<'a> {
    val: table::Value,
    pub path: AccessPath,
    phantom: PhantomData<&'a i32>,
}

impl std::ops::Deref for Item<'_> {
    type Target = table::Value;

    fn deref(&self) -> &Self::Target {
        &self.val
    }
}

impl Item<'_> {
    fn new() -> Self {
        Self {
            val: table::Value::new(),
            path: AccessPath::default(),
            phantom: Default::default(),
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct AccessPath {
    pub mem_table: u8,
    pub l0: u8,
    pub ln: u8,
}

#[derive(Clone)]
pub struct SnapAccess {
    pub core: Arc<SnapAccessCore>,
}

impl SnapAccess {
    pub fn new(shard: &Shard) -> Self {
        let core = Arc::new(SnapAccessCore::new(shard));
        Self { core }
    }
}

impl Deref for SnapAccess {
    type Target = SnapAccessCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Debug for SnapAccess {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "snap access {}:{}, seq: {}",
            self.id, self.ver, self.write_sequence,
        )
    }
}

pub struct SnapAccessCore {
    id: u64,
    ver: u64,
    managed_ts: u64,
    write_sequence: u64,
    data: ShardData,
    get_hint: Mutex<Hint>,
}

impl SnapAccessCore {
    pub fn new(shard: &Shard) -> Self {
        let data = shard.get_data();
        let write_sequence = shard.get_write_sequence();
        Self {
            id: shard.id,
            ver: shard.ver,
            write_sequence,
            managed_ts: 0,
            data,
            get_hint: Mutex::new(Hint::new()),
        }
    }

    pub fn new_iterator(
        &self,
        cf: usize,
        reversed: bool,
        all_versions: bool,
        read_ts: Option<u64>,
    ) -> Iterator {
        let read_ts = if let Some(ts) = read_ts {
            ts
        } else {
            if CF_MANAGED[cf] && self.managed_ts != 0 {
                self.managed_ts
            } else {
                u64::MAX
            }
        };
        Iterator {
            all_versions,
            reversed,
            read_ts,
            key: BytesMut::new(),
            val: table::Value::new(),
            inner: self.new_table_iterator(cf, reversed),
            start: self.clone_start_key(),
            end: self.clone_end_key(),
            bound: None,
            bound_include: false,
        }
    }

    /// get an Item by key. Caller need to call is_some() before get_value.
    /// We don't return Option because we may need AccessPath even if the item is none.
    pub fn get(&self, cf: usize, key: &[u8], version: u64) -> Item {
        let mut version = version;
        if version == 0 {
            version = u64::MAX;
        }
        let mut item = Item::new();
        item.val = self.get_value(cf, key, version, &mut item.path);
        item
    }

    fn get_value(
        &self,
        cf: usize,
        key: &[u8],
        version: u64,
        path: &mut AccessPath,
    ) -> table::Value {
        let key_hash = farmhash::fingerprint64(key);
        for i in 0..self.data.mem_tbls.len() {
            let tbl = self.data.mem_tbls.as_slice()[i].get_cf(cf);
            let v = if i == 0 && cf == 0 {
                // only use hint for the first mem-table and cf 0.
                let mut hint = self.get_hint.lock().unwrap();
                tbl.get_with_hint(key, version, &mut hint)
            } else {
                tbl.get(key, version)
            };
            path.mem_table += 1;
            if v.is_valid() {
                return v;
            }
        }
        for l0 in &self.data.l0_tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                let v = tbl.get(key, version, key_hash);
                path.l0 += 1;
                if v.is_valid() {
                    return v;
                }
            }
        }
        let scf = self.data.get_cf(cf);
        for lh in &scf.levels {
            let v = lh.get(key, version, key_hash);
            path.ln += 1;
            if v.is_valid() {
                return v;
            }
        }
        return table::Value::new();
    }

    pub fn multi_get(&self, cf: usize, keys: &[Vec<u8>], version: u64) -> Vec<Item> {
        let mut items = Vec::with_capacity(keys.len());
        for key in keys {
            let item = self.get(cf, key, version);
            items.push(item);
        }
        items
    }

    pub fn set_managed_ts(&mut self, managed_ts: u64) {
        self.managed_ts = managed_ts;
    }

    fn new_table_iterator(&self, cf: usize, reversed: bool) -> Box<dyn table::Iterator> {
        let mut iters: Vec<Box<dyn table::Iterator>> = Vec::new();
        for mem_tbl in &self.data.mem_tbls {
            iters.push(Box::new(mem_tbl.get_cf(cf).new_iterator(reversed)));
        }
        for l0 in &self.data.l0_tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                iters.push(tbl.new_iterator(reversed));
            }
        }
        let scf = self.data.get_cf(cf);
        for lh in scf.levels.as_slice() {
            if lh.tables.len() == 0 {
                continue;
            }
            if lh.tables.len() == 1 {
                iters.push(lh.tables[0].new_iterator(reversed));
                continue;
            }
            iters.push(Box::new(ConcatIterator::new(lh.clone(), reversed)));
        }
        table::new_merge_iterator(iters, reversed)
    }

    pub fn get_write_sequence(&self) -> u64 {
        self.write_sequence
    }

    pub fn get_start_key(&self) -> &[u8] {
        self.data.start.chunk()
    }

    pub fn clone_start_key(&self) -> Bytes {
        self.data.start.clone()
    }

    pub fn get_end_key(&self) -> &[u8] {
        self.data.end.chunk()
    }

    pub fn clone_end_key(&self) -> Bytes {
        self.data.end.clone()
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_version(&self) -> u64 {
        self.ver
    }

    pub(crate) fn may_contains_in_older_table(&self, key: &[u8], cf: usize) -> bool {
        let key_hash = farmhash::fingerprint64(key);
        for tbl in &self.data.mem_tbls[1..] {
            if tbl.get_cf(cf).get(key, u64::MAX).is_valid() {
                return true;
            }
        }
        for l0 in &self.data.l0_tbls {
            let l0_cf = l0.get_cf(cf);
            if l0_cf.is_none() {
                continue;
            }
            if l0_cf.as_ref().unwrap().may_contains(key_hash) {
                return true;
            }
        }
        for l in self.data.get_cf(cf).levels.as_slice() {
            if let Some(tbl) = l.get_table(key) {
                if tbl.may_contains(key_hash) {
                    return true;
                }
            }
        }
        false
    }

    pub fn get_all_files(&self) -> Vec<u64> {
        self.data.get_all_files()
    }
}

pub struct Iterator {
    all_versions: bool,
    reversed: bool,
    read_ts: u64,
    pub key: BytesMut,
    val: table::Value,
    pub inner: Box<dyn table::Iterator>,
    start: Bytes,
    end: Bytes,
    pub bound: Option<Bytes>,
    pub bound_include: bool,
}

impl Iterator {
    pub fn valid(&self) -> bool {
        self.val.is_valid()
    }

    pub fn key(&self) -> &[u8] {
        self.key.chunk()
    }

    pub fn item(&self) -> Item {
        Item {
            val: self.val,
            path: AccessPath::default(),
            phantom: Default::default(),
        }
    }

    pub fn valid_for_prefix(&self, prefix: &[u8]) -> bool {
        self.key.starts_with(prefix)
    }

    pub fn next(&mut self) {
        if self.all_versions && self.valid() && self.inner.next_version() {
            self.update_item();
            return;
        }
        self.inner.next();
        self.parse_item();
    }

    fn update_item(&mut self) {
        self.key.truncate(0);
        self.key.extend_from_slice(self.inner.key());
        self.val = self.inner.value();
    }

    fn parse_item(&mut self) {
        while self.inner.valid() {
            if self.is_inner_key_over_bound() {
                break;
            }
            let val = self.inner.value();
            if val.version > self.read_ts {
                if !self.inner.seek_to_version(self.read_ts) {
                    self.inner.next();
                    continue;
                }
            }
            self.update_item();
            if !self.all_versions && self.val.is_deleted() {
                self.inner.next();
                continue;
            }
            return;
        }
        self.val = table::Value::new();
    }

    // seek would seek to the provided key if present. If absent, it would seek to the next smallest key
    // greater than provided if iterating in the forward direction. Behavior would be reversed is
    // iterating backwards.
    pub fn seek(&mut self, key: &[u8]) {
        if !self.reversed {
            self.inner.seek(key);
        } else {
            if key.len() == 0 {
                self.inner.rewind();
            } else {
                self.inner.seek(key);
            }
        }
        self.parse_item();
    }

    // rewind would rewind the iterator cursor all the way to zero-th position, which would be the
    // smallest key if iterating forward, and largest if iterating backward. It does not keep track of
    // whether the cursor started with a seek().
    pub fn rewind(&mut self) {
        self.inner.rewind();
        if self.inner.valid() {
            if self.reversed {
                if self.inner.key() >= self.end.chunk() {
                    self.inner.seek(self.end.chunk());
                    if self.inner.key() == self.end.chunk() {
                        self.inner.next();
                    }
                }
            } else {
                if self.inner.key() < self.start.chunk() {
                    self.inner.seek(self.start.chunk())
                }
            }
        }
        self.parse_item();
    }

    pub fn set_all_versions(&mut self, all_versions: bool) {
        self.all_versions = all_versions;
    }

    pub fn is_reverse(&self) -> bool {
        return self.reversed;
    }

    pub fn set_bound(&mut self, bound: Bytes, bound_include: bool) {
        self.bound = Some(bound);
        self.bound_include = bound_include;
    }

    pub fn is_inner_key_over_bound(&self) -> bool {
        if let Some(bound) = &self.bound {
            if self.reversed {
                if self.bound_include {
                    self.inner.key() < bound.chunk()
                } else {
                    self.inner.key() <= bound.chunk()
                }
            } else {
                if self.bound_include {
                    self.inner.key() > bound.chunk()
                } else {
                    self.inner.key() >= bound.chunk()
                }
            }
        } else {
            if self.reversed {
                self.inner.key() < self.start.chunk()
            } else {
                self.inner.key() >= self.end.chunk()
            }
        }
    }
}
