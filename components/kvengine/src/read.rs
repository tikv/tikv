// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::{self};
use std::cell::RefCell;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::rc::Rc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use slog_global::info;

use crate::shard::{L0Tables, MemTables};
use crate::table::{memtable, sstable, table};
use crate::*;
use crossbeam_epoch as epoch;

pub struct Item<'a> {
    val: table::Value,
    phantom: PhantomData<&'a i32>,
}

impl std::ops::Deref for Item<'_> {
    type Target = table::Value;

    fn deref(&self) -> &Self::Target {
        &self.val
    }
}

impl Item<'_> {
    fn new(val: table::Value) -> Self {
        Self {
            val,
            phantom: Default::default(),
        }
    }
}

pub struct SnapAccess<'a> {
    cfs: [CFConfig; NUM_CFS],
    managed_ts: u64,
    hints: [RefCell<memtable::Hint>; NUM_CFS],
    splitting: Option<&'a SplitContext>,
    mem_tbls: &'a MemTables,
    l0_tbls: &'a L0Tables,

    scfs: Vec<&'a ShardCF>,
}

impl Debug for SnapAccess<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<'a> SnapAccess<'a> {
    pub(crate) fn new(
        cfs: [CFConfig; NUM_CFS],
        splitting: Option<&'a SplitContext>,
        mem_tbls: &'a MemTables,
        l0_tbls: &'a L0Tables,
        scfs: Vec<&'a ShardCF>,
    ) -> Self {
        let hints = [
            RefCell::new(memtable::Hint::new()),
            RefCell::new(memtable::Hint::new()),
            RefCell::new(memtable::Hint::new()),
        ];
        Self {
            cfs,
            managed_ts: 0,
            hints,
            mem_tbls,
            l0_tbls,
            splitting,
            scfs,
        }
    }

    pub fn get(&self, cf: usize, key: &[u8], version: u64) -> Option<Item> {
        let mut version = version;
        if version == 0 {
            version = u64::MAX;
        }
        let val = self.get_value(cf, key, version);
        if val.is_empty() {
            return None;
        }
        if val.is_deleted() {
            return None;
        }
        let item = Item::new(val);
        Some(item)
    }

    fn get_value(&self, cf: usize, key: &[u8], version: u64) -> table::Value {
        let key_hash = farmhash::fingerprint64(key);
        if let Some(split_ctx) = self.splitting {
            let tbl = split_ctx.get_spliting_table(key);
            let v = tbl.get_cf(cf).get(key, version);
            if v.is_valid() {
                return v;
            }
        }
        for i in 0..self.mem_tbls.tbls.len() {
            let tbl = self.mem_tbls.tbls[i].get_cf(cf);
            let v: table::Value;
            if i == 0 {
                v = tbl.get_with_hint(key, version, &mut self.hints[cf].try_borrow_mut().unwrap());
            } else {
                v = tbl.get(key, version);
            }
            if v.is_valid() {
                return v;
            }
        }
        for l0 in &self.l0_tbls.tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                let v = tbl.get(key, version, key_hash);
                if v.is_valid() {
                    return v;
                }
            }
        }
        let scf = self.scfs[cf];
        for lh in &scf.levels {
            let v = lh.get(key, version, key_hash);
            if v.is_valid() {
                return v;
            }
        }
        return table::Value::new();
    }

    pub fn multi_get(&self, cf: usize, keys: &[Vec<u8>], version: u64) -> Vec<Option<Item>> {
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

    pub fn new_iterator(&self, cf: usize, reversed: bool, all_versions: bool) -> Iterator<'a> {
        let read_ts: u64;
        if self.cfs[cf].managed && self.managed_ts != 0 {
            read_ts = self.managed_ts;
        } else {
            read_ts = u64::MAX;
        }
        Iterator {
            all_versions,
            reversed,
            read_ts,
            key: BytesMut::new(),
            val: table::Value::new(),
            inner: self.new_table_iterator(cf, reversed),
        }
    }

    fn new_table_iterator(&self, cf: usize, reversed: bool) -> Box<dyn table::Iterator + 'a> {
        let mut iters: Vec<Box<dyn table::Iterator + 'a>> = Vec::new();
        if let Some(split_ctx) = self.splitting {
            for tbl in &split_ctx.mem_tbls {
                iters.push(Box::new(tbl.get_cf(cf).new_iterator(reversed)));
            }
        }
        for mem_tbl in &self.mem_tbls.tbls {
            iters.push(Box::new(mem_tbl.get_cf(cf).new_iterator(reversed)));
        }
        for l0 in &self.l0_tbls.tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                iters.push(tbl.new_iterator(reversed));
            }
        }
        let scf = self.scfs[cf];
        for lh in &scf.levels {
            if lh.tables.len() == 0 {
                continue;
            }
            iters.push(Box::new(sstable::ConcatIterator::new(&lh.tables, reversed)));
        }
        table::new_merge_iterator(iters, reversed)
    }
}

pub struct Iterator<'a> {
    all_versions: bool,
    reversed: bool,
    read_ts: u64,
    pub key: BytesMut,
    val: table::Value,
    pub inner: Box<dyn table::Iterator + 'a>,
}

impl<'a> Iterator<'a> {
    pub fn valid(&self) -> bool {
        self.val.is_valid()
    }

    pub fn key(&self) -> &[u8] {
        self.key.chunk()
    }

    pub fn item(&self) -> Item {
        Item {
            val: self.val,
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
        self.parse_item();
    }

    pub fn set_all_versions(&mut self, all_versions: bool) {
        self.all_versions = all_versions;
    }
}
