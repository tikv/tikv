// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::Deref,
    ptr,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering::*},
        Arc, Mutex,
    },
};

use crate::table::table::{Iterator, Value};
use bytes::{Buf, BytesMut};

use super::arena::*;
use crate::table::is_deleted;
use crate::SnapAccess;
use std::cmp::Ordering::*;
use std::iter::Iterator as StdIterator;
use std::sync::atomic::Ordering;

pub const MAX_HEIGHT: usize = 14;
const HEIGHT_INCREASE: u32 = u32::MAX / 4;
const RAND_SEED: u32 = 410958445;

pub struct WriteBatch {
    pub(crate) entries: Vec<WriteBatchEntry>,
    pub(crate) buf: BytesMut,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            buf: BytesMut::new(),
        }
    }

    pub fn put(&mut self, key: &[u8], meta: u8, user_meta: &[u8], version: u64, val: &[u8]) {
        let offset = self.buf.len();
        let entry = WriteBatchEntry {
            buf_off: offset as u32,
            meta,
            user_meta_len: user_meta.len() as u8,
            key_len: key.len() as u16,
            version,
            val_len: val.len() as u32,
        };
        self.buf.extend_from_slice(key);
        self.buf.extend_from_slice(user_meta);
        self.buf.extend_from_slice(val);
        self.entries.push(entry);
    }

    pub fn get(&self, idx: usize) -> WriteBatchEntry {
        self.entries[idx]
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn reset(&mut self) {
        self.entries.clear();
        self.buf.clear();
    }

    pub fn iterate<F>(&mut self, f: F)
    where
        F: Fn(&mut WriteBatchEntry, &[u8]),
    {
        for e in &mut self.entries {
            f(e, self.buf.chunk())
        }
    }
}

#[derive(Clone, Copy, Default)]
pub struct WriteBatchEntry {
    buf_off: u32,
    pub meta: u8,
    pub user_meta_len: u8,
    key_len: u16,
    pub version: u64,
    pub val_len: u32,
}

impl WriteBatchEntry {
    pub fn key(self, buf: &[u8]) -> &[u8] {
        let start = self.buf_off as usize;
        let end = start + self.key_len as usize;
        &buf[start..end]
    }

    pub fn user_meta(self, buf: &[u8]) -> &[u8] {
        let start = self.buf_off as usize + self.key_len as usize;
        let end = start + self.user_meta_len as usize;
        &buf[start..end]
    }

    pub fn value(self, buf: &[u8]) -> &[u8] {
        let start = self.buf_off as usize + self.key_len as usize + self.user_meta_len as usize;
        let end = start + self.val_len as usize;
        &buf[start..end]
    }

    pub fn encoded_val_size(self) -> usize {
        1 + 1 + 8 + self.user_meta_len as usize + self.val_len as usize
    }

    pub fn encoded_full_size(self) -> usize {
        2 + self.key_len as usize + 1 + 1 + 8 + self.user_meta_len as usize + self.val_len as usize
    }
}

pub struct Node {
    pub addr: ArenaAddr,
    pub value_addr: AtomicU64,
    pub key_addr: ArenaAddr,
    pub height: usize,
    tower: [AtomicU64; MAX_HEIGHT],
}

impl Node {
    fn get_val_addr(&self) -> ArenaAddr {
        ArenaAddr(self.value_addr.load(SeqCst))
    }

    fn set_val_addr(&self, addr: ArenaAddr) {
        self.value_addr.store(addr.0, SeqCst)
    }

    fn get_next_off(&self, h: usize) -> ArenaAddr {
        ArenaAddr(self.tower[h].load(SeqCst))
    }

    fn cas_next_off(&self, h: usize, current: ArenaAddr, new: ArenaAddr) -> bool {
        let result = self.tower[h].compare_exchange(current.0, new.0, SeqCst, SeqCst);
        !result.is_err()
    }
}

pub fn deref<T>(x: *mut T) -> &'static mut T {
    unsafe { &mut *x }
}

pub fn get_node_offset(node: *mut Node) -> ArenaAddr {
    if node == ptr::null_mut() {
        return ArenaAddr(NULL_ARENA_ADDR);
    }
    deref(node).addr
}

#[derive(Clone)]
pub struct SkipList {
    core: Arc<SkipListCore>,
}

impl Deref for SkipList {
    type Target = SkipListCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl SkipList {
    pub fn new(arena: Option<Arc<Arena>>) -> Self {
        Self {
            core: Arc::new(SkipListCore::new(arena)),
        }
    }

    pub fn new_iterator(&self, reversed: bool) -> SKIterator {
        SKIterator {
            list: self.clone(),
            n: ArenaAddr::null(),
            uk: BytesMut::new(),
            v: Value::new(),
            val_list: Vec::new(),
            val_list_idx: 0,
            reversed,
        }
    }
}

pub struct SkipListCore {
    height: AtomicU32,
    head: ArenaAddr,
    arena: Arc<Arena>,
    rnd_x: AtomicU32,
    data_max_ts: AtomicU64,
    hint: Mutex<Hint>,
}

#[allow(dead_code)]
impl SkipListCore {
    pub fn new(arena: Option<Arc<Arena>>) -> Self {
        let a = arena.unwrap_or(Arc::new(Arena::new()));
        let head_node = a.put_node(MAX_HEIGHT, &[], &WriteBatchEntry::default());
        Self {
            height: AtomicU32::new(1),
            head: head_node.addr,
            arena: a,
            rnd_x: AtomicU32::new(RAND_SEED),
            data_max_ts: AtomicU64::new(0),
            hint: Mutex::new(Hint::new()),
        }
    }

    fn get_head(&self) -> *mut Node {
        self.arena.get_node(self.head)
    }

    fn random_height(&self) -> usize {
        let mut h = 1;
        while h < MAX_HEIGHT && self.next_rand() <= HEIGHT_INCREASE {
            h += 1;
        }
        h
    }

    // See https://en.wikipedia.org/wiki/Xorshift
    fn next_rand(&self) -> u32 {
        let mut x = self.rnd_x.load(SeqCst);
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.rnd_x.store(x, SeqCst);
        x
    }

    fn get_next(&self, n: ArenaAddr, height: usize) -> ArenaAddr {
        let n = self.arena.get_node(n);
        if n.is_null() {
            return ArenaAddr(NULL_ARENA_ADDR);
        }
        deref(n).get_next_off(height)
    }

    fn get_height(&self) -> usize {
        self.height.load(Acquire) as usize
    }

    pub fn put_batch(&self, batch: &mut WriteBatch, snap: &SnapAccess, cf: usize) {
        let mut hint = self.hint.lock().unwrap();
        let data_max_ts = self.data_max_ts.load(Ordering::Acquire);
        let mut batch_max_ts = 0;
        for i in 0..batch.entries.len() {
            let entry = &batch.entries[i];
            if is_deleted(entry.meta) {
                let key = entry.key(&batch.buf);
                if snap.contains_in_older_table(key, cf) {
                    self.put_with_hint(&batch.buf, entry, &mut hint);
                } else {
                    self.delete_with_hint(key, &mut hint);
                }
            } else {
                if batch_max_ts < entry.version {
                    batch_max_ts = entry.version;
                }
                self.put_with_hint(&batch.buf, entry, &mut hint);
            }
        }
        if batch_max_ts > data_max_ts {
            self.data_max_ts.store(batch_max_ts, Ordering::Release);
        }
    }

    pub fn put(&self, buf: &[u8], entry: &WriteBatchEntry) {
        let h = &mut Hint::new();
        self.put_with_hint(buf, entry, h)
    }

    pub fn put_with_hint(&self, buf: &[u8], entry: &WriteBatchEntry, h: &mut Hint) {
        // Since we allow overwrite, we may not need to create a new node. We might not even need to
        // increase the height. Let's defer these actions.
        let mut list_height = self.get_height();
        let height = self.random_height();

        if height > list_height {
            // As write is single threaded, we don't need CAS.
            self.height.store(height as u32, Release);
            list_height = height;
        }
        let mut splice_valid = true;
        let key = entry.key(buf);
        let recomput_height = self.calculate_recompute_height(key, h, list_height);
        if recomput_height > 0 {
            for i in (0..recomput_height).rev() {
                let (prev, next, matched) = self.find_splice_for_level(key, h.prev[i + 1], i);
                h.prev[i] = prev;
                h.next[i] = next;
                if matched {
                    // In-place update.
                    let node = deref(self.arena.get_node(h.next[i]));
                    self.set_value(node, buf, entry);
                    let mut j = i;
                    while j > 0 {
                        h.prev[j - 1] = h.prev[j];
                        h.next[j - 1] = h.next[j];
                        j -= 1;
                    }
                    return;
                }
            }
        } else {
            // Even the recomputeHeight is 0, we still need to check match and do in place update to insert the new version.
            if !h.next[0].is_null() {
                let node = deref(self.arena.get_node(h.next[0]));
                if self.arena.get_key(node).eq(key) {
                    self.set_value(node, buf, entry);
                    return;
                }
            }
        }

        // We do need to create a new node.
        let x = self.arena.put_node(height, buf, entry);

        // We always insert from the base level and up. After you add a node in base level, we cannot
        // create a node in the level above because it would have discovered the node in the base level.
        for i in 0..height {
            loop {
                let next_off = h.next[i];
                x.tower[i].store(next_off.0, SeqCst);
                if deref(self.arena.get_node(h.prev[i])).cas_next_off(i, next_off, x.addr) {
                    // Managed to insert x between prev[i] and next[i]. Go to the next level.
                    break;
                }
                // CAS failed. We need to recompute prev and next.
                // It is unlikely to be helpful to try to use a different level as we redo the search,
                // because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
                let (prev, next, _) = self.find_splice_for_level(key, h.prev[i], i);
                h.prev[i] = prev;
                h.next[i] = next;
                if i > 0 {
                    splice_valid = false
                }
            }
        }
        if splice_valid {
            for i in 0..height {
                h.prev[i] = x.addr;
                h.next[i] = self.get_next(x.addr, i);
            }
        } else {
            h.height = 0;
        }
    }

    pub fn set_value(&self, node: &mut Node, buf: &[u8], entry: &WriteBatchEntry) {
        {
            // check old value version.
            let old_val_addr = node.get_val_addr();
            let old_val_off: ArenaAddr;
            if old_val_addr.is_value_node_addr() {
                let vn = self.arena.get_value_node(old_val_addr);
                old_val_off = vn.val_addr;
            } else {
                old_val_off = old_val_addr;
            }
            let old_v = self.arena.get_val(old_val_off);
            if entry.version <= old_v.version {
                // Only happens in Restore backup, do nothing.
                return;
            }
        }
        let new_val_addr = self.arena.put_val(buf, entry);
        let vn = ValueNode {
            val_addr: new_val_addr,
            next_val_addr: node.get_val_addr(),
        };
        let vn_addr = self.arena.put_val_node(vn);
        node.set_val_addr(vn_addr);
    }

    pub fn get_with_hint(&self, key: &[u8], version: u64, h: &mut Hint) -> Value {
        let list_height = self.get_height();
        let recompute_height = self.calculate_recompute_height(key, h, list_height as usize);
        let mut n = ArenaAddr(NULL_ARENA_ADDR);
        if recompute_height > 0 {
            for i in (0..recompute_height).rev() {
                let (prev, next, matched) = self.find_splice_for_level(key, h.prev[i + 1], i);
                h.prev[i] = prev;
                h.next[i] = next;
                if matched {
                    n = next;
                    for j in (0..i).rev() {
                        h.prev[j] = n;
                        h.next[j] = self.get_next(n, j);
                    }
                    break;
                }
            }
        } else {
            n = h.next[0];
        }
        if n.is_null() {
            return Value::new();
        }
        let n_node = deref(self.arena.get_node(n));
        let next_key = self.arena.get_key(n_node);
        if next_key.ne(key) {
            return Value::new();
        }
        let mut val_off = n_node.get_val_addr();
        while val_off.is_value_node_addr() {
            let vn = self.arena.get_value_node(val_off);
            let v = self.arena.get_val(vn.val_addr);
            if v.version <= version {
                return v;
            }
            if vn.next_val_addr.0 == NULL_ARENA_ADDR {
                return Value::new();
            }
            val_off = vn.next_val_addr;
        }
        self.arena.get_val(val_off)
    }

    pub fn calculate_recompute_height(
        &self,
        key: &[u8],
        h: &mut Hint,
        list_height: usize,
    ) -> usize {
        if h.height < list_height {
            // Either splice is never used or list height has grown, we recompute all.
            h.prev[list_height] = self.head;
            h.next[list_height] = ArenaAddr::null();
            h.height = list_height;
            h.hit_height = h.height;
            return list_height;
        }
        let mut recompute_height = match h.hit_height.cmp(&2) {
            Less => 0,
            _ => h.hit_height - 2,
        };
        while recompute_height < list_height {
            let prev_node = h.prev[recompute_height];
            let next_node = h.next[recompute_height];
            let prev_next = self.get_next(prev_node, recompute_height);
            if prev_next != next_node {
                recompute_height += 1;
                continue;
            }
            if prev_node != self.head && !prev_node.is_null() {
                let prev_node_node = deref(self.arena.get_node(prev_node));
                if prev_node_node.addr.is_null() {
                    error!(
                        "get node {} on arena rand_id {}",
                        prev_node.0, self.arena.rand_id
                    );
                }
                if key <= self.arena.get_key(prev_node_node) {
                    // Key is before splice.
                    while prev_node == h.prev[recompute_height] {
                        recompute_height += 1;
                    }
                    continue;
                }
            }
            if !next_node.is_null()
                && key > self.arena.get_key(deref(self.arena.get_node(next_node)))
            {
                // Key is after splice.
                while next_node == h.next[recompute_height] {
                    recompute_height += 1;
                }
                continue;
            }
            break;
        }
        h.hit_height = recompute_height;
        recompute_height
    }

    fn find_splice_for_level(
        &self,
        key: &[u8],
        before: ArenaAddr,
        level: usize,
    ) -> (ArenaAddr, ArenaAddr, bool) {
        let mut before = before;
        loop {
            // Assume before.key < key.
            let next = self.get_next(before, level);
            if next.0 == NULL_ARENA_ADDR {
                return (before, next, false);
            }
            let next_node = self.arena.get_node(next);
            let next_key = self.arena.get_key(deref(next_node));
            let order = key.cmp(next_key);
            if order != Greater {
                return (before, next, order == Equal);
            }
            before = next;
        }
    }

    fn find_near(&self, key: &[u8], less: bool, allow_eq: bool) -> (ArenaAddr, bool) {
        let mut x = self.head;
        let mut level = self.get_height();
        let mut after_node = ArenaAddr::null();
        loop {
            // Assume x.key < key.
            let next = self.get_next(x, level);
            if next.is_null() {
                // x.key < key < END OF LIST
                if level > 0 {
                    // Can descend further to iterate closer to the end.
                    level -= 1;
                    continue;
                }
                // Level=0. Cannot descend further. Let's return something that makes sense.
                if !less {
                    return (ArenaAddr::null(), false);
                }
                // Try to return x. Make sure it is not a head node.
                if x == self.head {
                    return (ArenaAddr::null(), false);
                }
                return (x, false);
            }
            let cmp: std::cmp::Ordering;
            if next == after_node {
                // We compared the same node on the upper level, no need to compare again.
                cmp = Less;
            } else {
                let next_key = self.arena.get_key(deref(self.arena.get_node(next)));
                cmp = key.cmp(next_key);
            }
            if cmp == Greater {
                // x.key < next.key < key. We can continue to move right.
                x = next;
                continue;
            }
            if cmp == Equal {
                // x.key < key == next.key.
                if allow_eq {
                    return (next, true);
                }
                if !less {
                    // We want >, so go to base level to grab the next bigger note.
                    return (self.get_next(next, 0), false);
                }
                // We want <. If not base level, we should go closer in the next level.
                if level > 0 {
                    level -= 1;
                    continue;
                }
                // On base level. Return x.
                if x == self.head {
                    return (ArenaAddr::null(), false);
                }
                return (x, false);
            }
            // cmp < 0. In other words, x.key < key < next.
            if level > 0 {
                after_node = next;
                level -= 1;
                continue;
            }
            // At base level. Need to return something.
            if !less {
                return (next, false);
            }
            // Try to return x. Make sure it is not a head node.
            if x == self.head {
                return (ArenaAddr::null(), false);
            }
            return (x, false);
        }
    }

    // find_last returns the last element. If head (empty list), we return nil. All the find functions
    // will NEVER return the head nodes.
    fn find_last(&self) -> ArenaAddr {
        let mut n = self.head;
        let mut level = self.height.load(SeqCst) - 1;
        loop {
            let next = self.get_next(n, level as usize);
            if !next.is_null() {
                n = next;
                continue;
            }
            if level == 0 {
                if n == self.head {
                    return ArenaAddr::null();
                }
                return n;
            }
            level -= 1;
        }
    }

    pub fn get(&self, key: &[u8], version: u64) -> Value {
        let (n, _) = self.find_near(key, false, true);
        if n.is_null() {
            return Value::new();
        }
        let n_node = deref(self.arena.get_node(n));
        let next_key = self.arena.get_key(n_node);
        if key.ne(next_key) {
            return Value::new();
        }
        let mut value_off = n_node.get_val_addr();
        while value_off.is_value_node_addr() {
            let vn = self.arena.get_value_node(value_off);
            let v = self.arena.get_val(vn.val_addr);
            if version >= v.version {
                return v;
            }
            value_off = vn.next_val_addr;
        }
        let v = self.arena.get_val(value_off);
        if version >= v.version {
            return v;
        }
        Value::new()
    }

    pub fn get_newer(&self, key: &[u8], version: u64) -> Value {
        if self.data_max_ts.load(Ordering::Acquire) < version {
            return Value::new();
        }
        let val = self.get(key, u64::MAX);
        if val.version >= version {
            return val;
        }
        Value::new()
    }

    pub fn is_empty(&self) -> bool {
        self.get_next(self.head, 0).is_null()
    }

    pub fn delete_with_hint(&self, key: &[u8], h: &mut Hint) -> bool {
        let list_height = self.get_height();
        let recompute_height = self.calculate_recompute_height(key, h, list_height);
        let mut node_addr = ArenaAddr(NULL_ARENA_ADDR);
        if recompute_height > 0 {
            for i in (0..recompute_height).rev() {
                let (prev, next, matched) = self.find_splice_for_level(key, h.prev[i + 1], i);
                h.prev[i] = prev;
                h.next[i] = next;
                if matched {
                    node_addr = next;
                }
            }
        } else {
            node_addr = h.next[0];
        }
        if node_addr.is_null() {
            return false;
        }
        let node = deref(self.arena.get_node(node_addr));
        for i in (0..node.height).rev() {
            let prev_node = deref(self.arena.get_node(h.prev[i]));
            let next_addr = node.get_next_off(i);
            assert!(prev_node.cas_next_off(i, node_addr, next_addr));
            h.next[i] = next_addr;
        }
        true
    }
}

// Hint is used to speed up sequential write.
#[derive(Debug)]
pub struct Hint {
    height: usize,

    // hitHeight is used to reduce cost of calculate_recomput_height.
    // For random workload, comparing Hint keys from bottom up is wasted work.
    // So we record the hit height of the last operation, only grow recompute height from near that height.
    hit_height: usize,
    prev: [ArenaAddr; MAX_HEIGHT + 1],
    next: [ArenaAddr; MAX_HEIGHT + 1],
}

impl Hint {
    pub fn new() -> Hint {
        Hint {
            height: 0,
            hit_height: 0,
            prev: [ArenaAddr::null(); MAX_HEIGHT + 1],
            next: [ArenaAddr::null(); MAX_HEIGHT + 1],
        }
    }
}

pub struct SKIterator {
    list: SkipList,
    n: ArenaAddr,

    uk: BytesMut,
    v: Value,
    val_list: Vec<ArenaAddr>,
    val_list_idx: usize,
    reversed: bool,
}

unsafe impl Send for SKIterator {}

#[allow(dead_code)]
impl SKIterator {
    fn load_node(&mut self) {
        if self.n.is_null() {
            return;
        }
        if self.val_list.len() > 0 {
            self.val_list.truncate(0);
            self.val_list_idx = 0;
        }
        self.uk.truncate(0);
        let n_node = deref(self.list.arena.get_node(self.n));
        self.uk.extend_from_slice(self.list.arena.get_key(n_node));
        let mut val_addr = n_node.get_val_addr();
        if !val_addr.is_value_node_addr() {
            self.v = self.list.arena.get_val(val_addr);
            return;
        }
        loop {
            let vn = self.list.arena.get_value_node(val_addr);
            self.val_list.push(vn.val_addr);
            val_addr = vn.next_val_addr;
            if !val_addr.is_value_node_addr() {
                self.val_list.push(val_addr);
                break;
            }
        }
        self.set_value_list_idx(0);
    }

    fn set_value_list_idx(&mut self, idx: usize) {
        self.val_list_idx = idx;
        let off = self.val_list[idx];
        self.v = self.list.arena.get_val(off);
    }

    fn seek_to_first(&mut self) {
        self.n = self.list.get_next(self.list.head, 0);
        self.load_node()
    }

    fn seek_to_last(&mut self) {
        self.n = self.list.find_last();
        self.load_node()
    }

    fn seek_for_next(&mut self, key: &[u8]) {
        let (n, _) = self.list.find_near(key, false, true);
        self.n = n;
        self.load_node()
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        let (n, _) = self.list.find_near(key, true, true);
        self.n = n;
        self.load_node()
    }

    fn next_forward(&mut self) {
        self.n = self.list.get_next(self.n, 0);
        self.load_node()
    }

    fn next_backward(&mut self) {
        let (n, _) = self.list.find_near(self.uk.chunk(), true, false);
        self.n = n;
        self.load_node()
    }
}

impl Iterator for SKIterator {
    fn next(&mut self) {
        if self.reversed {
            self.next_backward()
        } else {
            self.next_forward()
        }
    }

    fn next_version(&mut self) -> bool {
        if self.val_list_idx + 1 < self.val_list.len() {
            self.set_value_list_idx(self.val_list_idx + 1);
            return true;
        }
        false
    }

    fn rewind(&mut self) {
        if self.reversed {
            self.seek_to_last()
        } else {
            self.seek_to_first()
        }
    }

    fn seek(&mut self, key: &[u8]) {
        if self.reversed {
            self.seek_for_prev(key)
        } else {
            self.seek_for_next(key)
        }
    }

    fn key(&self) -> &[u8] {
        self.uk.chunk()
    }

    fn value(&self) -> Value {
        self.v
    }

    fn valid(&self) -> bool {
        !self.n.is_null()
    }
}

#[cfg(test)]
mod tests {
    use byteorder::{ByteOrder, LittleEndian};
    use rand::Rng;

    use crate::table::memtable::WriteBatch;

    use super::*;

    const ARENA_SIZE: usize = 1 << 20;

    fn new_key(i: i32) -> String {
        format!("key{:05}", i)
    }

    fn new_value(v: i32) -> String {
        format!("{:05}", v)
    }

    #[test]
    fn test_empty() {
        let key = "aaa".as_bytes();
        let l = SkipList::new(None);
        let v = l.get(key, 1);
        assert_eq!(v.is_empty(), true);

        for less in vec![true, false] {
            for allow_eq in vec![true, false] {
                let (n, found) = l.find_near(key, less, allow_eq);
                assert!(n.is_null());
                assert_eq!(found, false);
            }
        }

        let mut it = l.new_iterator(false);
        assert_eq!(it.valid(), false);
        it.seek_to_first();
        assert_eq!(it.valid(), false);
        it.seek_to_last();
        assert_eq!(it.valid(), false);
        it.seek(key);
        assert_eq!(it.valid(), false);
    }

    #[test]
    #[ignore]
    fn test_basic() {
        let l = SkipList::new(None);
        let val1 = new_value(42);
        let val2 = new_value(52);
        let val3 = new_value(62);
        let val4 = new_value(72);

        let mut wb = WriteBatch::new();

        wb.put("key1".as_bytes(), 55, &[0], 0, val1.as_bytes());
        wb.put("key2".as_bytes(), 56, &[0], 2, val2.as_bytes());
        wb.put("key3".as_bytes(), 57, &[0], 0, val3.as_bytes());

        // l.put_batch(&mut wb);

        let mut v = l.get("key".as_bytes(), 0);
        assert_eq!(v.is_empty(), true);

        v = l.get("key1".as_bytes(), 0);
        assert_eq!(v.is_empty(), false);
        assert_eq!(v.get_value(), "00042".as_bytes());
        assert_eq!(v.meta, 55);

        v = l.get("key2".as_bytes(), 0);
        assert_eq!(v.is_empty(), true);

        v = l.get("key3".as_bytes(), 0);
        assert_eq!(v.is_empty(), false);
        assert_eq!(v.get_value(), "00062".as_bytes());
        assert_eq!(v.meta, 57);

        let mut wb = WriteBatch::new();
        wb.put("key3".as_bytes(), 12, &[0], 1, val4.as_bytes());
        // l.put_batch(&mut wb);
        v = l.get("key3".as_bytes(), 1);
        assert_eq!(v.is_empty(), false);
        assert_eq!(v.get_value(), "00072".as_bytes());
        assert_eq!(v.meta, 12);
    }

    #[test]
    #[ignore]
    fn test_find_near() {
        let l = SkipList::new(None);
        for i in 0..1000 {
            let key = format!("{:05}", i * 10 + 5);
            let mut wb = WriteBatch::new();
            wb.put(key.as_bytes(), 0, &[], 0, new_value(i).as_bytes());
            // l.put_batch(&mut wb);
        }

        let (n, eq) = l.find_near("00001".as_bytes(), false, false);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "00005".as_bytes()
        );
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("00001".as_bytes(), false, true);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "00005".as_bytes()
        );
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("00001".as_bytes(), true, false);
        assert_eq!(n.is_null(), true);
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("00001".as_bytes(), true, true);
        assert_eq!(n.is_null(), true);
        assert_eq!(eq, false);

        let (n, eq) = l.find_near("00005".as_bytes(), false, false);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "00015".as_bytes()
        );
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("00005".as_bytes(), false, true);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "00005".as_bytes()
        );
        assert_eq!(eq, true);
        let (n, eq) = l.find_near("00005".as_bytes(), true, false);
        assert_eq!(n.is_null(), true);
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("00005".as_bytes(), true, true);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "00005".as_bytes()
        );
        assert_eq!(eq, true);

        let (n, eq) = l.find_near("05555".as_bytes(), false, false);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "05565".as_bytes()
        );
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("05555".as_bytes(), false, true);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "05555".as_bytes()
        );
        assert_eq!(eq, true);
        let (n, eq) = l.find_near("05555".as_bytes(), true, false);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "05545".as_bytes()
        );
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("05555".as_bytes(), true, true);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "05555".as_bytes()
        );
        assert_eq!(eq, true);

        let (n, eq) = l.find_near("05558".as_bytes(), false, false);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "05565".as_bytes()
        );
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("05558".as_bytes(), false, true);
        assert_eq!(n.is_null(), false);

        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "05565".as_bytes()
        );
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("05558".as_bytes(), true, false);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "05555".as_bytes()
        );
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("05558".as_bytes(), true, true);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "05555".as_bytes()
        );
        assert_eq!(eq, false);

        let (n, eq) = l.find_near("09995".as_bytes(), false, false);
        assert_eq!(n.is_null(), true);
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("09995".as_bytes(), false, true);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "09995".as_bytes()
        );
        assert_eq!(eq, true);
        let (n, eq) = l.find_near("09995".as_bytes(), true, false);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "09985".as_bytes()
        );
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("09995".as_bytes(), true, true);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "09995".as_bytes()
        );
        assert_eq!(eq, true);

        let (n, eq) = l.find_near("59995".as_bytes(), false, false);
        assert_eq!(n.is_null(), true);
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("59995".as_bytes(), false, true);
        assert_eq!(n.is_null(), true);
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("59995".as_bytes(), true, false);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "09995".as_bytes()
        );
        assert_eq!(eq, false);
        let (n, eq) = l.find_near("59995".as_bytes(), true, true);
        assert_eq!(n.is_null(), false);
        assert_eq!(
            l.arena.get_key(deref(l.arena.get_node(n))),
            "09995".as_bytes()
        );
        assert_eq!(eq, false);
    }

    #[test]
    #[ignore]
    fn test_iterator_next() {
        let n = 100;
        let l = SkipList::new(None);
        let mut it = l.new_iterator(false);
        assert_eq!(it.valid(), false);
        it.seek_to_first();
        assert_eq!(it.valid(), false);
        for i in 0..n {
            let mut wb = WriteBatch::new();
            wb.put(new_key(i).as_bytes(), 0, &[0], 0, new_value(i).as_bytes());
            // l.put_batch(&mut wb)
        }
        let mut it = l.new_iterator(false);
        it.rewind();
        for i in 0..n {
            assert_eq!(it.key(), new_key(i).as_bytes());
            assert_eq!(it.value().get_value(), new_value(i).as_bytes());
            it.next();
        }
        assert_eq!(it.valid(), false);
    }

    #[test]
    #[ignore]
    fn test_iterator_prev() {
        let n = 100;
        let l = SkipList::new(None);
        let mut it = l.new_iterator(true);
        assert_eq!(it.valid(), false);
        it.seek_to_first();
        assert_eq!(it.valid(), false);
        for i in (0..n).rev() {
            let mut wb = WriteBatch::new();
            wb.put(new_key(i).as_bytes(), 0, &[0], 0, new_value(i).as_bytes());
            // l.put_batch(&mut wb)
        }
        it.seek_to_last();
        for i in (0..n).rev() {
            assert_eq!(it.valid(), true);
            assert_eq!(it.value().get_value(), new_value(i).as_bytes());
            it.next();
        }
        assert_eq!(it.valid(), false);
    }

    #[test]
    #[ignore]
    fn test_iterator_seek() {
        let n = 100;
        let l = SkipList::new(None);
        let mut it = l.new_iterator(false);
        assert_eq!(it.valid(), false);
        it.seek_to_first();
        assert_eq!(it.valid(), false);
        // 1000, 1010, 1020, ..., 1990.
        for i in (0..n).rev() {
            let v = i * 10 + 1000;
            let key = format!("{:05}", v);
            let mut wb = WriteBatch::new();
            wb.put(key.as_bytes(), 0, &[0], 0, new_value(v).as_bytes());
            // l.put_batch(&mut wb)
        }
        it.seek_to_first();
        assert_eq!(it.valid(), true);
        assert_eq!(it.value().get_value(), "01000".as_bytes());

        it.seek("01000".as_bytes());
        assert_eq!(it.valid(), true);
        assert_eq!(it.value().get_value(), "01000".as_bytes());

        it.seek("01005".as_bytes());
        assert_eq!(it.valid(), true);
        assert_eq!(it.value().get_value(), "01010".as_bytes());

        it.seek("01010".as_bytes());
        assert_eq!(it.valid(), true);
        assert_eq!(it.value().get_value(), "01010".as_bytes());

        it.seek("99999".as_bytes());
        assert_eq!(it.valid(), false);

        // try seek for prev
        it.seek_for_prev("00".as_bytes());
        assert_eq!(it.valid(), false);

        it.seek_for_prev("01000".as_bytes());
        assert_eq!(it.valid(), true);
        assert_eq!(it.value().get_value(), "01000".as_bytes());

        it.seek_for_prev("01005".as_bytes());
        assert_eq!(it.valid(), true);
        assert_eq!(it.value().get_value(), "01000".as_bytes());

        it.seek_for_prev("01010".as_bytes());
        assert_eq!(it.valid(), true);
        assert_eq!(it.value().get_value(), "01010".as_bytes());

        it.seek_for_prev("99999".as_bytes());
        assert_eq!(it.valid(), true);
        assert_eq!(it.value().get_value(), "01990".as_bytes());
    }

    fn random_key() -> Vec<u8> {
        let mut key = vec![0u8; 8];
        let buf = key.as_mut_slice();
        let mut rng = rand::thread_rng();
        let n1 = rng.gen::<u32>();
        let n2 = rng.gen::<u32>();
        LittleEndian::write_u32(buf, n1);
        LittleEndian::write_u32(&mut buf[4..], n2);
        key
    }

    #[test]
    #[ignore]
    fn test_put_with_hint() {
        let l = SkipList::new(None);
        let mut wb = WriteBatch::new();
        let mut cnt = 0;
        loop {
            if l.arena.size() > ARENA_SIZE - 256 {
                break;
            }
            let key = random_key();
            wb.put(key.as_slice(), 0, &[], 0, key.as_slice());
            cnt += 1;
            // l.put_batch(&mut wb);
            wb.reset();
        }
        let mut it = l.new_iterator(false);
        let mut last_key = Vec::new();
        let mut cnt_got = 0;
        it.seek_to_first();
        while it.valid() {
            assert_eq!(last_key.as_slice() <= it.key(), true);
            assert_eq!(it.key() == it.value().get_value(), true);
            cnt_got += 1;
            last_key.truncate(0);
            last_key.extend_from_slice(it.key());
            it.next();
        }
        assert_eq!(cnt, cnt_got);
    }
}
