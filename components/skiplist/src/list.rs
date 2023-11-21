// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{
    mem, ptr,
    ptr::NonNull,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    u32,
};

use bytes::Bytes;
use rand::Rng;
use slog_global::info;

use super::{arena::Arena, KeyComparator, MAX_HEIGHT};
use crate::arena::{tag, without_tag};

const HEIGHT_INCREASE: u32 = u32::MAX / 3;
pub const MAX_NODE_SIZE: usize = mem::size_of::<Node>();

/// A search result.
///
/// The result indicates whether the key was found, as well as what were the
/// adjacent nodes to the key on each level of the skip list.
struct Position {
    /// Reference to a node with the given key, if found.
    ///
    /// If this is `Some` then it will point to the same node as `right[0]`.
    found: Option<*mut Node>,

    /// Adjacent nodes with smaller keys (predecessors).
    left: [*mut Node; MAX_HEIGHT + 1],

    /// Adjacent nodes with equal or greater keys (successors).
    right: [*mut Node; MAX_HEIGHT + 1],
}

// Uses C layout to make sure tower is at the bottom
#[derive(Debug)]
#[repr(C)]
pub struct Node {
    key: Bytes,
    value: Bytes,
    height: usize,
    // PrevList for fast reverse scan.
    prev: AtomicUsize,
    tower: [AtomicUsize; MAX_HEIGHT],
}

impl Node {
    fn alloc(arena: &Arena, key: Bytes, value: Bytes, height: usize) -> usize {
        let size = mem::size_of::<Node>();
        // Not all values in Node::tower will be utilized.
        let not_used = (MAX_HEIGHT - height - 1) * mem::size_of::<AtomicUsize>();
        let node_offset = arena.alloc(size - not_used);
        unsafe {
            let node_ptr: *mut Node = arena.get_mut(node_offset);
            let node = &mut *node_ptr;
            ptr::write(&mut node.key, key);
            ptr::write(&mut node.value, value);
            node.height = height;
            ptr::write_bytes(node.tower.as_mut_ptr(), 0, height + 1);
        }
        node_offset
    }

    fn mark_tower(&self) -> bool {
        let height = self.height;

        for level in (0..height).rev() {
            let tag = { self.tower[level].fetch_or(1, Ordering::SeqCst) & 1 };

            // If the level 0 pointer was already marked, somebody else removed the node.
            if level == 0 && tag == 1 {
                return false;
            }
        }

        // We marked the level 0 pointer, therefore we removed the node.
        true
    }

    fn next_offset(&self, height: usize) -> usize {
        self.tower[height].load(Ordering::SeqCst)
    }
}

struct SkiplistInner {
    height: AtomicUsize,
    head: NonNull<Node>,
    arena: Arena,
}

#[derive(Clone)]
pub struct Skiplist<C: Clone> {
    inner: Arc<SkiplistInner>,
    c: C,
    allow_concurrent_write: bool,
}

impl<C: Clone> Skiplist<C> {
    pub fn with_capacity(c: C, arena_size: usize, allow_concurrent_write: bool) -> Skiplist<C> {
        let arena = Arena::with_capacity(arena_size);
        let head_offset = Node::alloc(&arena, Bytes::new(), Bytes::new(), MAX_HEIGHT - 1);
        let head = unsafe { NonNull::new_unchecked(arena.get_mut(head_offset)) };
        Skiplist {
            inner: Arc::new(SkiplistInner {
                height: AtomicUsize::new(0),
                head,
                arena,
            }),
            c,
            allow_concurrent_write,
        }
    }

    fn random_height(&self) -> usize {
        let mut rng = rand::thread_rng();
        for h in 0..(MAX_HEIGHT - 1) {
            if !rng.gen_ratio(HEIGHT_INCREASE, u32::MAX) {
                return h;
            }
        }
        MAX_HEIGHT - 1
    }

    fn height(&self) -> usize {
        self.inner.height.load(Ordering::SeqCst)
    }
}

impl<C: KeyComparator> Skiplist<C> {
    /// Finds the node near to key.
    ///
    /// If less=true, it finds rightmost node such that node.key < key (if
    /// allow_equal=false) or node.key <= key (if allow_equal=true).
    /// If less=false, it finds leftmost node such that node.key > key (if
    /// allow_equal=false) or node.key >= key (if allow_equal=true).
    ///
    /// Returns the node found. The bool returned is true if the node has key
    /// equal to given key.
    unsafe fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> *const Node {
        let mut cursor: *const Node = self.inner.head.as_ptr();
        let mut level = self.height();
        loop {
            let next_offset = (*cursor).next_offset(level);
            if next_offset == 0 {
                if level > 0 {
                    level -= 1;
                    continue;
                }
                if !less || cursor == self.inner.head.as_ptr() {
                    return ptr::null();
                }
                return cursor;
            }
            let next_ptr: *mut Node = self.inner.arena.get_mut(next_offset);
            let next = &*next_ptr;
            let res = self.c.compare_key(key, &next.key);
            if res == std::cmp::Ordering::Greater {
                cursor = next_ptr;
                continue;
            }
            if res == std::cmp::Ordering::Equal {
                if allow_equal {
                    return next;
                }
                if !less {
                    let offset = next.next_offset(0);
                    if offset != 0 {
                        return self.inner.arena.get_mut(offset);
                    } else {
                        return ptr::null();
                    }
                }
                if level > 0 {
                    level -= 1;
                    continue;
                }
                if cursor == self.inner.head.as_ptr() {
                    return ptr::null();
                }
                return cursor;
            }
            if level > 0 {
                level -= 1;
                continue;
            }
            if !less {
                return next;
            }
            if cursor == self.inner.head.as_ptr() {
                return ptr::null();
            }
            return cursor;
        }
    }

    /// Returns (nodeBefore, nodeAfter) with nodeBefore.key <= key <=
    /// nodeAfter.key.
    ///
    /// The input "before" tells us where to start looking.
    /// If we found a node with the same key, then we return nodeBefore =
    /// nodeAfter. Otherwise, nodeBefore.key < key < nodeAfter.key.
    unsafe fn find_splice_for_level_buck(
        &self,
        key: &[u8],
        mut before: *mut Node,
        level: usize,
    ) -> (*mut Node, *mut Node) {
        loop {
            let next_offset = (*before).next_offset(level);
            if next_offset == 0 {
                return (before, ptr::null_mut());
            }
            let next_ptr: *mut Node = self.inner.arena.get_mut(next_offset);
            let next_node = &*next_ptr;
            match self.c.compare_key(key, &next_node.key) {
                std::cmp::Ordering::Equal => return (next_ptr, next_ptr),
                std::cmp::Ordering::Less => return (before, next_ptr),
                _ => before = next_ptr,
            }
        }
    }

    unsafe fn find_splice_for_level(
        &self,
        key: &[u8],
        mut before: *mut Node,
        level: usize,
        for_remove: bool,
    ) -> (*mut Node, *mut Node) {
        'search: loop {
            let mut prev = before;
            let current_offset = (*prev).next_offset(level);

            let mut help_unlik = true;
            if tag(current_offset) == 1 {
                panic!("why here");
                // before is marked, but we have not idea what is the before of the before
                help_unlik = false;
            }

            if without_tag(current_offset) == 0 {
                return (prev, ptr::null_mut());
            }
            let mut current: *mut Node = self.inner.arena.get_mut(current_offset);

            while !current.is_null() {
                let succ_offset = (*current).tower[level].load(Ordering::SeqCst);
                if tag(succ_offset) == 1 {
                    if help_unlik {
                        // current node is marked, help to unlink
                        if let Some(c) = self.help_unlink(prev, current_offset, succ_offset, level)
                        {
                            if c == 0 {
                                return (prev, ptr::null_mut());
                            }
                            current = self.inner.arena.get_mut(c);
                            continue;
                        } else {
                            // On failure, we cannot do anything reasonable to continue
                            // searching from the current position. Restart the search.
                            continue 'search;
                        }
                    }
                }

                match self.c.compare_key(key, &(*current).key) {
                    std::cmp::Ordering::Equal => {
                        if for_remove {
                            return (prev, current);
                        } else {
                            return (current, current);
                        }
                    }
                    std::cmp::Ordering::Less => return (prev, current),
                    _ => {
                        if without_tag(succ_offset) == 0 {
                            return (current, ptr::null_mut());
                        }
                        prev = current;
                        current = self.inner.arena.get_mut(succ_offset);
                    }
                }
            }

            return (prev, current);
        }
    }

    unsafe fn help_unlink(
        &self,
        prev: *mut Node,
        curr: usize,
        succ: usize,
        level: usize,
    ) -> Option<usize> {
        // If `succ` is marked, that means `curr` is removed. Let's try
        // unlinking it from the skip list at this level.
        match (*prev).tower[level].compare_exchange(
            curr,
            without_tag(succ),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => Some(without_tag(succ)),
            Err(_) => None,
        }
    }

    pub fn split(&self, split_keys: Vec<impl Into<Bytes>>) -> Vec<Skiplist<C>> {
        let num = split_keys.len();
        let mut sklists = vec![];
        let mut iter = self.iter();
        iter.seek_to_first();
        for split_key in split_keys {
            let sk = Skiplist::with_capacity(
                self.c.clone(),
                self.inner.arena.cap(),
                self.allow_concurrent_write,
            );

            let split_key = split_key.into();
            while iter.valid()
                && self.c.compare_key(iter.key().as_slice(), &split_key) == std::cmp::Ordering::Less
            {
                sk.put(iter.key().clone(), iter.value().clone());
                iter.next();
            }
            sklists.push(sk);
        }

        let sk = Skiplist::with_capacity(
            self.c.clone(),
            self.inner.arena.cap(),
            self.allow_concurrent_write,
        );
        while iter.valid() {
            sk.put(iter.key().clone(), iter.value().clone());
            iter.next();
        }
        sklists.push(sk);

        assert_eq!(sklists.len(), num + 1);
        sklists
    }

    unsafe fn find_prev_for_level(
        &self,
        key: &[u8],
        mut before: *mut Node,
        level: usize,
    ) -> (*mut Node, *mut Node) {
        loop {
            let next_offset = (*before).next_offset(level);
            if next_offset == 0 {
                return (before, ptr::null_mut());
            }
            let next_ptr: *mut Node = self.inner.arena.get_mut(next_offset);
            let next_node = &*next_ptr;
            match self.c.compare_key(key, &next_node.key) {
                std::cmp::Ordering::Equal | std::cmp::Ordering::Less => return (before, next_ptr),
                _ => before = next_ptr,
            }
        }
    }

    unsafe fn search_for_remove(&self, key: &[u8]) -> Position {
        let list_height = self.height();
        let mut list_height = self.height();
        let mut left = [ptr::null_mut(); MAX_HEIGHT + 1];
        let mut right = [ptr::null_mut(); MAX_HEIGHT + 1];
        left[list_height + 1] = self.inner.head.as_ptr();
        right[list_height + 1] = ptr::null_mut();
        let mut found = None;
        for i in (0..=list_height).rev() {
            let (l, r) = unsafe { self.find_splice_for_level(&key, left[i + 1], i, true) };
            left[i] = l;
            right[i] = r;
            if found.is_none() && self.c.compare_key(key, &(*r).key) == std::cmp::Ordering::Equal {
                found = Some(r);
            }
        }

        Position { found, left, right }
    }

    pub fn remove(&self, key: impl Into<Bytes>) -> Option<Bytes> {
        let list_height = self.height();
        let key = key.into();

        unsafe {
            loop {
                let search = self.search_for_remove(&key);

                let n = search.found?;

                // Try removing the node by marking its tower.
                if (*n).mark_tower() {
                    // Unlink the node at each level of the skip list. We could
                    // do this by simply repeating the search, but it's usually
                    // faster to unlink it manually using the `left` and `right`
                    // lists.

                    let node = &(*n);
                    for level in (0..node.height).rev() {
                        let succ = without_tag(node.tower[level].load(Ordering::SeqCst));
                        match (*search.left[level]).tower[level].compare_exchange(
                            self.inner.arena.offset(node),
                            succ,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => {}
                            Err(_) => {
                                unimplemented!()
                            }
                        }
                    }

                    return Some((*n).value.clone());
                }
            }
        }
    }

    pub fn remove_t(&self, key: impl Into<Bytes>) -> Option<Bytes> {
        let list_height = self.height();
        let key = key.into();
        let mut value = None;
        let prev = self.inner.head.as_ptr();
        for i in (0..=list_height).rev() {
            if self.allow_concurrent_write {
                loop {
                    let (prev, current) =
                        unsafe { self.find_splice_for_level(&key, prev, i, true) };
                    unsafe {
                        if current != ptr::null_mut()
                            && self.c.same_key((*current).key.as_slice(), key.as_slice())
                        {
                            let next_offset = (*current).next_offset(i);
                            let current_offset = self.inner.arena.offset(current);
                            match unsafe { &*prev }.tower[i].compare_exchange(
                                current_offset,
                                next_offset,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) {
                                Ok(_) => {
                                    value = Some((*current).value.clone());
                                    break;
                                }
                                Err(_) => {}
                            }
                        } else {
                            // Not found in this level
                            break;
                        }
                    }
                }
            } else {
                let (prev, current) = unsafe { self.find_splice_for_level(&key, prev, i, true) };
                unsafe {
                    if current != ptr::null_mut()
                        && self.c.same_key((*current).key.as_slice(), key.as_slice())
                    {
                        (*prev).tower[i].store((*current).next_offset(i), Ordering::SeqCst);
                        value = Some((*current).value.clone());
                    }
                }
            }
        }
        value
    }

    /// Insert the key value pair to skiplist.
    ///
    /// Returns None if the insertion success.
    /// Returns Some(key, vaule) when insertion failed. This happens when the
    /// key already exists and the existed value not equal to the value
    /// passed to this function, returns the passed key and value.
    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Option<(Bytes, Bytes)> {
        let (key, value) = (key.into(), value.into());
        let mut list_height = self.height();
        let mut prev = [ptr::null_mut(); MAX_HEIGHT + 1];
        let mut next = [ptr::null_mut(); MAX_HEIGHT + 1];
        prev[list_height + 1] = self.inner.head.as_ptr();
        next[list_height + 1] = ptr::null_mut();
        for i in (0..=list_height).rev() {
            let (p, n) = unsafe { self.find_splice_for_level(&key, prev[i + 1], i, false) };
            prev[i] = p;
            next[i] = n;
            if p == n {
                unsafe {
                    if (*p).value != value {
                        info!(
                            "Different values with the same key";
                            "key" => ?key,
                            "prev_value" => ?((*p).value).as_slice(),
                            "value" => ?value.as_slice(),
                        );
                        // panic!("why this can happen");
                    }
                }
                return None;
            }
        }

        let height = self.random_height();
        let node_offset = Node::alloc(&self.inner.arena, key, value, height);
        if self.allow_concurrent_write {
            while height > list_height {
                match self.inner.height.compare_exchange_weak(
                    list_height,
                    height,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(h) => list_height = h,
                }
            }
        } else {
            // There is no need to use CAS for single thread writing.
            if height > list_height {
                self.inner.height.store(height, Ordering::Relaxed);
            }
        }

        let x: &mut Node = unsafe { &mut *self.inner.arena.get_mut(node_offset) };
        for i in 0..=height {
            if self.allow_concurrent_write {
                loop {
                    if prev[i].is_null() {
                        assert!(i > 1);
                        let (p, n) = unsafe {
                            self.find_splice_for_level(&x.key, self.inner.head.as_ptr(), i, false)
                        };
                        prev[i] = p;
                        next[i] = n;
                        assert_ne!(p, n);
                    }
                    let next_offset = self.inner.arena.offset(next[i]);
                    x.tower[i].store(next_offset, Ordering::SeqCst);
                    match unsafe { &*prev[i] }.tower[i].compare_exchange(
                        next_offset,
                        node_offset,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(_) => {
                            let (p, n) =
                                unsafe { self.find_splice_for_level(&x.key, prev[i], i, false) };
                            if p == n {
                                assert_eq!(i, 0);
                                if unsafe { &*p }.value != x.value {
                                    let key = mem::replace(&mut x.key, Bytes::new());
                                    let value = mem::replace(&mut x.value, Bytes::new());
                                    return Some((key, value));
                                }
                                unsafe {
                                    ptr::drop_in_place(x);
                                }
                                return None;
                            }
                            prev[i] = p;
                            next[i] = n;
                        }
                    }
                }
            } else {
                // There is no need to use CAS for single thread writing.
                if prev[i].is_null() {
                    assert!(i > 1);
                    let (p, n) = unsafe {
                        self.find_splice_for_level(&x.key, self.inner.head.as_ptr(), i, false)
                    };
                    prev[i] = p;
                    next[i] = n;
                    assert_ne!(p, n);
                }
                // Construct the PrevList for level 0.
                if i == 0 {
                    let prev_offset = self.inner.arena.offset(prev[0]);
                    x.prev.store(prev_offset, Ordering::Relaxed);
                    if !next[i].is_null() {
                        unsafe { &*next[i] }
                            .prev
                            .store(node_offset, Ordering::Release);
                    }
                }
                // Construct the NextList for level i.
                let next_offset = self.inner.arena.offset(next[i]);
                x.tower[i].store(next_offset, Ordering::Relaxed);
                unsafe { &*prev[i] }.tower[i].store(node_offset, Ordering::Release);
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        let node = self.inner.head.as_ptr();
        let next_offset = unsafe { (*node).next_offset(0) };
        next_offset == 0
    }

    pub fn len(&self) -> usize {
        let mut node = self.inner.head.as_ptr();
        let mut count = 0;
        loop {
            let next = unsafe { (*node).next_offset(0) };
            if next != 0 {
                count += 1;
                node = unsafe { self.inner.arena.get_mut(next) };
                continue;
            }
            return count;
        }
    }

    fn find_last(&self) -> *const Node {
        let mut node = self.inner.head.as_ptr();
        let mut level = self.height();
        loop {
            let next = unsafe { (*node).next_offset(level) };
            if next != 0 {
                node = unsafe { self.inner.arena.get_mut(next) };
                continue;
            }
            if level == 0 {
                if node == self.inner.head.as_ptr() {
                    return ptr::null();
                }
                return node;
            }
            level -= 1;
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&Bytes> {
        if let Some((_, value)) = self.get_with_key(key) {
            Some(value)
        } else {
            None
        }
    }

    pub fn get_with_key(&self, key: &[u8]) -> Option<(&Bytes, &Bytes)> {
        let node = unsafe { self.find_near(key, false, true) };
        if node.is_null() {
            return None;
        }
        if self.c.same_key(&unsafe { &*node }.key, key) {
            return Some(unsafe { (&(*node).key, &(*node).value) });
        }
        None
    }

    pub fn iter_ref(&self) -> IterRef<&Skiplist<C>, C> {
        IterRef {
            list: self,
            cursor: ptr::null(),
            _key_cmp: std::marker::PhantomData,
        }
    }

    pub fn iter(&self) -> IterRef<Skiplist<C>, C> {
        IterRef {
            list: self.clone(),
            cursor: ptr::null(),
            _key_cmp: std::marker::PhantomData,
        }
    }

    pub fn mem_size(&self) -> usize {
        self.inner.arena.len()
    }
}

impl<C: Clone> AsRef<Skiplist<C>> for Skiplist<C> {
    fn as_ref(&self) -> &Skiplist<C> {
        self
    }
}

impl Drop for SkiplistInner {
    fn drop(&mut self) {
        let mut node = self.head.as_ptr();
        loop {
            let next = unsafe { (*node).next_offset(0) };
            if next != 0 {
                let next_ptr = unsafe { self.arena.get_mut(next) };
                unsafe {
                    ptr::drop_in_place(node);
                }
                node = next_ptr;
                continue;
            }
            unsafe { ptr::drop_in_place(node) };
            return;
        }
    }
}

unsafe impl<C: Send + Clone> Send for Skiplist<C> {}
unsafe impl<C: Sync + Clone> Sync for Skiplist<C> {}

pub struct IterRef<T, C: Clone>
where
    T: AsRef<Skiplist<C>>,
{
    list: T,
    cursor: *const Node,
    _key_cmp: std::marker::PhantomData<C>,
}

impl<T: AsRef<Skiplist<C>>, C: KeyComparator> IterRef<T, C> {
    pub fn valid(&self) -> bool {
        !self.cursor.is_null()
    }

    pub fn key(&self) -> &Bytes {
        assert!(self.valid());
        unsafe { &(*self.cursor).key }
    }

    pub fn value(&self) -> &Bytes {
        assert!(self.valid());
        unsafe { &(*self.cursor).value }
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        unsafe {
            let cursor_offset = (*self.cursor).next_offset(0);
            self.cursor = self.list.as_ref().inner.arena.get_mut(cursor_offset);
            while !self.cursor.is_null() {
                let next = (*self.cursor).next_offset(0);
                if tag(next) == 1 {
                    // current is marked
                    self.cursor = self.list.as_ref().inner.arena.get_mut(next);
                } else {
                    break;
                }
            }
        }
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        if self.list.as_ref().allow_concurrent_write {
            unsafe {
                self.cursor = self.list.as_ref().find_near(self.key(), true, false);
            }
        } else {
            unsafe {
                let prev_offset = (*self.cursor).prev.load(Ordering::Acquire);
                let node = self.list.as_ref().inner.arena.get_mut(prev_offset);
                if node != self.list.as_ref().inner.head.as_ptr() {
                    self.cursor = node;
                } else {
                    self.cursor = ptr::null();
                }
            }
        }
    }

    pub fn seek(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.as_ref().find_near(target, false, true);
        }
    }

    pub fn seek_for_prev(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self.list.as_ref().find_near(target, true, true);
        }
    }

    pub fn seek_to_first(&mut self) {
        unsafe {
            let cursor_offset = (*self.list.as_ref().inner.head.as_ptr()).next_offset(0);
            self.cursor = self.list.as_ref().inner.arena.get_mut(cursor_offset);
        }
    }

    pub fn seek_to_last(&mut self) {
        self.cursor = self.list.as_ref().find_last();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{key::ByteWiseComparator, FixedLengthSuffixComparator};

    const ARENA_SIZE: usize = 1 << 20;

    fn with_skl_test(
        allow_concurrent_write: bool,
        f: impl FnOnce(Skiplist<FixedLengthSuffixComparator>),
    ) {
        let comp = FixedLengthSuffixComparator::new(8);
        let list = Skiplist::with_capacity(comp, ARENA_SIZE, allow_concurrent_write);
        f(list);
    }

    fn test_find_near_imp(allow_concurrent_write: bool) {
        with_skl_test(allow_concurrent_write, |list| {
            for i in 0..1000 {
                let key = Bytes::from(format!("{:05}{:08}", i * 10 + 5, 0));
                let value = Bytes::from(format!("{:05}", i));
                list.put(key, value);
            }
            let mut cases = vec![
                ("00001", false, false, Some("00005")),
                ("00001", false, true, Some("00005")),
                ("00001", true, false, None),
                ("00001", true, true, None),
                ("00005", false, false, Some("00015")),
                ("00005", false, true, Some("00005")),
                ("00005", true, false, None),
                ("00005", true, true, Some("00005")),
                ("05555", false, false, Some("05565")),
                ("05555", false, true, Some("05555")),
                ("05555", true, false, Some("05545")),
                ("05555", true, true, Some("05555")),
                ("05558", false, false, Some("05565")),
                ("05558", false, true, Some("05565")),
                ("05558", true, false, Some("05555")),
                ("05558", true, true, Some("05555")),
                ("09995", false, false, None),
                ("09995", false, true, Some("09995")),
                ("09995", true, false, Some("09985")),
                ("09995", true, true, Some("09995")),
                ("59995", false, false, None),
                ("59995", false, true, None),
                ("59995", true, false, Some("09995")),
                ("59995", true, true, Some("09995")),
            ];
            for (i, (key, less, allow_equal, exp)) in cases.drain(..).enumerate() {
                let seek_key = Bytes::from(format!("{}{:08}", key, 0));
                let res = unsafe { list.find_near(&seek_key, less, allow_equal) };
                if exp.is_none() {
                    assert!(res.is_null(), "{}", i);
                    continue;
                }
                let e = format!("{}{:08}", exp.unwrap(), 0);
                assert_eq!(&unsafe { &*res }.key, e.as_bytes(), "{}", i);
            }
        });
    }

    #[test]
    fn test_skl_find_near() {
        test_find_near_imp(true);
        test_find_near_imp(false);
    }

    #[test]
    fn test_skl_remove() {
        let sklist = Skiplist::with_capacity(ByteWiseComparator {}, 1 << 20, true);
        for i in 0..30 {
            let key = Bytes::from(format!("key{:03}", i));
            let value = Bytes::from(format!("value{:03}", i));
            sklist.put(key, value);
        }
        for i in 0..30 {
            let key = Bytes::from(format!("key{:03}", i));
            sklist.remove(key);
        }
        let mut iter = sklist.iter();
        iter.seek_to_first();
        let mut count = 0;
        while iter.valid() {
            let key = iter.key();
            let value = iter.value();
            println!("{:?}, {:?}", key, value);
            iter.next();
            count += 1;
        }
        assert!(count == 0);

        for i in 0..20 {
            let key = Bytes::from(format!("key{:03}", i));
            let value = Bytes::from(format!("value{:03}", i));
            sklist.put(key, value);
        }

        let res = sklist.get(b"key008");
        println!("{:?}", res);

        for i in 7..15 {
            let key = Bytes::from(format!("key{:03}", i));
            sklist.remove(key);
        }
        let mut iter = sklist.iter();
        iter.seek_to_first();
        let mut count = 0;
        while iter.valid() {
            let key = iter.key();
            let value = iter.value();
            println!("{:?}, {:?}", key, value);
            iter.next();
            count += 1;
        }
        assert!(count == 12);

        let res = sklist.remove(Bytes::from(b"key008".to_vec()));
        let res = sklist.get(b"key008");
        println!("{:?}", res);
    }

    #[test]
    fn test_skl_remove2() {
        let sklist = Skiplist::with_capacity(ByteWiseComparator {}, 1 << 30, true);
        let mut i = 0;
        while i < 1000 {
            let key = Bytes::from(format!("key{:04}", i));
            let value = Bytes::from(format!("value{:04}", i));
            sklist.put(key, value);
            i += 2;
        }

        let s1 = sklist.clone();
        let h1 = std::thread::spawn(move || {
            let mut i = 1;
            while i < 1000 {
                let key = Bytes::from(format!("key{:04}", i));
                let value = Bytes::from(format!("value{:04}", i));
                s1.put(key, value);
                i += 2;
            }
        });

        let s2 = sklist.clone();
        let h2 = std::thread::spawn(move || {
            let mut i = 0;
            while i < 1000 {
                let key = Bytes::from(format!("key{:04}", i));
                s2.remove(key);
                i += 2;
            }
        });

        let _ = h1.join();
        let _ = h2.join();

        let mut iter = sklist.iter();
        iter.seek_to_first();
        let mut count = 0;
        while iter.valid() {
            let key = iter.key();
            let value = iter.value();
            println!("{:?}, {:?}", key, value);
            iter.next();
            count += 1;
        }
        println!("{}", count);
    }

    #[test]
    fn test_split() {
        let sklist = Skiplist::with_capacity(ByteWiseComparator {}, 1 << 20, true);
        for i in 0..100 {
            let key = Bytes::from(format!("key{:03}", i));
            let value = Bytes::from(format!("value{:03}", i));
            sklist.put(key, value);
        }

        println!("===================");
        let sks = sklist.split(vec![
            Bytes::from(b"key040".to_vec()),
            Bytes::from(b"key060".to_vec()),
        ]);
        let mut iter = sks[0].iter();
        iter.seek_to_first();
        while iter.valid() {
            let key = iter.key();
            let value = iter.value();
            println!("{:?}, {:?}", key, value);
            iter.next();
        }

        println!("===================");
        let mut iter = sks[1].iter();
        iter.seek_to_first();
        while iter.valid() {
            let key = iter.key();
            let value = iter.value();
            println!("{:?}, {:?}", key, value);
            iter.next();
        }

        println!("===================");
        let mut iter = sks[2].iter();
        iter.seek_to_first();
        while iter.valid() {
            let key = iter.key();
            let value = iter.value();
            println!("{:?}, {:?}", key, value);
            iter.next();
        }
    }
}
