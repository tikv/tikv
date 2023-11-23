// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use core::slice::SlicePattern;
use std::{
    cmp, mem, ptr,
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering},
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

        for level in (0..=height).rev() {
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
    unsafe fn find_splice_for_level(
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

    pub fn split(&self, split_keys: &Vec<Vec<u8>>) -> Vec<Skiplist<C>> {
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

            while iter.valid()
                && self.c.compare_key(iter.key().as_slice(), split_key) == std::cmp::Ordering::Less
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

    unsafe fn search_bound(
        &self,
        key: &[u8],
        upper_bound: bool,
        allow_equal: bool,
    ) -> Option<*mut Node> {
        'search: loop {
            let mut level = self.height() + 1;
            // Fast loop to skip empty tower levels.
            while level >= 1 && (*self.inner.head.as_ptr()).next_offset(level - 1) == 0 {
                level -= 1;
            }

            let mut found = None;
            let mut pred = self.inner.head.as_ptr();

            while level >= 1 {
                level -= 1;

                // Two adjacent nodes at the current level.
                let mut curr = (*pred).next_offset(level);

                // If `curr` is marked, that means `pred` is removed and we have to restart the
                // search.
                if tag(curr) == 1 {
                    continue 'search;
                }

                // Iterate through the current level until we reach a node with a key greater
                // than or equal to `key`.
                let mut curr_node: *mut Node = self.inner.arena.get_mut(curr);
                while !curr_node.is_null() {
                    let succ = (*curr_node).next_offset(level);

                    if tag(succ) == 1 {
                        if let Some(c) = self.help_unlink(pred, curr, succ, level) {
                            curr = c;
                            curr_node = self.inner.arena.get_mut(curr);
                            continue;
                        } else {
                            // On failure, we cannot do anything reasonable to continue
                            // searching from the current position. Restart the search.
                            continue 'search;
                        }
                    }

                    // If `curr` contains a key that is greater than (or equal)
                    // to `key`, we're done with this level.
                    //
                    // The condition determines whether we should stop the
                    // search. For the upper
                    // bound, we return the last node before the condition
                    // became true. For the lower bound, we
                    // return the first node after the condition became true.
                    if upper_bound {
                        if !below_upper_bound(&self.c, key, &(*curr_node).key, allow_equal) {
                            break;
                        }
                        found = Some(curr_node);
                    } else {
                        if above_lower_bound(&self.c, key, &(*curr_node).key, allow_equal) {
                            found = Some(curr_node);
                            break;
                        }
                    }

                    // Move one step forward.
                    pred = curr_node;
                    curr_node = self.inner.arena.get_mut(succ);
                    curr = succ;
                }
            }
            return found;
        }
    }

    unsafe fn search_position(&self, key: &[u8]) -> Position {
        let mut left = [self.inner.head.as_ptr(); MAX_HEIGHT + 1];
        let mut right = [ptr::null_mut(); MAX_HEIGHT + 1];
        let mut found = None;
        unsafe {
            'search: loop {
                let mut level = self.height() + 1;
                // Fast loop to skip empty tower levels.
                while level >= 1 && (*self.inner.head.as_ptr()).next_offset(level - 1) == 0 {
                    level -= 1;
                }

                let mut pred = self.inner.head.as_ptr();

                while level >= 1 {
                    level -= 1;

                    // Two adjacent nodes at the current level.
                    let mut curr = (*pred).next_offset(level);

                    // If `curr` is marked, that means `pred` is removed and we have to restart the
                    // search.
                    if tag(curr) == 1 {
                        continue 'search;
                    }

                    // Iterate through the current level until we reach a node with a key greater
                    // than or equal to `key`.
                    let mut curr_node: *mut Node = self.inner.arena.get_mut(curr);
                    while !curr_node.is_null() {
                        let succ = (*curr_node).next_offset(level);

                        if tag(succ) == 1 {
                            if let Some(c) = self.help_unlink(pred, curr, succ, level) {
                                // On success, continue searching through the current level.
                                curr = c;
                                curr_node = self.inner.arena.get_mut(curr);
                                continue;
                            } else {
                                // On failure, we cannot do anything reasonable to continue
                                // searching from the current position. Restart the search.
                                continue 'search;
                            }
                        }

                        // If `curr` contains a key that is greater than or equal to `key`, we're
                        // done with this level.
                        match self.c.compare_key(&(*curr_node).key, key) {
                            cmp::Ordering::Greater => break,
                            cmp::Ordering::Equal => {
                                found = Some(curr_node);
                                break;
                            }
                            cmp::Ordering::Less => {}
                        }

                        // Move one step forward.
                        pred = curr_node;
                        curr_node = self.inner.arena.get_mut(succ);
                        curr = succ;
                    }

                    left[level] = pred;
                    right[level] = curr_node;
                }

                return Position { found, left, right };
            }
        }
    }

    unsafe fn _search_for_remove(&self, key: &[u8]) -> Position {
        let list_height = self.height();
        let mut left = [ptr::null_mut(); MAX_HEIGHT + 1];
        let mut right = [ptr::null_mut(); MAX_HEIGHT + 1];
        left[list_height + 1] = self.inner.head.as_ptr();
        right[list_height + 1] = ptr::null_mut();
        let mut found = None;
        for i in (0..=list_height).rev() {
            let (l, r) = unsafe { self.find_splice_for_level(&key, left[i + 1], i) };
            left[i] = l;
            right[i] = r;
            if found.is_none() && self.c.compare_key(key, &(*r).key) == std::cmp::Ordering::Equal {
                found = Some(r);
            }
        }

        Position { found, left, right }
    }

    pub fn remove(&self, key: &[u8]) -> Option<Bytes> {
        unsafe {
            loop {
                let search = self.search_position(key);

                let n = search.found?;

                // Try removing the node by marking its tower.
                if (*n).mark_tower() {
                    // Unlink the node at each level of the skip list. We could
                    // do this by simply repeating the search, but it's usually
                    // faster to unlink it manually using the `left` and `right`
                    // lists.
                    let node = &(*n);
                    for level in (0..=node.height).rev() {
                        let succ = without_tag(node.tower[level].load(Ordering::SeqCst));

                        match (*search.left[level]).tower[level].compare_exchange(
                            self.inner.arena.offset(node),
                            succ,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => {}
                            Err(_) => {
                                // Failed! Just repeat the search to completely unlink the node.
                                self.search_bound(key, false, true);
                                break;
                            }
                        }
                    }

                    return Some((*n).value.clone());
                }
            }
        }
    }

    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Option<(Bytes, Bytes)> {
        let (key, value) = (key.into(), value.into());
        let mut list_height = self.height();
        unsafe {
            let mut search;
            loop {
                // First try searching for the key.
                // Note that the `Ord` implementation for `K` may panic during the search.
                search = self.search_position(&key);
                let r = match search.found {
                    Some(r) => r,
                    None => break,
                };

                if (*r).value != value {
                    info!(
                        "Different values with the same key";
                        "key" => ?key,
                        "prev_value" => ?((*r).value).as_slice(),
                        "value" => ?value.as_slice(),
                    );
                    // If a node with the key was found and we should replace
                    // it, mark its tower and then repeat
                    // the search. todo: concurrent issue?
                    // (*r).value = value;
                }
                return None;
            }

            let height = self.random_height();
            let node_offset = Node::alloc(&self.inner.arena, key, value, height);
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

            let n: &mut Node = &mut *self.inner.arena.get_mut(node_offset);
            loop {
                // Set the lowest successor of `n` to `search.right[0]`.
                let right_offset = self.inner.arena.offset(search.right[0]);
                n.tower[0].store(right_offset, Ordering::SeqCst);
                // Try installing the new node into the skip list (at level 0).
                if (*search.left[0]).tower[0]
                    .compare_exchange(
                        right_offset,
                        node_offset,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    break;
                }

                // We failed. Let's search for the key and try again.
                search = self.search_position(&n.key);

                if let Some(r) = search.found {
                    if (*r).value != n.value {
                        info!(
                            "Different values with the same key";
                            "key" => ?n.key,
                            "prev_value" => ?((*r).value).as_slice(),
                            "value" => ?n.value.as_slice(),
                        );
                        // If a node with the key was found and we should replace it, mark its tower
                        // and then repeat the search.
                        // todo: concurrent issue?
                        panic!("why here");
                    }
                    return None;
                }
            }

            // Build the rest of the tower above level 0.
            'build: for level in 1..=height {
                loop {
                    // Obtain the predecessor and successor at the current level.
                    let pred = search.left[level];
                    let succ = search.right[level];
                    let succ_offset = self.inner.arena.offset(succ);

                    // Load the current value of the pointer in the tower at this level.
                    // TODO(Amanieu): can we use relaxed ordering here?
                    let next = n.tower[level].load(Ordering::SeqCst);

                    // If the current pointer is marked, that means another thread is already
                    // removing the node we've just inserted. In that case, let's just stop
                    // building the tower.
                    if tag(next) == 1 {
                        break 'build;
                    }

                    if !succ.is_null() && self.c.compare_key(&(*succ).key, &n.key).is_eq() {
                        search = self.search_position(&n.key);
                        continue;
                    }

                    // Change the pointer at the current level from `next` to `succ`. If this CAS
                    // operation fails, that means another thread has marked the pointer and we
                    // should stop building the tower.
                    if n.tower[level]
                        .compare_exchange(next, succ_offset, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        break 'build;
                    }

                    // Try installing the new node at the current level.
                    if (*pred).tower[level]
                        .compare_exchange(
                            succ_offset,
                            node_offset,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        )
                        .is_ok()
                    {
                        // Success! Continue on the next level.
                        break;
                    }

                    // We don't have the most up-to-date search results. Repeat the search.
                    //
                    // If this search panics, we simply stop building the tower without breaking
                    // any invariants. Note that building higher levels is completely optional.
                    // Only the lowest level really matters, and all the higher levels are there
                    // just to make searching faster.
                    search = self.search_position(&n.key);
                }
            }

            if tag(n.next_offset(height)) == 1 {
                self.search_bound(&n.key, false, true);
            }

            None
        }
    }

    /// Insert the key value pair to skiplist.
    ///
    /// Returns None if the insertion success.
    /// Returns Some(key, vaule) when insertion failed. This happens when the
    /// key already exists and the existed value not equal to the value
    /// passed to this function, returns the passed key and value.
    pub fn put_t(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Option<(Bytes, Bytes)> {
        let (key, value) = (key.into(), value.into());
        let mut list_height = self.height();
        let mut prev = [ptr::null_mut(); MAX_HEIGHT + 1];
        let mut next = [ptr::null_mut(); MAX_HEIGHT + 1];
        prev[list_height + 1] = self.inner.head.as_ptr();
        next[list_height + 1] = ptr::null_mut();
        for i in (0..=list_height).rev() {
            let (p, n) = unsafe { self.find_splice_for_level(&key, prev[i + 1], i) };
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
                            self.find_splice_for_level(&x.key, self.inner.head.as_ptr(), i)
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
                            let (p, n) = unsafe { self.find_splice_for_level(&x.key, prev[i], i) };
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
                    let (p, n) =
                        unsafe { self.find_splice_for_level(&x.key, self.inner.head.as_ptr(), i) };
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
        unsafe {
            self.cursor = self
                .list
                .as_ref()
                .search_bound(self.key(), true, false)
                .or_else(|| Some(ptr::null_mut()))
                .unwrap();
        }
    }

    pub fn seek(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self
                .list
                .as_ref()
                .search_bound(target, false, true)
                .or_else(|| Some(ptr::null_mut()))
                .unwrap();
        }
    }

    pub fn seek_for_prev(&mut self, target: &[u8]) {
        unsafe {
            self.cursor = self
                .list
                .as_ref()
                .search_bound(target, true, true)
                .or_else(|| Some(ptr::null_mut()))
                .unwrap();
        }
    }

    pub fn seek_to_first(&mut self) {
        unsafe {
            let cursor_offset = (*self.list.as_ref().inner.head.as_ptr()).next_offset(0);

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

    pub fn seek_to_last(&mut self) {
        self.cursor = self.list.as_ref().find_last();
    }
}

/// Helper function to check if a value is above a lower bound
fn above_lower_bound<C: KeyComparator>(
    c: &C,
    bound: &[u8],
    other: &[u8],
    allow_equal: bool,
) -> bool {
    if allow_equal {
        match c.compare_key(other, bound) {
            cmp::Ordering::Greater | cmp::Ordering::Equal => return true,
            _ => return false,
        }
    } else {
        match c.compare_key(other, bound) {
            cmp::Ordering::Greater => return true,
            _ => return false,
        }
    }
}

/// Helper function to check if a value is below an upper bound
fn below_upper_bound<C: KeyComparator>(
    c: &C,
    bound: &[u8],
    other: &[u8],
    allow_equal: bool,
) -> bool {
    if allow_equal {
        match c.compare_key(bound, other) {
            cmp::Ordering::Greater | cmp::Ordering::Equal => return true,
            _ => return false,
        }
    } else {
        match c.compare_key(bound, other) {
            cmp::Ordering::Greater => return true,
            _ => return false,
        }
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
                let res = unsafe {
                    list.search_bound(&seek_key, less, allow_equal)
                        .or_else(|| Some(ptr::null_mut()))
                        .unwrap()
                };
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
            let key = format!("key{:03}", i);
            sklist.remove(key.as_bytes());
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
            let key = format!("key{:03}", i);
            sklist.remove(key.as_bytes());
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

        let _ = sklist.remove(b"key008");
        let res = sklist.get(b"key008");
        println!("{:?}", res);
    }

    #[test]
    fn test_iter_remove() {
        let sklist = Skiplist::with_capacity(ByteWiseComparator {}, 1 << 30, true);
        let mut i = 0;
        let num = 10;
        while i < num {
            let key = Bytes::from(format!("key{:08}", i));
            let value = Bytes::from(format!("value{:08}", i));
            sklist.put(key, value);
            i += 1;
        }

        let mut iter = sklist.iter();
        iter.seek_to_first();
        while iter.valid() {
            let key = iter.key();
            sklist.remove(key.as_slice());
            iter.next();
        }

        let mut iter = sklist.iter();
        iter.seek_to_first();
        let mut count = 0;
        while iter.valid() {
            count += 1;
            iter.next();
        }
        assert!(count == 0);
    }

    #[test]
    fn test_skl_remove2() {
        let sklist = Skiplist::with_capacity(ByteWiseComparator {}, 1 << 30, true);
        let mut i = 0;

        let num = 1000000;
        while i < num {
            let key = Bytes::from(format!("key{:08}", i));
            let value = Bytes::from(format!("value{:08}", i));
            sklist.put(key, value);
            i += 2;

            if i % 100000 == 0 {
                println!("progress: {}", i);
            }
        }

        let s1 = sklist.clone();
        let h1 = std::thread::spawn(move || {
            let mut i = 1;
            while i < num {
                let key = Bytes::from(format!("key{:08}", i));
                let value = Bytes::from(format!("value{:08}", i));
                s1.put(key, value);
                i += 2;
            }
        });

        let s3 = sklist.clone();
        let h3 = std::thread::spawn(move || {
            let mut i = 0;
            while i < num {
                let key = format!("key{:08}", i);
                s3.remove(key.as_bytes());
                i += 2;
            }
        });

        let _ = h1.join();
        let _ = h3.join();

        let mut iter = sklist.iter();
        iter.seek_to_first();
        let mut count = 0;
        while iter.valid() {
            iter.next();
            count += 1;
        }
        println!("count {}", count);
    }

    #[test]
    fn test_skl_remove3() {
        let sklist = Skiplist::with_capacity(ByteWiseComparator {}, 1 << 30, true);
        let mut i = 0;
        while i < 10000 {
            let key = Bytes::from(format!("key{:05}", i));
            let value = Bytes::from(format!("value{:05}", i));
            sklist.put(key, value);
            i += 1;
        }

        let mut i = 0;
        while i < 10000 {
            let key = format!("key{:05}", i);
            sklist.remove(key.as_bytes());
            i += 1;
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
        let sks = sklist.split(&vec![b"key040".to_vec(), b"key060".to_vec()]);
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
