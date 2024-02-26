// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    fmt::Debug,
    mem,
    ops::{Deref, Index},
    ptr,
    sync::{
        atomic::{fence, AtomicUsize, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use crossbeam_epoch::{self, default_collector, pin, Atomic, Collector, Guard, Shared};
use crossbeam_utils::CachePadded;

use super::{arena::Arena, key::KeyComparator, memory_control::MemoryLimiter, Bound};

/// Number of bits needed to store height.
const HEIGHT_BITS: usize = 5;
const MAX_HEIGHT: usize = 1 << HEIGHT_BITS;
const HEIGHT_MASK: usize = (1 << HEIGHT_BITS) - 1;

const U64_MOD_BITS: usize = !(mem::size_of::<u64>() - 1);

/// The bits of `refs_and_height` that keep the height.

/// The tower of atomic pointers.
///
/// The actual size of the tower will vary depending on the height that a node
/// was allocated with.
#[repr(C)]
#[derive(Debug)]
struct Tower {
    pointers: [Atomic<Node>; 0],
}

impl Index<usize> for Tower {
    type Output = Atomic<Node>;
    fn index(&self, index: usize) -> &Atomic<Node> {
        // This implementation is actually unsafe since we don't check if the
        // index is in-bounds. But this is fine since this is only used internally.
        unsafe { self.pointers.get_unchecked(index) }
    }
}

/// A search result.
///
/// The result indicates whether the key was found, as well as what were the
/// adjacent nodes to the key on each level of the skip list.
struct Position<'a> {
    /// Reference to a node with the given key, if found.
    ///
    /// If this is `Some` then it will point to the same node as `right[0]`.
    found: Option<&'a Node>,

    /// Adjacent nodes with smaller keys (predecessors).
    left: [&'a Tower; MAX_HEIGHT],

    /// Adjacent nodes with equal or greater keys (successors).
    right: [Shared<'a, Node>; MAX_HEIGHT],
}

/// Tower at the head of a skip list.
///
/// This is located in the `SkipList` struct itself and holds a full height
/// tower.
#[repr(C)]
struct Head {
    pointers: [Atomic<Node>; MAX_HEIGHT],
}

impl Head {
    /// Initializes a `Head`.
    #[inline]
    fn new() -> Head {
        // Initializing arrays in rust is a pain...
        Head {
            pointers: Default::default(),
        }
    }
}

impl Deref for Head {
    type Target = Tower;
    fn deref(&self) -> &Tower {
        unsafe { &*(self as *const _ as *const Tower) }
    }
}

// Uses C layout to make sure tower is at the bottom
#[derive(Debug)]
#[repr(C)]
pub struct Node {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
    /// Keeps the reference count and the height of its tower.
    ///
    /// The reference count is equal to the number of `Entry`s pointing to this
    /// node, plus the number of levels in which this node is installed.
    refs_and_height: AtomicUsize,

    /// The tower of atomic pointers.
    tower: Tower,
}

impl Node {
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

pub const U_SIZE: usize = mem::size_of::<AtomicUsize>();

pub trait ReclaimableNode {
    fn size(&self) -> usize;

    fn drop_key_value(&mut self);
}

impl ReclaimableNode for Node {
    fn size(&self) -> usize {
        Node::node_size(self.height())
    }

    fn drop_key_value(&mut self) {
        println!("{:?}", self.key);
        unsafe {
            ptr::drop_in_place(&mut self.key);
            ptr::drop_in_place(&mut self.value);
        }
    }
}

impl Node {
    fn alloc<M: MemoryLimiter>(
        arena: &Arena<M>,
        key: Bytes,
        value: Bytes,
        height: usize,
        ref_count: usize,
    ) -> *mut Self {
        // Not all values in Node::tower will be utilized.
        let node_size = Node::node_size(height);
        let node_ptr = arena.alloc(node_size) as *mut Node;
        unsafe {
            let node = &mut *node_ptr;
            ptr::write(&mut node.key, key);
            ptr::write(&mut node.value, value);
            ptr::write(
                &mut node.refs_and_height,
                AtomicUsize::new((height - 1) | ref_count << HEIGHT_BITS),
            );
            ptr::write_bytes(node.tower.pointers.as_mut_ptr(), 0, height);
        }
        node_ptr
    }

    /// Returns the size of a node with tower of given `height` measured in
    /// `u64`s.
    fn node_size(height: usize) -> usize {
        assert!((1..=MAX_HEIGHT).contains(&height));
        assert!(mem::align_of::<Self>() <= mem::align_of::<u64>());

        let size_base = mem::size_of::<Self>();
        let size_ptr = mem::size_of::<Atomic<Self>>();

        let size_u64 = mem::size_of::<u64>();
        let size_self = size_base + size_ptr * height;

        (size_self + size_u64 - 1) & U64_MOD_BITS
    }

    /// Returns the height of this node's tower.
    #[inline]
    fn height(&self) -> usize {
        (self.refs_and_height.load(Ordering::Relaxed) & HEIGHT_MASK) + 1
    }

    fn mark_tower(&self) -> bool {
        let height = self.height();

        for level in (0..height).rev() {
            let tag = unsafe {
                // We're loading the pointer only for the tag, so it's okay to use
                // `epoch::unprotected()` in this situation.
                self.tower[level]
                    .fetch_or(1, Ordering::SeqCst, crossbeam_epoch::unprotected())
                    .tag()
            };

            // If the level 0 pointer was already marked, somebody else removed the node.
            if level == 0 && tag == 1 {
                return false;
            }
        }

        // We marked the level 0 pointer, therefore we removed the node.
        true
    }

    /// Attempts to increment the reference count of a node and returns `true`
    /// on success.
    ///
    /// The reference count can be incremented only if it is non-zero.
    ///
    /// # Panics
    ///
    /// Panics if the reference count overflows.
    #[inline]
    unsafe fn try_increment(&self) -> bool {
        let mut refs_and_height = self.refs_and_height.load(Ordering::Relaxed);

        loop {
            // If the reference count is zero, then the node has already been
            // queued for deletion. Incrementing it again could lead to a
            // double-free.
            if refs_and_height & !HEIGHT_MASK == 0 {
                return false;
            }

            // If all bits in the reference count are ones, we're about to overflow it.
            let new_refs_and_height = refs_and_height
                .checked_add(1 << HEIGHT_BITS)
                .expect("SkipList reference count overflow");

            // Try incrementing the count.
            match self.refs_and_height.compare_exchange_weak(
                refs_and_height,
                new_refs_and_height,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(current) => refs_and_height = current,
            }
        }
    }

    /// Decrements the reference count of a node, destroying it if the count
    /// becomes zero.
    #[inline]
    unsafe fn decrement<M: MemoryLimiter>(&self, arena: Arena<M>, guard: &Guard) {
        fail::fail_point!("on_decrement");
        let current_ref = self
            .refs_and_height
            .fetch_sub(1 << HEIGHT_BITS, Ordering::Release)
            >> HEIGHT_BITS;
        if current_ref == 1 {
            fence(Ordering::Acquire);
            let ptr = self as *const Self;
            guard.defer_unchecked(move || Self::finalize(ptr, arena));
            fail::fail_point!("on_finalize_scheduled");
        }
    }

    /// Drops the key and value of a node, then deallocates it.
    #[cold]
    unsafe fn finalize<M: MemoryLimiter>(ptr: *const Self, arena: Arena<M>) {
        let ptr = ptr as *mut Self;
        arena.free(ptr);
    }
}

/// Frequently modified data associated with a skip list.
struct HotData {
    /// The seed for random height generation.
    seed: AtomicUsize,

    /// Highest tower currently in use. This value is used as a hint for where
    /// to start lookups and never decreases.
    max_height: AtomicUsize,
}

struct SkiplistInner<M: MemoryLimiter> {
    /// The head of the skip list (just a dummy node, not a real entry).
    head: Head,
    /// Hot data associated with the skip list, stored in a dedicated cache
    /// line.
    hot_data: CachePadded<HotData>,
    /// The `Collector` associated with this skip list.
    collector: Collector,
    /// <emory management unit
    arena: Arena<M>,
}

unsafe impl<M: MemoryLimiter> Send for SkiplistInner<M> {}
unsafe impl<M: MemoryLimiter> Sync for SkiplistInner<M> {}

#[derive(Clone)]
pub struct Skiplist<C: KeyComparator, M: MemoryLimiter> {
    inner: Arc<SkiplistInner<M>>,
    c: C,
}

impl<C: KeyComparator, M: MemoryLimiter> Skiplist<C, M> {
    pub fn new(c: C, mem_limiter: Arc<M>) -> Skiplist<C, M> {
        let collector = default_collector().clone();
        let arena = Arena::new(mem_limiter);
        Skiplist {
            inner: Arc::new(SkiplistInner {
                hot_data: CachePadded::new(HotData {
                    seed: AtomicUsize::new(1),
                    max_height: AtomicUsize::new(1),
                }),
                collector,
                head: Head::new(),
                arena,
            }),
            c,
        }
    }

    /// Generates a random height and returns it.
    fn random_height(&self) -> usize {
        // Pseudorandom number generation from "Xorshift RNGs" by George Marsaglia.
        //
        // This particular set of operations generates 32-bit integers. See:
        // https://en.wikipedia.org/wiki/Xorshift#Example_implementation
        let mut num = self.inner.hot_data.seed.load(Ordering::Relaxed);
        num ^= num << 13;
        num ^= num >> 17;
        num ^= num << 5;
        self.inner.hot_data.seed.store(num, Ordering::Relaxed);

        let mut height = cmp::min(MAX_HEIGHT, num.trailing_zeros() as usize + 1);
        unsafe {
            // Keep decreasing the height while it's much larger than all towers currently
            // in the skip list.
            //
            // Note that we're loading the pointer only to check whether it is null, so it's
            // okay to use `epoch::unprotected()` in this situation.
            while height >= 4
                && self.inner.head[height - 2]
                    .load(Ordering::Relaxed, crossbeam_epoch::unprotected())
                    .is_null()
            {
                height -= 1;
            }
        }

        // Track the max height to speed up lookups
        let mut max_height = self.inner.hot_data.max_height.load(Ordering::Relaxed);
        while height > max_height {
            match self.inner.hot_data.max_height.compare_exchange_weak(
                max_height,
                height,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(h) => max_height = h,
            }
        }
        height
    }

    /// Ensures that all `Guard`s used with the skip list come from the same
    /// `Collector`.
    fn check_guard(&self, guard: &Guard) {
        if let Some(c) = guard.collector() {
            assert!(c == &self.inner.collector);
        }
    }

    #[inline]
    fn height(&self) -> usize {
        self.inner.hot_data.max_height.load(Ordering::Relaxed)
    }
}

impl<C: KeyComparator, M: MemoryLimiter> Skiplist<C, M> {
    /// If we encounter a deleted node while searching, help with the deletion
    /// by attempting to unlink the node from the list.
    ///
    /// If the unlinking is successful then this function returns the next node
    /// with which the search should continue on the current level.
    unsafe fn help_unlink<'a>(
        &'a self,
        pred: &'a Atomic<Node>,
        curr: &'a Node,
        succ: Shared<'a, Node>,
        guard: &'a Guard,
    ) -> Option<Shared<'a, Node>> {
        // If `succ` is marked, that means `curr` is removed. Let's try
        // unlinking it from the skip list at this level.
        match pred.compare_exchange(
            Shared::from(curr as *const _),
            succ.with_tag(0),
            Ordering::Release,
            Ordering::Relaxed,
            guard,
        ) {
            Ok(_) => {
                curr.decrement(self.inner.arena.clone(), guard);
                Some(succ.with_tag(0))
            }
            Err(_) => None,
        }
    }

    /// Finds the node near to key.
    ///
    /// If upper_bound=true, it finds rightmost node such that node.key < key
    /// (if allow_equal=false) or node.key <= key (if allow_equal=true).
    /// If upper_bound=false, it finds leftmost node such that node.key > key
    /// (if allow_equal=false) or node.key >= key (if allow_equal=true).
    unsafe fn search_bound<'a>(
        &'a self,
        bound: Bound<&[u8]>,
        upper_bound: bool,
        guard: &'a Guard,
    ) -> Option<&'a Node> {
        'search: loop {
            let mut level = self.height();
            // Fast loop to skip empty tower levels.
            while level >= 1
                && self.inner.head[level - 1]
                    .load(Ordering::Relaxed, guard)
                    .is_null()
            {
                level -= 1;
            }

            let mut found = None;
            let mut pred = &*self.inner.head;

            while level >= 1 {
                level -= 1;

                // Two adjacent nodes at the current level.
                let mut curr = pred[level].load_consume(guard);

                // If `curr` is marked, that means `pred` is removed and we have to restart the
                // search.
                if curr.tag() == 1 {
                    continue 'search;
                }

                // Iterate through the current level until we reach a node with a key greater
                // than or equal to `key`.
                while let Some(c) = curr.as_ref() {
                    let succ = c.tower[level].load_consume(guard);

                    if succ.tag() == 1 {
                        if let Some(c) = self.help_unlink(&pred[level], c, succ, guard) {
                            curr = c;
                            continue;
                        } else {
                            // On failure, we cannot do anything reasonable to continue
                            // searching from the current position. Restart the search.
                            continue 'search;
                        }
                    }

                    // If `curr` contains a key that is greater than (or equal) to `key`, we're
                    // done with this level.
                    //
                    // The condition determines whether we should stop the search. For the upper
                    // bound, we return the last node before the condition became true. For the
                    // lower bound, we return the first node after the condition became true.
                    if upper_bound {
                        if !below_upper_bound(&self.c, &bound, &c.key) {
                            break;
                        }
                        found = Some(c);
                    } else if above_lower_bound(&self.c, &bound, &c.key) {
                        found = Some(c);
                        break;
                    }

                    // Move one step forward.
                    pred = &c.tower;
                    curr = succ;
                }
            }
            return found;
        }
    }

    unsafe fn search_position<'a>(&'a self, key: &[u8], guard: &'a Guard) -> Position<'a> {
        unsafe {
            'search: loop {
                // The result of this search.
                let mut result = Position {
                    found: None,
                    left: [&*self.inner.head; MAX_HEIGHT],
                    right: [Shared::null(); MAX_HEIGHT],
                };

                let mut level = self.height();

                // Fast loop to skip empty tower levels.
                while level >= 1
                    && self.inner.head[level - 1]
                        .load(Ordering::Relaxed, guard)
                        .is_null()
                {
                    level -= 1;
                }

                let mut pred = &*self.inner.head;

                while level >= 1 {
                    level -= 1;

                    // Two adjacent nodes at the current level.
                    let mut curr = pred[level].load_consume(guard);

                    // If `curr` is marked, that means `pred` is removed and we have to restart the
                    // search.
                    if curr.tag() == 1 {
                        continue 'search;
                    }

                    // Iterate through the current level until we reach a node with a key greater
                    // than or equal to `key`.
                    while let Some(c) = curr.as_ref() {
                        let succ = c.tower[level].load_consume(guard);

                        if curr.tag() == 1 {
                            if let Some(c) = self.help_unlink(&pred[level], c, succ, guard) {
                                // On success, continue searching through the current level.
                                curr = c;
                                continue;
                            } else {
                                // On failure, we cannot do anything reasonable to continue
                                // searching from the current position. Restart the search.
                                continue 'search;
                            }
                        }

                        // If `curr` contains a key that is greater than or equal to `key`, we're
                        // done with this level.
                        match self.c.compare_key(&c.key, key) {
                            cmp::Ordering::Greater => break,
                            cmp::Ordering::Equal => {
                                result.found = Some(c);
                                break;
                            }
                            cmp::Ordering::Less => {}
                        }

                        // Move one step forward.
                        pred = &c.tower;
                        curr = succ;
                    }

                    // Store the position at the current level into the result.
                    result.left[level] = pred;
                    result.right[level] = curr;
                }

                return result;
            }
        }
    }

    pub fn remove(&self, key: &[u8]) -> bool {
        let guard = &crossbeam_epoch::pin();
        self.check_guard(guard);

        unsafe {
            // Rebind the guard to the lifetime of self. This is a bit of a
            // hack but it allows us to return references that are not bound to
            // the lifetime of the guard.
            let guard = &*(guard as *const _);

            loop {
                // Try searching for the key.
                let search = self.search_position(key, guard);

                let Some(n) = search.found else {
                    return false;
                };

                // Try removing the node by marking its tower.
                if n.mark_tower() {
                    for level in (0..n.height()).rev() {
                        let succ = n.tower[level].load(Ordering::SeqCst, guard).with_tag(0);

                        if search.left[level][level]
                            .compare_exchange(
                                Shared::from(n as *const _),
                                succ,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                                guard,
                            )
                            .is_ok()
                        {
                            // Success! Decrement the reference count.
                            n.decrement(self.inner.arena.clone(), guard);
                        } else {
                            self.search_bound(Bound::Included(key), false, guard);
                            break;
                        }
                    }

                    return true;
                }
            }
        }
    }

    pub fn put(&self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> bool {
        let guard = &crossbeam_epoch::pin();
        let (key, value) = (key.into(), value.into());
        self.check_guard(guard);

        unsafe {
            // Rebind the guard to the lifetime of self. This is a bit of a
            // hack but it allows us to return references that are not bound to
            // the lifetime of the guard.
            let guard = &*(guard as *const _);

            let mut search;
            // First try searching for the key.
            // Note that the `Ord` implementation for `K` may panic during the search.
            search = self.search_position(&key, guard);
            if search.found.is_some() {
                // panic!("Overwrite is not supported, {:?}", (*r).key);
                return false;
            }

            let height = self.random_height();
            let (node, n) = {
                let n = Node::alloc(&self.inner.arena, key, value, height, 1);
                (Shared::<Node>::from(n as *const _), &*n)
            };
            loop {
                // Set the lowest successor of `n` to `search.right[0]`.
                n.tower[0].store(search.right[0], Ordering::Relaxed);

                // Try installing the new node into the skip list (at level 0).
                if search.left[0][0]
                    .compare_exchange(
                        search.right[0],
                        node,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                        guard,
                    )
                    .is_ok()
                {
                    break;
                }

                // We failed. Let's search for the key and try again.
                {
                    // Create a guard that destroys the new node in case search panics.
                    let sg = scopeguard::guard((), |_| {
                        Node::finalize(node.as_raw(), self.inner.arena.clone())
                    });
                    search = self.search_position(&n.key, guard);
                    mem::forget(sg);
                }

                if search.found.is_some() {
                    // panic!("Overwrite is not supported, {:?}", (*r).key);
                    return false;
                }
            }

            // Build the rest of the tower above level 0.
            'build: for level in 1..height {
                loop {
                    // Obtain the predecessor and successor at the current level.
                    let pred = search.left[level];
                    let succ = search.right[level];

                    // Load the current value of the pointer in the tower at this level.
                    let next = n.tower[level].load(Ordering::SeqCst, guard);

                    // If the current pointer is marked, that means another thread is already
                    // removing the node we've just inserted. In that case, let's just stop
                    // building the tower.
                    if next.tag() == 1 {
                        break 'build;
                    }

                    // Change the pointer at the current level from `next` to `succ`. If this CAS
                    // operation fails, that means another thread has marked the pointer and we
                    // should stop building the tower.
                    if n.tower[level]
                        .compare_exchange(next, succ, Ordering::SeqCst, Ordering::SeqCst, guard)
                        .is_err()
                    {
                        break 'build;
                    }

                    // Increment the reference count. Heigher means more ref.
                    n.refs_and_height
                        .fetch_add(1 << HEIGHT_BITS, Ordering::Relaxed);

                    // Try installing the new node at the current level.
                    if pred[level]
                        .compare_exchange(succ, node, Ordering::SeqCst, Ordering::SeqCst, guard)
                        .is_ok()
                    {
                        // Success! Continue on the next level.
                        break;
                    }

                    // Installation failed. Decrement the reference count.
                    n.refs_and_height
                        .fetch_sub(1 << HEIGHT_BITS, Ordering::Relaxed);

                    // We don't have the most up-to-date search results. Repeat the search.
                    //
                    // If this search panics, we simply stop building the tower without breaking
                    // any invariants. Note that building higher levels is completely optional.
                    // Only the lowest level really matters, and all the higher levels are there
                    // just to make searching faster.
                    search = self.search_position(&n.key, guard);
                }
            }

            // If any pointer in the tower is marked, that means our node is in the process
            // of removal or already removed. It is possible that another thread
            // (either partially or completely) removed the new node while we
            // were building the tower, and just after that we installed the new
            // node at one of the higher levels. In order to undo that
            // installation, we must repeat the search, which will unlink the new node at
            // that level.
            // TODO(Amanieu): can we use relaxed ordering here?
            if n.tower[height - 1].load(Ordering::SeqCst, guard).tag() == 1 {
                self.search_bound(Bound::Included(&n.key), false, guard);
            }
        }

        true
    }

    pub fn is_empty(&self) -> bool {
        true
    }

    pub fn get(&self, key: &[u8]) -> Option<Entry<M>> {
        let guard = &crossbeam_epoch::pin();
        self.check_guard(guard);
        if let Some(n) = unsafe { self.search_bound(Bound::Included(key), false, guard) } {
            if self.c.same_key(&n.key, key) {
                return unsafe { Entry::try_acquire(n, self.inner.arena.clone()) };
            }
        }

        None
    }

    // Search the first node that we acquire successfully.
    unsafe fn search_bound_for_node(
        &self,
        bound: Bound<&[u8]>,
        upper_bound: bool,
        guard: &Guard,
    ) -> Option<Entry<M>> {
        loop {
            let node = self.search_bound(bound, upper_bound, guard)?;
            if let Some(e) = Entry::try_acquire(node, self.inner.arena.clone()) {
                return Some(e);
            }
        }
    }

    fn next_node(
        &self,
        pred: &Tower,
        lower_bound: Bound<&[u8]>,
        guard: &Guard,
    ) -> Option<Entry<M>> {
        unsafe {
            // Load the level 0 successor of the current node.
            let mut curr = pred[0].load_consume(guard);

            // If `curr` is marked, that means `pred` is removed and we have to use
            // a key search.
            if curr.tag() == 1 {
                return self.search_bound_for_node(lower_bound, false, guard);
            }

            while let Some(c) = curr.as_ref() {
                let succ = c.tower[0].load_consume(guard);

                if succ.tag() == 1 {
                    if let Some(c) = self.help_unlink(&pred[0], c, succ, guard) {
                        // On success, continue searching through the current level.
                        curr = c;
                        continue;
                    } else {
                        // On failure, we cannot do anything reasonable to continue
                        // searching from the current position. Restart the search.
                        return self.search_bound_for_node(lower_bound, false, guard);
                    }
                }

                if let Some(e) = Entry::try_acquire(c, self.inner.arena.clone()) {
                    return Some(e);
                }

                // acquire failed which means the node has been deleted
                curr = succ;
            }

            None
        }
    }

    pub fn iter(&self) -> IterRef<Skiplist<C, M>, C, M> {
        IterRef {
            list: self.clone(),
            cursor: None,
            _key_cmp: std::marker::PhantomData,
            _limiter: std::marker::PhantomData,
        }
    }

    pub fn mem_size(&self) -> usize {
        self.inner.arena.limiter.mem_usage()
    }
}

impl<C: KeyComparator, M: MemoryLimiter> AsRef<Skiplist<C, M>> for Skiplist<C, M> {
    fn as_ref(&self) -> &Skiplist<C, M> {
        self
    }
}

impl<M: MemoryLimiter> Drop for SkiplistInner<M> {
    fn drop(&mut self) {
        unsafe {
            let mut node = self.head[0]
                .load(Ordering::Relaxed, crossbeam_epoch::unprotected())
                .as_ref();

            // Iterate through the whole skip list and destroy every node.
            while let Some(n) = node {
                // Unprotected loads are okay because this function is the only one currently
                // using the skip list.
                let next = n.tower[0]
                    .load(Ordering::Relaxed, crossbeam_epoch::unprotected())
                    .as_ref();

                // Deallocate every node.
                Node::finalize(n, self.arena.clone());

                node = next;
            }
        }
    }
}

unsafe impl<C: Send + KeyComparator, M: MemoryLimiter> Send for Skiplist<C, M> {}
unsafe impl<C: Sync + KeyComparator, M: MemoryLimiter> Sync for Skiplist<C, M> {}

unsafe impl<M: MemoryLimiter> Send for Entry<M> {}

/// An entry in a skip list, protected by a `Guard`.
///
/// The lifetimes of the key and value are the same as that of the `Guard`
/// used when creating the `Entry` (`'g`).
pub struct Entry<M: MemoryLimiter> {
    node: *const Node,
    arena: Arena<M>,
}

impl<M: MemoryLimiter> Debug for Entry<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = unsafe { &*self.node };
        write!(f, "Entry: key {:?}, value {:?}", n.key, n.value)
    }
}

impl<M: MemoryLimiter> Drop for Entry<M> {
    fn drop(&mut self) {
        let guard = &pin();
        unsafe { (*self.node).decrement(self.arena.clone(), guard) }
    }
}

impl<M: MemoryLimiter> Entry<M> {
    unsafe fn try_acquire(node: &Node, arena: Arena<M>) -> Option<Entry<M>> {
        fail::fail_point!("on_try_acquire");
        if node.try_increment() {
            Some(Entry {
                node: node as *const _,
                arena,
            })
        } else {
            None
        }
    }

    pub fn key(&self) -> &Bytes {
        unsafe { &(*self.node).key }
    }

    pub fn value(&self) -> &Bytes {
        unsafe { &(*self.node).value }
    }
}

pub struct IterRef<T, C: KeyComparator, M: MemoryLimiter>
where
    T: AsRef<Skiplist<C, M>>,
{
    list: T,
    cursor: Option<Entry<M>>,
    _key_cmp: std::marker::PhantomData<C>,
    _limiter: std::marker::PhantomData<M>,
}

impl<T: AsRef<Skiplist<C, M>>, C: KeyComparator, M: MemoryLimiter> IterRef<T, C, M> {
    pub fn valid(&self) -> bool {
        self.cursor.is_some()
    }

    pub fn key(&self) -> &Bytes {
        assert!(self.valid());
        self.cursor.as_ref().unwrap().key()
    }

    pub fn value(&self) -> &Bytes {
        assert!(self.valid());
        self.cursor.as_ref().unwrap().value()
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        let guard = &pin();
        self.cursor = match &self.cursor {
            Some(n) => self.list.as_ref().next_node(
                unsafe { &(*n.node).tower },
                Bound::Excluded(n.key()),
                guard,
            ),
            None => unreachable!(),
        }
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        let guard = &pin();
        unsafe {
            self.cursor = match &self.cursor {
                Some(n) => {
                    self.list
                        .as_ref()
                        .search_bound_for_node(Bound::Excluded(n.key()), true, guard)
                }
                None => None,
            };
        }
    }

    pub fn seek(&mut self, target: &[u8]) {
        let guard = &pin();
        unsafe {
            self.cursor =
                self.list
                    .as_ref()
                    .search_bound_for_node(Bound::Included(target), false, guard);
        }
    }

    pub fn seek_for_prev(&mut self, target: &[u8]) {
        let guard = &pin();
        unsafe {
            self.cursor =
                self.list
                    .as_ref()
                    .search_bound_for_node(Bound::Included(target), true, guard);
        }
    }

    pub fn seek_to_first(&mut self) {
        let guard = &pin();
        self.list.as_ref().check_guard(guard);
        let pred = &self.list.as_ref().inner.head;
        self.cursor = self.list.as_ref().next_node(pred, Bound::Unbounded, guard);
    }
}

/// Helper function to check if a value is above a lower bound
fn above_lower_bound<C: KeyComparator>(c: &C, bound: &Bound<&[u8]>, key: &[u8]) -> bool {
    match *bound {
        Bound::Unbounded => true,
        Bound::Included(bound) => matches!(
            c.compare_key(key, bound),
            cmp::Ordering::Greater | cmp::Ordering::Equal
        ),
        Bound::Excluded(bound) => matches!(c.compare_key(key, bound), cmp::Ordering::Greater),
    }
}

/// Helper function to check if a value is below an upper bound
fn below_upper_bound<C: KeyComparator>(c: &C, bound: &Bound<&[u8]>, key: &[u8]) -> bool {
    match *bound {
        Bound::Unbounded => true,
        Bound::Included(bound) => matches!(
            c.compare_key(bound, key),
            cmp::Ordering::Greater | cmp::Ordering::Equal
        ),
        Bound::Excluded(bound) => matches!(c.compare_key(bound, key), cmp::Ordering::Greater),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use core::slice::SlicePattern;
    use std::thread;

    use super::*;
    use crate::skiplist::{key::ByteWiseComparator, memory_control::RecorderLimiter};

    fn construct_key(i: i32) -> Vec<u8> {
        format!("key-{:08}", i).into_bytes()
    }

    fn construct_val(i: i32) -> Vec<u8> {
        format!("val-{}", i).into_bytes()
    }

    fn default_list() -> Skiplist<ByteWiseComparator, RecorderLimiter> {
        Skiplist::new(ByteWiseComparator {}, Arc::default())
    }

    fn sl_insert(sl: &Skiplist<ByteWiseComparator, RecorderLimiter>, k: i32, v: i32) -> bool {
        let k = construct_key(k);
        let v = construct_val(v);
        sl.put(k, v)
    }

    fn sl_remove(sl: &Skiplist<ByteWiseComparator, RecorderLimiter>, k: i32) -> bool {
        let k = construct_key(k);
        sl.remove(&k)
    }

    fn sl_get_assert(sl: &Skiplist<ByteWiseComparator, RecorderLimiter>, k: i32, v: Option<i32>) {
        let k = construct_key(k);
        let res = sl.get(&k);
        if let Some(v) = v {
            let v = construct_val(v);
            assert_eq!(res.unwrap().value(), &v);
        } else {
            assert!(res.is_none());
        }
    }

    #[test]
    fn insert() {
        let insert = [0, 4, 2, 12, 8, 7, 11, 5];
        let not_present = [1, 3, 6, 9, 10];
        let s = default_list();

        for i in insert {
            sl_insert(&s, i, i * 10);
            sl_get_assert(&s, i, Some(i * 10));
        }

        for i in not_present {
            sl_get_assert(&s, i, None);
        }
    }

    #[test]
    fn remove() {
        let insert = [0, 4, 2, 12, 8, 7, 11, 5];
        let not_present = [1, 3, 6, 9, 10];
        let remove = [2, 12, 8];
        let remaining = [0, 4, 5, 7, 11];

        let s = default_list();

        for &x in &insert {
            sl_insert(&s, x, x * 10);
        }
        for &x in &not_present {
            sl_remove(&s, x);
        }
        for &x in &remove {
            sl_remove(&s, x);
        }

        let mut v = vec![];
        let mut iter = s.iter();
        iter.seek_to_first();
        iter.valid();
        while iter.valid() {
            v.push(iter.key().to_vec());
            iter.next();
        }

        for (&remain, k) in remaining.iter().zip(v.into_iter()) {
            let remain = construct_key(remain);
            assert_eq!(remain, k);
        }

        for &x in &insert {
            sl_remove(&s, x);
        }
        assert!(s.is_empty());
    }

    fn assert_keys(
        s: &Skiplist<ByteWiseComparator, RecorderLimiter>,
        expected: Vec<i32>,
        _guard: &Guard,
    ) {
        let mut iter = s.iter();
        iter.seek_to_first();
        let mut expect_iter = expected.iter();
        while iter.valid() {
            let expect_k = construct_key(*expect_iter.next().unwrap());
            assert_eq!(iter.key(), &expect_k);
            iter.next();
        }
        assert!(expect_iter.next().is_none());
    }

    #[test]
    fn insert_and_remove() {
        let collector = crossbeam_epoch::default_collector();
        let handle = collector.register();

        {
            let guard = &handle.pin();
            let s = default_list();
            sl_insert(&s, 3, 0);
            sl_insert(&s, 5, 0);
            sl_insert(&s, 1, 0);
            sl_insert(&s, 4, 0);
            sl_insert(&s, 2, 0);
            assert_keys(&s, vec![1, 2, 3, 4, 5], guard);

            assert!(sl_remove(&s, 4));
            assert_keys(&s, vec![1, 2, 3, 5], guard);
            assert!(sl_remove(&s, 3));
            assert_keys(&s, vec![1, 2, 5], guard);
            assert!(sl_remove(&s, 1));
            assert_keys(&s, vec![2, 5], guard);

            assert!(!sl_remove(&s, 1));
            assert_keys(&s, vec![2, 5], guard);
            assert!(!sl_remove(&s, 3));
            assert_keys(&s, vec![2, 5], guard);

            assert!(sl_remove(&s, 2));
            assert_keys(&s, vec![5], guard);
            assert!(sl_remove(&s, 5));
            assert_keys(&s, vec![], guard);

            sl_insert(&s, 3, 0);
            assert_keys(&s, vec![3], guard);
            sl_insert(&s, 1, 0);
            assert_keys(&s, vec![1, 3], guard);
            // overwrite
            assert!(!sl_insert(&s, 3, 0));
            assert_keys(&s, vec![1, 3], guard);
            sl_insert(&s, 5, 0);
            assert_keys(&s, vec![1, 3, 5], guard);

            assert!(sl_remove(&s, 3));
            assert_keys(&s, vec![1, 5], guard);
            assert!(sl_remove(&s, 1));
            assert_keys(&s, vec![5], guard);
            assert!(!sl_remove(&s, 3));
            assert_keys(&s, vec![5], guard);
            assert!(sl_remove(&s, 5));
            assert_keys(&s, vec![], guard);

            drop(s);
        }
    }

    #[test]
    fn get() {
        let s = default_list();
        sl_insert(&s, 30, 3);
        sl_insert(&s, 50, 5);
        sl_insert(&s, 10, 1);
        sl_insert(&s, 40, 4);
        sl_insert(&s, 20, 2);

        sl_get_assert(&s, 10, Some(1));
        sl_get_assert(&s, 20, Some(2));
        sl_get_assert(&s, 30, Some(3));
        sl_get_assert(&s, 40, Some(4));
        sl_get_assert(&s, 50, Some(5));

        sl_get_assert(&s, 7, None);
        sl_get_assert(&s, 27, None);
        sl_get_assert(&s, 31, None);
        sl_get_assert(&s, 97, None);
    }

    #[test]
    fn iter() {
        let guard = &crossbeam_epoch::pin();
        let s = default_list();
        for &x in &[4, 2, 12, 8, 7, 11, 5] {
            sl_insert(&s, x, x * 10);
        }

        assert_keys(&s, vec![2, 4, 5, 7, 8, 11, 12], guard);

        let mut it = s.iter();
        it.seek_to_first();
        // `it` is already pointing on 2
        sl_remove(&s, 2);
        let k = construct_key(2);
        assert_eq!(it.key(), &k);
        // init another iter (it2), `it2` should not read `2`
        let mut it2 = s.iter();
        it2.seek_to_first();
        assert_ne!(it2.key(), &k);

        // deleting the next key that the iterator would point to makes the iterator
        // skip the key
        sl_remove(&s, 4);
        it.next();
        let k = construct_key(5);
        assert_eq!(it.key(), &k);

        sl_remove(&s, 8);
        it.next();
        let k = construct_key(7);
        assert_eq!(it.key(), &k);

        sl_remove(&s, 12);
        it.next();
        let k = construct_key(11);
        assert_eq!(it.key(), &k);

        it.next();
        assert!(!it.valid());
    }

    #[test]
    fn test_seek() {
        let insert = [0, 4, 2, 12, 8, 7, 11, 5];
        let s = default_list();

        for i in insert {
            sl_insert(&s, i, i * 10);
        }

        let mut iter = s.iter();
        let k = construct_key(3);
        iter.seek(&k);
        let expected_k = construct_key(4);
        assert_eq!(iter.key(), &expected_k);

        let k = construct_key(12);
        iter.seek(&k);
        assert_eq!(iter.key(), &k);

        let k = construct_key(13);
        iter.seek(&k);
        assert!(!iter.valid());
    }

    #[test]
    fn test_prev() {
        let insert = [0, 4, 2, 12, 8, 7, 11, 5];
        let s = default_list();

        for i in insert {
            sl_insert(&s, i, i * 10);
        }

        let mut iter = s.iter();
        let mut iter2 = s.iter();
        let k = construct_key(20);
        iter.seek_for_prev(&k);
        for &i in [0, 2, 4, 5, 7, 8, 11, 12].iter().rev() {
            let k = construct_key(i);
            assert_eq!(iter.key(), &k);
            iter2.seek_for_prev(&k);
            assert_eq!(iter2.key(), &k);
            iter.prev()
        }
    }

    #[test]
    fn test_mem() {
        let sl = Skiplist::<ByteWiseComparator, RecorderLimiter>::new(
            ByteWiseComparator {},
            Arc::default(),
        );
        let n = 100000000;
        for i in 0..n {
            let k = format!("k{:0200}", i).into_bytes();
            let v = format!("v{:0200}", i).into_bytes();
            sl.put(k.clone(), v);
            sl.remove(&k);
        }
    }

    #[test]
    fn concurrent_put_and_remove() {
        for _ in 0..5 {
            let sl = Skiplist::<ByteWiseComparator, RecorderLimiter>::new(
                ByteWiseComparator {},
                Arc::default(),
            );
            let n = 100000;
            for i in (0..n).step_by(3) {
                let k = format!("k{:06}", i).into_bytes();
                let v = format!("v{:06}", i).into_bytes();
                sl.put(k, v);
            }
            let sl1 = sl.clone();
            let h1 = thread::spawn(move || {
                for i in (1..n).step_by(3) {
                    let k = format!("k{:06}", i).into_bytes();
                    let v = format!("v{:06}", i).into_bytes();
                    sl1.put(k, v);
                }
            });
            let sl2 = sl.clone();
            let h2 = thread::spawn(move || {
                for i in (0..n).step_by(3) {
                    let k = format!("k{:06}", i);
                    sl2.remove(k.as_bytes());
                }
            });

            let sl3 = sl.clone();
            let h3 = thread::spawn(move || {
                for i in (0..n).step_by(3) {
                    let k = format!("k{:06}", i);
                    sl3.remove(k.as_bytes());
                }
            });

            let sl4 = sl.clone();
            let h4 = thread::spawn(move || {
                for i in (2..n).step_by(3) {
                    let k = format!("k{:06}", i);
                    sl4.remove(k.as_bytes());
                }
            });

            h1.join().unwrap();
            h2.join().unwrap();
            h3.join().unwrap();
            h4.join().unwrap();

            let mut iter = sl.iter();
            iter.seek_to_first();
            for i in (1..n).step_by(3) {
                let expect_k = format!("k{:06}", i);
                let v = format!("v{:06}", i);
                let k = iter.key();
                assert_eq!(k.as_slice(), expect_k.as_bytes());
                iter.next();
            }
        }
    }

    #[test]
    fn test_get_with_delete() {
        let sl = Skiplist::<ByteWiseComparator, RecorderLimiter>::new(
            ByteWiseComparator {},
            Arc::default(),
        );

        sl.put(b"aaa".to_vec(), b"val-a".to_vec());

        let e = sl.get(b"aaa").unwrap();
        let ref_count = unsafe { &*e.node }.refs_and_height.load(Ordering::Relaxed) >> HEIGHT_BITS;
        assert_eq!(ref_count, 2);
        sl.remove(b"aaa");
        let ref_count = unsafe { &*e.node }.refs_and_height.load(Ordering::Relaxed) >> HEIGHT_BITS;
        assert_eq!(ref_count, 1);
        assert_eq!(e.value(), &b"val-a".to_vec());
    }
}
