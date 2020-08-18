mod n16;
mod n256;
mod n4;
mod n48;

pub use n16::N16;
pub use n256::N256;
pub use n4::N4;
pub use n48::N48;

use super::NeedRestart;

use crossbeam_epoch::Guard;
use std::{
    mem,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

pub const INLINE_PREFIX_LEN: usize = 4;

pub trait Node {
    fn get_child(&self, key: u8) -> Option<TaggedPtr>;

    fn insert(&mut self, key: u8, child: TaggedPtr) -> bool;

    fn remove(&mut self, key: u8, force: bool) -> bool;

    fn copy_to(&self, node: NodePtr);

    fn change(&self, key: u8, val: TaggedPtr);

    fn get_any_child(&self) -> (TaggedPtr, u8);

    fn get_second_child(&self, key: u8) -> (TaggedPtr, u8);

    fn get_children(&self, start: u8, end: u8, children: &mut [(u8, TaggedPtr)]) -> usize;
}

#[repr(C)]
#[derive(Debug)]
pub struct NodeImpl<Footer> {
    meta: NodeMeta,
    prefix: AtomicU64,
    level: u32,
    count: u16,
    compact_count: u16,
    leaf: AtomicUsize,
    footer: Footer,
}

impl<Footer> NodeImpl<Footer> {
    pub fn get_level(&self) -> usize {
        self.level as usize
    }

    pub fn get_prefix(&self) -> Prefix {
        unsafe { mem::transmute(self.prefix.load(Ordering::SeqCst)) }
    }

    pub fn get_leaf(&self) -> Option<TaggedPtr> {
        NonZeroUsize::new(self.leaf.load(Ordering::SeqCst)).map(TaggedPtr)
    }

    pub fn set_leaf(&self, val: TaggedPtr) {
        self.leaf.store(val.0.get(), Ordering::Release);
    }
}

impl<Footer> NodeImpl<Footer>
where
    Footer: NodeFooter,
{
    pub fn new(level: usize, prefix: Prefix) -> Self {
        let prefix = unsafe { AtomicU64::new(mem::transmute(prefix)) };
        Self {
            meta: NodeMeta::new(Footer::node_type()),
            prefix,
            level: level as u32,
            count: 0,
            compact_count: 0,
            leaf: AtomicUsize::new(0),
            footer: Footer::default(),
        }
    }
}

pub trait NodeFooter: Default {
    fn node_type() -> NodeType;
}

/// From high to low, 2 bits for the type of the node, 60 bits for the version,
/// 1 bit marking the locking state, 1 bit marking obsolescence.
#[derive(Debug)]
pub struct NodeMeta(AtomicU64);

impl NodeMeta {
    pub fn new(node_type: NodeType) -> NodeMeta {
        let type_bits = (node_type as u64) << 62;
        NodeMeta(AtomicU64::new(0b100 | type_bits))
    }

    pub fn node_type(&self) -> NodeType {
        match self.0.load(Ordering::Relaxed) >> 62 {
            0 => NodeType::N4,
            1 => NodeType::N16,
            2 => NodeType::N48,
            3 => NodeType::N256,
            _ => unreachable!(),
        }
    }

    pub fn get_version(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }

    pub fn check_read_version(&self, start_read: u64) -> bool {
        start_read == self.0.load(Ordering::SeqCst)
    }

    pub fn write_lock(&self) -> Result<(), NeedRestart> {
        // TODO: reduce atomic operations
        loop {
            let mut version = self.0.load(Ordering::SeqCst);
            // wait until unlocked
            while Self::is_locked(version) {
                std::sync::atomic::spin_loop_hint();
                version = self.0.load(Ordering::SeqCst);
            }
            // restart if obsolete
            if Self::is_obsolete(version) {
                return Err(NeedRestart);
            }
            if self
                .0
                .compare_exchange_weak(version, version + 0b10, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    pub fn lock_version(&self, version: u64) -> Result<u64, NeedRestart> {
        if Self::is_locked(version) || Self::is_obsolete(version) {
            return Err(NeedRestart);
        }
        self.0
            .compare_exchange(version, version + 0b10, Ordering::SeqCst, Ordering::SeqCst)
            .map(|_| version + 0b10)
            .map_err(|_| NeedRestart)
    }

    pub fn write_unlock(&self) {
        self.0.fetch_add(0b10, Ordering::SeqCst);
    }

    pub fn write_unlock_obsolete(&self) {
        self.0.fetch_add(0b11, Ordering::SeqCst);
    }

    pub fn is_locked(version: u64) -> bool {
        version & 0b10 == 0b10
    }

    pub fn is_obsolete(version: u64) -> bool {
        version & 1 == 1
    }
}

#[derive(Debug)]
pub enum NodeType {
    N4 = 0,
    N16 = 1,
    N48 = 2,
    N256 = 3,
}

#[derive(Debug, Copy, Clone, Default)]
pub struct Prefix {
    pub len: u32,
    pub inline_prefix: [u8; INLINE_PREFIX_LEN],
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct NodePtr(NonNull<NodeImpl<()>>);

impl NodePtr {
    pub const fn dangling() -> Self {
        NodePtr(NonNull::dangling())
    }

    pub fn meta(&self) -> &NodeMeta {
        unsafe { &self.0.as_ref().meta }
    }

    pub fn get_type(&self) -> NodeType {
        unsafe { self.0.as_ref().meta.node_type() }
    }

    pub fn to_typed(&self) -> TypedNodePtr {
        match self.get_type() {
            NodeType::N4 => TypedNodePtr::N4(self.0.cast::<NodeImpl<N4>>()),
            NodeType::N16 => TypedNodePtr::N16(self.0.cast::<NodeImpl<N16>>()),
            NodeType::N48 => TypedNodePtr::N48(self.0.cast::<NodeImpl<N48>>()),
            NodeType::N256 => TypedNodePtr::N256(self.0.cast::<NodeImpl<N256>>()),
        }
    }

    pub fn get_level(&self) -> usize {
        unsafe { self.0.as_ref().get_level() }
    }

    pub fn get_prefix(&self) -> Prefix {
        unsafe { self.0.as_ref().get_prefix() }
    }

    pub fn get_count(&self) -> usize {
        unsafe { self.0.as_ref().count as usize }
    }

    pub fn get_leaf(&self) -> Option<TaggedPtr> {
        unsafe { self.0.as_ref().get_leaf() }
    }

    pub fn get_any_child_entry<K, V>(&self) -> NonNull<Entry<K, V>> {
        let mut next_node = *self;
        loop {
            let node = next_node;
            match node.get_any_child().0.untag() {
                UntaggedPtr::NodePtr(child) => next_node = child,
                UntaggedPtr::EntryPtr(entry) => return entry,
            }
        }
    }

    pub fn set_prefix(&self, prefix: Prefix) {
        unsafe {
            self.0
                .as_ref()
                .prefix
                .store(mem::transmute(prefix), Ordering::Release);
        }
    }

    pub fn set_leaf(&self, val: TaggedPtr) {
        unsafe { self.0.as_ref().set_leaf(val) }
    }

    pub fn remove_leaf(&self) {
        unsafe {
            self.0.as_ref().leaf.store(0, Ordering::Release);
        }
    }

    pub unsafe fn insert_and_unlock(
        &self,
        parent_node: NodePtr,
        parent_key: u8,
        key: u8,
        val: TaggedPtr,
        guard: &Guard,
    ) -> Result<(), NeedRestart> {
        match self.to_typed() {
            TypedNodePtr::N4(mut ptr) => {
                let node = ptr.as_mut();
                if node.compact_count == 4 && node.count <= 3 {
                    insert_compact(ptr, parent_node, parent_key, key, val, guard)?;
                    return Ok(());
                }
                insert_grow::<_, N16>(ptr, parent_node, parent_key, key, val, guard)?;
            }
            TypedNodePtr::N16(mut ptr) => {
                let node = ptr.as_mut();
                if node.compact_count == 16 && node.count <= 14 {
                    insert_compact(ptr, parent_node, parent_key, key, val, guard)?;
                    return Ok(());
                }
                insert_grow::<_, N48>(ptr, parent_node, parent_key, key, val, guard)?;
            }
            TypedNodePtr::N48(mut ptr) => {
                let node = ptr.as_mut();
                if node.compact_count == 48 && node.count != 48 {
                    insert_compact(ptr, parent_node, parent_key, key, val, guard)?;
                    return Ok(());
                }
                insert_grow::<_, N256>(ptr, parent_node, parent_key, key, val, guard)?;
            }
            TypedNodePtr::N256(mut ptr) => {
                let node = ptr.as_mut();
                node.insert(key, val);
                node.meta.write_unlock();
            }
        }
        Ok(())
    }

    pub unsafe fn add_prefix_before(&self, node: NodePtr, key: u8) {
        let this_prefix = self.get_prefix();
        let node_prefix = node.get_prefix();
        let copy_count = usize::min(INLINE_PREFIX_LEN, node_prefix.len as usize + 1);
        let mut new_prefix = this_prefix;
        for i in copy_count..usize::min(INLINE_PREFIX_LEN, this_prefix.len as usize + copy_count) {
            new_prefix.inline_prefix[i] = this_prefix.inline_prefix[i - copy_count];
        }
        for i in 0..usize::min(copy_count, node_prefix.len as usize) {
            new_prefix.inline_prefix[i] = node_prefix.inline_prefix[i];
        }
        if (node_prefix.len as usize) < INLINE_PREFIX_LEN {
            new_prefix.inline_prefix[copy_count - 1] = key;
        }
        new_prefix.len += node_prefix.len + 1;
        self.set_prefix(new_prefix);
    }

    pub unsafe fn remove_and_unlock(
        &self,
        parent_node: NodePtr,
        parent_key: u8,
        key: u8,
        guard: &Guard,
    ) -> Result<(), NeedRestart> {
        match self.to_typed() {
            TypedNodePtr::N4(mut ptr) => {
                let node = ptr.as_mut();
                node.remove(key, false);
                node.meta.write_unlock();
            }
            TypedNodePtr::N16(ptr) => {
                remove_and_shrink::<_, N4>(ptr, parent_node, parent_key, key, guard)?;
            }
            TypedNodePtr::N48(ptr) => {
                remove_and_shrink::<_, N16>(ptr, parent_node, parent_key, key, guard)?;
            }
            TypedNodePtr::N256(ptr) => {
                remove_and_shrink::<_, N48>(ptr, parent_node, parent_key, key, guard)?;
            }
        }
        Ok(())
    }

    pub unsafe fn drop(&self) {
        match self.to_typed() {
            TypedNodePtr::N4(ptr) => drop(Box::from_raw(ptr.as_ptr())),
            TypedNodePtr::N16(ptr) => drop(Box::from_raw(ptr.as_ptr())),
            TypedNodePtr::N48(ptr) => drop(Box::from_raw(ptr.as_ptr())),
            TypedNodePtr::N256(ptr) => drop(Box::from_raw(ptr.as_ptr())),
        }
    }
}

unsafe fn insert_grow<F1, F2>(
    mut node: NonNull<NodeImpl<F1>>,
    parent_node: NodePtr,
    parent_key: u8,
    key: u8,
    val: TaggedPtr,
    guard: &Guard,
) -> Result<(), NeedRestart>
where
    F1: NodeFooter,
    NodeImpl<F1>: Node,
    F2: NodeFooter,
    NodeImpl<F2>: Node,
{
    if node.as_mut().insert(key, val) {
        node.as_ref().meta.write_unlock();
        return Ok(());
    }
    let mut big_node = Box::new(NodeImpl::<F2>::new(
        node.as_ref().get_level(),
        node.as_ref().get_prefix(),
    ));
    big_node.insert(key, val);
    let big_node = NodePtr::from(big_node);
    node.as_ref().copy_to(big_node);

    if let Err(NeedRestart) = parent_node.meta().write_lock() {
        big_node.drop();
        node.as_ref().meta.write_unlock();
        return Err(NeedRestart);
    }

    parent_node.change(parent_key, big_node.into());
    parent_node.meta().write_unlock();

    node.as_ref().meta.write_unlock_obsolete();
    guard.defer_unchecked(move || {
        drop(Box::from_raw(node.as_ptr()));
    });

    Ok(())
}

unsafe fn insert_compact<Footer>(
    node: NonNull<NodeImpl<Footer>>,
    parent_node: NodePtr,
    parent_key: u8,
    key: u8,
    val: TaggedPtr,
    guard: &Guard,
) -> Result<(), NeedRestart>
where
    Footer: NodeFooter,
    NodeImpl<Footer>: Node,
{
    let mut new_node = Box::new(NodeImpl::<Footer>::new(
        node.as_ref().get_level(),
        node.as_ref().get_prefix(),
    ));
    new_node.insert(key, val);
    let new_node = NodePtr::from(new_node);
    node.as_ref().copy_to(new_node);

    if let Err(NeedRestart) = parent_node.meta().write_lock() {
        new_node.drop();
        node.as_ref().meta.write_unlock();
        return Err(NeedRestart);
    }

    parent_node.change(parent_key, new_node.into());
    parent_node.meta().write_unlock();

    node.as_ref().meta.write_unlock_obsolete();
    guard.defer_unchecked(move || {
        drop(Box::from_raw(node.as_ptr()));
    });

    Ok(())
}

unsafe fn remove_and_shrink<F1, F2>(
    mut node: NonNull<NodeImpl<F1>>,
    parent_node: NodePtr,
    parent_key: u8,
    key: u8,
    guard: &Guard,
) -> Result<(), NeedRestart>
where
    F1: NodeFooter,
    NodeImpl<F1>: Node,
    F2: NodeFooter,
    NodeImpl<F2>: Node,
{
    if node
        .as_mut()
        .remove(key, parent_node == NodePtr::dangling())
    {
        node.as_ref().meta.write_unlock();
        return Ok(());
    }

    let small_node = Box::new(NodeImpl::<F2>::new(
        node.as_ref().get_level(),
        node.as_ref().get_prefix(),
    ));

    if let Err(NeedRestart) = parent_node.meta().write_lock() {
        node.as_ref().meta.write_unlock();
        return Err(NeedRestart);
    }

    node.as_mut().remove(key, true);
    let small_node = NodePtr::from(small_node);
    node.as_ref().copy_to(small_node);
    parent_node.change(parent_key, small_node.into());

    parent_node.meta().write_unlock();
    node.as_ref().meta.write_unlock_obsolete();
    guard.defer_unchecked(move || {
        drop(Box::from_raw(node.as_ptr()));
    });

    Ok(())
}

impl<Footer> From<Box<NodeImpl<Footer>>> for NodePtr {
    fn from(node: Box<NodeImpl<Footer>>) -> NodePtr {
        let ptr = NonNull::from(Box::leak(node));
        NodePtr(ptr.cast())
    }
}

// Actually unsafe
impl Deref for NodePtr {
    type Target = dyn Node;

    fn deref(&self) -> &Self::Target {
        unsafe {
            match self.to_typed() {
                TypedNodePtr::N4(ptr) => &*ptr.as_ptr(),
                TypedNodePtr::N16(ptr) => &*ptr.as_ptr(),
                TypedNodePtr::N48(ptr) => &*ptr.as_ptr(),
                TypedNodePtr::N256(ptr) => &*ptr.as_ptr(),
            }
        }
    }
}

// Actually unsafe
impl DerefMut for NodePtr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            match self.to_typed() {
                TypedNodePtr::N4(ptr) => &mut *ptr.as_ptr(),
                TypedNodePtr::N16(ptr) => &mut *ptr.as_ptr(),
                TypedNodePtr::N48(ptr) => &mut *ptr.as_ptr(),
                TypedNodePtr::N256(ptr) => &mut *ptr.as_ptr(),
            }
        }
    }
}

pub enum TypedNodePtr {
    N4(NonNull<NodeImpl<N4>>),
    N16(NonNull<NodeImpl<N16>>),
    N48(NonNull<NodeImpl<N48>>),
    N256(NonNull<NodeImpl<N256>>),
}

pub struct Entry<K, V> {
    pub key: K,
    pub value: V,
}

#[derive(Debug, Copy, Clone)]
pub struct TaggedPtr(NonZeroUsize);

impl TaggedPtr {
    pub fn is_node(self) -> bool {
        self.0.get() & 1 == 0
    }

    pub fn is_entry(self) -> bool {
        !self.is_node()
    }

    pub fn untag<K, V>(self) -> UntaggedPtr<K, V> {
        let addr = self.0.get();
        if addr & 1 == 0 {
            unsafe { UntaggedPtr::NodePtr(NodePtr(NonNull::new_unchecked(addr as _))) }
        } else {
            let real_addr = addr ^ 1;
            unsafe { UntaggedPtr::EntryPtr(NonNull::new_unchecked(real_addr as _)) }
        }
    }
}

impl<K, V> From<NonNull<Entry<K, V>>> for TaggedPtr {
    fn from(ptr: NonNull<Entry<K, V>>) -> Self {
        if mem::align_of::<Entry<K, V>>() % 2 != 0 {
            panic!("the alignment of the entry must be a multiple of 2");
        }
        unsafe { TaggedPtr(NonZeroUsize::new_unchecked(ptr.as_ptr() as usize | 1)) }
    }
}

impl From<NodePtr> for TaggedPtr {
    fn from(ptr: NodePtr) -> Self {
        unsafe { TaggedPtr(NonZeroUsize::new_unchecked(ptr.0.as_ptr() as usize)) }
    }
}

pub enum UntaggedPtr<K, V> {
    NodePtr(NodePtr),
    EntryPtr(NonNull<Entry<K, V>>),
}
