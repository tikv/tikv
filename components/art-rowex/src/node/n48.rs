use super::{Node, NodeFooter, NodeImpl, NodePtr, NodeType, TaggedPtr};
use std::{
    mem::MaybeUninit,
    num::NonZeroUsize,
    sync::atomic::{AtomicU8, AtomicUsize, Ordering},
};

const EMPTY_MARKER: u8 = 48;

pub struct N48 {
    pub child_index: [AtomicU8; 256],
    pub children: [AtomicUsize; 48],
}

impl Default for N48 {
    fn default() -> Self {
        let mut child_index: [AtomicU8; 256] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut children: [AtomicUsize; 48] = unsafe { MaybeUninit::uninit().assume_init() };
        for elem in &mut child_index[..] {
            *elem = AtomicU8::new(EMPTY_MARKER);
        }
        for elem in &mut children[..] {
            *elem = AtomicUsize::default();
        }
        N48 {
            child_index,
            children,
        }
    }
}

impl NodeFooter for N48 {
    fn node_type() -> NodeType {
        NodeType::N48
    }
}

impl Node for NodeImpl<N48> {
    fn get_child(&self, key: u8) -> Option<TaggedPtr> {
        let index = self.footer.child_index[key as usize].load(Ordering::SeqCst);
        if index == EMPTY_MARKER {
            return None;
        }
        let child = self.footer.children[index as usize].load(Ordering::SeqCst);
        unsafe { Some(TaggedPtr(NonZeroUsize::new_unchecked(child))) }
    }

    fn insert(&mut self, key: u8, child: TaggedPtr) -> bool {
        let compact_count = self.compact_count as usize;
        if compact_count == 48 {
            return false;
        }
        self.footer.child_index[key as usize].store(compact_count as u8, Ordering::Release);
        self.footer.children[compact_count].store(child.0.get(), Ordering::Release);
        self.compact_count += 1;
        self.count += 1;
        return true;
    }

    fn remove(&mut self, key: u8, force: bool) -> bool {
        if self.count == 12 && !force {
            return false;
        }
        let index = self.footer.child_index[key as usize].load(Ordering::SeqCst);
        assert_ne!(index, EMPTY_MARKER);
        self.footer.children[index as usize].store(0, Ordering::Release);
        self.footer.child_index[index as usize].store(EMPTY_MARKER, Ordering::Release);
        self.count -= 1;
        true
    }

    fn copy_to(&self, mut node: NodePtr) {
        for i in 0..256 {
            let index = self.footer.child_index[i].load(Ordering::SeqCst);
            if index != EMPTY_MARKER {
                let child = self.footer.children[index as usize].load(Ordering::SeqCst);
                unsafe {
                    node.insert(i as u8, TaggedPtr(NonZeroUsize::new_unchecked(child)));
                }
            }
        }
        if let Some(leaf) = self.get_leaf() {
            node.set_leaf(leaf);
        }
    }

    fn change(&self, key: u8, val: TaggedPtr) {
        let index = self.footer.child_index[key as usize].load(Ordering::SeqCst);
        assert_ne!(index, EMPTY_MARKER);
        self.footer.children[index as usize].store(val.0.get(), Ordering::Release);
    }

    fn get_any_child(&self) -> (TaggedPtr, u8) {
        let mut res = None;
        for i in 0..48 {
            let child = self.footer.children[i].load(Ordering::SeqCst);
            if let Some(child) = NonZeroUsize::new(child).map(TaggedPtr) {
                if child.is_entry() {
                    return (child, 0);
                }
                res = Some((child, 0));
            }
        }
        res.expect("no child found")
    }

    fn get_second_child(&self, _key: u8) -> (TaggedPtr, u8) {
        unimplemented!("unsupported")
    }

    fn get_children(&self, start: u8, end: u8, children: &mut [(u8, TaggedPtr)]) -> usize {
        let mut children_count = 0;
        for key in start..=end {
            let index = self.footer.child_index[key as usize].load(Ordering::SeqCst);
            if index != EMPTY_MARKER {
                let child = self.footer.children[index as usize].load(Ordering::SeqCst);
                if let Some(child) = NonZeroUsize::new(child).map(TaggedPtr) {
                    children[children_count] = (key, child);
                    children_count += 1;
                }
            }
        }
        children_count
    }
}
