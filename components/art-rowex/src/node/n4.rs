use super::{Node, NodeFooter, NodeImpl, NodePtr, NodeType, TaggedPtr};
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicU8, AtomicUsize, Ordering},
};

#[derive(Default)]
pub struct N4 {
    pub keys: [AtomicU8; 4],
    pub children: [AtomicUsize; 4],
}

impl NodeFooter for N4 {
    fn node_type() -> NodeType {
        NodeType::N4
    }
}

impl Node for NodeImpl<N4> {
    fn get_child(&self, key: u8) -> Option<TaggedPtr> {
        for i in 0..4 {
            let child = self.footer.children[i].load(Ordering::SeqCst);
            if child != 0 && self.footer.keys[i].load(Ordering::SeqCst) == key {
                unsafe {
                    return Some(TaggedPtr(NonZeroUsize::new_unchecked(child)));
                }
            }
        }
        None
    }

    fn insert(&mut self, key: u8, child: TaggedPtr) -> bool {
        let compact_count = self.compact_count as usize;
        if compact_count == 4 {
            return false;
        }
        self.footer.keys[compact_count].store(key, Ordering::Release);
        self.footer.children[compact_count].store(child.0.get(), Ordering::Release);
        self.compact_count += 1;
        self.count += 1;
        true
    }

    fn remove(&mut self, key: u8, _force: bool) -> bool {
        for i in 0..self.compact_count as usize {
            if self.footer.children[i].load(Ordering::SeqCst) != 0
                && self.footer.keys[i].load(Ordering::SeqCst) == key
            {
                self.count -= 1;
                self.footer.children[i].store(0, Ordering::SeqCst);
                return true;
            }
        }
        unreachable!()
    }

    fn copy_to(&self, mut node: NodePtr) {
        for i in 0..self.compact_count as usize {
            let child = self.footer.children[i].load(Ordering::SeqCst);
            if let Some(child) = NonZeroUsize::new(child).map(TaggedPtr) {
                let key = self.footer.keys[i].load(Ordering::SeqCst);
                node.insert(key, child);
            }
        }
        if let Some(leaf) = self.get_leaf() {
            node.set_leaf(leaf);
        }
    }

    fn change(&self, key: u8, val: TaggedPtr) {
        for i in 0..self.compact_count as usize {
            let child = self.footer.children[i].load(Ordering::SeqCst);
            if child != 0 && self.footer.keys[i].load(Ordering::SeqCst) == key {
                self.footer.children[i].store(val.0.get(), Ordering::Release);
                return;
            }
        }
        unreachable!()
    }

    fn get_any_child(&self) -> (TaggedPtr, u8) {
        let mut res = None;
        for i in 0..4 {
            let child = self.footer.children[i].load(Ordering::SeqCst);
            if let Some(child) = NonZeroUsize::new(child).map(TaggedPtr) {
                if child.is_entry() {
                    return (child, self.footer.keys[i].load(Ordering::SeqCst));
                }
                res = Some((child, self.footer.keys[i].load(Ordering::SeqCst)));
            }
        }
        res.expect("no child found")
    }

    fn get_second_child(&self, key: u8) -> (TaggedPtr, u8) {
        for i in 0..self.compact_count as usize {
            let child = self.footer.children[i].load(Ordering::SeqCst);
            if let Some(child) = NonZeroUsize::new(child).map(TaggedPtr) {
                let child_key = self.footer.keys[i].load(Ordering::SeqCst);
                if key != child_key {
                    return (child, child_key);
                }
            }
        }
        unreachable!()
    }

    fn get_children(&self, start: u8, end: u8, children: &mut [(u8, TaggedPtr)]) -> usize {
        let mut children_count = 0;
        for i in 0..4 {
            let key = self.footer.keys[i].load(Ordering::SeqCst);
            if key >= start && key <= end {
                let child = self.footer.children[i].load(Ordering::SeqCst);
                if let Some(child) = NonZeroUsize::new(child).map(TaggedPtr) {
                    children[children_count] = (key, child);
                    children_count += 1;
                }
            }
        }
        children[..children_count].sort_by_key(|c| c.0);
        children_count
    }
}
