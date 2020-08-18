use super::{Node, NodeFooter, NodeImpl, NodePtr, NodeType, TaggedPtr};
use std::{
    mem::MaybeUninit,
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};

pub struct N256 {
    pub children: [AtomicUsize; 256],
}

impl Default for N256 {
    fn default() -> Self {
        let mut children: [AtomicUsize; 256] = unsafe { MaybeUninit::uninit().assume_init() };
        for elem in &mut children[..] {
            *elem = AtomicUsize::default();
        }
        N256 { children }
    }
}

impl NodeFooter for N256 {
    fn node_type() -> NodeType {
        NodeType::N256
    }
}

impl Node for NodeImpl<N256> {
    fn get_child(&self, key: u8) -> Option<TaggedPtr> {
        return NonZeroUsize::new(self.footer.children[key as usize].load(Ordering::SeqCst))
            .map(TaggedPtr);
    }

    fn insert(&mut self, key: u8, child: TaggedPtr) -> bool {
        self.footer.children[key as usize].store(child.0.get(), Ordering::Release);
        self.count += 1;
        true
    }

    fn remove(&mut self, key: u8, force: bool) -> bool {
        if self.count == 37 && !force {
            return false;
        }
        self.footer.children[key as usize].store(0, Ordering::Release);
        self.count -= 1;
        true
    }

    fn copy_to(&self, mut node: NodePtr) {
        for i in 0..256 {
            let child = self.footer.children[i].load(Ordering::SeqCst);
            if let Some(child) = NonZeroUsize::new(child).map(TaggedPtr) {
                node.insert(i as u8, child);
            }
        }
        if let Some(leaf) = self.get_leaf() {
            node.set_leaf(leaf);
        }
    }

    fn change(&self, key: u8, val: TaggedPtr) {
        self.footer.children[key as usize].store(val.0.get(), Ordering::Release);
    }

    fn get_any_child(&self) -> (TaggedPtr, u8) {
        let mut res = None;
        for i in 0..256 {
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
        unimplemented!("not supported")
    }

    fn get_children(&self, start: u8, end: u8, children: &mut [(u8, TaggedPtr)]) -> usize {
        let mut children_count = 0;
        for key in start..=end {
            let child = self.footer.children[key as usize].load(Ordering::SeqCst);
            if let Some(child) = NonZeroUsize::new(child).map(TaggedPtr) {
                children[children_count] = (key, child);
                children_count += 1;
            }
        }
        children_count
    }
}
