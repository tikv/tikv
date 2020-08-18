#![feature(backtrace)]

mod node;

use crossbeam_epoch::Guard;
use node::*;
use std::{
    marker::PhantomData,
    mem::MaybeUninit,
    ops::{Bound, RangeBounds},
    ptr::NonNull,
};

pub struct Tree<K, V> {
    root: NodePtr,
    _phantom: PhantomData<(K, V)>,
}

unsafe impl<K: Send, V: Send> Send for Tree<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for Tree<K, V> {}

impl<K, V> Tree<K, V>
where
    K: AsRef<[u8]>,
{
    pub fn new() -> Self {
        Self {
            root: Box::new(NodeImpl::<N256>::new(0, Prefix::default())).into(),
            _phantom: PhantomData,
        }
    }

    pub fn lookup<'g>(&self, key: &K, _guard: &'g Guard) -> Option<&'g V> {
        let key = key.as_ref();
        let mut node = self.root;
        let mut level = 0;
        let mut opt_prefix_match = false;
        loop {
            let (res, next_level) = check_prefix(node, key, level);
            level = next_level;
            match res {
                CheckPrefixRes::NoMatch => return None,
                CheckPrefixRes::OptMatch => opt_prefix_match = true,
                _ => {}
            }
            if key.len() < level {
                return None;
            } else if key.len() == level {
                match node.get_leaf()?.untag::<K, V>() {
                    UntaggedPtr::EntryPtr(entry) => unsafe {
                        if opt_prefix_match && key != entry.as_ref().key.as_ref() {
                            return None;
                        } else {
                            return Some(&(*entry.as_ptr()).value);
                        }
                    },
                    _ => return None,
                }
            }

            match node.get_child(key[level])?.untag::<K, V>() {
                UntaggedPtr::NodePtr(child) => {
                    node = child;
                    level += 1;
                }
                UntaggedPtr::EntryPtr(entry) => unsafe {
                    if level < key.len() || opt_prefix_match {
                        if key != entry.as_ref().key.as_ref() {
                            return None;
                        }
                    }
                    return Some(&(*entry.as_ptr()).value);
                },
            }
        }
    }

    pub fn lookup_range<T>(
        &self,
        range: impl RangeBounds<K>,
        iter_fn: impl FnMut(&K, &V) -> Option<T>,
    ) -> Option<T> {
        let start_key = match range.start_bound() {
            Bound::Excluded(key) => Bound::Excluded(key.as_ref()),
            Bound::Included(key) => Bound::Included(key.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_key = match range.end_bound() {
            Bound::Excluded(key) => Bound::Excluded(key.as_ref()),
            Bound::Included(key) => Bound::Included(key.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let guard = crossbeam_epoch::pin();
        Iterator {
            prev_key: None,
            iter_fn,
            _guard: &guard,
            _phantom: PhantomData,
        }
        .iterate(self.root, start_key, end_key)
    }

    pub fn insert<'g>(&self, key: K, value: V, overwrite: bool, guard: &'g Guard) -> bool {
        let new_entry = NonNull::from(Box::leak(Box::new(Entry { key, value })));
        let key = unsafe { new_entry.as_ref().key.as_ref() };
        let insert_impl = || -> Result<bool, NeedRestart> {
            let mut node = NodePtr::dangling();
            let mut next_node = self.root;
            let mut parent_node;
            let mut parent_key;
            let mut node_key = 0;
            let mut level = 0;
            loop {
                parent_node = node;
                parent_key = node_key;
                node = next_node;
                let version = node.meta().get_version();
                let (res, next_level) = check_prefix_pessimistic::<K, V>(node, key, level);
                match res {
                    CheckPrefixPessimisticRes::SkippedLevel => return Err(NeedRestart),
                    CheckPrefixPessimisticRes::NoMatch {
                        non_matching_key,
                        non_matching_prefix,
                    } => {
                        node.meta().lock_version(version)?;

                        // 1) Create new node which will be parent of node, Set common prefix, level to this node
                        let mut prefix = node.get_prefix();
                        prefix.len = (next_level - level) as u32;
                        let mut new_node = Box::new(NodeImpl::<N4>::new(next_level, prefix));

                        // 2)  add old node and new node as children
                        new_node.insert(non_matching_key, node.into());
                        if next_level == key.len() {
                            // new key is prefix of old node
                            new_node.set_leaf(new_entry.into());
                        } else {
                            let child_key = key[next_level];
                            new_node.insert(child_key, new_entry.into());
                        }

                        // 3) update parent node to point to the new node, unlock
                        if let Err(NeedRestart) = parent_node.meta().write_lock() {
                            node.meta().write_unlock();
                            return Err(NeedRestart);
                        }
                        parent_node.change(parent_key, NodePtr::from(new_node).into());
                        parent_node.meta().write_unlock();

                        // 4) update prefix of node, unlock
                        node.set_prefix(Prefix {
                            len: node.get_prefix().len - (next_level - level + 1) as u32,
                            inline_prefix: non_matching_prefix,
                        });

                        node.meta().write_unlock();
                        return Ok(true);
                    }
                    CheckPrefixPessimisticRes::Match => {
                        // duplicate key
                        if next_level == key.len() {
                            node.meta().lock_version(version)?;
                            let success = if overwrite {
                                node.set_leaf(new_entry.into());
                                true
                            } else {
                                unsafe {
                                    drop(Box::from_raw(new_entry.as_ptr()));
                                }
                                false
                            };
                            node.meta().write_unlock();
                            return Ok(success);
                        }

                        level = next_level;
                        node_key = key[level];
                        match node.get_child(node_key).map(TaggedPtr::untag::<K, V>) {
                            Some(UntaggedPtr::NodePtr(child)) => {
                                next_node = child;
                                level += 1;
                            }
                            Some(UntaggedPtr::EntryPtr(entry)) => {
                                node.meta().lock_version(version)?;
                                let stored_key = unsafe { entry.as_ref().key.as_ref() };
                                level += 1;

                                let mut common_len = level;
                                while common_len < usize::min(stored_key.len(), key.len())
                                    && stored_key[common_len] == key[common_len]
                                {
                                    common_len += 1;
                                }

                                // duplicate key
                                if stored_key.len() == key.len() && common_len == stored_key.len() {
                                    let success = if overwrite {
                                        node.change(node_key, new_entry.into());
                                        true
                                    } else {
                                        unsafe {
                                            drop(Box::from_raw(new_entry.as_ptr()));
                                        }
                                        false
                                    };
                                    node.meta().write_unlock();
                                    return Ok(success);
                                }

                                let mut inline_prefix = [0; INLINE_PREFIX_LEN];
                                let prefix_len = common_len - level;
                                for i in 0..usize::min(prefix_len, INLINE_PREFIX_LEN) {
                                    inline_prefix[i] = key[level + i];
                                }
                                let mut n4 = Box::new(NodeImpl::<N4>::new(
                                    common_len,
                                    Prefix {
                                        len: prefix_len as u32,
                                        inline_prefix,
                                    },
                                ));

                                if common_len == stored_key.len() {
                                    // the key of the entry is the prefix of the new key
                                    n4.set_leaf(entry.into());
                                    n4.insert(key[common_len], new_entry.into());
                                } else if common_len == key.len() {
                                    // the new key is the prefix of the key of the entry
                                    n4.set_leaf(new_entry.into());
                                    n4.insert(stored_key[common_len], entry.into());
                                } else {
                                    n4.insert(key[common_len], new_entry.into());
                                    n4.insert(stored_key[common_len], entry.into());
                                }

                                node.change(key[level - 1], NodePtr::from(n4).into());
                                node.meta().write_unlock();
                                return Ok(true);
                            }
                            None => {
                                node.meta().lock_version(version)?;
                                unsafe {
                                    node.insert_and_unlock(
                                        parent_node,
                                        parent_key,
                                        node_key,
                                        new_entry.into(),
                                        guard,
                                    )?;
                                }
                                return Ok(true);
                            }
                        }
                    }
                }
            }
        };
        loop {
            match insert_impl() {
                Ok(res) => return res,
                Err(NeedRestart) => {}
            }
        }
    }

    pub fn remove(&self, key: &K, guard: &Guard) {
        let key = key.as_ref();
        let remove_impl = || -> Result<(), NeedRestart> {
            let mut node = NodePtr::dangling();
            let mut next_node = self.root;
            let mut parent_node;
            let mut parent_key;
            let mut node_key = 0;
            let mut level = 0;
            loop {
                parent_node = node;
                parent_key = node_key;
                node = next_node;
                let version = node.meta().get_version();

                let (res, next_level) = check_prefix(node, key, level);
                level = next_level;
                if let CheckPrefixRes::NoMatch = res {
                    if NodeMeta::is_obsolete(version) || !node.meta().check_read_version(version) {
                        return Err(NeedRestart);
                    }
                    return Ok(());
                }

                // to remove leaf
                if level == key.len() {
                    if let Some(UntaggedPtr::EntryPtr(entry)) =
                        node.get_leaf().map(TaggedPtr::untag::<K, V>)
                    {
                        node.remove_leaf();
                        unsafe {
                            guard.defer_unchecked(move || {
                                drop(Box::from_raw(entry.as_ptr()));
                            });
                        }
                        // if node has only one child, do a path compression
                        if node.get_count() == 1 && node != self.root {
                            node.meta().lock_version(version)?;
                            let (child, child_key) = node.get_any_child();
                            compress_and_unlock::<K, V>(
                                node,
                                parent_node,
                                parent_key,
                                child,
                                child_key,
                                guard,
                            )?;
                        }
                    }
                    return Ok(());
                }

                node_key = key[level];
                match node.get_child(node_key).map(TaggedPtr::untag::<K, V>) {
                    None => {
                        if NodeMeta::is_obsolete(version)
                            || !node.meta().check_read_version(version)
                        {
                            return Err(NeedRestart);
                        }
                        return Ok(());
                    }
                    Some(UntaggedPtr::EntryPtr(entry)) => {
                        node.meta().lock_version(version)?;
                        if unsafe { entry.as_ref().key.as_ref() } != key {
                            node.meta().write_unlock();
                            return Ok(());
                        }

                        let leaf = node.get_leaf();
                        if node.get_count() == 2 && node != self.root && leaf.is_none() {
                            let (child2, child2_key) = node.get_second_child(node_key);
                            compress_and_unlock::<K, V>(
                                node,
                                parent_node,
                                parent_key,
                                child2,
                                child2_key,
                                guard,
                            )?;
                        } else if node.get_count() == 1 && leaf.is_some() {
                            if let Err(NeedRestart) = parent_node.meta().write_lock() {
                                node.meta().write_unlock();
                                return Err(NeedRestart);
                            }
                            parent_node.change(parent_key, leaf.unwrap());
                            parent_node.meta().write_unlock();
                            node.meta().write_unlock_obsolete();
                            unsafe {
                                guard.defer_unchecked(move || {
                                    node.drop();
                                });
                            }
                        } else {
                            unsafe {
                                node.remove_and_unlock(parent_node, parent_key, key[level], guard)?;
                            }
                        }
                        unsafe {
                            guard.defer_unchecked(move || {
                                drop(Box::from_raw(entry.as_ptr()));
                            });
                        }
                        return Ok(());
                    }
                    Some(UntaggedPtr::NodePtr(child)) => {
                        next_node = child;
                        level += 1;
                    }
                }
            }
        };
        loop {
            match remove_impl() {
                Ok(res) => return res,
                Err(NeedRestart) => {}
            }
        }
    }
}

impl<K, V> Drop for Tree<K, V> {
    fn drop(&mut self) {
        unsafe {
            self.root.drop();
        }
    }
}

enum CheckPrefixRes {
    Match,
    NoMatch,
    OptMatch,
}

fn check_prefix(node: NodePtr, key: &[u8], mut level: usize) -> (CheckPrefixRes, usize) {
    let node_level = node.get_level();
    if key.len() < node_level {
        return (CheckPrefixRes::NoMatch, level);
    }
    let prefix = node.get_prefix();
    let prefix_len = prefix.len as usize;
    if prefix_len + level < node_level {
        return (CheckPrefixRes::OptMatch, node_level);
    }
    if prefix_len > 0 {
        for k in &prefix.inline_prefix
            [(level + prefix_len - node_level)..usize::min(prefix_len, INLINE_PREFIX_LEN)]
        {
            if *k != key[level] {
                return (CheckPrefixRes::NoMatch, level);
            }
            level += 1;
        }
        if prefix_len > INLINE_PREFIX_LEN {
            level += prefix_len - INLINE_PREFIX_LEN;
            return (CheckPrefixRes::OptMatch, level);
        }
    }
    (CheckPrefixRes::Match, level)
}

enum CheckPrefixPessimisticRes {
    Match,
    SkippedLevel,
    NoMatch {
        non_matching_key: u8,
        non_matching_prefix: [u8; INLINE_PREFIX_LEN],
    },
}

fn check_prefix_pessimistic<K, V>(
    node: NodePtr,
    key: &[u8],
    mut level: usize,
) -> (CheckPrefixPessimisticRes, usize)
where
    K: AsRef<[u8]>,
{
    let node_level = node.get_level();
    let prefix = node.get_prefix();
    let prefix_len = prefix.len as usize;

    if prefix_len + level < node_level {
        return (CheckPrefixPessimisticRes::SkippedLevel, level);
    }

    if prefix_len > 0 {
        let prev_level = level;
        let mut kt: &[u8] = &[];
        for i in (level + prefix_len - node_level)..prefix_len {
            if i == INLINE_PREFIX_LEN {
                let entry = node.get_any_child_entry::<K, V>();
                unsafe {
                    kt = (*entry.as_ptr()).key.as_ref();
                }
            }
            let cur_key = if i >= INLINE_PREFIX_LEN {
                kt[level]
            } else {
                prefix.inline_prefix[i]
            };
            if level >= key.len() || cur_key != key[level] {
                let mut non_matching_prefix = [0u8; INLINE_PREFIX_LEN];
                if prefix_len > INLINE_PREFIX_LEN {
                    if i < INLINE_PREFIX_LEN {
                        let entry = node.get_any_child_entry::<K, V>();
                        unsafe {
                            kt = (*entry.as_ptr()).key.as_ref();
                        }
                    }
                    for j in 0..usize::min(prefix_len - (level - prev_level) - 1, INLINE_PREFIX_LEN)
                    {
                        non_matching_prefix[j] = kt[level + j + 1];
                    }
                } else {
                    let rem_len = prefix_len - i - 1;
                    non_matching_prefix[..rem_len]
                        .copy_from_slice(&prefix.inline_prefix[(i + 1)..(i + 1 + rem_len)]);
                }
                return (
                    CheckPrefixPessimisticRes::NoMatch {
                        non_matching_key: cur_key,
                        non_matching_prefix,
                    },
                    level,
                );
            }
            level += 1;
        }
    }
    (CheckPrefixPessimisticRes::Match, level)
}

fn compress_and_unlock<K, V>(
    node: NodePtr,
    parent_node: NodePtr,
    parent_key: u8,
    child: TaggedPtr,
    child_key: u8,
    guard: &Guard,
) -> Result<(), NeedRestart> {
    match child.untag::<K, V>() {
        UntaggedPtr::EntryPtr(_) => {
            if let Err(NeedRestart) = parent_node.meta().write_lock() {
                node.meta().write_unlock();
                return Err(NeedRestart);
            }
            parent_node.change(parent_key, child);
            parent_node.meta().write_unlock();
            node.meta().write_unlock_obsolete();
            unsafe {
                guard.defer_unchecked(move || {
                    node.drop();
                });
            }
        }
        UntaggedPtr::NodePtr(child) => {
            let child_ver = child.meta().get_version();
            if let Err(NeedRestart) = child.meta().lock_version(child_ver) {
                node.meta().write_unlock();
                return Err(NeedRestart);
            }
            if let Err(NeedRestart) = parent_node.meta().write_lock() {
                node.meta().write_unlock();
                child.meta().write_unlock();
                return Err(NeedRestart);
            }
            unsafe {
                child.add_prefix_before(node, child_key);
            }
            parent_node.change(parent_key, child.into());

            parent_node.meta().write_unlock();
            node.meta().write_unlock_obsolete();
            child.meta().write_unlock();
            unsafe {
                guard.defer_unchecked(move || {
                    node.drop();
                })
            }
        }
    }
    Ok(())
}

struct Iterator<'a, K, V, T, F> {
    prev_key: Option<Bound<&'a [u8]>>,
    iter_fn: F,
    _guard: &'a Guard,
    _phantom: PhantomData<fn(&K, &V) -> Option<T>>,
}

impl<'a, K, V, T, F> Iterator<'a, K, V, T, F>
where
    K: AsRef<[u8]> + 'a,
    F: FnMut(&K, &V) -> Option<T>,
{
    fn iterate(
        &mut self,
        root: NodePtr,
        start_key: Bound<&[u8]>,
        end_key: Bound<&[u8]>,
    ) -> Option<T> {
        let mut iterate_impl = || -> Result<Option<T>, NeedRestart> {
            let start_key = if let Some(prev_key) = self.prev_key {
                prev_key
            } else {
                start_key
            };
            let mut level = 0;
            let mut node;
            let mut next_node = root;
            loop {
                node = next_node;
                let (res, next_level) = prefix_equals::<K, V>(node, start_key, end_key, level);
                level = next_level;
                match res {
                    PrefixEqualsRes::SkippedLevel => return Err(NeedRestart),
                    PrefixEqualsRes::NoMatch => return Ok(None),
                    PrefixEqualsRes::Contained => {
                        return Ok(self.iterate_child(node.into()));
                    }
                    PrefixEqualsRes::BothMatch => {
                        let handle_leaf = match start_key {
                            Bound::Unbounded => true,
                            Bound::Included(start) => level == start.len() - 1,
                            Bound::Excluded(_) => false,
                        };
                        if handle_leaf {
                            if let Some(res) = node.get_leaf().map(|leaf| self.iterate_child(leaf))
                            {
                                return Ok(res);
                            }
                        }

                        let start_level = match start_key {
                            Bound::Unbounded => None,
                            Bound::Included(start) | Bound::Excluded(start) => {
                                start.get(level).cloned()
                            }
                        };
                        let end_level = match end_key {
                            Bound::Unbounded => None,
                            Bound::Included(end) | Bound::Excluded(end) => end.get(level).cloned(),
                        };
                        let start_child = start_level.unwrap_or(0);
                        let end_child = end_level.unwrap_or(255);
                        if start_child != end_child {
                            let mut children: [(u8, TaggedPtr); 256] =
                                unsafe { MaybeUninit::uninit().assume_init() };
                            let children_count =
                                node.get_children(start_child, end_child, &mut children);
                            for &(key, child) in &children[..children_count] {
                                let res = if Some(key) == start_level {
                                    let start_key = match start_key {
                                        Bound::Unbounded => unreachable!(),
                                        Bound::Included(key) | Bound::Excluded(key) => key,
                                    };
                                    self.find_start(node.into(), level + 1, start_key)?
                                } else if Some(key) == end_level {
                                    let end_key = match end_key {
                                        Bound::Unbounded => unreachable!(),
                                        Bound::Included(key) | Bound::Excluded(key) => key,
                                    };
                                    self.find_end(node.into(), level + 1, end_key)?
                                } else if (start_level.is_none() || key > start_child)
                                    && (end_level.is_none() || key < end_child)
                                {
                                    self.iterate_child(child)
                                } else {
                                    None
                                };
                                if res.is_some() {
                                    return Ok(res);
                                }
                            }
                            return Ok(None);
                        } else {
                            next_node =
                                match node.get_child(start_child).map(TaggedPtr::untag::<K, V>) {
                                    Some(UntaggedPtr::NodePtr(ptr)) => ptr,
                                    _ => unreachable!(),
                                };
                            level += 1;
                        }
                    }
                }
            }
        };

        loop {
            match iterate_impl() {
                Ok(res) => return res,
                Err(NeedRestart) => {}
            }
        }
    }

    fn iterate_child(&mut self, child: TaggedPtr) -> Option<T> {
        match child.untag::<K, V>() {
            UntaggedPtr::EntryPtr(entry) => unsafe {
                let res = (self.iter_fn)(&entry.as_ref().key, &entry.as_ref().value);
                if res.is_some() {
                    return res;
                }
                self.prev_key = Some(Bound::Excluded(&(*entry.as_ptr()).key.as_ref()));
            },
            UntaggedPtr::NodePtr(node) => {
                if let Some(UntaggedPtr::EntryPtr(entry)) =
                    node.get_leaf().map(TaggedPtr::untag::<K, V>)
                {
                    unsafe {
                        let res = (self.iter_fn)(&entry.as_ref().key, &entry.as_ref().value);
                        if res.is_some() {
                            return res;
                        }
                        self.prev_key = Some(Bound::Excluded(&(*entry.as_ptr()).key.as_ref()));
                    }
                }
                let mut children: [(u8, TaggedPtr); 256] =
                    unsafe { MaybeUninit::uninit().assume_init() };
                let children_count = node.get_children(0, 255, &mut children);
                for &(_, child) in &children[..children_count] {
                    let res = self.iterate_child(child);
                    if res.is_some() {
                        return res;
                    }
                }
            }
        }
        None
    }

    fn find_start(
        &mut self,
        child: TaggedPtr,
        mut level: usize,
        start_key: &[u8],
    ) -> Result<Option<T>, NeedRestart> {
        match child.untag::<K, V>() {
            UntaggedPtr::EntryPtr(_) => Ok(self.iterate_child(child)),
            UntaggedPtr::NodePtr(node) => {
                let (res, next_level) = prefix_compare::<K, V>(node, start_key, level);
                level = next_level;
                match res {
                    PrefixCompareRes::Bigger => {
                        return Ok(self.iterate_child(child));
                    }
                    PrefixCompareRes::Equal => {
                        let start_level = start_key.get(level).cloned().unwrap_or(0);
                        let mut children: [(u8, TaggedPtr); 256] =
                            unsafe { MaybeUninit::uninit().assume_init() };
                        let children_count = node.get_children(start_level, 255, &mut children);
                        for &(key, child) in &children[..children_count] {
                            let res = if key == start_level {
                                self.find_start(child, level + 1, start_key)?
                            } else if key > start_level {
                                self.iterate_child(child)
                            } else {
                                None
                            };
                            if res.is_some() {
                                return Ok(res);
                            }
                        }
                        Ok(None)
                    }
                    PrefixCompareRes::SkippedLevel => Err(NeedRestart),
                    PrefixCompareRes::Smaller => Ok(None),
                }
            }
        }
    }

    fn find_end(
        &mut self,
        child: TaggedPtr,
        mut level: usize,
        end_key: &[u8],
    ) -> Result<Option<T>, NeedRestart> {
        match child.untag::<K, V>() {
            UntaggedPtr::EntryPtr(_) => Ok(None),
            UntaggedPtr::NodePtr(node) => {
                let (res, next_level) = prefix_compare::<K, V>(node, end_key, level);
                level = next_level;
                match res {
                    PrefixCompareRes::Smaller => {
                        return Ok(self.iterate_child(child));
                    }
                    PrefixCompareRes::Equal => {
                        let end_level = end_key.get(level).cloned().unwrap_or(255);
                        let mut children: [(u8, TaggedPtr); 256] =
                            unsafe { MaybeUninit::uninit().assume_init() };
                        let children_count = node.get_children(0, end_level, &mut children);
                        for &(key, child) in &children[..children_count] {
                            let res = if key == end_level {
                                self.find_end(child, level + 1, end_key)?
                            } else if key < end_level {
                                self.iterate_child(child)
                            } else {
                                None
                            };
                            if res.is_some() {
                                return Ok(res);
                            }
                        }
                        Ok(None)
                    }
                    PrefixCompareRes::SkippedLevel => Err(NeedRestart),
                    PrefixCompareRes::Bigger => Ok(None),
                }
            }
        }
    }
}

enum PrefixCompareRes {
    Smaller,
    Equal,
    Bigger,
    SkippedLevel,
}

fn prefix_compare<K, V>(node: NodePtr, key: &[u8], mut level: usize) -> (PrefixCompareRes, usize)
where
    K: AsRef<[u8]>,
{
    let prefix = node.get_prefix();
    let prefix_len = prefix.len as usize;
    let node_level = node.get_level();
    if prefix_len + level < node_level {
        return (PrefixCompareRes::SkippedLevel, level);
    }

    if prefix_len > 0 {
        let mut kt: &[u8] = &[];
        for i in (level + prefix_len - node_level)..prefix_len {
            if i == INLINE_PREFIX_LEN {
                let entry = node.get_any_child_entry::<K, V>();
                unsafe {
                    kt = (*entry.as_ptr()).key.as_ref();
                }
            }
            let key_level = key.get(level).cloned().unwrap_or(0);
            let cur_key = if i >= INLINE_PREFIX_LEN {
                kt[level]
            } else {
                prefix.inline_prefix[i]
            };
            if cur_key < key_level {
                return (PrefixCompareRes::Smaller, level);
            } else if cur_key > key_level {
                return (PrefixCompareRes::Bigger, level);
            }
            level += 1;
        }
    }
    (PrefixCompareRes::Equal, level)
}

enum PrefixEqualsRes {
    BothMatch,
    Contained,
    NoMatch,
    SkippedLevel,
}

fn prefix_equals<K, V>(
    node: NodePtr,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
    mut level: usize,
) -> (PrefixEqualsRes, usize)
where
    K: AsRef<[u8]>,
{
    let prefix = node.get_prefix();
    let prefix_len = prefix.len as usize;
    let node_level = node.get_level();
    if prefix_len + level < node_level {
        return (PrefixEqualsRes::SkippedLevel, level);
    }

    if prefix_len > 0 {
        let mut kt: &[u8] = &[];
        for i in (level + prefix_len - node_level)..prefix_len {
            if i == INLINE_PREFIX_LEN {
                let entry = node.get_any_child_entry::<K, V>();
                unsafe {
                    kt = (*entry.as_ptr()).key.as_ref();
                }
            }
            let cur_key = if i >= INLINE_PREFIX_LEN {
                kt[level]
            } else {
                prefix.inline_prefix[i]
            };

            #[derive(Copy, Clone, Eq, PartialEq)]
            enum BoundCheck {
                In,
                Out,
                Unknown,
            }

            let start_check = match start {
                Bound::Unbounded => BoundCheck::In,
                Bound::Included(start) => {
                    if level >= start.len() || cur_key > start[level] {
                        BoundCheck::In
                    } else if cur_key < start[level] {
                        BoundCheck::Out
                    } else if level == start.len() - 1 {
                        BoundCheck::In
                    } else {
                        BoundCheck::Unknown
                    }
                }
                Bound::Excluded(start) => {
                    if level >= start.len() || cur_key > start[level] {
                        BoundCheck::In
                    } else if cur_key < start[level] {
                        BoundCheck::Out
                    } else {
                        BoundCheck::Unknown
                    }
                }
            };

            let end_check = match end {
                Bound::Unbounded => BoundCheck::In,
                Bound::Included(end) => {
                    if level >= end.len() || cur_key > end[level] {
                        BoundCheck::Out
                    } else if cur_key < end[level] {
                        BoundCheck::In
                    } else {
                        BoundCheck::Unknown
                    }
                }
                Bound::Excluded(end) => {
                    if level >= end.len() || cur_key > end[level] {
                        BoundCheck::Out
                    } else if cur_key < end[level] {
                        BoundCheck::In
                    } else if level == end.len() - 1 {
                        BoundCheck::Out
                    } else {
                        BoundCheck::Unknown
                    }
                }
            };

            if start_check == BoundCheck::In && end_check == BoundCheck::In {
                return (PrefixEqualsRes::Contained, level);
            } else if start_check == BoundCheck::Out || end_check == BoundCheck::Out {
                return (PrefixEqualsRes::NoMatch, level);
            }
            level += 1;
        }
    }
    (PrefixEqualsRes::BothMatch, level)
}

pub struct NeedRestart;

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_epoch::pin;
    use rand::prelude::*;
    use std::collections::HashSet;

    #[test]
    fn iter() {
        let tree = Tree::new();
        tree.insert("ab", 2, true, &pin());
        tree.insert("a", 1, true, &pin());
        tree.insert("abc", 3, true, &pin());
        tree.lookup_range(.., |k, v| -> Option<()> {
            println!("{:?} {:?}", k, v);
            None
        });
    }

    #[test]
    fn random_insert_get() {
        let mut rng = thread_rng();
        let tree = Tree::new();
        let mut buf = [0; 64];
        let mut stored = HashSet::new();
        for i in 0..1000000 {
            let insert: f64 = rng.gen();
            if insert < 0.8 {
                let len = if rng.gen() {
                    rng.gen_range(0, 64)
                } else {
                    rng.gen_range(0, 8)
                };
                rng.fill_bytes(&mut buf[..len]);
                let v = buf[..len].to_vec();
                stored.insert(v.clone());
                tree.insert(v, i, true, &pin());
            } else {
                if rng.gen() {
                    if let Some(v) = stored.iter().next().cloned() {
                        stored.remove(&v);
                        tree.remove(&v, &pin());
                    }
                } else {
                    let len = if rng.gen() {
                        rng.gen_range(0, 64)
                    } else {
                        rng.gen_range(0, 8)
                    };
                    rng.fill_bytes(&mut buf[..len]);
                    let v = buf[..len].to_vec();
                    tree.remove(&v, &pin());
                }
            }
        }
    }
}
