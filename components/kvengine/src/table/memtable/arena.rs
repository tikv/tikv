// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use rand::Rng;
use std::fmt::Display;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};
use std::sync::Arc;
use std::{mem, ptr, slice};

use super::super::table::Value;
use super::skl::{deref, Node, MAX_HEIGHT};
use super::WriteBatchEntry;

pub const NULL_ARENA_ADDR: u64 = 0;

const NULL_BLOCK_OFF: u32 = 0xffff_ffff;
const MAX_VAL_SIZE: u32 = 1 << 24 - 1;

const BLOCK_ALIGN: u32 = 8;
const ALIGN_MASK: u32 = 0xffff_fff8;
const BLOCK_IDX_MASK: u64 = 0xff00_0000_0000_0000;
const BLOCK_IDX_SHIFT: u64 = 56;
const BLOCK_OFF_MASK: u64 = 0x00ff_ffff_0000_0000;
const BLOCK_OFF_SHIFT: u64 = 32;
const SIZE_MASK: u64 = 0x0000_0000_00ff_ffff;
const FLAG_MASK: u64 = 0x0000_0000_ff00_0000;
const FLAG_SHIFT: u64 = 24;
const FLAG_VALUE_NODE: u8 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArenaAddr(pub u64);

impl Display for ArenaAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{}_{}_{}", self.block_idx(), self.block_off(), self.size()).as_str())
    }
}

impl ArenaAddr {
    fn new(block_idx: usize, block_off: u32, size: u32) -> ArenaAddr {
        ArenaAddr(
            ((block_idx as u64 + 1) << BLOCK_IDX_SHIFT)
                | ((block_off as u64) << BLOCK_OFF_SHIFT)
                | (size as u64),
        )
    }

    pub fn null() -> ArenaAddr {
        ArenaAddr(NULL_ARENA_ADDR)
    }

    pub fn block_idx(self) -> usize {
        (((self.0 & BLOCK_IDX_MASK) >> BLOCK_IDX_SHIFT) - 1) as usize
    }

    pub fn block_off(self) -> u32 {
        ((self.0 & BLOCK_OFF_MASK) >> BLOCK_OFF_SHIFT) as u32
    }

    pub fn size(self) -> usize {
        (self.0 & SIZE_MASK) as usize
    }

    fn mark_value_node_addr(&mut self) {
        self.0 |= (FLAG_VALUE_NODE as u64) << FLAG_SHIFT
    }

    pub fn is_value_node_addr(self) -> bool {
        ((self.0 & FLAG_MASK) >> FLAG_SHIFT) as u8 & FLAG_VALUE_NODE > 0
    }

    pub fn is_null(self) -> bool {
        return self.0 == NULL_ARENA_ADDR;
    }
}

const MAX_NUM_BLOCKS: usize = 256;

pub struct Arena {
    nodes: ArenaSegment,
    values: ArenaSegment,
    total_size: Arc<AtomicU32>,
    pub(crate) rand_id: i32,
}

#[allow(dead_code)]
impl Arena {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        let rand_id = rng.gen_range(0..i32::MAX);
        let total_size = Arc::new(AtomicU32::new(0));
        let s = Self {
            nodes: ArenaSegment::new(total_size.clone()),
            values: ArenaSegment::new(total_size.clone()),
            total_size,
            rand_id,
        };
        s
    }

    pub fn put_key(&self, key: &[u8]) -> ArenaAddr {
        let addr = self.nodes.alloc(key.len() as u32);
        let buf = self.nodes.get_mut_bytes(addr);
        unsafe {
            ptr::copy(key.as_ptr(), buf.as_mut_ptr(), key.len());
        }
        addr
    }

    pub fn put_val(&self, buf: &[u8], entry: &WriteBatchEntry) -> ArenaAddr {
        let size = entry.encoded_val_size();
        let addr = self.values.alloc(size as u32);
        let m_buf = self.values.get_mut_bytes(addr);
        m_buf[0] = entry.meta;
        m_buf[1] = entry.user_meta_len;
        LittleEndian::write_u64(&mut m_buf[2..], entry.version);
        m_buf[10..10 + entry.user_meta_len as usize].copy_from_slice(entry.user_meta(buf));
        let offset = 10 + entry.user_meta_len as usize;
        m_buf[offset..offset + entry.val_len as usize].copy_from_slice(entry.value(buf));
        addr
    }

    pub fn put_node(&self, height: usize, buf: &[u8], entry: &WriteBatchEntry) -> &mut Node {
        let node_size = mem::size_of::<Node>() - (MAX_HEIGHT - height) * 8;
        let node_addr = self.nodes.alloc(node_size as u32);
        let key_addr = self.put_key(entry.key(buf));
        let val_addr = self.put_val(buf, entry);
        let node_ptr = self.get_node(node_addr);
        let node = deref(node_ptr);
        node.addr = node_addr;
        node.height = height;
        node.key_addr = key_addr;
        node.value_addr.store(val_addr.0, Ordering::Relaxed);
        node
    }

    pub fn get_node(&self, addr: ArenaAddr) -> *mut Node {
        if addr.0 == NULL_ARENA_ADDR {
            return ptr::null_mut();
        }
        let bin = self.nodes.get_mut_bytes(addr);
        let ptr = bin.as_mut_ptr() as *mut Node;
        unsafe { &mut *ptr }
    }

    pub fn get_key(&self, node: &mut Node) -> &[u8] {
        self.nodes.get_bytes(node.key_addr)
    }

    pub fn get_val(&self, addr: ArenaAddr) -> Value {
        let bin = self.values.get_bytes(addr);
        Value::decode(bin)
    }

    pub fn put_val_node(&self, vn: ValueNode) -> ArenaAddr {
        let mut addr = self.nodes.alloc(VALUE_NODE_SIZE as u32);
        vn.encode(self.nodes.get_mut_bytes(addr));
        addr.mark_value_node_addr();
        addr
    }

    pub fn get_value_node(&self, addr: ArenaAddr) -> ValueNode {
        let mut vn: ValueNode = Default::default();
        vn.decode(self.nodes.get_bytes(addr));
        vn
    }

    pub fn size(&self) -> usize {
        self.total_size.load(Ordering::Acquire) as usize
    }
}

struct ArenaSegment {
    blocks: Vec<AtomicPtr<ArenaBlock>>,
    block_idx: AtomicU32,
    total_size: Arc<AtomicU32>,
}

impl ArenaSegment {
    fn new(total_size: Arc<AtomicU32>) -> Self {
        let mut blocks = Vec::with_capacity(MAX_NUM_BLOCKS);
        blocks.resize_with(MAX_NUM_BLOCKS, || AtomicPtr::<ArenaBlock>::default());
        let s = Self {
            blocks,
            block_idx: Default::default(),
            total_size,
        };
        let new_block = Box::into_raw(Box::new(ArenaBlock::new(block_cap(0))));
        s.blocks[0].store(new_block, Ordering::Release);
        s
    }

    fn alloc(&self, size: u32) -> ArenaAddr {
        if size > MAX_VAL_SIZE {
            panic!("value {} is too large", size)
        }
        let block_idx = self.block_idx.load(Ordering::Acquire) as usize;
        let block = self.get_block(block_idx);
        let block_off = block.alloc(size);
        if block_off != NULL_BLOCK_OFF {
            self.total_size.fetch_add(size, Ordering::AcqRel);
            return ArenaAddr::new(block_idx, block_off, size);
        }
        self.grow(size);
        self.alloc(size)
    }

    fn grow(&self, min_size: u32) {
        let block_idx = self.block_idx.load(Ordering::Acquire) as usize;
        let new_block_idx = block_idx + 1;
        let mut new_block_size = block_cap(block_idx);
        if new_block_size < min_size {
            new_block_size = min_size;
        }
        let new_block = Box::into_raw(Box::new(ArenaBlock::new(new_block_size)));
        self.blocks[new_block_idx].store(new_block, Ordering::Release);
        self.block_idx
            .store(new_block_idx as u32, Ordering::Release);
    }

    #[inline(always)]
    fn get_block(&self, block_idx: usize) -> &ArenaBlock {
        unsafe {
            if let Some(block) = self.blocks[block_idx].load(Ordering::Acquire).as_ref() {
                block
            } else {
                panic!(
                    "failed to get block idx {}, max {}",
                    block_idx,
                    self.block_idx.load(Ordering::Acquire),
                );
            }
        }
    }

    fn get_bytes(&self, addr: ArenaAddr) -> &[u8] {
        self.get_block(addr.block_idx())
            .get_bytes(addr.block_off(), addr.size())
    }

    fn get_mut_bytes(&self, addr: ArenaAddr) -> &mut [u8] {
        self.get_block(addr.block_idx())
            .get_mut_bytes(addr.block_off(), addr.size())
    }
}

impl Drop for ArenaSegment {
    fn drop(&mut self) {
        for i in 0..MAX_NUM_BLOCKS {
            let block = self.blocks[i].load(Ordering::Acquire);
            if block.is_null() {
                break;
            }
            unsafe {
                ptr::drop_in_place(block);
            }
        }
    }
}

fn block_cap(idx: usize) -> u32 {
    (idx as u32 + 1) * 32 * 1024
}

struct ArenaBlock {
    len: AtomicU32,
    cap: u32,
    ptr: *mut u8,
}

impl ArenaBlock {
    fn new(cap: u32) -> Self {
        let mut buf: Vec<u64> = vec![0; cap as usize / 8];
        let ptr = buf.as_mut_ptr() as *mut u8;
        mem::forget(buf);
        Self {
            len: AtomicU32::new(0),
            cap,
            ptr,
        }
    }

    pub fn get_mut_bytes(&self, offset: u32, size: usize) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut::<u8>(self.ptr.add(offset as usize), size) }
    }

    pub fn get_bytes(&self, offset: u32, size: usize) -> &[u8] {
        unsafe { slice::from_raw_parts::<u8>(self.ptr.add(offset as usize), size) }
    }

    pub fn alloc(&self, size: u32) -> u32 {
        // The returned addr should be aligned in 8 bytes.
        let offset = (self.len.load(Ordering::SeqCst) + BLOCK_ALIGN - 1) & ALIGN_MASK;
        let length = offset + size;
        if length > self.cap {
            return NULL_BLOCK_OFF;
        }
        self.len.store(length as u32, Ordering::Release);
        offset as u32
    }
}

impl Drop for ArenaBlock {
    fn drop(&mut self) {
        unsafe {
            Vec::from_raw_parts(self.ptr as *mut u64, 0, self.cap as usize / 8);
        }
    }
}

const VALUE_NODE_SIZE: usize = 16;

#[derive(Clone, Copy, Default)]
pub struct ValueNode {
    pub val_addr: ArenaAddr,
    pub next_val_addr: ArenaAddr,
}

impl ValueNode {
    fn encode(&self, buf: &mut [u8]) {
        LittleEndian::write_u64(buf, self.val_addr.0);
        LittleEndian::write_u64(&mut buf[8..], self.next_val_addr.0);
    }

    fn decode(&mut self, bin: &[u8]) {
        self.val_addr = ArenaAddr(LittleEndian::read_u64(bin));
        self.next_val_addr = ArenaAddr(LittleEndian::read_u64(&bin[8..]));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_addr() {
        let addr = ArenaAddr::new(1, 2, 3);
        assert_eq!(addr.block_idx(), 1);
        assert_eq!(addr.block_off(), 2);
        assert_eq!(addr.size(), 3);
    }

    #[test]
    fn test_arena() {
        let arena = Arena::new();
        let addr = arena.keys.alloc(8);
        let buf = arena.keys.get_mut_bytes(addr);
        buf[0] = 3;
        println!("{:?}", arena.keys.get_bytes(addr))
    }
}
