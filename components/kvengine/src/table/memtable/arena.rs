// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use rand::Rng;
use std::fmt::Display;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};
use std::{mem, ptr, slice};

use super::super::table::Value;
use super::skl::{deref, Node, MAX_HEIGHT};
use super::WriteBatchEntry;

pub const NULL_ARENA_ADDR: u64 = 0;

const BLOCK_ALIGN: u32 = 7;
const ALIGN_MASK: u32 = 0xffff_fff8;
const NULL_BLOCK_OFF: u32 = 0xffff_ffff;
const MAX_VAL_SIZE: u32 = 1 << 24 - 1;

const VALUE_NODE_SHIFT: u64 = 63;
const VALUE_NODE_MASK: u64 = 0x8000_0000_0000_0000;
const BLOCK_IDX_SHIFT: u64 = 56;
const BLOCK_IDX_MASK: u64 = 0x7f00_0000_0000_0000;
const BLOCK_OFF_SHIFT: u64 = 24;
const BLOCK_OFF_MASK: u64 = 0x00ff_ffff_ff00_0000;
const SIZE_MASK: u64 = 0x0000_0000_00ff_ffff;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ArenaAddr(pub u64);

impl Display for ArenaAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(format!("{}_{}_{}", self.block_idx(), self.block_off(), self.size()).as_str())
    }
}

impl ArenaAddr {
    fn new(block_idx: usize, block_off: u32, size: u32) -> ArenaAddr {
        assert!(block_idx < 32);
        ArenaAddr(
            (block_idx as u64 + 1) << BLOCK_IDX_SHIFT
                | (block_off as u64) << BLOCK_OFF_SHIFT
                | size as u64,
        )
    }

    pub fn null() -> ArenaAddr {
        ArenaAddr(NULL_ARENA_ADDR)
    }

    pub fn block_idx(self) -> usize {
        assert!(self.0 > 0);
        (((self.0 & BLOCK_IDX_MASK) >> BLOCK_IDX_SHIFT) - 1) as usize
    }

    pub fn block_off(self) -> u32 {
        ((self.0 & BLOCK_OFF_MASK) >> BLOCK_OFF_SHIFT) as u32
    }

    pub fn size(self) -> usize {
        (self.0 & SIZE_MASK) as usize
    }

    fn mark_value_node_addr(&mut self) {
        self.0 = 1 << VALUE_NODE_SHIFT | self.0
    }

    pub fn is_value_node_addr(self) -> bool {
        self.0 & VALUE_NODE_MASK != 0
    }

    pub fn is_null(self) -> bool {
        return self.0 == NULL_ARENA_ADDR;
    }
}

const MB: u32 = 1024 * 1024;

const BLOCK_SIZE_ARRAY: [u32; 32] = [
    64 * 1024, // Make the first arena block small to save memory.
    1 * MB,
    1 * MB,
    1 * MB,
    2 * MB,
    2 * MB,
    3 * MB,
    3 * MB,
    4 * MB,
    5 * MB,
    6 * MB,
    7 * MB,
    8 * MB,
    10 * MB,
    12 * MB,
    14 * MB,
    16 * MB,
    20 * MB,
    24 * MB,
    28 * MB,
    32 * MB,
    40 * MB,
    48 * MB,
    56 * MB,
    64 * MB,
    96 * MB,
    128 * MB,
    192 * MB,
    256 * MB,
    384 * MB,
    512 * MB,
    768 * MB,
];

pub struct Arena {
    blocks: [AtomicPtr<ArenaBlock>; 32],
    block_idx: AtomicU32,
    pub(crate) rand_id: i32,
}

#[allow(dead_code)]
impl Arena {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        let rand_id = rng.gen_range(0..i32::MAX);

        let s = Self {
            blocks: Default::default(),
            block_idx: Default::default(),
            rand_id,
        };
        let new_block = Box::into_raw(Box::new(ArenaBlock::new(BLOCK_SIZE_ARRAY[0])));
        s.blocks[0].store(new_block, Ordering::Release);
        s
    }

    pub fn alloc(&self, size: u32) -> ArenaAddr {
        if size > MAX_VAL_SIZE {
            panic!("value {} is too large", size)
        }
        let block_idx = self.block_idx.load(Ordering::Acquire) as usize;
        let block = self.get_block(block_idx);
        let block_off = block.alloc(size);
        if block_off != NULL_BLOCK_OFF {
            return ArenaAddr::new(block_idx, block_off, size);
        }
        self.grow(size);
        self.alloc(size)
    }

    pub fn put_key(&self, key: &[u8]) -> ArenaAddr {
        let addr = self.alloc(key.len() as u32);
        let buf = self.get_mut_bytes(addr);
        unsafe {
            ptr::copy(key.as_ptr(), buf.as_mut_ptr(), key.len());
        }
        addr
    }

    pub fn put_val(&self, buf: &[u8], entry: &WriteBatchEntry) -> ArenaAddr {
        let size = entry.encoded_val_size();
        let addr = self.alloc(size as u32);
        let m_buf = self.get_mut_bytes(addr);
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
        let node_addr = self.alloc(node_size as u32);
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

    fn get_block(&self, block_idx: usize) -> &ArenaBlock {
        unsafe {
            self.blocks[block_idx]
                .load(Ordering::Acquire)
                .as_ref()
                .unwrap()
        }
    }

    pub fn get(&self, addr: ArenaAddr) -> &[u8] {
        self.get_block(addr.block_idx())
            .get_bytes(addr.block_off(), addr.size())
    }

    pub fn get_mut_bytes(&self, addr: ArenaAddr) -> &mut [u8] {
        self.get_block(addr.block_idx())
            .get_mut_bytes(addr.block_off(), addr.size())
    }

    fn grow(&self, min_size: u32) {
        let block_idx = self.block_idx.load(Ordering::Acquire) as usize;
        let new_block_idx = block_idx + 1;
        let mut new_block_size = BLOCK_SIZE_ARRAY[new_block_idx];
        if new_block_size < min_size {
            new_block_size = (min_size + BLOCK_ALIGN) & ALIGN_MASK;
        }
        let new_block = Box::into_raw(Box::new(ArenaBlock::new(new_block_size)));
        self.blocks[new_block_idx].store(new_block, Ordering::Release);
        self.block_idx
            .store(new_block_idx as u32, Ordering::Release);
    }

    pub fn get_node(&self, addr: ArenaAddr) -> *mut Node {
        if addr.0 == NULL_ARENA_ADDR {
            return ptr::null_mut();
        }
        if addr.block_idx() > 32 {
            error!(
                "{}, {}, {}, {}",
                addr.block_idx(),
                addr.block_off(),
                addr.size(),
                addr.0
            );
        }
        assert!(addr.block_idx() < 32, "{}, {}", addr.block_idx(), addr.0);
        let bin = self.get_mut_bytes(addr);
        let ptr = bin.as_mut_ptr() as *mut Node;
        unsafe { &mut *ptr }
    }

    fn get_key(&self, node: &mut Node) -> &[u8] {
        self.get(node.key_addr)
    }

    pub fn get_val(&self, addr: ArenaAddr) -> Value {
        let bin = self.get(addr);
        Value::decode(bin)
    }

    pub fn put_val_node(&self, vn: ValueNode) -> ArenaAddr {
        let mut addr = self.alloc(VALUE_NODE_SIZE as u32);
        vn.encode(self.get_mut_bytes(addr));
        addr.mark_value_node_addr();
        addr
    }

    pub fn get_value_node(&self, addr: ArenaAddr) -> ValueNode {
        let mut vn: ValueNode = Default::default();
        vn.decode(self.get(addr));
        vn
    }

    pub fn size(&self) -> usize {
        let block_idx = self.block_idx.load(Ordering::Acquire) as usize;
        let mut sum: usize = 0;
        for i in 0..block_idx {
            sum += BLOCK_SIZE_ARRAY[i] as usize;
        }
        sum += self.get_block(block_idx).len.load(Ordering::Acquire) as usize;
        sum
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        for i in 0..32 {
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
        let offset = (self.len.load(Ordering::Acquire) + BLOCK_ALIGN) & ALIGN_MASK;
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
        let addr = arena.alloc(8);
        let buf = arena.get_mut_bytes(addr);
        buf[0] = 3;
        println!("{:?}", arena.get(addr))
    }
}
