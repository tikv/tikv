// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};

use super::{
    interfaces,
    interfaces::root::DB::{
        BaseBuffView, ColumnFamilyType, RaftCmdHeader, RawRustPtr, RawVoidPtr, WriteCmdType,
        WriteCmdsView,
    },
};

pub fn name_to_cf(cf: &str) -> ColumnFamilyType {
    if cf.is_empty() {
        return ColumnFamilyType::Default;
    }
    if cf == CF_LOCK {
        ColumnFamilyType::Lock
    } else if cf == CF_WRITE {
        ColumnFamilyType::Write
    } else if cf == CF_DEFAULT {
        ColumnFamilyType::Default
    } else {
        unreachable!()
    }
}

impl From<usize> for ColumnFamilyType {
    fn from(i: usize) -> Self {
        match i {
            0 => ColumnFamilyType::Lock,
            1 => ColumnFamilyType::Write,
            2 => ColumnFamilyType::Default,
            _ => unreachable!(),
        }
    }
}

#[derive(Default)]
pub struct WriteCmds {
    keys: Vec<BaseBuffView>,
    vals: Vec<BaseBuffView>,
    cmd_type: Vec<WriteCmdType>,
    cf: Vec<ColumnFamilyType>,
}

impl WriteCmds {
    pub fn with_capacity(cap: usize) -> WriteCmds {
        WriteCmds {
            keys: Vec::<BaseBuffView>::with_capacity(cap),
            vals: Vec::<BaseBuffView>::with_capacity(cap),
            cmd_type: Vec::<WriteCmdType>::with_capacity(cap),
            cf: Vec::<ColumnFamilyType>::with_capacity(cap),
        }
    }

    pub fn new() -> WriteCmds {
        WriteCmds::default()
    }

    pub fn push(&mut self, key: &[u8], val: &[u8], cmd_type: WriteCmdType, cf: ColumnFamilyType) {
        self.keys.push(key.into());
        self.vals.push(val.into());
        self.cmd_type.push(cmd_type);
        self.cf.push(cf);
    }

    pub fn len(&self) -> usize {
        self.cmd_type.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn gen_view(&self) -> WriteCmdsView {
        WriteCmdsView {
            keys: self.keys.as_ptr(),
            vals: self.vals.as_ptr(),
            cmd_types: self.cmd_type.as_ptr(),
            cmd_cf: self.cf.as_ptr(),
            len: self.cmd_type.len() as u64,
        }
    }
}

impl RaftCmdHeader {
    pub fn new(region_id: u64, index: u64, term: u64) -> Self {
        RaftCmdHeader {
            region_id,
            index,
            term,
        }
    }
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum RawRustPtrType {
    None = 0,
    ReadIndexTask = 1,
    ArcFutureWaker = 2,
    TimerTask = 3,
}

impl From<u32> for RawRustPtrType {
    fn from(x: u32) -> Self {
        unsafe { std::mem::transmute(x) }
    }
}

// TODO remove this warn.
#[allow(clippy::from_over_into)]
impl Into<u32> for RawRustPtrType {
    fn into(self) -> u32 {
        unsafe { std::mem::transmute(self) }
    }
}

pub extern "C" fn ffi_gc_rust_ptr(data: RawVoidPtr, type_: interfaces::root::DB::RawRustPtrType) {
    if data.is_null() {
        return;
    }
    let type_: RawRustPtrType = type_.into();
    match type_ {
        RawRustPtrType::ReadIndexTask => unsafe {
            drop(Box::from_raw(
                data as *mut crate::read_index_helper::ReadIndexTask,
            ));
        },
        RawRustPtrType::ArcFutureWaker => unsafe {
            drop(Box::from_raw(data as *mut crate::utils::ArcNotifyWaker));
        },
        RawRustPtrType::TimerTask => unsafe {
            drop(Box::from_raw(data as *mut crate::utils::TimerTask));
        },
        _ => unreachable!(),
    }
}

impl Default for RawRustPtr {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            type_: RawRustPtrType::None.into(),
        }
    }
}

impl RawRustPtr {
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }
}
