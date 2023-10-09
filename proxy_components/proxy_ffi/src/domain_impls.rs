// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::pin::Pin;

use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};

use super::{
    interfaces_ffi,
    interfaces_ffi::{
        BaseBuffView, ColumnFamilyType, RaftCmdHeader, RawRustPtr, RawVoidPtr, RustStrWithView,
        RustStrWithViewVec, SSTView, SSTViewVec, WriteCmdType, WriteCmdsView,
    },
    read_index_helper, utils,
};

pub fn into_sst_views(snaps: Vec<(&[u8], ColumnFamilyType)>) -> Vec<SSTView> {
    let mut snaps_view = vec![];
    for (path, cf) in snaps {
        snaps_view.push(SSTView {
            type_: cf,
            path: path.into(),
        })
    }
    snaps_view
}

impl From<Pin<&Vec<SSTView>>> for SSTViewVec {
    fn from(snaps_view: Pin<&Vec<SSTView>>) -> Self {
        Self {
            views: snaps_view.as_ptr(),
            len: snaps_view.len() as u64,
        }
    }
}

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

pub fn cf_to_name(cf: ColumnFamilyType) -> &'static str {
    match cf {
        ColumnFamilyType::Default => CF_DEFAULT,
        ColumnFamilyType::Lock => CF_LOCK,
        ColumnFamilyType::Write => CF_WRITE,
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
    String = 4,
    VecOfString = 5,
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

pub extern "C" fn ffi_gc_rust_ptr(data: RawVoidPtr, type_: interfaces_ffi::RawRustPtrType) {
    if data.is_null() {
        return;
    }
    let type_: RawRustPtrType = type_.into();
    match type_ {
        RawRustPtrType::ReadIndexTask => unsafe {
            drop(Box::from_raw(data as *mut read_index_helper::ReadIndexTask));
        },
        RawRustPtrType::ArcFutureWaker => unsafe {
            drop(Box::from_raw(data as *mut utils::ArcNotifyWaker));
        },
        RawRustPtrType::TimerTask => unsafe {
            drop(Box::from_raw(data as *mut utils::TimerTask));
        },
        RawRustPtrType::String => unsafe {
            drop(Box::from_raw(data as *mut RustStrWithViewInner));
        },
        RawRustPtrType::VecOfString => unsafe {
            drop(Box::from_raw(data as *mut RustStrWithViewVecInner));
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

#[derive(Default)]
pub struct TestGcObjectMonitor {
    rust: std::sync::Mutex<collections::HashMap<interfaces_ffi::RawRustPtrType, isize>>,
}

impl TestGcObjectMonitor {
    pub fn add_rust(&self, t: &interfaces_ffi::RawRustPtrType, x: isize) {
        use std::collections::hash_map::Entry;
        let data = &mut *self.rust.lock().unwrap();
        match data.entry(*t) {
            Entry::Occupied(mut v) => {
                *v.get_mut() += x;
            }
            Entry::Vacant(v) => {
                v.insert(x);
            }
        }
    }
    pub fn valid_clean_rust(&self) -> bool {
        let data = &*self.rust.lock().unwrap();
        for (k, v) in data {
            if *v != 0 {
                tikv_util::error!(
                    "TestGcObjectMonitor::valid_clean failed at type {} refcount {}",
                    k,
                    v
                );
                return false;
            }
        }
        true
    }
    pub fn is_empty_rust(&self) -> bool {
        let data = &*self.rust.lock().unwrap();
        data.is_empty()
    }
}

#[cfg(any(test, feature = "testexport"))]
lazy_static::lazy_static! {
    pub static ref TEST_GC_OBJ_MONITOR: TestGcObjectMonitor = TestGcObjectMonitor::default();
}

impl Default for RustStrWithViewVec {
    fn default() -> Self {
        RustStrWithViewVec {
            buffs: std::ptr::null_mut(),
            len: 0,
            inner: RawRustPtr::default(),
        }
    }
}

struct RustStrWithViewVecInner {
    // Hold the Vec of String.
    #[allow(clippy::box_collection)]
    _data: Pin<Box<Vec<Vec<u8>>>>,
    // Hold the BaseBuffView array.
    #[allow(clippy::box_collection)]
    buff_view_vec: Pin<Box<Vec<BaseBuffView>>>,
}

impl Drop for RustStrWithViewVecInner {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "testexport"))]
        {
            TEST_GC_OBJ_MONITOR.add_rust(&RawRustPtrType::VecOfString.into(), -1);
        }
    }
}

pub fn build_from_vec_string(s: Vec<Vec<u8>>) -> RustStrWithViewVec {
    let vec_len = s.len();
    let vec_len_64: u64 = vec_len as u64;
    let inner_vec_of_string = Box::pin(s);
    let mut buff_view_vec: Vec<BaseBuffView> = Vec::with_capacity(vec_len);
    for i in 0..vec_len {
        buff_view_vec.push(BaseBuffView {
            data: inner_vec_of_string[i].as_ptr() as *const _,
            len: inner_vec_of_string[i].len() as u64,
        });
    }
    let inner = Box::new(RustStrWithViewVecInner {
        _data: inner_vec_of_string,
        buff_view_vec: Box::pin(buff_view_vec),
    });
    #[cfg(any(test, feature = "testexport"))]
    {
        TEST_GC_OBJ_MONITOR.add_rust(&RawRustPtrType::VecOfString.into(), 1);
    }
    let inner_wrapped = RawRustPtr {
        ptr: inner.as_ref() as *const _ as RawVoidPtr,
        type_: RawRustPtrType::VecOfString.into(),
    };
    let buff_view_vec_ptr = inner.buff_view_vec.as_ptr();
    std::mem::forget(inner);

    RustStrWithViewVec {
        buffs: buff_view_vec_ptr,
        len: vec_len_64,
        inner: inner_wrapped,
    }
}

impl Default for RustStrWithView {
    fn default() -> Self {
        RustStrWithView {
            buff: BaseBuffView {
                data: std::ptr::null(),
                len: 0,
            },
            inner: RawRustPtr::default(),
        }
    }
}

struct RustStrWithViewInner {
    // Hold the String.
    #[allow(clippy::box_collection)]
    _data: Pin<Box<Vec<u8>>>,
}

impl Drop for RustStrWithViewInner {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "testexport"))]
        {
            TEST_GC_OBJ_MONITOR.add_rust(&RawRustPtrType::String.into(), -1);
        }
    }
}

pub fn build_from_string(s: Vec<u8>) -> RustStrWithView {
    let str_len = s.len();
    let inner_string = Box::pin(s);
    let buff = BaseBuffView {
        data: inner_string.as_ptr() as *const _,
        len: str_len as u64,
    };
    let inner = Box::new(RustStrWithViewInner {
        _data: inner_string,
    });
    #[cfg(any(test, feature = "testexport"))]
    {
        TEST_GC_OBJ_MONITOR.add_rust(&RawRustPtrType::String.into(), 1);
    }
    let inner_wrapped = RawRustPtr {
        ptr: inner.as_ref() as *const _ as RawVoidPtr,
        type_: RawRustPtrType::String.into(),
    };
    std::mem::forget(inner);

    RustStrWithView {
        buff,
        inner: inner_wrapped,
    }
}
