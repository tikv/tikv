// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Mutex, time::Duration};

use int_enum::IntEnum;

use super::common::*;

pub unsafe fn create_cpp_str_parts(
    s: Option<Vec<u8>>,
) -> (interfaces_ffi::RawCppPtr, interfaces_ffi::BaseBuffView) {
    match s {
        Some(s) => {
            let len = s.len() as u64;
            let ptr = Box::into_raw(Box::new(s)); // leak
            (
                interfaces_ffi::RawCppPtr {
                    ptr: ptr as RawVoidPtr,
                    type_: RawCppPtrTypeImpl::String.into(),
                },
                interfaces_ffi::BaseBuffView {
                    data: (*ptr).as_ptr() as *const _,
                    len,
                },
            )
        }
        None => (
            interfaces_ffi::RawCppPtr {
                ptr: std::ptr::null_mut(),
                type_: RawCppPtrTypeImpl::None.into(),
            },
            interfaces_ffi::BaseBuffView {
                data: std::ptr::null(),
                len: 0,
            },
        ),
    }
}

pub unsafe fn create_cpp_str(s: Option<Vec<u8>>) -> interfaces_ffi::CppStrWithView {
    let (p, v) = create_cpp_str_parts(s);
    interfaces_ffi::CppStrWithView { inner: p, view: v }
}

#[repr(u32)]
#[derive(IntEnum, Copy, Clone)]
pub enum RawCppPtrTypeImpl {
    None = 0,
    String = 1,
    PreHandledSnapshotWithBlock = 11,
    WakerNotifier = 12,
    PSWriteBatch = 13,
    PSUniversalPage = 14,
    PSPageAndCppStr = 15,
}

impl From<RawCppPtrTypeImpl> for interfaces_ffi::RawCppPtrType {
    fn from(value: RawCppPtrTypeImpl) -> Self {
        assert_type_eq::assert_type_eq!(interfaces_ffi::RawCppPtrType, u32);
        value.int_value()
    }
}

impl From<interfaces_ffi::RawCppPtrType> for RawCppPtrTypeImpl {
    fn from(value: interfaces_ffi::RawCppPtrType) -> Self {
        if let Ok(s) = RawCppPtrTypeImpl::from_int(value) {
            s
        } else {
            panic!("unknown RawCppPtrType {:?}", value);
        }
    }
}

pub extern "C" fn ffi_gen_cpp_string(s: interfaces_ffi::BaseBuffView) -> interfaces_ffi::RawCppPtr {
    let str = Box::new(Vec::from(s.to_slice()));
    let ptr = Box::into_raw(str);
    interfaces_ffi::RawCppPtr {
        ptr: ptr as *mut _,
        type_: RawCppPtrTypeImpl::String.into(),
    }
}

pub struct RawCppStringPtrGuard(interfaces_ffi::RawCppStringPtr);

impl Default for RawCppStringPtrGuard {
    fn default() -> Self {
        Self(std::ptr::null_mut())
    }
}

impl std::convert::AsRef<interfaces_ffi::RawCppStringPtr> for RawCppStringPtrGuard {
    fn as_ref(&self) -> &interfaces_ffi::RawCppStringPtr {
        &self.0
    }
}
impl std::convert::AsMut<interfaces_ffi::RawCppStringPtr> for RawCppStringPtrGuard {
    fn as_mut(&mut self) -> &mut interfaces_ffi::RawCppStringPtr {
        &mut self.0
    }
}

impl Drop for RawCppStringPtrGuard {
    fn drop(&mut self) {
        interfaces_ffi::RawCppPtr {
            ptr: self.0 as *mut _,
            type_: RawCppPtrTypeImpl::String.into(),
        };
    }
}

impl RawCppStringPtrGuard {
    pub fn as_str(&self) -> &[u8] {
        let s = self.0 as *mut Vec<u8>;
        unsafe { &*s }
    }
}

pub struct ProxyNotifier {
    cv: std::sync::Condvar,
    mutex: Mutex<()>,
    // multi notifiers single receiver model. use another flag to avoid waiting until timeout.
    flag: std::sync::atomic::AtomicBool,
}

impl ProxyNotifier {
    pub fn blocked_wait_for(&self, timeout: Duration) {
        // if flag from false to false, wait for notification.
        // if flag from true to false, do nothing.
        if !self.flag.swap(false, std::sync::atomic::Ordering::AcqRel) {
            {
                let lock = self.mutex.lock().unwrap();
                if !self.flag.load(std::sync::atomic::Ordering::Acquire) {
                    let _cv = self.cv.wait_timeout(lock, timeout);
                }
            }
            self.flag.store(false, std::sync::atomic::Ordering::Release);
        }
    }

    pub fn wake(&self) {
        // if flag from false -> true, then wake up.
        // if flag from true -> true, do nothing.
        if !self.flag.swap(true, std::sync::atomic::Ordering::AcqRel) {
            let _lock = self.mutex.lock().unwrap();
            self.cv.notify_one();
        }
    }

    pub fn new_raw() -> RawCppPtr {
        let notifier = Box::new(Self {
            cv: Default::default(),
            mutex: Mutex::new(()),
            flag: std::sync::atomic::AtomicBool::new(false),
        });

        RawCppPtr {
            ptr: Box::into_raw(notifier) as _,
            type_: RawCppPtrTypeImpl::WakerNotifier.into(),
        }
    }
}
