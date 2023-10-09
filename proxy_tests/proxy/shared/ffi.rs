// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_store_ffi::ffi::{
    ffi_gc_rust_ptr, get_engine_store_server_helper,
    interfaces_ffi::{RawCppPtr, RawCppPtrArr, RawCppPtrTuple, RawVoidPtr, RustStrWithView},
    UnwrapExternCFunc, TEST_GC_OBJ_MONITOR,
};
use mock_engine_store::{mock_cluster::init_global_ffi_helper_set, mock_store::RawCppPtrTypeImpl};
use proxy_ffi::build_from_string;

#[test]
fn test_tuple_of_raw_cpp_ptr() {
    tikv_util::set_panic_hook(true, "./");
    unsafe {
        init_global_ffi_helper_set();
        let helper = get_engine_store_server_helper();

        let len = 10;
        let mut v: Vec<RawCppPtr> = vec![];

        for i in 0..len {
            let s = format!("s{}", i);
            let raw_cpp_ptr = (helper.fn_gen_cpp_string.into_inner())(s.as_bytes().into());
            v.push(raw_cpp_ptr);
        }

        let (ptr_v, l, cap) = v.into_raw_parts();
        for i in l..cap {
            let v = ptr_v.add(i);
            (*v).ptr = std::ptr::null_mut();
            (*v).type_ = RawCppPtrTypeImpl::None.into();
        }
        assert_ne!(l, cap);
        let cpp_ptr_tp = RawCppPtrTuple {
            inner: ptr_v,
            len: cap as u64,
        };
        drop(cpp_ptr_tp);
    }
}

#[test]
fn test_array_of_raw_cpp_ptr() {
    tikv_util::set_panic_hook(true, "./");
    unsafe {
        init_global_ffi_helper_set();
        let helper = get_engine_store_server_helper();

        let len = 10;
        let mut v: Vec<RawVoidPtr> = vec![];

        for i in 0..len {
            let s = format!("s{}", i);
            let raw_cpp_ptr = (helper.fn_gen_cpp_string.into_inner())(s.as_bytes().into());
            let raw_void_ptr = raw_cpp_ptr.into_raw();
            v.push(raw_void_ptr);
        }

        let (ptr_v, l, cap) = v.into_raw_parts();
        for i in l..cap {
            let v = ptr_v.add(i);
            *v = std::ptr::null_mut();
        }
        assert_ne!(l, cap);
        let cpp_ptr_arr = RawCppPtrArr {
            inner: ptr_v,
            type_: RawCppPtrTypeImpl::String.into(),
            len: cap as u64,
        };
        drop(cpp_ptr_arr);
    }
}

#[test]
fn test_carray_of_raw_cpp_ptr() {
    tikv_util::set_panic_hook(true, "./");
    unsafe {
        init_global_ffi_helper_set();
        let helper = get_engine_store_server_helper();

        const LEN: usize = 10;
        let mut v: [RawVoidPtr; LEN] = [std::ptr::null_mut(); LEN];

        for i in 0..LEN {
            let i = i as usize;
            let s = format!("s{}", i);
            let raw_cpp_ptr = (helper.fn_gen_cpp_string.into_inner())(s.as_bytes().into());
            let raw_void_ptr = raw_cpp_ptr.into_raw();
            v[i] = raw_void_ptr;
        }

        let pv1 = Box::into_raw(Box::new(v));
        (helper.fn_gc_raw_cpp_ptr_carr.into_inner())(
            pv1 as RawVoidPtr,
            RawCppPtrTypeImpl::String.into(),
            LEN as u64,
        );
    }
}

#[test]
fn test_rust_owned_objects() {
    let s = String::from("hello");
    let sv: Vec<u8> = s.as_bytes().to_owned();
    let rs: RustStrWithView = build_from_string(sv.clone());
    assert_eq!(rs.buff.to_slice(), s.as_bytes());
    ffi_gc_rust_ptr(rs.inner.ptr, rs.inner.type_);
    let df = RustStrWithView::default();
    ffi_gc_rust_ptr(df.inner.ptr, df.inner.type_);
    assert!(TEST_GC_OBJ_MONITOR.valid_clean_rust());
}
