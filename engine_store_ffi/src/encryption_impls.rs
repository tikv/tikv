// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{EncryptionKeyManager, EncryptionMethod, FileEncryptionInfo};

use crate::{
    get_engine_store_server_helper,
    interfaces::root::DB::{
        BaseBuffView, EncryptionMethod as EncryptionMethodImpl, FileEncryptionInfoRaw,
        FileEncryptionRes, RaftStoreProxyPtr, RawCppStringPtr,
    },
};

impl From<EncryptionMethod> for EncryptionMethodImpl {
    fn from(o: EncryptionMethod) -> Self {
        unsafe { std::mem::transmute(o) }
    }
}

pub extern "C" fn ffi_is_encryption_enabled(proxy_ptr: RaftStoreProxyPtr) -> u8 {
    unsafe { proxy_ptr.as_ref().key_manager.is_some().into() }
}

pub extern "C" fn ffi_encryption_method(proxy_ptr: RaftStoreProxyPtr) -> EncryptionMethodImpl {
    unsafe {
        proxy_ptr
            .as_ref()
            .key_manager
            .as_ref()
            .map_or(EncryptionMethod::Plaintext, |x| x.encryption_method())
            .into()
    }
}

impl FileEncryptionInfoRaw {
    fn new(res: FileEncryptionRes) -> Self {
        FileEncryptionInfoRaw {
            res,
            method: EncryptionMethod::Unknown.into(),
            key: std::ptr::null_mut(),
            iv: std::ptr::null_mut(),
            error_msg: std::ptr::null_mut(),
        }
    }

    fn error(error_msg: RawCppStringPtr) -> Self {
        FileEncryptionInfoRaw {
            res: FileEncryptionRes::Error,
            method: EncryptionMethod::Unknown.into(),
            key: std::ptr::null_mut(),
            iv: std::ptr::null_mut(),
            error_msg,
        }
    }

    fn from(f: FileEncryptionInfo) -> Self {
        FileEncryptionInfoRaw {
            res: FileEncryptionRes::Ok,
            method: f.method.into(),
            key: get_engine_store_server_helper().gen_cpp_string(&f.key),
            iv: get_engine_store_server_helper().gen_cpp_string(&f.iv),
            error_msg: std::ptr::null_mut(),
        }
    }
}

pub extern "C" fn ffi_handle_get_file(
    proxy_ptr: RaftStoreProxyPtr,
    name: BaseBuffView,
) -> FileEncryptionInfoRaw {
    unsafe {
        proxy_ptr.as_ref().key_manager.as_ref().map_or(
            FileEncryptionInfoRaw::new(FileEncryptionRes::Disabled),
            |key_manager| {
                let p = key_manager.get_file(std::str::from_utf8_unchecked(name.to_slice()));
                p.map_or_else(
                    |e| {
                        FileEncryptionInfoRaw::error(
                            get_engine_store_server_helper().gen_cpp_string(
                                format!("Encryption key manager get file failure: {}", e).as_ref(),
                            ),
                        )
                    },
                    FileEncryptionInfoRaw::from,
                )
            },
        )
    }
}

pub extern "C" fn ffi_handle_new_file(
    proxy_ptr: RaftStoreProxyPtr,
    name: BaseBuffView,
) -> FileEncryptionInfoRaw {
    unsafe {
        proxy_ptr.as_ref().key_manager.as_ref().map_or(
            FileEncryptionInfoRaw::new(FileEncryptionRes::Disabled),
            |key_manager| {
                let p = key_manager.new_file(std::str::from_utf8_unchecked(name.to_slice()));
                p.map_or_else(
                    |e| {
                        FileEncryptionInfoRaw::error(
                            get_engine_store_server_helper().gen_cpp_string(
                                format!("Encryption key manager new file failure: {}", e).as_ref(),
                            ),
                        )
                    },
                    FileEncryptionInfoRaw::from,
                )
            },
        )
    }
}

pub extern "C" fn ffi_handle_delete_file(
    proxy_ptr: RaftStoreProxyPtr,
    name: BaseBuffView,
) -> FileEncryptionInfoRaw {
    unsafe {
        proxy_ptr.as_ref().key_manager.as_ref().map_or(
            FileEncryptionInfoRaw::new(FileEncryptionRes::Disabled),
            |key_manager| {
                let p = key_manager.delete_file(std::str::from_utf8_unchecked(name.to_slice()));
                p.map_or_else(
                    |e| {
                        FileEncryptionInfoRaw::error(
                            get_engine_store_server_helper().gen_cpp_string(
                                format!("Encryption key manager delete file failure: {}", e)
                                    .as_ref(),
                            ),
                        )
                    },
                    |_| FileEncryptionInfoRaw::new(FileEncryptionRes::Ok),
                )
            },
        )
    }
}

pub extern "C" fn ffi_handle_link_file(
    proxy_ptr: RaftStoreProxyPtr,
    src: BaseBuffView,
    dst: BaseBuffView,
) -> FileEncryptionInfoRaw {
    unsafe {
        proxy_ptr.as_ref().key_manager.as_ref().map_or(
            FileEncryptionInfoRaw::new(FileEncryptionRes::Disabled),
            |key_manager| {
                let p = key_manager.link_file(
                    std::str::from_utf8_unchecked(src.to_slice()),
                    std::str::from_utf8_unchecked(dst.to_slice()),
                );
                p.map_or_else(
                    |e| {
                        FileEncryptionInfoRaw::error(
                            get_engine_store_server_helper().gen_cpp_string(
                                format!("Encryption key manager link file failure: {}", e).as_ref(),
                            ),
                        )
                    },
                    |_| FileEncryptionInfoRaw::new(FileEncryptionRes::Ok),
                )
            },
        )
    }
}
