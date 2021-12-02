// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::SharedTagPtr;

use std::cell::RefCell;

thread_local! {
    /// `STORAGE` is a thread-localized instance of [LocalStorage].
    pub static STORAGE: RefCell<LocalStorage> = RefCell::new(LocalStorage::default());
}

/// `LocalStorage` is a thread-local structure that contains all necessary data of submodules.
///
/// In order to facilitate mutual reference, the thread-local data of all sub-modules
/// need to be stored centrally in `LocalStorage`.
#[derive(Clone, Default)]
pub struct LocalStorage {
    pub registered: bool,
    pub register_failed_times: u32,
    pub is_set: bool,
    pub shared_ptr: SharedTagPtr,
}

/// This structure is transmitted as a event in [STORAGE_CHAN].
///
/// See [STORAGE] for more information.
#[derive(Clone)]
pub struct LocalStorageRef {
    pub id: usize,

    // TODO(zhongzc): change to shared_ptr to keep `LocalStorage` one per thread.
    pub storage: LocalStorage,
}
