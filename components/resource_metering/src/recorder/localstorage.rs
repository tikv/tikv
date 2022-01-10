// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::model::SummaryRecord;
use crate::TagInfos;

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex};

use collections::HashMap;
use tikv_util::sys::thread::Pid;

thread_local! {
    /// `STORAGE` is a thread-localized instance of [LocalStorage].
    pub static STORAGE: RefCell<LocalStorage> = RefCell::new(LocalStorage::default());
}

/// This is a version of [ResourceMeteringTag] that can be shared across threads.
///
/// The typical scenario is that we need to access all threads' tags in the
/// [Recorder] thread for collection purposes, so we need a non-copy way to pass tags.
#[derive(Default, Clone)]
pub struct SharedTagPtr {
    ptr: Arc<AtomicPtr<TagInfos>>,
}

impl SharedTagPtr {
    pub fn new(v: Arc<TagInfos>) -> Self {
        Self {
            ptr: Arc::new(AtomicPtr::new(Arc::into_raw(v) as _)),
        }
    }

    /// Gets the tag under the pointer and replace the original value with parameter v.
    pub fn swap(&self, v: Option<Arc<TagInfos>>) -> Option<Arc<TagInfos>> {
        let ptr = v.map(|v| Arc::into_raw(v)).unwrap_or(std::ptr::null_mut());
        let prev = self.ptr.swap(ptr as _, Ordering::SeqCst);
        (!prev.is_null()).then(|| unsafe { Arc::from_raw(prev as _) })
    }

    /// Gets a clone of the tag under the pointer and put it back.
    pub fn load_full(&self) -> Option<Arc<TagInfos>> {
        self.swap(None).map(|req_tag| {
            let tag = req_tag.clone();
            // Put it back as quickly as possible.
            assert!(self.swap(Some(req_tag)).is_none());
            tag
        })
    }
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
    pub attached_tag: SharedTagPtr,
    pub summary_enable: Arc<AtomicBool>,
    pub summary_cur_record: Arc<SummaryRecord>,
    pub summary_records: Arc<Mutex<HashMap<Arc<TagInfos>, SummaryRecord>>>,
}

/// This structure is transmitted as a event in [Scheduler] of [Recorder].
///
/// See [ResourceTagFactory::register_local_storage] for more information.
#[derive(Clone)]
pub struct LocalStorageRef {
    pub id: Pid,
    pub storage: LocalStorage,
}
