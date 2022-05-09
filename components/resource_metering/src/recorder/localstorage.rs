// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    sync::{atomic::AtomicBool, Arc, Mutex},
};

use collections::HashMap;
use crossbeam::atomic::AtomicCell;
use tikv_util::sys::thread::Pid;

use crate::{model::SummaryRecord, TagInfos};

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
    pub attached_tag: SharedTagInfos,
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

#[derive(Clone, Default)]
pub struct SharedTagInfos {
    tag: Arc<AtomicCell<Option<Arc<TagInfos>>>>,
}

impl SharedTagInfos {
    pub fn new(tag: Arc<TagInfos>) -> Self {
        Self {
            tag: Arc::new(AtomicCell::new(Some(tag))),
        }
    }

    pub fn swap(&self, tag: Option<Arc<TagInfos>>) -> Option<Arc<TagInfos>> {
        self.tag.swap(tag)
    }

    pub fn load_full(&self) -> Option<Arc<TagInfos>> {
        self.tag.swap(None).map(|t| {
            let r = t.clone();
            assert!(self.tag.swap(Some(t)).is_none());
            r
        })
    }
}
