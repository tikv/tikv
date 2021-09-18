// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::summary::SummaryRecord;
use crate::SharedTagPtr;
use collections::HashMap;
use crossbeam::channel::{unbounded, Receiver, Sender};
use lazy_static::lazy_static;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

lazy_static! {
    /// `STORAGE_CHAN` is used to transfer the necessary thread registration events.
    pub static ref STORAGE_CHAN: (Sender<LocalStorageRef>, Receiver<LocalStorageRef>) = unbounded();
}

thread_local! {
    /// `STORAGE` is a thread-localized instance of [LocalStorage].
    ///
    /// When a new thread tries to read `STORAGE`, it will actively send a message
    /// to [STORAGE_CHAN] during the initialization phase of thread local storage.
    /// The message([LocalStorageRef]) contains the thread id and a reference to
    /// the thread local storage structure.
    pub static STORAGE: LocalStorage = {
        let storage = LocalStorage {
            is_set: Arc::new(AtomicBool::new(false)),
            shared_ptr: SharedTagPtr::default(),
            summary_cur_record: Arc::new(SummaryRecord::default()),
            summary_records: Arc::new(Mutex::new(HashMap::default())),
        };
        STORAGE_CHAN.0.send(LocalStorageRef{id: thread_id::get(), storage: storage.clone()}).ok();
        storage
    };
}

/// `LocalStorage` is a thread-local structure that contains all necessary data of submodules.
///
/// In order to facilitate mutual reference, the thread-local data of all sub-modules
/// need to be stored centrally in `LocalStorage`.
#[derive(Clone)]
pub struct LocalStorage {
    pub is_set: Arc<AtomicBool>,
    pub shared_ptr: SharedTagPtr,
    pub summary_cur_record: Arc<SummaryRecord>,
    pub summary_records: Arc<Mutex<HashMap<Vec<u8>, SummaryRecord>>>,
}

/// This structure is transmitted as a event in [STORAGE_CHAN].
///
/// See [STORAGE] for more information.
pub struct LocalStorageRef {
    pub id: usize,
    pub storage: LocalStorage,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_chan() {
        STORAGE.with(|_| {}); // Just to trigger registration.
        std::thread::spawn(move || {
            STORAGE.with(|_| {});
        })
        .join()
        .unwrap();
        let mut count = 0;
        while let Ok(r) = STORAGE_CHAN.1.try_recv() {
            assert_ne!(r.id, 0);
            count += 1;
        }
        assert_eq!(count, 2);
    }
}
