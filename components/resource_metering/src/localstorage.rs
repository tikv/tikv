// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{utils, SharedTagPtr};

use std::cell::Cell;
use std::sync::Mutex;

use crossbeam::channel::Sender;
use lazy_static::lazy_static;

lazy_static! {
    /// `STORAGE_CHANS` is used to transfer the necessary thread registration events.
    static ref STORAGE_CHANS: Mutex<Vec<Sender<LocalStorageRef>>> = Mutex::new(Vec::new());
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
            is_set: Cell::new(false),
            shared_ptr: SharedTagPtr::default(),
        };
        let lsr = LocalStorageRef{id: utils::thread_id(), storage: storage.clone()};
        STORAGE_CHANS.lock().unwrap().iter().for_each(|sender| {
            sender.send(lsr.clone()).ok();
        });
        storage
    };
}

/// `LocalStorage` is a thread-local structure that contains all necessary data of submodules.
///
/// In order to facilitate mutual reference, the thread-local data of all sub-modules
/// need to be stored centrally in `LocalStorage`.
#[derive(Clone)]
pub struct LocalStorage {
    pub is_set: Cell<bool>,
    pub shared_ptr: SharedTagPtr,
}

/// This structure is transmitted as a event in [STORAGE_CHAN].
///
/// See [STORAGE] for more information.
#[derive(Clone)]
pub struct LocalStorageRef {
    pub id: usize,
    pub storage: LocalStorage,
}

/// Register a channel to notify thread creation events.
pub fn register_storage_chan_tx(tx: Sender<LocalStorageRef>) {
    STORAGE_CHANS.lock().unwrap().push(tx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::channel::unbounded;

    #[test]
    fn test_storage_chan() {
        let (tx, rx) = unbounded();
        register_storage_chan_tx(tx);
        STORAGE.with(|_| {}); // Just to trigger registration.
        std::thread::spawn(move || {
            STORAGE.with(|_| {});
        })
        .join()
        .unwrap();
        let mut count = 0;
        while let Ok(r) = rx.try_recv() {
            assert_ne!(r.id, 0);
            count += 1;
        }
        // This value may be greater than 2 if other test threads access `STORAGE` in parallel.
        assert!(count >= 2);
    }
}
