// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! Provides util functions to manage share properties across threads.

use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Default)]
struct GroupPropertiesInner {
    shutdown: AtomicBool,
}

#[derive(Default, Clone)]
pub struct GroupProperties {
    inner: Arc<GroupPropertiesInner>,
}

impl GroupProperties {
    #[inline]
    pub fn mark_shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
    }
}

thread_local! {
    static PROPERTIES: RefCell<Option<GroupProperties>> = RefCell::new(None);
}

pub fn current_properties() -> Option<GroupProperties> {
    PROPERTIES.with(|p| p.borrow().clone())
}

pub fn set_properties(props: Option<GroupProperties>) {
    PROPERTIES.with(move |p| {
        p.replace(props);
    })
}

/// Checks if the system is shutdown.
pub fn is_shutdown(ensure_set: bool) -> bool {
    PROPERTIES.with(|p| {
        if let Some(props) = &*p.borrow() {
            props.inner.shutdown.load(Ordering::SeqCst)
        } else if ensure_set {
            safe_panic!("group properties is not set");
            false
        } else {
            false
        }
    })
}

pub fn mark_shutdown() {
    PROPERTIES.with(|p| {
        if let Some(props) = &mut *p.borrow_mut() {
            props.mark_shutdown();
        }
    })
}
