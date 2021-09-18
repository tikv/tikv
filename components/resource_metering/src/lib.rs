// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// TODO(mornyx): crate doc.

#![feature(shrink_to)]
#![feature(hash_drain_filter)]

use config::GLOBAL_ENABLE;
use localstorage::STORAGE;
use std::pin::Pin;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::Arc;
use std::task::{Context, Poll};

mod client;
mod collector;
mod config;
mod cpu;
mod localstorage;
mod recorder;
mod reporter;
mod summary;
mod utils;

pub use client::{Client, GrpcClient};
pub use collector::Collector;
pub use config::{Config, ConfigManager};
pub use cpu::{CpuRecorder, CpuRecords, CpuReporter, RawCpuRecords};
pub use recorder::{build_default_recorder, Recorder, RecorderBuilder, RecorderHandle};
pub use reporter::{build_default_reporter, Reporter};
pub use summary::{
    record_read_keys, record_write_keys, SummaryRecord, SummaryRecorder, SummaryReporter,
};

/// This structure is used as a label to distinguish different request contexts.
///
/// In order to associate `ResourceMeteringTag` with a certain piece of code logic,
/// we added a function to [Future] to bind `ResourceMeteringTag` to the specified
/// future context. It is used in the main business logic of TiKV.
///
/// [Future]: futures::Future
#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct ResourceMeteringTag {
    infos: Arc<TagInfos>,
}

impl From<Arc<TagInfos>> for ResourceMeteringTag {
    fn from(infos: Arc<TagInfos>) -> Self {
        Self { infos }
    }
}

impl ResourceMeteringTag {
    /// Get data from [Context] and construct [ResourceMeteringTag].
    ///
    /// [Context]: kvproto::kvrpcpb::Context
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        Arc::new(TagInfos::from_rpc_context(context)).into()
    }

    /// This method is used to provide [ResourceMeteringTag] with the ability
    /// to attach to the thread local context.
    ///
    /// When you call this method, the `ResourceMeteringTag` itself will be
    /// attached to [STORAGE], and a [Guard] used to control the life of the
    /// tag is returned. When the `Guard` is discarded, the tag (and other
    /// fields if necessary) in `STORAGE` will be cleaned up.
    ///
    /// [STORAGE]: crate::localstorage::STORAGE
    pub fn attach(&self) -> Guard {
        STORAGE.with(|s| {
            if s.is_set.load(SeqCst) {
                panic!("nested attachment is not allowed")
            }
            let prev = s.shared_ptr.swap(self.clone());
            assert!(prev.is_none());
            s.is_set.store(true, SeqCst);
            s.summary_cur_record.reset();
            Guard { tag: self.clone() }
        })
    }
}

/// An RAII implementation of a [ResourceMeteringTag]. When this structure is
/// dropped (falls out of scope), the tag will be removed. You can also clean
/// up other data here if necessary.
///
/// See [ResourceMeteringTag::attach] for more information.
///
/// [ResourceMeteringTag]: crate::ResourceMeteringTag
/// [ResourceMeteringTag::attach]: crate::ResourceMeteringTag::attach
#[derive(Default)]
pub struct Guard {
    tag: ResourceMeteringTag,
}

// Unlike shared_ptr in STORAGE, summary_records will continue to grow as the
// query arrives. If the recorder thread is not working properly, these maps
// will never be cleaned up, so here we need to make some restrictions.
const MAX_SUMMARY_RECORDS_LEN: usize = 1000;

impl Drop for Guard {
    fn drop(&mut self) {
        STORAGE.with(|s| {
            while s.shared_ptr.take().is_none() {}
            s.is_set.store(false, SeqCst);
            // Judge GLOBAL_ENABLE to avoid unnecessary data accumulation when the switch is closed.
            if GLOBAL_ENABLE.load(Relaxed) {
                let mut records = s.summary_records.lock().unwrap();
                let k = &self.tag.infos.extra_attachment;
                match records.get(k) {
                    Some(record) => {
                        record.merge(s.summary_cur_record.as_ref());
                    }
                    None => {
                        // See MAX_SUMMARY_RECORDS_LEN.
                        if records.len() < MAX_SUMMARY_RECORDS_LEN {
                            records.insert(k.clone(), s.summary_cur_record.as_ref().clone());
                        }
                    }
                }
            }
        })
    }
}

/// This trait extends the standard [Future].
///
/// When the user imports [FutureExt], all futures in its module (such as async block)
/// will additionally support the [FutureExt::in_resource_metering_tag] method. This method
/// can bind a [ResourceMeteringTag] to the scope of this future (actually, it is stored in
/// the local storage of the thread where `Future` is located). During the polling period of
/// the future, we can continue to observe the system resources used by the thread in which
/// it is located, which is associated with `ResourceMeteringTag` and is also stored in thread
/// local storage. There is a background thread that continuously summarizes the storage of
/// each thread and reports it regularly.
///
/// [Future]: futures::Future
pub trait FutureExt: Sized {
    /// Call this method on the async block where you want to observe metrics to
    /// bind the [ResourceMeteringTag] extracted from the request context.
    #[inline]
    fn in_resource_metering_tag(self, tag: ResourceMeteringTag) -> InTags<Self> {
        InTags { inner: self, tag }
    }
}

impl<T: std::future::Future> FutureExt for T {}

/// This structure is the return value of the [FutureExt::in_resource_metering_tag] method,
/// which wraps the original [Future] with a [ResourceMeteringTag].
///
/// see [FutureExt] for more information.
///
/// [Future]: futures::Future
#[pin_project::pin_project]
pub struct InTags<T> {
    #[pin]
    inner: T,
    tag: ResourceMeteringTag,
}

impl<T: std::future::Future> std::future::Future for InTags<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = this.tag.attach();
        this.inner.poll(cx)
    }
}

/// This structure is the actual internal data of [ResourceMeteringTag], and all
/// internal data comes from the requested [Context].
///
/// [Context]: kvproto::kvrpcpb::Context
#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct TagInfos {
    pub store_id: u64,
    pub region_id: u64,
    pub peer_id: u64,
    pub extra_attachment: Vec<u8>,
}

impl TagInfos {
    fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        let peer = context.get_peer();
        Self {
            store_id: peer.get_store_id(),
            peer_id: peer.get_id(),
            region_id: context.get_region_id(),
            extra_attachment: Vec::from(context.get_resource_group_tag()),
        }
    }
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
    /// Gets the tag under the pointer and replace the original value with null.
    pub fn take(&self) -> Option<ResourceMeteringTag> {
        let prev = self.ptr.swap(std::ptr::null_mut(), SeqCst);
        (!prev.is_null()).then(|| unsafe { ResourceMeteringTag::from(Arc::from_raw(prev as _)) })
    }

    /// Gets the tag under the pointer and replace the original value with parameter v.
    pub fn swap(&self, v: ResourceMeteringTag) -> Option<ResourceMeteringTag> {
        let ptr = Arc::into_raw(v.infos);
        let prev = self.ptr.swap(ptr as _, SeqCst);
        (!prev.is_null()).then(|| unsafe { ResourceMeteringTag::from(Arc::from_raw(prev as _)) })
    }

    /// Gets a clone of the tag under the pointer and put it back.
    pub fn take_clone(&self) -> Option<ResourceMeteringTag> {
        self.take().map(|req_tag| {
            let tag = req_tag.clone();
            // Put it back as quickly as possible.
            assert!(self.swap(req_tag).is_none());
            tag
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attach() {
        let tag = ResourceMeteringTag {
            infos: Arc::new(TagInfos {
                store_id: 1,
                region_id: 2,
                peer_id: 3,
                extra_attachment: b"12345".to_vec(),
            }),
        };
        {
            let guard = tag.attach();
            assert_eq!(guard.tag.infos, tag.infos);
            STORAGE.with(|s| {
                let local_tag = s.shared_ptr.take();
                assert!(matches!(local_tag, Some(_)));
                let local_tag = local_tag.unwrap();
                assert_eq!(local_tag.infos, tag.infos);
                assert_eq!(local_tag.infos, guard.tag.infos);
                assert!(matches!(s.shared_ptr.swap(local_tag), None));
            });
            // drop here.
        }
        STORAGE.with(|s| {
            let local_tag = s.shared_ptr.take();
            assert!(matches!(local_tag, None));
        });
    }

    #[test]
    fn test_shared_tag_ptr_take_clone() {
        let info = TagInfos {
            store_id: 0,
            region_id: 0,
            peer_id: 0,
            extra_attachment: b"abc".to_vec(),
        };
        let ptr = unsafe {
            SharedTagPtr {
                ptr: Arc::new(AtomicPtr::new(std::mem::transmute(&info))),
            }
        };
        assert!(matches!(ptr.take_clone(), Some(_)));
        assert!(matches!(ptr.take_clone(), Some(_)));
        assert!(matches!(ptr.take_clone(), Some(_)));
    }
}
