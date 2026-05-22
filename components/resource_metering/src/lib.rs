// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// TODO(mornyx): crate doc.

#![allow(internal_features)]
#![feature(core_intrinsics)]

use std::{
    intrinsics::unlikely,
    pin::Pin,
    sync::{
        Arc,
        atomic::Ordering::{Relaxed, SeqCst},
    },
    task::{Context, Poll},
};

pub use collector::Collector;
pub use config::{Config, ConfigManager, ENABLE_NETWORK_IO_COLLECTION};
pub use model::*;
pub use recorder::{
    CollectorGuard, CollectorId, CollectorRegHandle,
    ConfigChangeNotifier as RecorderConfigChangeNotifier, CpuRecorder, Recorder, RecorderBuilder,
    SummaryRecorder, init_recorder, record_logical_read_bytes, record_logical_write_bytes,
    record_network_in_bytes, record_network_out_bytes, record_read_keys, record_write_keys,
};
use recorder::{LocalStorage, LocalStorageRef, STORAGE};
pub use reporter::{
    ConfigChangeNotifier as ReporterConfigChangeNotifier, Reporter, Task,
    data_sink::DataSink,
    data_sink_reg::DataSinkRegHandle,
    init_reporter,
    pubsub::PubSubService,
    single_target::{AddressChangeNotifier, SingleTargetDataSink, init_single_target},
};
use tikv_util::{
    memory::HeapSize,
    sys::thread,
    warn,
    worker::{Scheduler, Worker},
};

mod collector;
mod config;
pub mod error;
mod model;
mod recorder;
mod reporter;

pub(crate) mod metrics;

pub const MAX_THREAD_REGISTER_RETRY: u32 = 10;

/// This structure is used as a label to distinguish different request contexts.
///
/// In order to associate `ResourceMeteringTag` with a certain piece of code
/// logic, we added a function to [Future] to bind `ResourceMeteringTag` to the
/// specified future context. It is used in the main business logic of TiKV.
///
/// [Future]: futures::Future
pub struct ResourceMeteringTag {
    infos: Arc<TagInfos>,
    resource_tag_factory: ResourceTagFactory,
}

impl ResourceMeteringTag {
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
        STORAGE.with(move |s| {
            let mut ls = s.borrow_mut();

            if unlikely(!ls.registered && ls.register_failed_times < MAX_THREAD_REGISTER_RETRY) {
                ls.registered = self.resource_tag_factory.register_local_storage(&ls);
                if !ls.registered {
                    ls.register_failed_times += 1;
                }
            }

            // unexpected nested attachment
            if ls.is_set {
                debug_assert!(false, "nested attachment is not allowed");
                return Guard;
            }

            let prev_tag = ls.attached_tag.swap(Some(self.infos.clone()));
            debug_assert!(prev_tag.is_none());
            ls.is_set = true;
            ls.summary_cur_record.reset();

            Guard
        })
    }
}

impl HeapSize for ResourceMeteringTag {
    fn approximate_heap_size(&self) -> usize {
        self.infos.approximate_mem_size()
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
pub struct Guard;

// Unlike attached_tag in STORAGE, summary_records will continue to grow as the
// request arrives. If the recorder thread is not working properly, these maps
// will never be cleaned up, so here we need to make some restrictions.
const MAX_SUMMARY_RECORDS_LEN: usize = 1000;

impl Drop for Guard {
    fn drop(&mut self) {
        STORAGE.with(|s| {
            let mut ls = s.borrow_mut();

            if !ls.is_set {
                return;
            }
            ls.is_set = false;

            // If the shared tag is occupied by the recorder thread
            // with `SharedTagInfos::load_full`, spin wait for releasing.
            let tag = loop {
                let tag = ls.attached_tag.swap(None);
                if let Some(t) = tag {
                    break t;
                }
            };

            if !ls.summary_enable.load(SeqCst) {
                return;
            }
            if tag.extra_attachment.is_empty() {
                return;
            }
            let cur_record = ls.summary_cur_record.take_and_reset();
            if cur_record.read_keys.load(Relaxed) == 0
                && cur_record.logical_read_bytes.load(Relaxed) == 0
                && cur_record.logical_write_bytes.load(Relaxed) == 0
            {
                return;
            }
            let mut records = ls.summary_records.lock().unwrap();
            if let Some(record) = records.get(&tag) {
                record.merge(&cur_record);
            } else {
                // See MAX_SUMMARY_RECORDS_LEN.
                if records.len() < MAX_SUMMARY_RECORDS_LEN {
                    records.insert(tag, cur_record);
                }
            }
        })
    }
}

#[derive(Clone)]
pub struct ResourceTagFactory {
    scheduler: Scheduler<recorder::Task>,
}

impl ResourceTagFactory {
    fn new(scheduler: Scheduler<recorder::Task>) -> Self {
        Self { scheduler }
    }

    pub fn new_for_test() -> Self {
        Self {
            scheduler: Worker::new("mock-resource-tag-factory")
                .lazy_build("mock-resource-tag-factory")
                .scheduler(),
        }
    }

    pub fn new_tag(&self, context: &kvproto::kvrpcpb::Context) -> ResourceMeteringTag {
        let tag_infos = TagInfos::from_rpc_context(context);
        ResourceMeteringTag {
            infos: Arc::new(tag_infos),
            resource_tag_factory: self.clone(),
        }
    }

    // create a new tag with key ranges for a read request.
    pub fn new_tag_with_key_ranges(
        &self,
        context: &kvproto::kvrpcpb::Context,
        key_ranges: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> ResourceMeteringTag {
        let tag_infos = TagInfos::from_rpc_context_with_key_ranges(context, key_ranges);
        ResourceMeteringTag {
            infos: Arc::new(tag_infos),
            resource_tag_factory: self.clone(),
        }
    }

    fn register_local_storage(&self, storage: &LocalStorage) -> bool {
        let lsr = LocalStorageRef {
            id: thread::thread_id(),
            storage: storage.clone(),
        };
        match self.scheduler.schedule(recorder::Task::ThreadReg(lsr)) {
            Ok(_) => true,
            Err(err) => {
                warn!("failed to register thread"; "err" => ?err);
                false
            }
        }
    }
}

/// This trait extends the standard [Future].
///
/// When the user imports [FutureExt], all futures in its module (such as async
/// block) will additionally support the [FutureExt::in_resource_metering_tag]
/// method. This method can bind a [ResourceMeteringTag] to the scope of this
/// future (actually, it is stored in the local storage of the thread where
/// `Future` is located). During the polling period of the future, we can
/// continue to observe the system resources used by the thread in which it is
/// located, which is associated with `ResourceMeteringTag` and is also stored
/// in thread local storage. There is a background thread that continuously
/// summarizes the storage of each thread and reports it regularly.
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

/// See [FutureExt].
pub trait StreamExt: Sized {
    #[inline]
    fn in_resource_metering_tag(self, tag: ResourceMeteringTag) -> InTags<Self> {
        InTags { inner: self, tag }
    }
}

impl<T: futures::Stream> StreamExt for T {}

/// This structure is the return value of the
/// [FutureExt::in_resource_metering_tag] method, which wraps the original
/// [Future] with a [ResourceMeteringTag].
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

impl<T: futures::Stream> futures::Stream for InTags<T> {
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let _guard = this.tag.attach();
        this.inner.poll_next(cx)
    }
}

/// This structure is the actual internal data of [ResourceMeteringTag], and all
/// internal data comes from the request's [Context] and the request itself.
///
/// [Context]: kvproto::kvrpcpb::Context
#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct TagInfos {
    pub store_id: u64,
    pub region_id: u64,
    pub peer_id: u64,
    // Only a read request contains the key ranges.
    pub key_ranges: Vec<(Vec<u8>, Vec<u8>)>,
    pub extra_attachment: Arc<Vec<u8>>,
}

impl TagInfos {
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        let peer = context.get_peer();
        Self {
            store_id: peer.get_store_id(),
            peer_id: peer.get_id(),
            region_id: context.get_region_id(),
            key_ranges: vec![],
            extra_attachment: Arc::new(Vec::from(context.get_resource_group_tag())),
        }
    }

    // create a TagInfos with start and end keys for a read request.
    pub fn from_rpc_context_with_key_ranges(
        context: &kvproto::kvrpcpb::Context,
        key_ranges: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Self {
        let peer = context.get_peer();
        Self {
            store_id: peer.get_store_id(),
            peer_id: peer.get_id(),
            region_id: context.get_region_id(),
            key_ranges,
            extra_attachment: Arc::new(Vec::from(context.get_resource_group_tag())),
        }
    }
}

impl HeapSize for TagInfos {
    fn approximate_heap_size(&self) -> usize {
        self.key_ranges.approximate_heap_size() + self.extra_attachment.approximate_heap_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attach() {
        // Use a thread created by ourself. If we use unit test thread directly,
        // the test results may be affected by parallel testing.
        std::thread::spawn(|| {
            let resource_tag_factory = ResourceTagFactory::new_for_test();
            let tag = ResourceMeteringTag {
                infos: Arc::new(TagInfos {
                    store_id: 1,
                    region_id: 2,
                    peer_id: 3,
                    key_ranges: vec![],
                    extra_attachment: Arc::new(b"12345".to_vec()),
                }),
                resource_tag_factory,
            };
            {
                let _guard = tag.attach();
                STORAGE.with(|s| {
                    let ls = s.borrow_mut();
                    let local_tag = ls.attached_tag.swap(None);
                    assert!(local_tag.is_some());
                    let tag_infos = local_tag.unwrap();
                    assert_eq!(tag_infos, tag.infos);
                    assert!(ls.attached_tag.swap(Some(tag_infos)).is_none());
                });
                // drop here.
            }
            STORAGE.with(|s| {
                let ls = s.borrow_mut();
                let local_tag = ls.attached_tag.swap(None);
                assert!(local_tag.is_none());
            });
        })
        .join()
        .unwrap();
    }
}
