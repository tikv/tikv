// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// TODO(mornyx): crate doc.

#![feature(shrink_to)]
#![feature(hash_drain_filter)]
#![feature(core_intrinsics)]

mod client;
mod collector;
mod config;
mod localstorage;
mod model;
mod recorder;
mod reporter;
pub mod utils;

pub(crate) mod metrics;

pub use client::{Client, GrpcClient};
pub use collector::{Collector, CollectorHandle, CollectorId, CollectorRegHandle};
pub use config::{Config, ConfigManager};
pub use model::*;
pub use recorder::{init_recorder, CpuRecorder, Recorder, RecorderBuilder, RecorderHandle};
pub use reporter::{Reporter, Task};

pub const MAX_THREAD_REGISTER_RETRY: u32 = 10;
pub const TEST_TAG_PREFIX: &[u8] = b"__resource_metering::tests::";

use crate::localstorage::{LocalStorage, LocalStorageRef, STORAGE};

use std::intrinsics::unlikely;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crossbeam::channel::Sender;
use tikv_util::warn;

/// This structure is used as a label to distinguish different request contexts.
///
/// In order to associate `ResourceMeteringTag` with a certain piece of code logic,
/// we added a function to [Future] to bind `ResourceMeteringTag` to the specified
/// future context. It is used in the main business logic of TiKV.
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

            assert!(!ls.is_set, "nested attachment is not allowed");
            ls.attached_tag.store(Some(self.infos.clone()));
            ls.is_set = true;

            Guard {
                _tag: self.infos.clone(),
            }
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
pub struct Guard {
    _tag: Arc<TagInfos>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        STORAGE.with(|s| {
            let mut ls = s.borrow_mut();
            ls.attached_tag.store(None);
            ls.is_set = false;
        })
    }
}

#[derive(Clone)]
pub struct ResourceTagFactory {
    tx: Sender<LocalStorageRef>,
}

impl ResourceTagFactory {
    fn new(tx: Sender<LocalStorageRef>) -> Self {
        Self { tx }
    }

    pub fn new_for_test() -> Self {
        let (tx, _) = crossbeam::channel::unbounded();
        Self { tx }
    }

    pub fn new_tag(&self, context: &kvproto::kvrpcpb::Context) -> ResourceMeteringTag {
        let tag_infos = TagInfos::from_rpc_context(context);
        ResourceMeteringTag {
            infos: Arc::new(tag_infos),
            resource_tag_factory: self.clone(),
        }
    }

    fn register_local_storage(&self, storage: &LocalStorage) -> bool {
        let lsr = LocalStorageRef {
            id: utils::thread_id(),
            storage: storage.clone(),
        };
        match self.tx.send(lsr) {
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

/// See [FutureExt].
pub trait StreamExt: Sized {
    #[inline]
    fn in_resource_metering_tag(self, tag: ResourceMeteringTag) -> InTags<Self> {
        InTags { inner: self, tag }
    }
}

impl<T: futures::Stream> StreamExt for T {}

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

impl<T: futures::Stream> futures::Stream for InTags<T> {
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let _guard = this.tag.attach();
        this.inner.poll_next(cx)
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
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        let peer = context.get_peer();
        Self {
            store_id: peer.get_store_id(),
            peer_id: peer.get_id(),
            region_id: context.get_region_id(),
            extra_attachment: Vec::from(context.get_resource_group_tag()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam::channel::unbounded;

    #[test]
    fn test_attach() {
        // Use a thread created by ourself. If we use unit test thread directly,
        // the test results may be affected by parallel testing.
        std::thread::spawn(|| {
            let (tx, _rx) = unbounded();
            let resource_tag_factory = ResourceTagFactory::new(tx);
            let tag = ResourceMeteringTag {
                infos: Arc::new(TagInfos {
                    store_id: 1,
                    region_id: 2,
                    peer_id: 3,
                    extra_attachment: b"12345".to_vec(),
                }),
                resource_tag_factory,
            };
            {
                let guard = tag.attach();
                assert_eq!(guard._tag, tag.infos);
                STORAGE.with(|s| {
                    let ls = s.borrow_mut();
                    let local_tag = ls.attached_tag.swap(None);
                    assert!(local_tag.is_some());
                    let tag_infos = local_tag.unwrap();
                    assert_eq!(tag_infos, tag.infos);
                    assert_eq!(tag_infos, guard._tag);
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
