// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::cpu::thread::ThreadRegister;
use crate::liveness_probe::{self, Alive, Probe};

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct ResourceMeteringTag {
    pub infos: Arc<TagInfos>,
}

impl ResourceMeteringTag {
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        Arc::new(TagInfos::from_rpc_context(context)).into()
    }

    pub fn attach(&self) -> Guard {
        ATTACHED_TAG.with(|s| {
            if s.is_set.get() {
                panic!("Nested attachment is not allowed.")
            }

            let prev = s.shared_ptr.swap(self.clone());
            assert!(prev.is_none());
            s.is_set.set(true);
        });

        Guard::default()
    }
}

#[derive(Default)]
pub struct Guard {
    // A trick to impl !Send, !Sync
    _p: PhantomData<*const ()>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        ATTACHED_TAG.with(|s| {
            while s.shared_ptr.take().is_none() {}
            s.is_set.set(false);
        });
    }
}

impl From<Arc<TagInfos>> for ResourceMeteringTag {
    fn from(infos: Arc<TagInfos>) -> Self {
        Self { infos }
    }
}

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
        TagInfos {
            store_id: peer.get_store_id(),
            peer_id: peer.get_id(),
            region_id: context.get_region_id(),
            extra_attachment: Vec::from(context.get_resource_group_tag()),
        }
    }
}

#[derive(Clone)]
pub struct TagCell {
    tag: Arc<AtomicPtr<TagInfos>>,
    probe: Probe,
}

impl TagCell {
    pub fn new(probe: Probe) -> Self {
        Self {
            tag: Default::default(),
            probe,
        }
    }

    pub fn take(&self) -> Option<ResourceMeteringTag> {
        let prev_ptr = self.tag.swap(std::ptr::null_mut(), SeqCst);
        (!prev_ptr.is_null())
            .then(|| unsafe { ResourceMeteringTag::from(Arc::from_raw(prev_ptr as _)) })
    }

    pub fn swap(&self, value: ResourceMeteringTag) -> Option<ResourceMeteringTag> {
        let tag_arc_ptr = Arc::into_raw(value.infos);
        let prev_ptr = self.tag.swap(tag_arc_ptr as _, SeqCst);
        (!prev_ptr.is_null())
            .then(|| unsafe { ResourceMeteringTag::from(Arc::from_raw(prev_ptr as _)) })
    }

    pub fn is_owner_alive(&self) -> bool {
        self.probe.is_alive()
    }
}

struct LocalTag {
    is_set: Cell<bool>,
    shared_ptr: TagCell,
    _alive: Alive,
}

thread_local! {
    static ATTACHED_TAG: LocalTag = {
        let (alive, probe) = liveness_probe::make_pair();
        let shared_ptr = TagCell::new(probe);
        ThreadRegister::register(shared_ptr.clone());

        LocalTag {
            is_set: Cell::new(false),
            shared_ptr,
            _alive: alive,
        }
    };
}
