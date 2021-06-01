// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(shrink_to)]

pub mod cpu;

use std::sync::Arc;

#[derive(Debug, Default, Eq, PartialEq, Clone, Hash)]
pub struct ResourceMeteringTag {
    pub infos: Arc<TagInfos>,
}

impl ResourceMeteringTag {
    pub fn from_rpc_context(context: &kvproto::kvrpcpb::Context) -> Self {
        Arc::new(TagInfos::from_rpc_context(context)).into()
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
