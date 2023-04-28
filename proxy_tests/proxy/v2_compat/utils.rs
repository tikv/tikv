// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::raft_serverpb::RaftMessage;
use mock_engine_store::mock_cluster::v1::transport_simulate::{Filter, FilterFactory};
use raftstore::errors::Result;

pub struct ForwardFactory {
    pub node_id: u64,
    pub chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
}

impl FilterFactory for ForwardFactory {
    fn generate(&self, _: u64) -> Vec<Box<dyn Filter>> {
        vec![Box::new(ForwardFilter {
            node_id: self.node_id,
            chain_send: self.chain_send.clone(),
        })]
    }
}

pub struct ForwardFilter {
    node_id: u64,
    chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
}

impl Filter for ForwardFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        for m in msgs.drain(..) {
            if self.node_id == m.get_to_peer().get_store_id() {
                (self.chain_send)(m);
            }
        }
        Ok(())
    }
}
