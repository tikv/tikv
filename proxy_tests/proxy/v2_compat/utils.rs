// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::raft_serverpb::RaftMessage;
use raftstore::errors::Result;

pub struct ForwardFactoryV2 {
    pub node_id: u64,
    pub chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
    pub keep_msg: bool,
}

impl test_raftstore::FilterFactory for ForwardFactoryV2 {
    fn generate(&self, _: u64) -> Vec<Box<dyn test_raftstore::Filter>> {
        vec![Box::new(ForwardFilterV2 {
            node_id: self.node_id,
            chain_send: self.chain_send.clone(),
            keep_msg: self.keep_msg,
        })]
    }
}

pub struct ForwardFilterV2 {
    pub node_id: u64,
    pub chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
    pub keep_msg: bool,
}

impl test_raftstore::Filter for ForwardFilterV2 {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        if self.keep_msg {
            for m in msgs {
                if self.node_id == m.get_to_peer().get_store_id() {
                    (self.chain_send)(m.clone());
                }
            }
        } else {
            for m in msgs.drain(..) {
                if self.node_id == m.get_to_peer().get_store_id() {
                    (self.chain_send)(m);
                }
            }
        }
        Ok(())
    }
}

pub struct ForwardFactoryV1 {
    pub node_id: u64,
    pub chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
    pub keep_msg: bool,
}

impl mock_engine_store::mock_cluster::v1::transport_simulate::FilterFactory for ForwardFactoryV1 {
    fn generate(
        &self,
        _: u64,
    ) -> Vec<Box<dyn mock_engine_store::mock_cluster::v1::transport_simulate::Filter>> {
        vec![Box::new(ForwardFilterV1 {
            node_id: self.node_id,
            chain_send: self.chain_send.clone(),
            keep_msg: self.keep_msg,
        })]
    }
}

pub struct ForwardFilterV1 {
    pub node_id: u64,
    pub chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
    pub keep_msg: bool,
}

impl mock_engine_store::mock_cluster::v1::transport_simulate::Filter for ForwardFilterV1 {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        if self.keep_msg {
            for m in msgs {
                if self.node_id == m.get_to_peer().get_store_id() {
                    (self.chain_send)(m.clone());
                }
            }
        } else {
            for m in msgs.drain(..) {
                if self.node_id == m.get_to_peer().get_store_id() {
                    (self.chain_send)(m);
                }
            }
        }
        Ok(())
    }
}
