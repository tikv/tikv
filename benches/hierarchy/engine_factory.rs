// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;

use tikv::storage::{
    kv::{BTreeEngine, RocksEngine},
    Engine, TestEngineBuilder,
};

pub trait EngineFactory<E: Engine>: Clone + Copy + fmt::Debug + 'static {
    fn build(&self) -> E;
}

#[derive(Clone, Copy)]
pub struct BTreeEngineFactory {}

impl EngineFactory<BTreeEngine> for BTreeEngineFactory {
    fn build(&self) -> BTreeEngine {
        BTreeEngine::default()
    }
}

impl fmt::Debug for BTreeEngineFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BTree")
    }
}

#[derive(Clone, Copy)]
pub struct RocksEngineFactory {}

impl EngineFactory<RocksEngine> for RocksEngineFactory {
    fn build(&self) -> RocksEngine {
        TestEngineBuilder::new().build().unwrap()
    }
}

impl fmt::Debug for RocksEngineFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Rocks")
    }
}
