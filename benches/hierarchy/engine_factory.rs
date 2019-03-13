// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use tikv::storage::{
    engine::{BTreeEngine, RocksEngine},
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
