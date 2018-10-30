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

use raftstore::store::engine::IterOption;
use storage::engine::{
    Callback as EngineCallback, CbContext, Cursor, Engine, Error as EngineError, Iterator, Modify,
    Result as EngineResult, ScanMode, Snapshot,
};
use storage::{CfName, Key, Value, CF_DEFAULT, CF_LOCK, CF_WRITE};

use kvproto::kvrpcpb::Context;
use std::collections::BTreeMap;
use std::collections::Bound::{self, Excluded, Included, Unbounded};
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, RwLock};

pub struct BTreeEngineIterator {
    tree: Arc<RwLock<BTreeMap<Key, Value>>>,
    cur_key: Option<Key>,
    cur_value: Option<Value>,
    valid: bool,
    lower_bound: Bound<Key>,
    upper_bound: Bound<Key>,
}

impl BTreeEngineIterator {
    pub fn new(
        tree: Arc<RwLock<BTreeMap<Key, Value>>>,
        iter_opt: IterOption,
    ) -> BTreeEngineIterator {
        let cur_key: Option<Key> = None;
        let cur_value: Option<Value> = None;

        let lower_bound = match iter_opt.lower_bound() {
            None => Unbounded,
            Some(key) => Included(Key::from_raw(key)),
        };

        let upper_bound = match iter_opt.upper_bound() {
            None => Unbounded,
            Some(key) => Excluded(Key::from_raw(key)),
        };

        Self {
            tree: tree.clone(),
            cur_key,
            cur_value,
            valid: false,
            lower_bound,
            upper_bound,
        }
    }
}

impl Iterator for BTreeEngineIterator {
    fn next(&mut self) -> bool {
        assert!(self.valid());
        let tree = self.tree.read().unwrap();
        let mut range = tree.range((
            Excluded(self.cur_key.clone().unwrap()),
            self.upper_bound.clone(),
        ));
        match range.next() {
            None => {
                self.valid = false;
            }
            Some((k, v)) => {
                self.cur_key = Some(k.clone());
                self.cur_value = Some(v.clone());
                self.valid = true;
            }
        }
        self.valid()
    }

    fn prev(&mut self) -> bool {
        assert!(self.valid());
        let tree = self.tree.read().unwrap();
        let mut range = tree.range((
            self.lower_bound.clone(),
            Excluded(self.cur_key.clone().unwrap()),
        ));
        match range.next_back() {
            None => {
                self.valid = false;
            }
            Some((k, v)) => {
                self.cur_key = Some(k.clone());
                self.cur_value = Some(v.clone());
                self.valid = true;
            }
        }
        self.valid()
    }

    fn seek(&mut self, key: &Key) -> EngineResult<bool> {
        debug!("Iterator.seek({:?})", unsafe {
            String::from_utf8_unchecked(key.to_raw().unwrap())
        });
        match self
            .tree
            .read()
            .unwrap()
            .range((Included(key.clone()), self.upper_bound.clone()))
            .next()
        {
            None => {
                self.valid = false;
            }
            Some((k, v)) => {
                self.cur_key = Some(k.clone());
                self.cur_value = Some(v.clone());
                self.valid = true;
            }
        }

        Ok(self.valid())
    }

    fn seek_for_prev(&mut self, key: &Key) -> EngineResult<bool> {
        let tree = self.tree.read().unwrap();
        match tree
            .range((self.lower_bound.clone(), Included(key.clone())))
            .next_back()
        {
            None => {
                self.valid = false;
            }
            Some((k, v)) => {
                self.cur_key = Some(k.clone());
                self.cur_value = Some(v.clone());
                self.valid = true;
            }
        }
        Ok(self.valid())
    }

    fn seek_to_first(&mut self) -> bool {
        let tree = self.tree.read().unwrap();
        match tree
            .range((self.lower_bound.clone(), self.upper_bound.clone()))
            .next()
        {
            None => {
                self.valid = false;
            }
            Some((k, v)) => {
                self.cur_key = Some(k.clone());
                self.cur_value = Some(v.clone());
                self.valid = true;
            }
        }
        self.valid()
    }

    fn seek_to_last(&mut self) -> bool {
        let tree = self.tree.read().unwrap();
        match tree
            .range((self.lower_bound.clone(), self.upper_bound.clone()))
            .next_back()
        {
            None => {
                self.valid = false;
            }
            Some((k, v)) => {
                self.cur_key = Some(k.clone());
                self.cur_value = Some(v.clone());
                self.valid = true;
            }
        }
        self.valid()
    }

    #[inline]
    fn valid(&self) -> bool {
        debug!("Iterator.valid(): {:?}", self.valid);
        self.valid
    }

    fn key(&self) -> &[u8] {
        debug!("Iterator.key() -> {:?}", unsafe {
            String::from_utf8_unchecked(self.cur_key.clone().unwrap().to_raw().unwrap())
        });
        self.cur_key.as_ref().unwrap().as_encoded()
    }

    fn value(&self) -> &[u8] {
        self.cur_value.as_ref().unwrap().as_slice()
    }
}

impl Snapshot for BTreeEngineSnapshot {
    type Iter = BTreeEngineIterator;

    fn get(&self, key: &Key) -> EngineResult<Option<Value>> {
        let tree = self.default_cf.read().unwrap();
        let v = tree.get(key);

        match v {
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    }
    fn get_cf(&self, cf: CfName, key: &Key) -> EngineResult<Option<Value>> {
        let tree_cf = match cf {
            CF_DEFAULT => &self.default_cf,
            CF_WRITE => &self.write_cf,
            CF_LOCK => &self.lock_cf,
            _ => &self.default_cf,
        };
        let tree = tree_cf.read().unwrap();
        let v = tree.get(key);
        match v {
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    }
    fn iter(&self, iter_opt: IterOption, mode: ScanMode) -> EngineResult<Cursor<Self::Iter>> {
        let itr = BTreeEngineIterator::new(self.default_cf.clone(), iter_opt);
        Ok(Cursor::new(itr, mode))
    }
    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: IterOption,
        mode: ScanMode,
    ) -> EngineResult<Cursor<Self::Iter>> {
        let tree = match cf {
            CF_DEFAULT => &self.default_cf,
            CF_WRITE => &self.write_cf,
            CF_LOCK => &self.lock_cf,
            _ => unreachable!(),
        };

        Ok(Cursor::new(
            BTreeEngineIterator::new(tree.clone(), iter_opt),
            mode,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct BTreeEngineSnapshot {
    write_cf: Arc<RwLock<BTreeMap<Key, Value>>>,
    lock_cf: Arc<RwLock<BTreeMap<Key, Value>>>,
    default_cf: Arc<RwLock<BTreeMap<Key, Value>>>,
}

impl BTreeEngineSnapshot {
    pub fn new(engine: &BTreeEngine) -> Self {
        Self {
            write_cf: engine.write_cf.clone(),
            lock_cf: engine.lock_cf.clone(),
            default_cf: engine.default_cf.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BTreeEngine {
    pub write_cf: Arc<RwLock<BTreeMap<Key, Value>>>,
    pub lock_cf: Arc<RwLock<BTreeMap<Key, Value>>>,
    pub default_cf: Arc<RwLock<BTreeMap<Key, Value>>>,
}

impl BTreeEngine {
    #[allow(dead_code)]
    pub fn new() -> Self {
        let default_cf = Arc::new(RwLock::new(BTreeMap::new()));
        let lock_cf = Arc::new(RwLock::new(BTreeMap::new()));
        let write_cf = Arc::new(RwLock::new(BTreeMap::new()));

        Self {
            write_cf,
            lock_cf,
            default_cf,
        }
    }

    pub fn get_cf(&self, cf: CfName) -> Arc<RwLock<BTreeMap<Key, Value>>> {
        match cf {
            CF_LOCK => self.lock_cf.clone(),
            CF_WRITE => self.write_cf.clone(),
            CF_DEFAULT => self.default_cf.clone(),
            //            _ => unreachable!(),
            _ => self.default_cf.clone(),
        }
    }
}

impl Engine for BTreeEngine {
    type Iter = BTreeEngineIterator;
    type Snap = BTreeEngineSnapshot;

    fn async_write(
        &self,
        _ctx: &Context,
        modifies: Vec<Modify>,
        cb: EngineCallback<()>,
    ) -> EngineResult<()> {
        if modifies.is_empty() {
            return Err(EngineError::EmptyRequest);
        }
        cb((CbContext::new(), write_modifies(&self, modifies)));

        Ok(())
    }
    fn async_snapshot(&self, _ctx: &Context, cb: EngineCallback<Self::Snap>) -> EngineResult<()> {
        cb((CbContext::new(), Ok(BTreeEngineSnapshot::new(&self))));
        Ok(())
    }
}

impl Debug for BTreeEngine {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "BTreeEngine []",)
    }
}

fn write_modifies(engine: &BTreeEngine, modifies: Vec<Modify>) -> EngineResult<()> {
    for rev in modifies {
        match rev {
            Modify::Delete(cf, k) => {
                let cf_tree = engine.get_cf(cf);
                cf_tree.write().unwrap().remove(&k);
            }
            Modify::Put(cf, k, v) => {
                let cf_tree = engine.get_cf(cf);
                cf_tree.write().unwrap().insert(k, v);
            }

            Modify::DeleteRange(_cf, _start_key, _end_key) => unimplemented!(),
        };
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::super::tests::*;
    use super::super::CFStatistics;
    use super::*;

    #[test]
    fn test_btree_engine() {
        let engine = BTreeEngine::new();
        test_base_curd_options(&engine)
    }

    #[test]
    fn test_linear_of_btree_engine() {
        let engine = BTreeEngine::new();
        test_linear(&engine);
    }

    #[test]
    fn test_statistic_of_btree_engine() {
        let engine = BTreeEngine::new();
        test_cfs_statistics(&engine);
    }

    #[test]
    fn test_bound_of_btree_engine() {
        let engine = BTreeEngine::new();
        let test_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
        ];
        for (k, v) in &test_data {
            must_put(&engine, k.as_slice(), v.as_slice());
        }

        let snap = engine.snapshot(&Context::new()).unwrap();

        let mut iter_op = IterOption::default();
        iter_op.set_lower_bound(b"a3".to_vec());
        iter_op.set_upper_bound(b"a7".to_vec());
        let mut iter = snap.iter(iter_op, ScanMode::Forward).unwrap();
        let mut statistics = CFStatistics::default();
        assert!(iter.seek_to_last(&mut statistics));

        let mut ret = vec![];

        loop {
            ret.push((
                Key::from_encoded(iter.key(&mut statistics).to_vec())
                    .to_raw()
                    .unwrap(),
                iter.value(&mut statistics).to_vec(),
            ));

            if !iter.prev(&mut statistics) {
                break;
            }
        }

        ret.sort();
        assert_eq!(ret, test_data[1..3].to_vec());
    }
}
