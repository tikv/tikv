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

use std::collections::BTreeMap;
use std::collections::Bound::{self, Excluded, Included, Unbounded};
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, RwLock};

use kvproto::kvrpcpb::Context;

use raftstore::store::engine::IterOption;
use storage::engine::{
    Callback as EngineCallback, CbContext, Cursor, Engine, Error as EngineError, Iterator, Modify,
    Result as EngineResult, ScanMode, Snapshot,
};
use storage::{CfName, Key, Value, CF_DEFAULT, CF_LOCK, CF_WRITE};

#[derive(Clone)]
pub struct BTreeEngine {
    cf_names: Vec<CfName>,
    cf_contents: Vec<Arc<RwLock<BTreeMap<Key, Value>>>>,
}

impl BTreeEngine {
    fn new(cfs: &[CfName]) -> Self {
        let mut cf_names = vec![];
        let mut cf_contents = vec![];

        // create default cf if missing
        if cfs.iter().find(|&&c| c == CF_DEFAULT).is_none() {
            cf_names.push(CF_DEFAULT);
            cf_contents.push(Arc::new(RwLock::new(BTreeMap::new())))
        }

        for cf in cfs.iter() {
            cf_names.push(*cf);
            cf_contents.push(Arc::new(RwLock::new(BTreeMap::new())))
        }

        Self {
            cf_names,
            cf_contents,
        }
    }

    pub fn default() -> Self {
        let cfs = &[CF_WRITE, CF_DEFAULT, CF_LOCK];
        Self::new(cfs)
    }

    pub fn get_cf(&self, cf: CfName) -> Arc<RwLock<BTreeMap<Key, Value>>> {
        let index = self.cf_names.iter().position(|&c| c == cf);
        match index {
            None => unreachable!(
                "Not exist CF:[{}]! Please create it by BTreeEngine::new(&[CfName])!",
                cf
            ),
            Some(i) => self.cf_contents[i].clone(),
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
            cur_key: None,
            cur_value: None,
            valid: false,
            lower_bound,
            upper_bound,
        }
    }
}

impl Iterator for BTreeEngineIterator {
    fn next(&mut self) -> bool {
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
        assert!(self.valid());
        debug!("Iterator.key() -> {:?}", unsafe {
            String::from_utf8_unchecked(self.cur_key.clone().unwrap().to_raw().unwrap())
        });
        self.cur_key.as_ref().unwrap().as_encoded()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        self.cur_value.as_ref().unwrap().as_slice()
    }
}

impl Snapshot for BTreeEngineSnapshot {
    type Iter = BTreeEngineIterator;

    fn get(&self, key: &Key) -> EngineResult<Option<Value>> {
        self.get_cf(CF_DEFAULT, key)
    }
    fn get_cf(&self, cf: CfName, key: &Key) -> EngineResult<Option<Value>> {
        let tree_cf = self.inner_engine.get_cf(cf);
        let tree = tree_cf.read().unwrap();
        let v = tree.get(key);
        match v {
            None => Ok(None),
            Some(v) => Ok(Some(v.clone())),
        }
    }
    fn iter(&self, iter_opt: IterOption, mode: ScanMode) -> EngineResult<Cursor<Self::Iter>> {
        self.iter_cf(CF_DEFAULT, iter_opt, mode)
    }
    #[inline]
    fn iter_cf(
        &self,
        cf: CfName,
        iter_opt: IterOption,
        mode: ScanMode,
    ) -> EngineResult<Cursor<Self::Iter>> {
        let tree = self.inner_engine.get_cf(cf);

        Ok(Cursor::new(
            BTreeEngineIterator::new(tree.clone(), iter_opt),
            mode,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct BTreeEngineSnapshot {
    inner_engine: BTreeEngine,
}

impl BTreeEngineSnapshot {
    pub fn new(engine: &BTreeEngine) -> Self {
        Self {
            inner_engine: engine.clone(),
        }
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
        let engine = BTreeEngine::new(TEST_ENGINE_CFS);
        test_base_curd_options(&engine)
    }

    #[test]
    fn test_linear_of_btree_engine() {
        let engine = BTreeEngine::default();
        test_linear(&engine);
    }

    #[test]
    fn test_statistic_of_btree_engine() {
        let engine = BTreeEngine::default();
        test_cfs_statistics(&engine);
    }

    #[test]
    fn test_bound_of_btree_engine() {
        let engine = BTreeEngine::default();
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
        let mut cursor = snap.iter(iter_op, ScanMode::Forward).unwrap();
        let mut statistics = CFStatistics::default();
        assert!(cursor.seek_to_last(&mut statistics));

        let mut ret = vec![];

        loop {
            ret.push((
                Key::from_encoded(cursor.key(&mut statistics).to_vec())
                    .to_raw()
                    .unwrap(),
                cursor.value(&mut statistics).to_vec(),
            ));

            if !cursor.prev(&mut statistics) {
                break;
            }
        }

        ret.sort();
        assert_eq!(ret, test_data[1..3].to_vec());
    }

    #[test]
    fn test_iterator() {
        let engine = BTreeEngine::default();
        let test_data = vec![
            (b"a1".to_vec(), b"v1".to_vec()),
            (b"a3".to_vec(), b"v3".to_vec()),
            (b"a5".to_vec(), b"v5".to_vec()),
            (b"a7".to_vec(), b"v7".to_vec()),
        ];
        for (k, v) in &test_data {
            must_put(&engine, k.as_slice(), v.as_slice());
        }

        let iter_op = IterOption::default();
        let tree = engine.get_cf(CF_DEFAULT).clone();
        let mut iter = BTreeEngineIterator::new(tree, iter_op);
        assert!(!iter.valid());

        assert!(iter.seek_to_first());
        assert!(iter.valid());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());
        assert!(!iter.prev());
        assert!(!iter.valid());
        assert!(iter.next());
        assert!(iter.valid());
        assert_eq!(iter.key(), Key::from_raw(b"a3").as_encoded().as_slice());

        assert!(iter.seek(&Key::from_raw(b"a1")).unwrap());
        assert!(iter.valid());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());

        assert!(iter.seek_to_last());
        assert!(iter.valid());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());
        assert!(!iter.next());
        assert!(!iter.valid());
        assert!(iter.prev());
        assert!(iter.valid());
        assert_eq!(iter.key(), Key::from_raw(b"a5").as_encoded().as_slice());

        assert!(iter.seek(&Key::from_raw(b"a7")).unwrap());
        assert!(iter.valid());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());

        assert!(!iter.seek_for_prev(&Key::from_raw(b"a0")).unwrap());

        assert!(iter.seek_for_prev(&Key::from_raw(b"a1")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());

        assert!(iter.seek_for_prev(&Key::from_raw(b"a8")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());
    }

}
