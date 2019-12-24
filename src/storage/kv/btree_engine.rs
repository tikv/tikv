// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::Bound::{self, Excluded, Included, Unbounded};
use std::default::Default;
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

use engine::IterOption;
use engine::{CfName, CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::Context;
use txn_types::{Key, Value};

use crate::storage::kv::{
    Callback as EngineCallback, CbContext, Cursor, Engine, Error as EngineError,
    ErrorInner as EngineErrorInner, Iterator, Modify, Result as EngineResult, ScanMode, Snapshot,
};

type RwLockTree = RwLock<BTreeMap<Key, Value>>;

/// The BTreeEngine(based on `BTreeMap`) is in memory and only used in tests and benchmarks.
/// Note: The `snapshot()` and `async_snapshot()` methods are fake, the returned snapshot is not isolated,
/// they will be affected by the later modifies.
#[derive(Clone)]
pub struct BTreeEngine {
    cf_names: Vec<CfName>,
    cf_contents: Vec<Arc<RwLockTree>>,
}

impl BTreeEngine {
    pub fn new(cfs: &[CfName]) -> Self {
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

    pub fn get_cf(&self, cf: CfName) -> Arc<RwLockTree> {
        let index = self
            .cf_names
            .iter()
            .position(|&c| c == cf)
            .expect("CF not exist!");
        self.cf_contents[index].clone()
    }
}

impl Default for BTreeEngine {
    fn default() -> Self {
        let cfs = &[CF_WRITE, CF_DEFAULT, CF_LOCK];
        Self::new(cfs)
    }
}

impl Engine for BTreeEngine {
    type Snap = BTreeEngineSnapshot;

    fn async_write(
        &self,
        _ctx: &Context,
        modifies: Vec<Modify>,
        cb: EngineCallback<()>,
    ) -> EngineResult<()> {
        if modifies.is_empty() {
            return Err(EngineError::from(EngineErrorInner::EmptyRequest));
        }
        cb((CbContext::new(), write_modifies(&self, modifies)));

        Ok(())
    }
    /// warning: It returns a fake snapshot whose content will be affected by the later modifies!
    fn async_snapshot(&self, _ctx: &Context, cb: EngineCallback<Self::Snap>) -> EngineResult<()> {
        cb((CbContext::new(), Ok(BTreeEngineSnapshot::new(&self))));
        Ok(())
    }
}

impl Display for BTreeEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BTreeEngine",)
    }
}

impl Debug for BTreeEngine {
    // TODO: Provide more debug info.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BTreeEngine",)
    }
}

pub struct BTreeEngineIterator {
    tree: Arc<RwLockTree>,
    cur_key: Option<Key>,
    cur_value: Option<Value>,
    valid: bool,
    bounds: (Bound<Key>, Bound<Key>),
}

impl BTreeEngineIterator {
    pub fn new(tree: Arc<RwLockTree>, iter_opt: IterOption) -> BTreeEngineIterator {
        let lower_bound = match iter_opt.lower_bound() {
            None => Unbounded,
            Some(key) => Included(Key::from_raw(key)),
        };

        let upper_bound = match iter_opt.upper_bound() {
            None => Unbounded,
            Some(key) => Excluded(Key::from_raw(key)),
        };
        let bounds = (lower_bound.clone(), upper_bound.clone());
        Self {
            tree: tree.clone(),
            cur_key: None,
            cur_value: None,
            valid: false,
            bounds,
        }
    }

    /// In general, there are 2 endpoints in a range, the left one and the right one.
    /// This method will seek to the left one if left is `true`, else seek to the right one.
    /// Returns true when the endpoint is valid, which means the endpoint exist and in `self.bounds`.
    fn seek_to_range_endpoint(&mut self, range: (Bound<Key>, Bound<Key>), left: bool) -> bool {
        let tree = self.tree.read().unwrap();
        let mut range = tree.range(range);
        let item = if left {
            range.next() // move to the left endpoint
        } else {
            range.next_back() // move to the right endpoint
        };
        match item {
            Some((k, v)) if self.bounds.contains(k) => {
                self.cur_key = Some(k.clone());
                self.cur_value = Some(v.clone());
                self.valid = true;
            }
            _ => {
                self.valid = false;
            }
        }
        self.valid().unwrap()
    }
}

impl Iterator for BTreeEngineIterator {
    fn next(&mut self) -> EngineResult<bool> {
        let range = (Excluded(self.cur_key.clone().unwrap()), Unbounded);
        Ok(self.seek_to_range_endpoint(range, true))
    }

    fn prev(&mut self) -> EngineResult<bool> {
        let range = (Unbounded, Excluded(self.cur_key.clone().unwrap()));
        Ok(self.seek_to_range_endpoint(range, false))
    }

    fn seek(&mut self, key: &Key) -> EngineResult<bool> {
        let range = (Included(key.clone()), Unbounded);
        Ok(self.seek_to_range_endpoint(range, true))
    }

    fn seek_for_prev(&mut self, key: &Key) -> EngineResult<bool> {
        let range = (Unbounded, Included(key.clone()));
        Ok(self.seek_to_range_endpoint(range, false))
    }

    fn seek_to_first(&mut self) -> EngineResult<bool> {
        let range = (self.bounds.0.clone(), self.bounds.1.clone());
        Ok(self.seek_to_range_endpoint(range, true))
    }

    fn seek_to_last(&mut self) -> EngineResult<bool> {
        let range = (self.bounds.0.clone(), self.bounds.1.clone());
        Ok(self.seek_to_range_endpoint(range, false))
    }

    #[inline]
    fn valid(&self) -> EngineResult<bool> {
        Ok(self.valid)
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid().unwrap());
        self.cur_key.as_ref().unwrap().as_encoded()
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid().unwrap());
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
    inner_engine: Arc<BTreeEngine>,
}

impl BTreeEngineSnapshot {
    pub fn new(engine: &BTreeEngine) -> Self {
        Self {
            inner_engine: Arc::new(engine.clone()),
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

            Modify::DeleteRange(_cf, _start_key, _end_key, _notify_only) => unimplemented!(),
        };
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::super::tests::*;
    use super::super::CfStatistics;
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
    fn test_bounds_of_btree_engine() {
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
        let snap = engine.snapshot(&Context::default()).unwrap();
        let mut statistics = CfStatistics::default();

        // lower bound > upper bound, seek() returns false.
        let mut iter_op = IterOption::default();
        iter_op.set_lower_bound(b"a7", 0);
        iter_op.set_upper_bound(b"a3", 0);
        let mut cursor = snap.iter(iter_op, ScanMode::Forward).unwrap();
        assert!(!cursor.seek(&Key::from_raw(b"a5"), &mut statistics).unwrap());

        let mut iter_op = IterOption::default();
        iter_op.set_lower_bound(b"a3", 0);
        iter_op.set_upper_bound(b"a7", 0);
        let mut cursor = snap.iter(iter_op, ScanMode::Forward).unwrap();

        assert!(cursor.seek(&Key::from_raw(b"a5"), &mut statistics).unwrap());
        assert!(!cursor.seek(&Key::from_raw(b"a8"), &mut statistics).unwrap());
        assert!(!cursor.seek(&Key::from_raw(b"a0"), &mut statistics).unwrap());

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
        assert!(!iter.valid().unwrap());

        assert!(iter.seek_to_first().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());
        assert!(!iter.prev().unwrap());
        assert!(iter.next().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a3").as_encoded().as_slice());

        assert!(iter.seek(&Key::from_raw(b"a1")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());

        assert!(iter.seek_to_last().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());
        assert!(!iter.next().unwrap());
        assert!(iter.prev().unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a5").as_encoded().as_slice());

        assert!(iter.seek(&Key::from_raw(b"a7")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());

        assert!(!iter.seek_for_prev(&Key::from_raw(b"a0")).unwrap());

        assert!(iter.seek_for_prev(&Key::from_raw(b"a1")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a1").as_encoded().as_slice());

        assert!(iter.seek_for_prev(&Key::from_raw(b"a8")).unwrap());
        assert_eq!(iter.key(), Key::from_raw(b"a7").as_encoded().as_slice());
    }

    #[test]
    fn test_get_not_exist_cf() {
        let engine = BTreeEngine::new(&[]);
        assert!(::panic_hook::recover_safe(|| engine.get_cf("not_exist_cf")).is_err());
    }
}
