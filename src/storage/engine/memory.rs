use std::sync::RwLock;
use std::collections::BTreeMap;
use std::collections::Bound::{Included, Unbounded};
use std::fmt::{self, Formatter, Debug};
use kvproto::kvrpcpb::Context;
use storage::{Key, Value, KvPair};
use util::HandyRwLock;
use super::{Engine, Modify, Result};

pub struct EngineBtree {
    map: RwLock<BTreeMap<Vec<u8>, Value>>,
}

impl EngineBtree {
    pub fn new() -> EngineBtree {
        info!("EngineBtree: creating");
        EngineBtree { map: RwLock::new(BTreeMap::new()) }
    }
}

impl Debug for EngineBtree {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Memory")
    }
}

impl Engine for EngineBtree {
    fn get(&self, _: &Context, key: &Key) -> Result<Option<Value>> {
        trace!("EngineBtree: get {:?}", key);
        let m = self.map.rl();
        Ok(m.get(key.raw()).cloned())
    }

    fn seek(&self, _: &Context, key: &Key) -> Result<Option<KvPair>> {
        trace!("EngineBtree: seek {:?}", key);
        let m = self.map.rl();
        let mut iter = m.range::<Vec<u8>, Vec<u8>>(Included(key.raw()), Unbounded);
        Ok(iter.next().map(|(k, v)| (k.clone(), v.clone())))
    }

    fn write(&self, _: &Context, batch: Vec<Modify>) -> Result<()> {
        let mut m = self.map.wl();
        for rev in batch {
            match rev {
                Modify::Delete(k) => {
                    trace!("EngineBtree: delete {:?}", k);
                    m.remove(k.raw());
                }
                Modify::Put((k, v)) => {
                    trace!("EngineBtree: put {:?},{:?}", k, v);
                    m.insert(k.raw().clone(), v);
                }
            }
        }
        Ok(())
    }
}
