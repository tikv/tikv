use std::sync::RwLock;
use std::collections::BTreeMap;
use std::collections::Bound::{Included, Unbounded};
use super::{Engine, Modify, Result};

#[derive(Debug)]
pub struct EngineBtree {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl EngineBtree {
    pub fn new() -> EngineBtree {
        info!("EngineBtree: creating");
        EngineBtree { map: RwLock::new(BTreeMap::new()) }
    }
}

impl Engine for EngineBtree {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        trace!("EngineBtree: get {:?}", key);
        let m = self.map.read().unwrap(); // Use unwrap here since the usage pattern is simple,
                                          // should never be poisoned.
        Ok(m.get(key).cloned())
    }

    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        trace!("EngineBtree: seek {:?}", key);
        let m = self.map.read().unwrap(); // Use unwrap here since the usage pattern is simple,
                                          // should never be poisoned.
        let mut iter = m.range::<Vec<u8>, Vec<u8>>(Included(&key.to_owned()), Unbounded);
        Ok(iter.next().map(|(k, v)| (k.clone(), v.clone())))
    }

    fn write(&self, batch: Vec<Modify>) -> Result<()> {
        let mut m = self.map.write().unwrap(); // Use unwrap here since the usage pattern is
                                               // simple, should never be poisoned.
        for rev in batch {
            match rev {
                Modify::Delete(k) => {
                    trace!("EngineBtree: delete {:?}", k);
                    m.remove(k);
                }
                Modify::Put((k, v)) => {
                    trace!("EngineBtree: put {:?},{:?}", k, v);
                    m.insert(k.to_owned(), v.to_owned());
                }
            }
        }
        Ok(())
    }
}
