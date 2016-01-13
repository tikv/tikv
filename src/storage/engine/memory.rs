use std::collections::BTreeMap;
use std::collections::Bound::{Included, Unbounded};
use super::{Engine, Modify, Result};

#[derive(Debug)]
pub struct EngineBtree {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl EngineBtree {
    pub fn new() -> EngineBtree {
        info!("EngineBtree: creating");
        EngineBtree { map: BTreeMap::new() }
    }
}

impl Engine for EngineBtree {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        trace!("EngineBtree: get {:?}", key);
        Ok(self.map.get(key).map(|v| v.clone()))
    }

    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        trace!("EngineBtree: seek {:?}", key);
        let mut iter = self.map.range::<Vec<u8>, Vec<u8>>(Included(&key.to_owned()), Unbounded);
        Ok(iter.next().map(|(k, v)| (k.clone(), v.clone())))
    }

    fn write(&mut self, batch: Vec<Modify>) -> Result<()> {
        for rev in batch {
            match rev {
                Modify::Delete(k) => {
                    trace!("EngineBtree: delete {:?}", k);
                    self.map.remove(k);
                }
                Modify::Put((k, v)) => {
                    trace!("EngineBtree: put {:?},{:?}", k, v);
                    self.map.insert(k.to_owned(), v.to_owned());
                }
            }
        }
        Ok(())
    }
}
