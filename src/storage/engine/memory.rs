use std::collections::BTreeMap;
use super::{Engine, Modify};

#[derive(Debug)]
pub struct BTreeEngine {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BTreeEngine {
    pub fn new() -> BTreeEngine {
        info!("BTreeEngine: creating");
        BTreeEngine { map: BTreeMap::new() }
    }
}

impl Engine for BTreeEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        trace!("BTreeEngine: get {:?}", key);
        Ok(self.map.get(key).map(|v| v.clone()))
    }

    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, String> {
        trace!("BtreeEngine: seek {:?}", key);
        // TODO (disksing)
        panic!("BTreeEngine.seek not implemented.");
    }

    fn write(&mut self, batch: Vec<Modify>) -> Result<(), String> {
        for rev in batch {
            match rev {
                Modify::Delete(k) => {
                    trace!("BTreeEngine: delete {:?}", k);
                    self.map.remove(k);
                }
                Modify::Put((k, v)) => {
                    trace!("BTreeEngine: put {:?},{:?}", k, v);
                    self.map.insert(k.to_owned(), v.to_owned());
                }
            }
        }
        Ok(())
    }
}
