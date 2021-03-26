use engine_traits::util::append_expire_ts;
use kvproto::import_sstpb::*;
use std::sync::Arc;

use encryption::DataKeyManager;
use engine_traits::{KvEngine, SstWriter};

use super::import_file::ImportPath;
use super::Result;

pub struct RawSSTWriter<E: KvEngine> {
    default: E::SstWriter,
    default_entries: u64,
    default_path: ImportPath,
    default_meta: SstMeta,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl<E: KvEngine> RawSSTWriter<E> {
    pub fn new(
        default: E::SstWriter,
        default_path: ImportPath,
        default_meta: SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Self {
        RawSSTWriter {
            default,
            default_path,
            default_entries: 0,
            default_meta,
            key_manager,
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8], op: PairOp) -> Result<()> {
        let k = keys::data_key(key);
        match op {
            PairOp::Delete => self.default.delete(&k)?,
            PairOp::Put => {
                self.default.put(&k, value)?;
                self.default_entries += 1;
            }
        }
        Ok(())
    }

    pub fn write(&mut self, mut batch: RawWriteBatch) -> Result<()> {
        let ttl = batch.get_ttl();
        for mut m in batch.take_pairs().into_iter() {
            let mut value = m.take_value();
            append_expire_ts(&mut value, ttl);
            self.put(m.get_key(), &value, m.get_op())?;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<SstMeta>> {
        if self.default_entries > 0 {
            self.default.finish()?;
            self.default_path.save(self.key_manager.as_deref())?;
            info!("finish write to sst"; "default entries" => self.default_entries);
            return Ok(vec![self.default_meta]);
        }
        Ok(vec![])
    }
}
