// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::util::{append_expire_ts, ttl_to_expire_ts};
use kvproto::import_sstpb::*;
use std::sync::Arc;

use encryption::DataKeyManager;
use engine_traits::{KvEngine, SstWriter};
use txn_types::{is_short_value, Key, TimeStamp, Write as KvWrite, WriteType};

use crate::import_file::ImportPath;
use crate::Result;

pub struct TxnSSTWriter<E: KvEngine> {
    default: E::SstWriter,
    default_entries: u64,
    default_path: ImportPath,
    default_meta: SstMeta,
    write: E::SstWriter,
    write_entries: u64,
    write_path: ImportPath,
    write_meta: SstMeta,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl<E: KvEngine> TxnSSTWriter<E> {
    pub fn new(
        default: E::SstWriter,
        write: E::SstWriter,
        default_path: ImportPath,
        write_path: ImportPath,
        default_meta: SstMeta,
        write_meta: SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Self {
        TxnSSTWriter {
            default,
            default_path,
            default_entries: 0,
            default_meta,
            write,
            write_path,
            write_entries: 0,
            write_meta,
            key_manager,
        }
    }

    pub fn write(&mut self, batch: WriteBatch) -> Result<()> {
        let commit_ts = TimeStamp::new(batch.get_commit_ts());
        for m in batch.get_pairs().iter() {
            let k = Key::from_raw(m.get_key()).append_ts(commit_ts);
            self.put(k.as_encoded(), m.get_value(), m.get_op())?;
        }
        Ok(())
    }

    fn put(&mut self, key: &[u8], value: &[u8], op: PairOp) -> Result<()> {
        let k = keys::data_key(key);
        let (_, commit_ts) = Key::split_on_ts_for(key)?;
        let w = match (op, is_short_value(value)) {
            (PairOp::Delete, _) => KvWrite::new(WriteType::Delete, commit_ts, None),
            (PairOp::Put, true) => KvWrite::new(WriteType::Put, commit_ts, Some(value.to_vec())),
            (PairOp::Put, false) => {
                self.default.put(&k, value)?;
                self.default_entries += 1;
                KvWrite::new(WriteType::Put, commit_ts, None)
            }
        };
        self.write.put(&k, &w.as_ref().to_bytes())?;
        self.write_entries += 1;
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<SstMeta>> {
        let default_meta = self.default_meta.clone();
        let write_meta = self.write_meta.clone();
        let mut metas = Vec::with_capacity(2);
        let (default_entries, write_entries) = (self.default_entries, self.write_entries);
        let (p1, p2) = (self.default_path.clone(), self.write_path.clone());
        let (w1, w2, key_manager) = (self.default, self.write, self.key_manager);
        if default_entries > 0 {
            w1.finish()?;
            p1.save(key_manager.as_deref())?;
            metas.push(default_meta);
        }
        if write_entries > 0 {
            w2.finish()?;
            p2.save(key_manager.as_deref())?;
            metas.push(write_meta);
        }
        info!("finish write to sst";
            "default entries" => default_entries,
            "write entries" => write_entries,
        );
        Ok(metas)
    }
}

pub struct RawSSTWriter<E: KvEngine> {
    default: E::SstWriter,
    default_entries: u64,
    default_path: ImportPath,
    default_meta: SstMeta,
    key_manager: Option<Arc<DataKeyManager>>,
    enable_ttl: bool,
}

impl<E: KvEngine> RawSSTWriter<E> {
    pub fn new(
        default: E::SstWriter,
        default_path: ImportPath,
        default_meta: SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
        enable_ttl: bool,
    ) -> Self {
        RawSSTWriter {
            default,
            default_path,
            default_entries: 0,
            default_meta,
            key_manager,
            enable_ttl,
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
        if self.enable_ttl {
            let ttl = batch.get_ttl();
            let expire_ts = ttl_to_expire_ts(ttl);
            for mut m in batch.take_pairs().into_iter() {
                let mut value = m.take_value();
                append_expire_ts(&mut value, expire_ts);
                self.put(m.get_key(), &value, m.get_op())?;
            }
        } else {
            if batch.get_ttl() != 0 {
                return Err(crate::Error::TTLNotEnabled);
            }
            for mut m in batch.take_pairs().into_iter() {
                let value = m.take_value();
                self.put(m.get_key(), &value, m.get_op())?;
            }
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
