// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Engines, KvEngine, RaftEngine};

pub struct Destroyer<E, R>
where
    E: KvEngine,
    R: RaftEngine,
{
    _name: String,
    engines: Engines<E, R>,
}

impl<E, R> Destroyer<E, R>
where
    E: KvEngine,
    R: RaftEngine,
{
    pub fn new<S: Into<String>>(name: S, engines: Engines<E, R>) -> Self {
        Self {
            _name: name.into(),
            engines,
        }
    }

    pub fn shutdown(&mut self) {
        // Disable manul compaction jobs before shutting down the engines. And it
        // will stop the compaction thread in advance, so it won't block the
        // cleanup thread when exiting.
        let _ = self.engines.kv.disable_manual_compaction();
    }
}
#[cfg(test)]
mod tests {
    use engine_test::new_temp_engine;
    use engine_traits::{
        CompactExt, ManualCompactionOptions, MiscExt, Mutable, WriteBatch, WriteBatchExt,
        CF_DEFAULT,
    };
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_destroyer_shutdown() {
        let path = Builder::new()
            .prefix("test_destroyer_shutdown")
            .tempdir()
            .unwrap();
        let engines = new_temp_engine(&path);
        let db = engines.kv.clone();
        let mut destroyer = Destroyer::new("test-destroyer", engines.clone());

        // Generate the first SST file.
        let mut wb = db.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // Generate another SST file has the same content with first SST file.
        let mut wb = db.write_batch();
        for i in 0..1000 {
            let k = format!("key_{}", i);
            wb.put_cf(CF_DEFAULT, k.as_bytes(), b"whatever content")
                .unwrap();
        }
        wb.write().unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();

        // Get the total SST files size.
        let old_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();

        // Shutdown the destroyer.
        destroyer.shutdown();
        // Manually compact range.
        let _ = db.compact_range_cf(
            CF_DEFAULT,
            None,
            None,
            ManualCompactionOptions::new(false, 1, true),
        );

        // Get the total SST files size after compact range.
        let new_sst_files_size = db.get_total_sst_files_size_cf(CF_DEFAULT).unwrap().unwrap();
        assert_eq!(old_sst_files_size, new_sst_files_size);
    }
}
