// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{Display, Formatter},
    fs,
    fs::Metadata,
    path::Path,
    sync::Arc,
    time::Duration,
};

use collections::HashSet;
use kvengine::table::sstable;
use kvproto::import_sstpb::SwitchMode;
use sst_importer::SstImporter;
use tikv_util::{error, info, warn, worker::Runnable};

pub struct GcTask {}

impl Display for GcTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GcTask")
    }
}

/// The GC worker periodically removes unused sst files to release storage resource.
pub struct GcRunner {
    kv: kvengine::Engine,
    importer: Arc<SstImporter>,
    timeout: Duration,
}

impl Runnable for GcRunner {
    type Task = GcTask;

    fn run(&mut self, _: GcTask) {
        if let Err(err) = self.gc_kv_files() {
            error!("local file gc kv files failed {:?}", err);
        }
        if let Err(err) = self.gc_importer_files() {
            error!("local file gc importer files failed {:?}", err);
        }
    }
}

impl GcRunner {
    pub fn new(kv: kvengine::Engine, importer: Arc<SstImporter>, timeout: Duration) -> Self {
        Self {
            kv,
            importer,
            timeout,
        }
    }

    fn gc_kv_files(&self) -> kvengine::Result<()> {
        let kv_file_ids = self.collect_kv_file_ids();
        self.remove_kv_garbage_files(&kv_file_ids)?;
        Ok(())
    }

    fn collect_kv_file_ids(&self) -> HashSet<u64> {
        loop {
            if let Some(all_file_ids) = self.try_collect_kv_file_ids() {
                return all_file_ids;
            }
        }
    }

    fn try_collect_kv_file_ids(&self) -> Option<HashSet<u64>> {
        let shard_id_vers = self.kv.get_all_shard_id_vers();
        let mut all_file_ids = HashSet::default();
        for &id_ver in &shard_id_vers {
            let shard = self.kv.get_shard_with_ver(id_ver.id, id_ver.ver).ok()?;
            all_file_ids.extend(shard.get_all_files());
        }
        Some(all_file_ids)
    }

    fn remove_kv_garbage_files(&self, kv_file_ids: &HashSet<u64>) -> kvengine::Result<()> {
        let entries = fs::read_dir(&self.kv.opts.local_dir)?;
        for e in entries {
            let entry = e?;
            let meta = entry.metadata()?;
            if !self.is_old_file(meta) {
                continue;
            }
            let path = entry.path();
            let path_str = path.to_str().unwrap();
            if path_str.ends_with(".tmp") {
                Self::remove_file(&path)?;
            } else if path_str.ends_with(".sst") {
                let id = sstable::parse_file_id(&path)?;
                if !kv_file_ids.contains(&id) {
                    Self::remove_file(&path)?;
                }
            } else if !path_str.ends_with("LOCK") {
                warn!("unexpected file {:?}", path);
            }
        }
        Ok(())
    }

    fn remove_file(file: &Path) -> std::io::Result<()> {
        info!("local file GC remove file {:?}", file);
        fs::remove_file(file)
    }

    fn gc_importer_files(&self) -> sst_importer::Result<()> {
        if self.importer.get_mode() == SwitchMode::Import {
            return Ok(());
        }
        let ssts = self.importer.list_ssts()?;
        for sst_meta in &ssts {
            let path = self.importer.get_path(sst_meta);
            let meta = fs::metadata(&path)?;
            if self.is_old_file(meta) {
                self.importer.delete(sst_meta)?;
            }
        }
        Ok(())
    }

    fn is_old_file(&self, meta: Metadata) -> bool {
        let modified = meta.modified().unwrap();
        let dur = modified.elapsed().unwrap_or_default();
        dur > self.timeout
    }
}
