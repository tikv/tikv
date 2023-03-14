// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp,
    path::{Path, PathBuf},
    u64,
};

use file_system::{
    File,
};
use tikv::config::TikvConfig;
use tikv_util::{
    sys::{
        disk, path_in_diff_mount_point,
    },
};

pub struct TikvServerCore {
    pub config: TikvConfig,
    pub store_path: PathBuf,
    pub lock_files: Vec<File>,
}

impl TikvServerCore {
    pub fn init_fs(&mut self) {
        let lock_path = self.store_path.join(Path::new("LOCK"));

        let f = File::create(lock_path.as_path())
            .unwrap_or_else(|e| fatal!("failed to create lock at {}: {}", lock_path.display(), e));
        if f.try_lock_exclusive().is_err() {
            fatal!(
                "lock {} failed, maybe another instance is using this directory.",
                self.store_path.display()
            );
        }
        self.lock_files.push(f);

        if tikv_util::panic_mark_file_exists(&self.config.storage.data_dir) {
            fatal!(
                "panic_mark_file {} exists, there must be something wrong with the db. \
                     Do not remove the panic_mark_file and force the TiKV node to restart. \
                     Please contact TiKV maintainers to investigate the issue. \
                     If needed, use scale in and scale out to replace the TiKV node. \
                     https://docs.pingcap.com/tidb/stable/scale-tidb-using-tiup",
                tikv_util::panic_mark_file_path(&self.config.storage.data_dir).display()
            );
        }

        // We truncate a big file to make sure that both raftdb and kvdb of TiKV have
        // enough space to do compaction and region migration when TiKV recover.
        // This file is created in data_dir rather than db_path, because we must not
        // increase store size of db_path.
        fn calculate_reserved_space(capacity: u64, reserved_size_from_config: u64) -> u64 {
            let mut reserved_size = reserved_size_from_config;
            if reserved_size_from_config != 0 {
                reserved_size =
                    cmp::max((capacity as f64 * 0.05) as u64, reserved_size_from_config);
            }
            reserved_size
        }
        fn reserve_physical_space(data_dir: &String, available: u64, reserved_size: u64) {
            let path = Path::new(data_dir).join(file_system::SPACE_PLACEHOLDER_FILE);
            if let Err(e) = file_system::remove_file(path) {
                warn!("failed to remove space holder on starting: {}", e);
            }

            // place holder file size is 20% of total reserved space.
            if available > reserved_size {
                file_system::reserve_space_for_recover(data_dir, reserved_size / 5)
                    .map_err(|e| panic!("Failed to reserve space for recovery: {}.", e))
                    .unwrap();
            } else {
                warn!("no enough disk space left to create the place holder file");
            }
        }

        let disk_stats = fs2::statvfs(&self.config.storage.data_dir).unwrap();
        let mut capacity = disk_stats.total_space();
        if self.config.raft_store.capacity.0 > 0 {
            capacity = cmp::min(capacity, self.config.raft_store.capacity.0);
        }
        // reserve space for kv engine
        let kv_reserved_size =
            calculate_reserved_space(capacity, self.config.storage.reserve_space.0);
        disk::set_disk_reserved_space(kv_reserved_size);
        reserve_physical_space(
            &self.config.storage.data_dir,
            disk_stats.available_space(),
            kv_reserved_size,
        );

        let raft_data_dir = if self.config.raft_engine.enable {
            self.config.raft_engine.config().dir
        } else {
            self.config.raft_store.raftdb_path.clone()
        };

        let separated_raft_mount_path =
            path_in_diff_mount_point(&self.config.storage.data_dir, &raft_data_dir);
        if separated_raft_mount_path {
            let raft_disk_stats = fs2::statvfs(&raft_data_dir).unwrap();
            // reserve space for raft engine if raft engine is deployed separately
            let raft_reserved_size = calculate_reserved_space(
                raft_disk_stats.total_space(),
                self.config.storage.reserve_raft_space.0,
            );
            disk::set_raft_disk_reserved_space(raft_reserved_size);
            reserve_physical_space(
                &raft_data_dir,
                raft_disk_stats.available_space(),
                raft_reserved_size,
            );
        }
    }
}
