//! Conveniences for creating a TiKV server

use crate::config::TiKvConfig;
use crate::fatal;
use crate::util;

const RESERVED_OPEN_FDS: u64 = 1000;

/// Various sanity-checks before running a server.
///
/// Warnings are logged and fatal errors exit.
///
/// # Warnings
///
/// - if `net.core.somaxconn` < 32768
/// - if `net.ipv4.tcp_syncookies` is not 0
/// - if `vm.swappiness` is not 0
/// - if data directories are not on SSDs
///
/// # Fatal errors
///
/// If the max open file descriptor limit is not high enough to support
/// the main database and the raft database.
///
/// # See also
///
/// See the `check_*` functions in `util::config`:
///
/// - [`tikv_util::config::check_max_open_fs`](../tikv_util/config/fn.check_max_open_fds.html
/// - [`tikv_util::config::check_kernel`](../tikv_util/config/fn.check_kernel.html
/// - [`tikv_util::config::check_data_dir`](../tikv_util/config/fn.check_data_dir.html
///
pub fn check_system_config(config: &TiKvConfig) {
    if let Err(e) = util::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (config.rocksdb.max_open_files + config.raftdb.max_open_files) as u64,
    ) {
        fatal!("{}", e);
    }

    for e in util::config::check_kernel() {
        warn!(
            "check-kernel";
            "err" => %e
        );
    }

    // Check RocksDB data dir
    if let Err(e) = util::config::check_data_dir(&config.storage.data_dir) {
        warn!(
            "rocksdb check data dir";
            "err" => %e
        );
    }
    // Check raft data dir
    if let Err(e) = util::config::check_data_dir(&config.raft_store.raftdb_path) {
        warn!(
            "raft check data dir";
            "err" => %e
        );
    }
}
