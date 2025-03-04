// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::ToOwned, str, string::ToString, sync::LazyLock, u64};

use clap::{crate_authors, AppSettings};
use engine_traits::{SstCompressionType, CF_DEFAULT};
use raft_engine::ReadableSize;
use structopt::StructOpt;

const RAW_KEY_HINT: &str = "Raw key (generally starts with \"z\") in escaped form";
static VERSION_INFO: LazyLock<String> = LazyLock::new(|| {
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::tikv_version_info(build_timestamp)
});

#[derive(StructOpt)]
#[structopt(
    name = "TiKV Control (tikv-ctl)",
    about = "A tool for interacting with TiKV deployments.",
    author = crate_authors!(),
    version = &**VERSION_INFO,
    long_version = &**VERSION_INFO,
    setting = AppSettings::DontCollapseArgsInUsage,
)]
pub struct Opt {
    #[structopt(long)]
    /// Set the address of pd
    pub pd: Option<String>,

    #[structopt(long, default_value = "warn")]
    /// Set the log level
    pub log_level: String,

    #[structopt(long, default_value = "text")]
    pub log_format: String,

    #[structopt(long)]
    /// Set the remote host
    pub host: Option<String>,

    #[structopt(long)]
    /// Set the CA certificate path
    pub ca_path: Option<String>,

    #[structopt(long)]
    /// Set the certificate path
    pub cert_path: Option<String>,

    #[structopt(long)]
    /// Set the private key path
    pub key_path: Option<String>,

    #[structopt(long)]
    /// TiKV config path, by default it's <deploy-dir>/conf/tikv.toml
    pub config: Option<String>,

    #[structopt(long)]
    /// TiKV data-dir, check <deploy-dir>/scripts/run.sh to get it
    pub data_dir: Option<String>,

    #[structopt(long)]
    /// Skip paranoid checks when open rocksdb
    pub skip_paranoid_checks: bool,

    #[allow(dead_code)]
    #[structopt(
        long,
        validator = |_| Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned()),
    )]
    /// Set the rocksdb path
    pub db: Option<String>,

    #[allow(dead_code)]
    #[structopt(
        long,
        validator = |_| Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned()),
    )]
    /// Set the raft rocksdb path
    pub raftdb: Option<String>,

    #[structopt(conflicts_with = "escaped-to-hex", long = "to-escaped")]
    /// Convert a hex key to escaped key
    pub hex_to_escaped: Option<String>,

    #[structopt(conflicts_with = "hex-to-escaped", long = "to-hex")]
    /// Convert an escaped key to hex key
    pub escaped_to_hex: Option<String>,

    #[structopt(
        conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
        long,
    )]
    /// Decode a key in escaped format
    pub decode: Option<String>,

    #[structopt(
        conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
        long,
    )]
    /// Encode a key in escaped format
    pub encode: Option<String>,

    #[structopt(subcommand)]
    pub cmd: Option<Cmd>,
}

#[derive(StructOpt)]
pub enum Cmd {
    /// Print a raft log entry
    Raft {
        #[structopt(subcommand)]
        cmd: RaftCmd,
    },
    /// Print region size
    Size {
        #[structopt(short = "r")]
        /// Set the region id, if not specified, print all regions
        region: Option<u64>,

        #[structopt(
            short = "c",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ",",
            default_value = "default,write,lock"
        )]
        /// Set the cf name, if not specified, print all cf
        cf: Vec<String>,
    },
    /// Print the range db range
    Scan {
        #[structopt(
            short = "f",
            long,
            help = RAW_KEY_HINT,
        )]
        from: String,

        #[structopt(
            short = "t",
            long,
            help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[structopt(long)]
        /// Set the scan limit
        limit: Option<u64>,

        #[structopt(long)]
        /// Set the scan start_ts as filter
        start_ts: Option<u64>,

        #[structopt(long)]
        /// Set the scan commit_ts as filter
        commit_ts: Option<u64>,

        #[structopt(
            long,
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ",",
            default_value = CF_DEFAULT,
        )]
        /// Column family names, combined from default/lock/write
        show_cf: Vec<String>,
    },
    /// Print all raw keys in the range
    RawScan {
        #[structopt(
            short = "f",
            long,
            default_value = "",
            help = RAW_KEY_HINT,
        )]
        from: String,

        #[structopt(
            short = "t",
            long,
            default_value = "",
            help = RAW_KEY_HINT,
        )]
        to: String,

        #[structopt(long, default_value = "30")]
        /// Limit the number of keys to scan
        limit: usize,

        #[structopt(
            long,
            default_value = "default",
            possible_values = &["default", "lock", "write"],
        )]
        /// The column family name.
        cf: String,
    },
    /// Print the raw value
    Print {
        #[structopt(
            short = "c",
            default_value = CF_DEFAULT,
            possible_values = &["default", "lock", "write"],
        )]
        /// The column family name.
        cf: String,

        #[structopt(
            short = "k",
            help = RAW_KEY_HINT,
        )]
        key: String,
    },
    /// Print the mvcc value
    Mvcc {
        #[structopt(
            short = "k",
            help = RAW_KEY_HINT,
        )]
        key: String,

        #[structopt(
            long,
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ",",
            default_value = CF_DEFAULT,
        )]
        /// Column family names, combined from default/lock/write
        show_cf: Vec<String>,

        #[structopt(long)]
        /// Set start_ts as filter
        start_ts: Option<u64>,

        #[structopt(long)]
        /// Set commit_ts as filter
        commit_ts: Option<u64>,
    },
    /// Calculate difference of region keys from different dbs
    Diff {
        #[structopt(short = "r")]
        /// Specify region id
        region: u64,

        #[allow(dead_code)]
        #[structopt(
            conflicts_with = "to_host",
            long,
            validator = |_| Err("DEPRECATED!!! Use --to-data-dir and --to-config instead".to_owned()),
        )]
        /// To which db path
        to_db: Option<String>,

        #[structopt(conflicts_with = "to_host", long)]
        /// data-dir of the target TiKV
        to_data_dir: Option<String>,

        #[structopt(conflicts_with = "to_host", long)]
        /// config of the target TiKV
        to_config: Option<String>,

        #[structopt(
            required_unless = "to_data_dir",
            conflicts_with = "to_db",
            long,
            conflicts_with = "to_db"
        )]
        /// To which remote host
        to_host: Option<String>,
    },
    /// Compact a column family in a specified range
    Compact {
        #[structopt(
            short = "d",
            default_value = "kv",
            possible_values = &["kv", "raft"],
        )]
        /// Which db to compact
        db: String,

        #[structopt(
            short = "c",
            default_value = CF_DEFAULT,
            possible_values = &["default", "lock", "write"],
        )]
        /// The column family name.
        cf: String,

        #[structopt(
            short = "f",
            long,
            help = RAW_KEY_HINT,
        )]
        from: Option<String>,

        #[structopt(
            short = "t",
            long,
            help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[structopt(short = "n", long, default_value = "8")]
        /// Number of threads in one compaction
        threads: u32,

        #[structopt(short = "r", long)]
        /// Set the region id
        region: Option<u64>,

        #[structopt(
            short = "b",
            long,
            default_value = "default",
            possible_values = &["skip", "force", "default"],
        )]
        /// Set how to compact the bottommost level
        bottommost: String,
    },
    /// Set some regions on the node to tombstone by manual
    Tombstone {
        #[structopt(
            short = "r",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// The target regions, separated with commas if multiple
        regions: Vec<u64>,

        #[structopt(
            short = "p",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// PD endpoints
        pd: Option<Vec<String>>,

        #[structopt(long)]
        /// force execute without pd
        force: bool,
    },
    /// Recover mvcc data on one node by deleting corrupted keys
    RecoverMvcc {
        #[structopt(short = "a", long)]
        /// Recover the whole db
        all: bool,

        #[structopt(
            required_unless = "all",
            conflicts_with = "all",
            short = "r",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// The target regions, separated with commas if multiple
        regions: Vec<u64>,

        #[structopt(
            required_unless = "all",
            short = "p",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// PD endpoints
        pd: Vec<String>,

        #[structopt(long, default_value_if("all", None, "4"), requires = "all")]
        /// The number of threads to do recover, only for --all mode
        threads: Option<usize>,

        #[structopt(short = "R", long)]
        /// Skip write RocksDB
        read_only: bool,
    },
    /// Unsafely recover when the store can not start normally, this recover may
    /// lose data
    UnsafeRecover {
        #[structopt(subcommand)]
        cmd: UnsafeRecoverCmd,
    },
    /// Recreate a region with given metadata, but alloc new id for it
    RecreateRegion {
        #[structopt(
            short = "p",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// PD endpoints
        pd: Vec<String>,

        #[structopt(short = "r")]
        /// The origin region id
        region: u64,
    },
    /// Print the metrics
    Metrics {
        #[structopt(
            short = "t",
            long,
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ",",
            default_value = crate::executor::METRICS_PROMETHEUS,
            possible_values = &["prometheus", "jemalloc", "rocksdb_raft", "rocksdb_kv"],
        )]
        /// Set the metrics tag
        /// Options: prometheus/jemalloc/rocksdb_raft/rocksdb_kv
        /// If not specified, print prometheus
        tag: Vec<String>,
    },
    /// Force a consistency-check for a specified region
    ConsistencyCheck {
        #[structopt(short = "r")]
        /// The target region
        region: u64,
    },
    /// Get all regions with corrupt raft
    BadRegions {},
    /// Modify tikv config.
    /// Eg. tikv-ctl --host ip:port modify-tikv-config -n
    /// rocksdb.defaultcf.disable-auto-compactions -v true
    ModifyTikvConfig {
        #[structopt(short = "n")]
        /// The config name are same as the name used on config file.
        /// eg. raftstore.messages-per-tick, raftdb.max-background-jobs
        config_name: String,

        #[structopt(short = "v")]
        /// The config value, eg. 8, true, 1h, 8MB
        config_value: String,
    },
    /// Dump snapshot meta file
    DumpSnapMeta {
        #[structopt(short = "f", long)]
        /// Output meta file path
        file: String,
    },
    /// Compact the whole cluster in a specified range in one or more column
    /// families
    CompactCluster {
        #[structopt(
            short = "d",
            default_value = "kv",
            possible_values = &["kv", "raft"],
        )]
        /// The db to use
        db: String,

        #[structopt(
            short = "c",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ",",
            default_value = CF_DEFAULT,
            possible_values = &["default", "lock", "write"],
        )]
        /// Column family names, for kv db, combine from default/lock/write; for
        /// raft db, can only be default
        cf: Vec<String>,

        #[structopt(
            short = "f",
            long,
            help = RAW_KEY_HINT,
        )]
        from: Option<String>,

        #[structopt(
            short = "t",
            long,
            help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[structopt(short = "n", long, default_value = "8")]
        /// Number of threads in one compaction
        threads: u32,

        #[structopt(
            short = "b",
            long,
            default_value = "default",
            possible_values = &["skip", "force", "default"],
        )]
        /// How to compact the bottommost level
        bottommost: String,
    },
    /// Show region properties
    RegionProperties {
        #[structopt(short = "r")]
        /// The target region id
        region: u64,
    },
    /// Show range properties
    RangeProperties {
        #[structopt(long, default_value = "")]
        /// hex start key (not starts with "z")
        start: String,

        #[structopt(long, default_value = "")]
        /// hex end key (not starts with "z")
        end: String,
    },
    /// Split the region
    SplitRegion {
        #[structopt(short = "r")]
        /// The target region id
        region: u64,

        #[structopt(short = "k")]
        /// The key to split it, in unencoded escaped format
        key: String,
    },
    /// Inject failures to TiKV and recovery
    Fail {
        #[structopt(subcommand)]
        cmd: FailCmd,
    },
    /// Print the store id and api version
    Store {},
    /// Print the cluster id
    Cluster {},
    /// Decrypt an encrypted file
    DecryptFile {
        #[structopt(long)]
        /// input file path
        file: String,

        #[structopt(long)]
        /// output file path
        out_file: String,
    },
    /// Dump encryption metadata
    EncryptionMeta {
        #[structopt(subcommand)]
        cmd: EncryptionMetaCmd,
    },
    /// Delete encryption keys that are no longer associated with physical
    /// files.
    CleanupEncryptionMeta {},
    /// Print bad ssts related infos
    BadSsts {
        #[structopt(long)]
        /// specify manifest, if not set, it will look up manifest file in db
        /// path
        manifest: Option<String>,

        #[structopt(long, value_delimiter = ",")]
        /// PD endpoints
        pd: String,
    },
    /// Reset data in a TiKV to a certain version
    ResetToVersion {
        #[structopt(short = "v")]
        /// The version to reset TiKV to
        version: u64,
    },
    /// Control for Raft Engine
    /// Usage: tikv-ctl raft-engine-ctl -- --help
    RaftEngineCtl {
        #[structopt(last = true)]
        args: Vec<String>,
    },
    #[structopt(external_subcommand)]
    External(Vec<String>),
    /// Usage: tikv-ctl show-cluster-id --config <config-path>
    ShowClusterId {
        /// Data directory path of the given TiKV instance.
        #[structopt(long)]
        data_dir: String,
    },
    /// Usage: tikv-ctl fork-readonly-tikv
    ///
    /// fork-readonly-tikv is for creating a tikv-server agent based on a
    /// read-only TiKV remains. The agent can be used for recovery because
    /// all committed transactions can be accessed correctly, without any
    /// modifications on the remained TiKV.
    ///
    /// NOTE: The remained TiKV can't run concurrently with the agent.
    ReuseReadonlyRemains {
        /// Data directory path of the remained TiKV.
        #[structopt(long)]
        data_dir: String,

        /// Data directory to create the agent.
        #[structopt(long)]
        agent_dir: String,

        /// Reuse snapshot files of the remained TiKV: symlink or copy.
        #[structopt(long, default_value = "symlink")]
        snaps: String,

        /// Reuse rocksdb files of the remained TiKV: symlink or copy.
        ///
        /// NOTE: the last one WAL file will still be copied even if `symlink`
        /// is specified, because the last one WAL file isn't read-only when
        /// opening a RocksDB instance.
        #[structopt(long, default_value = "symlink")]
        rocksdb_files: String,
    },
    /// flashback data in cluster to a certain version
    ///
    /// NOTE: Should use `./pd-ctl config set halt-scheduling true` to halt PD
    /// scheduling before flashback.
    Flashback {
        #[structopt(short = "v")]
        /// the version to flashback
        version: u64,

        #[structopt(
            short = "r",
            aliases = &["region"],
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// specific regions to flashback
        regions: Option<Vec<u64>>,

        #[structopt(long, default_value = "")]
        /// hex start key
        start: String,

        #[structopt(long, default_value = "")]
        /// hex end key
        end: String,
    },
    CompactLogBackup {
        #[structopt(
            short,
            long,
            default_value = "compaction",
            help(
                "name of the compaction, register this will help you find the compaction easier."
            )
        )]
        name: String,
        #[structopt(
            long = "from",
            help(
                "from when we need to include files into the compaction.\
                files contains any record within the [--from, --until) will be selected."
            )
        )]
        from_ts: u64,
        #[structopt(
            long = "until",
            help(
                "until when we need to include files into the compaction.\
                files contains any record within the [--from, --until) will be selected."
            )
        )]
        until_ts: u64,
        #[structopt(
            short = "N",
            long = "concurrency",
            default_value = "32",
            help("how many compactions can be executed concurrently.")
        )]
        max_concurrent_compactions: u64,
        #[structopt(
            short = "s",
            long = "storage-base64",
            help(
                "the base-64 encoded protocol buffer message `StorageBackend`. \
                `br` CLI should provide a subcommand that converts an URL to it."
            )
        )]
        storage_base64: String,
        #[structopt(
            long,
            default_value = "lz4",
            help(
                "the compression method will use when generating SSTs. (hint: zstd | lz4 | snappy)"
            )
        )]
        compression: SstCompressionType,
        #[structopt(
            long,
            help(
                "the compression level. it definition and effect varies by the algorithm we choose."
            )
        )]
        compression_level: Option<i32>,

        #[structopt(
            long,
            help(
                "if set, all checkpoints will be ignored. i.e. all finished compaction will be regenerated."
            )
        )]
        force_regenerate: bool,

        #[structopt(
            long,
            default_value = "16M",
            help(
                "specify the minimal compaction size in bytes, if backup data of a region doesn't reach this threshold, it won't be compacted"
            )
        )]
        minimal_compaction_size: ReadableSize,
    },
    /// Get the state of a region's RegionReadProgress.
    GetRegionReadProgress {
        #[structopt(short = "r", long)]
        /// The target region id
        region: u64,

        #[structopt(long)]
        /// When specified, prints the locks associated with the transaction
        /// that has the smallest 'start_ts' in the resolver, which is
        /// preventing the 'resolved_ts' from advancing.
        log: bool,

        #[structopt(long, requires = "log")]
        /// The smallest start_ts of the target transaction. Namely, only the
        /// transaction whose start_ts is greater than or equal to this value
        /// can be recorded in TiKV logs.
        min_start_ts: Option<u64>,
    },
}

#[derive(StructOpt)]
pub enum RaftCmd {
    /// Print the raft log entry info
    Log {
        #[structopt(required_unless = "key", conflicts_with = "key", short = "r")]
        /// Set the region id
        region: Option<u64>,

        #[structopt(required_unless = "key", conflicts_with = "key", short = "i")]
        /// Set the raft log index
        index: Option<u64>,

        #[structopt(
            required_unless_one = &["region", "index"],
            conflicts_with_all = &["region", "index"],
            short = "k",
            help = RAW_KEY_HINT,
        )]
        key: Option<String>,
        #[structopt(short = "b")]
        binary: bool,
    },
    /// print region info
    Region {
        #[structopt(
            short = "r",
            aliases = &["region"],
            conflicts_with = "all-regions",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// Print info for these regions
        regions: Option<Vec<u64>>,

        // `regions` must be None when `all_regions` is present,
        // so we left `all_regions` unused.
        #[allow(dead_code)]
        #[structopt(long, conflicts_with = "regions")]
        /// Print info for all regions
        all_regions: bool,

        #[structopt(long, default_value = "")]
        /// hex start key
        start: String,

        #[structopt(long, default_value = "")]
        /// hex end key
        end: String,

        #[structopt(long, default_value = "16")]
        /// Limit the number of keys to scan
        limit: usize,

        #[structopt(long)]
        /// Skip tombstone regions
        skip_tombstone: bool,
    },
}

#[derive(StructOpt)]
pub enum FailCmd {
    /// Inject failures
    Inject {
        /// Inject fail point and actions pairs.
        /// E.g. tikv-ctl fail inject a=off b=panic
        args: Vec<String>,

        #[structopt(short = "f")]
        /// Read a file of fail points and actions to inject
        file: Option<String>,
    },
    /// Recover failures
    Recover {
        /// Recover fail points. Eg. tikv-ctl fail recover a b
        args: Vec<String>,

        #[structopt(short = "f")]
        /// Recover from a file of fail points
        file: Option<String>,
    },
    /// List all fail points
    List {},
}

#[derive(StructOpt)]
pub enum EncryptionMetaCmd {
    /// Dump data keys
    DumpKey {
        #[structopt(long, use_delimiter = true)]
        /// List of data key ids. Dump all keys if not provided.
        ids: Option<Vec<u64>>,
    },
    /// Dump file encryption info
    DumpFile {
        #[structopt(long)]
        /// Path to the file. Dump for all files if not provided.
        path: Option<String>,
    },
}

#[derive(StructOpt)]
pub enum UnsafeRecoverCmd {
    /// Remove the failed machines from the peer list for the regions
    RemoveFailStores {
        #[structopt(
            short = "s",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// Stores to be removed
        stores: Vec<u64>,

        #[structopt(
            required_unless = "all-regions",
            conflicts_with = "all-regions",
            short = "r",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// Only for these regions
        regions: Option<Vec<u64>>,

        #[structopt(long)]
        /// Promote learner to voter
        promote_learner: bool,

        // `regions` must be None when `all_regions` is present,
        // so we left `all_regions` unused.
        #[allow(dead_code)]
        #[structopt(required_unless = "regions", conflicts_with = "regions", long)]
        /// Do the command for all regions
        all_regions: bool,
    },
    /// Remove unapplied raftlogs on the regions
    DropUnappliedRaftlog {
        #[structopt(
            required_unless = "all-regions",
            conflicts_with = "all-regions",
            short = "r",
            use_delimiter = true,
            require_delimiter = true,
            value_delimiter = ","
        )]
        /// Only for these regions
        regions: Option<Vec<u64>>,

        // `regions` must be None when `all_regions` is present,
        // so we left `all_regions` unused.
        #[allow(dead_code)]
        #[structopt(required_unless = "regions", conflicts_with = "regions", long)]
        /// Do the command for all regions
        all_regions: bool,
    },
}
