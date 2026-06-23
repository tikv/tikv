// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{borrow::ToOwned, str, sync::LazyLock};

<<<<<<< HEAD
use clap::{AppSettings, crate_authors};
=======
use clap::{Parser, Subcommand, crate_authors};
use compact_log_backup::ShardConfig;
>>>>>>> eb51ff3230 (deny: replace `structopt` with `clap-v3` derives to resolve RUSTSEC-2022-0104 (#19744))
use engine_traits::{CF_DEFAULT, SstCompressionType};
use raft_engine::ReadableSize;
const RAW_KEY_HINT: &str = "Raw key (generally starts with \"z\") in escaped form";
static VERSION_INFO: LazyLock<String> = LazyLock::new(|| {
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::tikv_version_info(build_timestamp)
});

#[derive(Parser)]
#[clap(
    name = "TiKV Control (tikv-ctl)",
    about = "A tool for interacting with TiKV deployments.",
    author = crate_authors!(),
    version = &**VERSION_INFO,
    long_version = &**VERSION_INFO,
    dont_collapse_args_in_usage = true,
)]
pub struct Opt {
    #[clap(long)]
    /// Set the address of pd
    pub pd: Option<String>,

    #[clap(long, default_value = "warn")]
    /// Set the log level
    pub log_level: String,

    #[clap(long, default_value = "text")]
    pub log_format: String,

    #[clap(long)]
    /// Set the remote host
    pub host: Option<String>,

    #[clap(long)]
    /// Set the CA certificate path
    pub ca_path: Option<String>,

    #[clap(long)]
    /// Set the certificate path
    pub cert_path: Option<String>,

    #[clap(long)]
    /// Set the private key path
    pub key_path: Option<String>,

    #[clap(long)]
    /// TiKV config path, by default it's <deploy-dir>/conf/tikv.toml
    pub config: Option<String>,

    #[clap(long)]
    /// TiKV data-dir, check <deploy-dir>/scripts/run.sh to get it
    pub data_dir: Option<String>,

    #[clap(long)]
    /// Skip paranoid checks when open rocksdb
    pub skip_paranoid_checks: bool,

    #[allow(dead_code)]
    #[clap(
        long,
        validator = |_: &str| -> Result<(), String> {
            Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned())
        },
    )]
    /// Set the rocksdb path
    pub db: Option<String>,

    #[allow(dead_code)]
    #[clap(
        long,
        validator = |_: &str| -> Result<(), String> {
            Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned())
        },
    )]
    /// Set the raft rocksdb path
    pub raftdb: Option<String>,

    #[clap(conflicts_with = "escaped-to-hex", long = "to-escaped")]
    /// Convert a hex key to escaped key
    pub hex_to_escaped: Option<String>,

    #[clap(conflicts_with = "hex-to-escaped", long = "to-hex")]
    /// Convert an escaped key to hex key
    pub escaped_to_hex: Option<String>,

    #[clap(
        conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
        long,
    )]
    /// Decode a key in escaped format
    pub decode: Option<String>,

    #[clap(
        conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
        long,
    )]
    /// Encode a key in escaped format
    pub encode: Option<String>,

    #[clap(subcommand)]
    pub cmd: Option<Cmd>,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Print a raft log entry
    Raft {
        #[clap(subcommand)]
        cmd: RaftCmd,
    },
    /// Print region size
    Size {
        #[clap(short = 'r')]
        /// Set the region id, if not specified, print all regions
        region: Option<u64>,

        #[clap(
            short = 'c',
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ',',
            default_value = "default,write,lock"
        )]
        /// Set the cf name, if not specified, print all cf
        cf: Vec<String>,
    },
    /// Print the range db range
    Scan {
        #[clap(
            short = 'f',
            long,
            help = RAW_KEY_HINT,
        )]
        from: String,

        #[clap(
            short = 't',
            long,
            help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[clap(long)]
        /// Set the scan limit
        limit: Option<u64>,

        #[clap(long)]
        /// Set the scan start_ts as filter
        start_ts: Option<u64>,

        #[clap(long)]
        /// Set the scan commit_ts as filter
        commit_ts: Option<u64>,

        #[clap(
            long,
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ',',
            default_value = CF_DEFAULT,
        )]
        /// Column family names, combined from default/lock/write
        show_cf: Vec<String>,
    },
    /// Print all raw keys in the range
    RawScan {
        #[clap(
            short = 'f',
            long,
            default_value = "",
            help = RAW_KEY_HINT,
        )]
        from: String,

        #[clap(
            short = 't',
            long,
            default_value = "",
            help = RAW_KEY_HINT,
        )]
        to: String,

        #[clap(long, default_value = "30")]
        /// Limit the number of keys to scan
        limit: usize,

        #[clap(
            long,
            default_value = "default",
            possible_values = &["default", "lock", "write"],
        )]
        /// The column family name.
        cf: String,
    },
    /// Print the raw value
    Print {
        #[clap(
            short = 'c',
            default_value = CF_DEFAULT,
            possible_values = &["default", "lock", "write"],
        )]
        /// The column family name.
        cf: String,

        #[clap(
            short = 'k',
            help = RAW_KEY_HINT,
        )]
        key: String,
    },
    /// Print the mvcc value
    Mvcc {
        #[clap(
            short = 'k',
            help = RAW_KEY_HINT,
        )]
        key: String,

        #[clap(
            long,
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ',',
            default_value = CF_DEFAULT,
        )]
        /// Column family names, combined from default/lock/write
        show_cf: Vec<String>,

        #[clap(long)]
        /// Set start_ts as filter
        start_ts: Option<u64>,

        #[clap(long)]
        /// Set commit_ts as filter
        commit_ts: Option<u64>,
    },
    /// Calculate difference of region keys from different dbs
    Diff {
        #[clap(short = 'r')]
        /// Specify region id
        region: u64,

        #[allow(dead_code)]
        #[clap(
            conflicts_with = "to_host",
            long,
            validator = |_: &str| -> Result<(), String> {
                Err("DEPRECATED!!! Use --to-data-dir and --to-config instead".to_owned())
            },
        )]
        /// To which db path
        to_db: Option<String>,

        #[clap(conflicts_with = "to_host", long)]
        /// data-dir of the target TiKV
        to_data_dir: Option<String>,

        #[clap(conflicts_with = "to_host", long)]
        /// config of the target TiKV
        to_config: Option<String>,

        #[clap(
            required_unless_present = "to_data_dir",
            conflicts_with = "to_db",
            long,
            conflicts_with = "to_db"
        )]
        /// To which remote host
        to_host: Option<String>,
    },
    /// Compact a column family in a specified range
    Compact {
        #[clap(
            short = 'd',
            default_value = "kv",
            possible_values = &["kv", "raft"],
        )]
        /// Which db to compact
        db: String,

        #[clap(
            short = 'c',
            default_value = CF_DEFAULT,
            possible_values = &["default", "lock", "write"],
        )]
        /// The column family name.
        cf: String,

        #[clap(
            short = 'f',
            long,
            help = RAW_KEY_HINT,
        )]
        from: Option<String>,

        #[clap(
            short = 't',
            long,
            help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[clap(short = 'n', long, default_value = "8")]
        /// Number of threads in one compaction
        threads: u32,

        #[clap(short = 'r', long)]
        /// Set the region id
        region: Option<u64>,

        #[clap(
            short = 'b',
            long,
            default_value = "default",
            possible_values = &["skip", "force", "default"],
        )]
        /// Set how to compact the bottommost level
        bottommost: String,
    },
    /// Set some regions on the node to tombstone by manual
    Tombstone {
        #[clap(
            short = 'r',
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ','
        )]
        /// The target regions, separated with commas if multiple
        regions: Vec<u64>,

        #[clap(
            short = 'p',
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ','
        )]
        /// PD endpoints
        pd: Option<Vec<String>>,

        #[clap(long)]
        /// force execute without pd
        force: bool,
    },
    /// Recover mvcc data on one node by deleting corrupted keys
    RecoverMvcc {
        #[clap(short = 'a', long)]
        /// Recover the whole db
        all: bool,

        #[clap(
            required_unless_present = "all",
            conflicts_with = "all",
            short = 'r',
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ','
        )]
        /// The target regions, separated with commas if multiple
        regions: Vec<u64>,

        #[clap(
            required_unless_present = "all",
            short = 'p',
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ','
        )]
        /// PD endpoints
        pd: Vec<String>,

        #[clap(long, default_value_if("all", None, Some("4")), requires = "all")]
        /// The number of threads to do recover, only for --all mode
        threads: Option<usize>,

        #[clap(short = 'R', long)]
        /// Skip write RocksDB
        read_only: bool,
    },
    /// Unsafely recover when the store can not start normally, this recover may
    /// lose data
    UnsafeRecover {
        #[clap(subcommand)]
        cmd: UnsafeRecoverCmd,
    },
    /// Recreate a region with given metadata, but alloc new id for it
    RecreateRegion {
        #[clap(
            short = 'p',
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ','
        )]
        /// PD endpoints
        pd: Vec<String>,

        #[clap(short = 'r')]
        /// The origin region id
        region: u64,
    },
    /// Print the metrics
    Metrics {
        #[clap(
            short = 't',
            long,
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ',',
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
        #[clap(short = 'r')]
        /// The target region
        region: u64,
    },
    /// Get all regions with corrupt raft
    BadRegions {},
    /// Modify tikv config.
    /// Eg. tikv-ctl --host ip:port modify-tikv-config -n
    /// rocksdb.defaultcf.disable-auto-compactions -v true
    ModifyTikvConfig {
        #[clap(short = 'n')]
        /// The config name are same as the name used on config file.
        /// eg. raftstore.messages-per-tick, raftdb.max-background-jobs
        config_name: String,

        #[clap(short = 'v')]
        /// The config value, eg. 8, true, 1h, 8MB
        config_value: String,
    },
    /// Dump snapshot meta file
    DumpSnapMeta {
        #[clap(short = 'f', long)]
        /// Output meta file path
        file: String,
    },
    /// Compact the whole cluster in a specified range in one or more column
    /// families
    CompactCluster {
        #[clap(
            short = 'd',
            default_value = "kv",
            possible_values = &["kv", "raft"],
        )]
        /// The db to use
        db: String,

        #[clap(
            short = 'c',
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ',',
            default_value = CF_DEFAULT,
            possible_values = &["default", "lock", "write"],
        )]
        /// Column family names, for kv db, combine from default/lock/write; for
        /// raft db, can only be default
        cf: Vec<String>,

        #[clap(
            short = 'f',
            long,
            help = RAW_KEY_HINT,
        )]
        from: Option<String>,

        #[clap(
            short = 't',
            long,
            help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[clap(short = 'n', long, default_value = "8")]
        /// Number of threads in one compaction
        threads: u32,

        #[clap(
            short = 'b',
            long,
            default_value = "default",
            possible_values = &["skip", "force", "default"],
        )]
        /// How to compact the bottommost level
        bottommost: String,
    },
    /// Show region properties
    RegionProperties {
        #[clap(short = 'r')]
        /// The target region id
        region: u64,
    },
    /// Show range properties
    RangeProperties {
        #[clap(long, default_value = "")]
        /// hex start key (not starts with "z")
        start: String,

        #[clap(long, default_value = "")]
        /// hex end key (not starts with "z")
        end: String,
    },
    /// Split the region
    SplitRegion {
        #[clap(short = 'r')]
        /// The target region id
        region: u64,

        #[clap(short = 'k')]
        /// The key to split it, in unencoded escaped format
        key: String,
    },
    /// Inject failures to TiKV and recovery
    Fail {
        #[clap(subcommand)]
        cmd: FailCmd,
    },
    /// Print the store id and api version
    Store {},
    /// Print the cluster id
    Cluster {},
    /// Decrypt an encrypted file
    DecryptFile {
        #[clap(long)]
        /// input file path
        file: String,

        #[clap(long)]
        /// output file path
        out_file: String,
    },
    /// Dump encryption metadata
    EncryptionMeta {
        #[clap(subcommand)]
        cmd: EncryptionMetaCmd,
    },
    /// Delete encryption keys that are no longer associated with physical
    /// files.
    CleanupEncryptionMeta {},
    /// Print bad ssts related infos
    BadSsts {
        #[clap(long)]
        /// specify manifest, if not set, it will look up manifest file in db
        /// path
        manifest: Option<String>,

        #[clap(long, value_delimiter = ',')]
        /// PD endpoints
        pd: String,
    },
    /// Reset data in a TiKV to a certain version
    ResetToVersion {
        #[clap(short = 'v')]
        /// The version to reset TiKV to
        version: u64,
    },
    /// Control for Raft Engine
    /// Usage: tikv-ctl raft-engine-ctl -- --help
    RaftEngineCtl {
        #[clap(last = true)]
        args: Vec<String>,
    },
    #[clap(external_subcommand)]
    External(Vec<String>),
    /// Usage: tikv-ctl show-cluster-id --config <config-path>
    ShowClusterId {
        /// Data directory path of the given TiKV instance.
        #[clap(long)]
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
        #[clap(long)]
        data_dir: String,

        /// Data directory to create the agent.
        #[clap(long)]
        agent_dir: String,

        /// Reuse snapshot files of the remained TiKV: symlink or copy.
        #[clap(long, default_value = "symlink")]
        snaps: String,

        /// Reuse rocksdb files of the remained TiKV: symlink or copy.
        ///
        /// NOTE: the last one WAL file will still be copied even if `symlink`
        /// is specified, because the last one WAL file isn't read-only when
        /// opening a RocksDB instance.
        #[clap(long, default_value = "symlink")]
        rocksdb_files: String,
    },
    /// flashback data in cluster to a certain version
    ///
    /// NOTE: Should use `./pd-ctl config set halt-scheduling true` to halt PD
    /// scheduling before flashback.
    Flashback {
        #[clap(short = 'v')]
        /// the version to flashback
        version: u64,

        #[clap(
            short = 'r',
            aliases = &["region"],
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ','
        )]
        /// specific regions to flashback
        regions: Option<Vec<u64>>,

        #[clap(long, default_value = "")]
        /// hex start key
        start: String,

        #[clap(long, default_value = "")]
        /// hex end key
        end: String,
    },
    CompactLogBackup {
        #[clap(
            short,
            long,
            default_value = "compaction",
            help(
                "name of the compaction, register this will help you find the compaction easier."
            )
        )]
        name: String,
<<<<<<< HEAD
        #[structopt(
=======
        #[clap(
            long,
            value_name = "INDEX/TOTAL",
            help(
                "shard the compaction work by backup-stream store id. Prefer \
                `Metadata.store_id`; if it is absent, fall back to the backup-stream metadata \
                filename format \
                (`v1/backupmeta/{flush_ts}{store_id}-d{min_begin_ts}l{min_ts}u{max_ts}p{flags}.meta`), \
                then backup-stream physical log paths, then a stable metadata-path hash. Only \
                data from stores assigned to this shard is compacted. Format: INDEX/TOTAL, \
                where INDEX is 1-based (e.g. 1/3)."
            )
        )]
        shard: Option<ShardConfig>,
        #[clap(
>>>>>>> eb51ff3230 (deny: replace `structopt` with `clap-v3` derives to resolve RUSTSEC-2022-0104 (#19744))
            long = "from",
            help(
                "from when we need to include files into the compaction.\
                files contains any record within the [--from, --until) will be selected."
            )
        )]
        from_ts: u64,
        #[clap(
            long = "until",
            help(
                "until when we need to include files into the compaction.\
                files contains any record within the [--from, --until) will be selected."
            )
        )]
<<<<<<< HEAD
        until_ts: u64,
        #[structopt(
            short = "N",
=======
        until_ts: Option<u64>,
        #[clap(
            long = "cal-shift-ts",
            help(
                "extend the default CF scan lower bound to the minimum min_begin_default_ts in \
                metadata overlapping [--from, --until)."
            )
        )]
        cal_shift_ts: bool,
        #[clap(
            short = 'N',
>>>>>>> eb51ff3230 (deny: replace `structopt` with `clap-v3` derives to resolve RUSTSEC-2022-0104 (#19744))
            long = "concurrency",
            default_value = "32",
            help("how many compactions can be executed concurrently.")
        )]
        max_concurrent_compactions: u64,
        #[clap(
            short = 's',
            long = "storage-base64",
            help(
                "the base-64 encoded protocol buffer message `StorageBackend`. \
                `br` CLI should provide a subcommand that converts an URL to it."
            )
        )]
        storage_base64: String,
        #[clap(
            long,
            default_value = "lz4",
            help(
                "the compression method will use when generating SSTs. (hint: zstd | lz4 | snappy)"
            )
        )]
        compression: SstCompressionType,
        #[clap(
            long,
            help(
                "the compression level. it definition and effect varies by the algorithm we choose."
            )
        )]
        compression_level: Option<i32>,

        #[clap(
            long,
            help(
                "if set, all checkpoints will be ignored. i.e. all finished compaction will be regenerated."
            )
        )]
        force_regenerate: bool,

        #[clap(
            long,
            default_value = "16M",
            help(
                "specify the minimal compaction size in bytes, if backup data of a region doesn't reach this threshold, it won't be compacted"
            )
        )]
        minimal_compaction_size: ReadableSize,

        #[clap(
            long,
            default_value = "128",
            help("specify the maximum count of running tasks to download a metadata")
        )]
        prefetch_running_count: u64,

        #[clap(
            long,
            default_value = "1024",
            help("specify the maximum count of spawning tasks to download a metadata")
        )]
        prefetch_buffer_count: u64,

<<<<<<< HEAD
        #[structopt(
=======
        #[clap(
            long,
            default_value = "0",
            help(
                "specify memory reserved for caching physical log files, such as 64G. \
                Zero disables the cache."
            )
        )]
        physical_file_cache_capacity: ReadableSize,

        #[clap(
>>>>>>> eb51ff3230 (deny: replace `structopt` with `clap-v3` derives to resolve RUSTSEC-2022-0104 (#19744))
            long = "gcp-v2-enable",
            parse(try_from_str),
            default_value = "true",
            possible_values = &["true", "false"],
            help("whether to enable GCP v2 external storage backend for compact-log-backup")
        )]
        gcp_v2_enable: bool,
    },
    /// Get the state of a region's RegionReadProgress.
    GetRegionReadProgress {
        #[clap(short = 'r', long)]
        /// The target region id
        region: u64,

        #[clap(long)]
        /// When specified, prints the locks associated with the transaction
        /// that has the smallest 'start_ts' in the resolver, which is
        /// preventing the 'resolved_ts' from advancing.
        log: bool,

        #[clap(long, requires = "log")]
        /// The smallest start_ts of the target transaction. Namely, only the
        /// transaction whose start_ts is greater than or equal to this value
        /// can be recorded in TiKV logs.
        min_start_ts: Option<u64>,
    },
}

#[derive(Subcommand)]
pub enum RaftCmd {
    /// Print the raft log entry info
    Log {
        #[clap(required_unless_present = "key", conflicts_with = "key", short = 'r')]
        /// Set the region id
        region: Option<u64>,

        #[clap(required_unless_present = "key", conflicts_with = "key", short = 'i')]
        /// Set the raft log index
        index: Option<u64>,

        #[clap(
            required_unless_present_any = &["region", "index"],
            conflicts_with_all = &["region", "index"],
            short = 'k',
            help = RAW_KEY_HINT,
        )]
        key: Option<String>,
        #[clap(short = 'b')]
        binary: bool,
    },
    /// print region info
    Region {
        #[clap(
            short = 'r',
            aliases = &["region"],
            conflicts_with = "all-regions",
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ','
        )]
        /// Print info for these regions
        regions: Option<Vec<u64>>,

        // `regions` must be None when `all_regions` is present,
        // so we left `all_regions` unused.
        #[allow(dead_code)]
        #[clap(long, conflicts_with = "regions")]
        /// Print info for all regions
        all_regions: bool,

        #[clap(long, default_value = "")]
        /// hex start key
        start: String,

        #[clap(long, default_value = "")]
        /// hex end key
        end: String,

        #[clap(long, default_value = "16")]
        /// Limit the number of keys to scan
        limit: usize,

        #[clap(long)]
        /// Skip tombstone regions
        skip_tombstone: bool,
    },
}

#[derive(Subcommand)]
pub enum FailCmd {
    /// Inject failures
    Inject {
        /// Inject fail point and actions pairs.
        /// E.g. tikv-ctl fail inject a=off b=panic
        args: Vec<String>,

        #[clap(short = 'f')]
        /// Read a file of fail points and actions to inject
        file: Option<String>,
    },
    /// Recover failures
    Recover {
        /// Recover fail points. Eg. tikv-ctl fail recover a b
        args: Vec<String>,

        #[clap(short = 'f')]
        /// Recover from a file of fail points
        file: Option<String>,
    },
    /// List all fail points
    List {},
}

#[derive(Subcommand)]
pub enum EncryptionMetaCmd {
    /// Dump data keys
    DumpKey {
        #[clap(long, use_value_delimiter = true)]
        /// List of data key ids. Dump all keys if not provided.
        ids: Option<Vec<u64>>,
    },
    /// Dump file encryption info
    DumpFile {
        #[clap(long)]
        /// Path to the file. Dump for all files if not provided.
        path: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum UnsafeRecoverCmd {
    /// Remove the failed machines from the peer list for the regions
    RemoveFailStores {
        #[clap(
            short = 's',
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ','
        )]
        /// Stores to be removed
        stores: Vec<u64>,

        #[clap(
            required_unless_present = "all-regions",
            conflicts_with = "all-regions",
            short = 'r',
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ','
        )]
        /// Only for these regions
        regions: Option<Vec<u64>>,

        #[clap(long)]
        /// Promote learner to voter
        promote_learner: bool,

        // `regions` must be None when `all_regions` is present,
        // so we left `all_regions` unused.
        #[allow(dead_code)]
        #[clap(required_unless_present = "regions", conflicts_with = "regions", long)]
        /// Do the command for all regions
        all_regions: bool,
    },
    /// Remove unapplied raftlogs on the regions
    DropUnappliedRaftlog {
        #[clap(
            required_unless_present = "all-regions",
            conflicts_with = "all-regions",
            short = 'r',
            use_value_delimiter = true,
            require_value_delimiter = true,
            value_delimiter = ','
        )]
        /// Only for these regions
        regions: Option<Vec<u64>>,

        // `regions` must be None when `all_regions` is present,
        // so we left `all_regions` unused.
        #[allow(dead_code)]
        #[clap(required_unless_present = "regions", conflicts_with = "regions", long)]
        /// Do the command for all regions
        all_regions: bool,
    },
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cmd, Opt};

    #[test]
    fn compact_log_backup_gcp_v2_enable_default_true() {
        let opt = Opt::try_parse_from([
            "tikv-ctl",
            "compact-log-backup",
            "--from",
            "1",
            "--until",
            "2",
            "--storage-base64",
            "AA==",
        ])
        .unwrap();

        match opt.cmd.unwrap() {
            Cmd::CompactLogBackup { gcp_v2_enable, .. } => assert!(gcp_v2_enable),
            cmd => panic!("unexpected command: {:?}", std::mem::discriminant(&cmd)),
        }
    }

    #[test]
    fn compact_log_backup_gcp_v2_enable_false() {
        let opt = Opt::try_parse_from([
            "tikv-ctl",
            "compact-log-backup",
            "--from",
            "1",
            "--until",
            "2",
            "--storage-base64",
            "AA==",
            "--gcp-v2-enable",
            "false",
        ])
        .unwrap();

        match opt.cmd.unwrap() {
            Cmd::CompactLogBackup { gcp_v2_enable, .. } => assert!(!gcp_v2_enable),
            cmd => panic!("unexpected command: {:?}", std::mem::discriminant(&cmd)),
        }
    }
<<<<<<< HEAD
=======

    #[test]
    fn compact_log_backup_cal_shift_ts_flag() {
        let opt = Opt::try_parse_from([
            "tikv-ctl",
            "compact-log-backup",
            "--from",
            "1",
            "--until",
            "2",
            "--storage-base64",
            "AA==",
            "--cal-shift-ts",
        ])
        .unwrap();

        match opt.cmd.unwrap() {
            Cmd::CompactLogBackup { cal_shift_ts, .. } => assert!(cal_shift_ts),
            cmd => panic!("unexpected command: {:?}", std::mem::discriminant(&cmd)),
        }
    }

    #[test]
    fn compact_log_backup_allows_omitting_until() {
        let opt = Opt::try_parse_from([
            "tikv-ctl",
            "compact-log-backup",
            "--from",
            "1",
            "--storage-base64",
            "AA==",
        ])
        .unwrap();

        match opt.cmd.unwrap() {
            Cmd::CompactLogBackup { until_ts, .. } => {
                assert_eq!(until_ts, None);
            }
            cmd => panic!("unexpected command: {:?}", std::mem::discriminant(&cmd)),
        }
    }
>>>>>>> eb51ff3230 (deny: replace `structopt` with `clap-v3` derives to resolve RUSTSEC-2022-0104 (#19744))
}
