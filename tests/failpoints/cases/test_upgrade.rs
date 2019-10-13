// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use std::fs;

use rocksdb::{CFHandle, DBIterator, Writable, WriteBatch as RawWriteBatch, DB, DBOptions, load_latest_options, Env, ColumnFamilyOptions, CColumnFamilyDescriptor};
use protobuf::Message;
use kvproto::metapb::Region;
use kvproto::raft_serverpb::*;
use raft::eraftpb::Entry;
use tempfile::Builder;

use test_raftstore::*;
use tikv::config::DbConfig;
use tikv::raftstore::store::{
    keys, Engines, Mutable, Peekable, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER,
};
use tikv::storage::kv::{DBOptions, Writable, DB};
use tikv::storage::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use tikv_util::rocksdb_util;
use tikv_util::config::MB;
use pd_client::PdClient;
use engine_rocks::Rocks;
use engine_traits::{Error, IterOptions, Result, CF_RAFT, WriteOptions, CF_DEFAULT, CF_LOCK, MAX_DELETE_BATCH_SIZE, Peekable, Mutable, Iterable, KvEngine};

const CLUSTER_ID: u64 = 1_000_000_000;
const STOER_ID: u64 = 1;
const REGION_ID: u64 = 3;
const PEER_ID: u64 = 4;

const TERM: u64 = 6;
const APPLIED_INDEX: u64 = 9;
const SNAPSHOT_INDEX: u64 = APPLIED_INDEX;
const COMMITTED_INDEX: u64 = 10;
const LAST_INDEX: u64 = 11;
const STALE_LAST_INDEX: u64 = APPLIED_INDEX - 1;

fn write_store_ident(db: &DB, cf: &str, key: &[u8]) -> StoreIdent {
    let mut store_ident = StoreIdent::default();
    store_ident.set_cluster_id(CLUSTER_ID);
    store_ident.set_store_id(STOER_ID);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &store_ident).unwrap();

    store_ident
}

fn write_region(db: &DB, cf: &str, key: &[u8]) -> Region {
    let mut region = Region::default();
    region.set_id(REGION_ID);
    region.set_peers(vec![new_peer(STOER_ID, PEER_ID)].into());
    region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
    region.mut_region_epoch().set_version(INIT_EPOCH_VER);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &region).unwrap();

    region
}

fn write_apply_state(db: &DB, cf: &str, key: &[u8]) -> RaftApplyState {
    let mut apply_state = RaftApplyState::default();
    apply_state.set_applied_index(APPLIED_INDEX);
    let mut truncated_state = RaftTruncatedState::default();
    truncated_state.set_index(SNAPSHOT_INDEX);
    truncated_state.set_term(TERM);
    apply_state.set_truncated_state(truncated_state);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &apply_state).unwrap();

    apply_state
}

fn write_snap_raft_state(db: &DB, cf: &str, key: &[u8]) -> RaftLocalState {
    let mut snap_raft_state = RaftLocalState::default();
    snap_raft_state.set_last_index(SNAPSHOT_INDEX);
    snap_raft_state.mut_hard_state().set_term(TERM);
    snap_raft_state.mut_hard_state().set_vote(PEER_ID);
    snap_raft_state.mut_hard_state().set_commit(COMMITTED_INDEX);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &snap_raft_state).unwrap();

    snap_raft_state
}

fn write_region_state(db: &DB, cf: &str, key: &[u8], region: Region) -> RegionLocalState {
    let mut region_state = RegionLocalState::default();
    region_state.set_region(region);
    region_state.set_state(PeerState::Normal);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &region_state).unwrap();

    region_state
}

fn write_stale_raft_state(
    db: &DB,
    cf: &str,
    key: &[u8],
    mut raft_state: RaftLocalState,
) -> RaftLocalState {
    raft_state.set_last_index(STALE_LAST_INDEX);
    raft_state.mut_hard_state().set_commit(STALE_LAST_INDEX);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &raft_state).unwrap();

    raft_state
}

fn write_log_entry(db: &DB, cf: &str, key: &[u8], idx: u64) -> Entry {
    let mut entry = Entry::default();
    entry.set_term(TERM);
    entry.set_index(idx);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &entry).unwrap();

    entry
}

struct CFOptions<'a> {
    cf: &'a str,
    options: ColumnFamilyOptions,
}

impl<'a> CFOptions<'a> {
    pub fn new(cf: &'a str, options: ColumnFamilyOptions) -> Self {
        Self { cf, options }
    }
}

fn db_exists(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }

    // If path is not an empty directory, we say db exists. If path is not an empty directory
    // but db has not been created, `DB::list_column_families` fails and we can clean up
    // the directory by this indication.
    fs::read_dir(&path).unwrap().next().is_some()
}


/// Upgrade from v2.x to v3.x
///
/// For backward compatibility, it needs to check whether there are any
/// meta data in the raft cf of the kv engine, if there are, it moves them
/// into raft engine.
fn maybe_upgrade_from_2_to_3(
    raft_engine: Arc<Rocks>,
    kv_path: &str,
    kv_db_opts: DBOptions,
    kv_cfs_opts: Vec<CFOptions>,
) -> Result<()> {
    if !db_exists(kv_path) {
        debug!("no need upgrade to v3.x");
        return Ok(());
    }

    if DB::list_column_families(&kv_db_opts, kv_path)
        .unwrap()
        .into_iter()
        .find(|cf| *cf == CF_RAFT)
        .is_none()
    {
        // We have upgraded from v2.x to v3.x.
        return Ok(());
    }

    info!("start upgrading from v2.x to v3.x");
    let t = Instant::now();

    // Create v2.0.x kv engine.
    let mut kv_engine = new_engine_opt(kv_path, kv_db_opts, kv_cfs_opts)?;

    // Move meta data from kv engine to raft engine.
    let upgrade_raft_wb = raft_engine.write_batch();
    // Cleanup meta data in kv engine.
    let cleanup_kv_wb = kv_engine.write_batch();

    // For meta data in the default CF.
    //
    //  1. store_ident_key: 0x01 0x01
    //  2. prepare_bootstrap_key: 0x01 0x02
    if let Some(m) =
        kv_engine.get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)?
    {
        info!("upgrading STORE_IDENT_KEY";
            "store_id" => m.get_store_id(),
            "cluster_id" => m.get_cluster_id(),
        );
        box_try!(upgrade_raft_wb.put_msg(keys::STORE_IDENT_KEY, &m));
        box_try!(cleanup_kv_wb.delete(keys::STORE_IDENT_KEY));
    }
    if let Some(m) = kv_engine.get_msg::<kvproto::metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)? {
        info!("upgrading PREPARE_BOOTSTRAP_KEY"; "region" => ?m);
        box_try!(upgrade_raft_wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, &m));
        box_try!(cleanup_kv_wb.delete(keys::PREPARE_BOOTSTRAP_KEY));
    }

    // For meta data in the raft CF.
    //
    //  1. apply_state_key:         0x01 0x02 region_id 0x03
    //  2. snapshot_raft_state_key: 0x01 0x02 region_id 0x04
    //  3. region_state_key:        0x01 0x03 region_id 0x01
    let start_key = keys::LOCAL_MIN_KEY;
    let end_key = keys::LOCAL_MAX_KEY;
    kv_engine.scan_cf(CF_RAFT, start_key, end_key, false, |key, value| {
        if let Ok((region_id, suffix)) = keys::decode_region_raft_key(key) {
            if suffix == keys::APPLY_STATE_SUFFIX {
                // apply_state_key
                box_try!(upgrade_raft_wb.put(key, value));
                info!("upgrading apply state"; "region_id" => region_id);
                return Ok(true);
            } else if suffix == keys::SNAPSHOT_RAFT_STATE_SUFFIX {
                // snapshot_raft_state_key
                //
                // In v2.x, we keep an raft local state in kv engine too,
                // in case of restart happen when we just write region state
                // to Applying, but not write raft_local_state to
                // raft engine in time.
                let raft_state_key = keys::raft_state_key(region_id);
                let raft_state = raft_engine
                    .get_msg(&raft_state_key)?
                    .unwrap_or_else(RaftLocalState::default);
                let mut snapshot_raft_state = RaftLocalState::default();
                box_try!(snapshot_raft_state.merge_from_bytes(value));
                // if we recv append log when applying snapshot, last_index in
                // raft_local_state will larger than snapshot_index. since
                // raft_local_state is written to raft engine, and raft
                // write_batch is written after kv write_batch, raft_local_state
                // may wrong if restart happen between the two write. so we copy
                // raft_local_state to kv engine (snapshot_raft_state), and set
                // snapshot_raft_state.last_index = snapshot_index.
                // After restart, we need check last_index.
                if snapshot_raft_state.get_last_index() > raft_state.get_last_index() {
                    box_try!(upgrade_raft_wb.put(&raft_state_key, value));
                    info!(
                        "upgrading snapshot raft state";
                        "region_id" => region_id,
                        "snapshot_raft_state" => ?snapshot_raft_state,
                        "raft_state" => ?raft_state
                    );
                }
                return Ok(true);
            }
        } else if let Ok((region_id, suffix)) = keys::decode_region_meta_key(key) {
            if suffix == keys::REGION_STATE_SUFFIX {
                box_try!(upgrade_raft_wb.put(key, value));
                info!("upgrading region state"; "region_id" => region_id);
                return Ok(true);
            }
        }
        Err(box_err!(
            "unexpect key {:?} when upgrading from v2.x to v3.x",
            key
        ))
    })?;

    let mut sync_opt = WriteOptions::new();
    sync_opt.set_sync(true);

    fail_point!("upgrade_2_3_before_update_raft", |_| {
        Err(box_err!("injected error: upgrade_2_3_before_update_raft"))
    });
    raft_engine.write_opt(&sync_opt, &upgrade_raft_wb).unwrap();

    fail_point!("upgrade_2_3_before_update_kv", |_| {
        Err(box_err!("injected error: upgrade_2_3_before_update_kv"))
    });
    kv_engine.write_opt(&sync_opt, &cleanup_kv_wb).unwrap();

    // Drop the raft cf.
    fail_point!("upgrade_2_3_before_drop_raft_cf", |_| {
        Err(box_err!("injected error: upgrade_2_3_before_drop_raft_cf"))
    });
    kv_engine.drop_cf(CF_RAFT).unwrap();

    info!(
        "finish upgrading from v2.x to v3.x";
        "takes" => ?t.elapsed(),
    );
    Ok(())
}

fn new_engine_opt(
    path: &str,
    mut db_opt: DBOptions,
    cfs_opts: Vec<CFOptions>,
) -> Result<Rocks> {
    // Creates a new db if it doesn't exist.
    if !db_exists(path) {
        db_opt.create_if_missing(true);

        let mut cfs_v = vec![];
        let mut cf_opts_v = vec![];
        if let Some(x) = cfs_opts.iter().find(|x| x.cf == CF_DEFAULT) {
            cfs_v.push(x.cf);
            cf_opts_v.push(x.options.clone());
        }
        let mut db = DB::open_cf(db_opt, path, cfs_v.into_iter().zip(cf_opts_v).collect())?;
        for x in cfs_opts {
            if x.cf == CF_DEFAULT {
                continue;
            }
            db.create_cf((x.cf, x.options))?;
        }

        return Ok(Rocks::from_db(Arc::new(db)));
    }

    db_opt.create_if_missing(false);

    // Lists all column families in current db.
    let cfs_list = DB::list_column_families(&db_opt, path)?;
    let existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<&str> = cfs_opts.iter().map(|x| x.cf).collect();

    let cf_descs = if !existed.is_empty() {
        let env = match db_opt.env() {
            Some(env) => env,
            None => Arc::new(Env::default()),
        };
        // panic if OPTIONS not found for existing instance?
        let (_, tmp) = load_latest_options(path, &env, true)
            .unwrap_or_else(|e| panic!("failed to load_latest_options {:?}", e))
            .unwrap_or_else(|| panic!("couldn't find the OPTIONS file"));
        tmp
    } else {
        vec![]
    };

    // If all column families exist, just open db.
    if existed == needed {
        let mut cfs_v = vec![];
        let mut cfs_opts_v = vec![];
        for mut x in cfs_opts {
            adjust_dynamic_level_bytes(&cf_descs, &mut x);
            cfs_v.push(x.cf);
            cfs_opts_v.push(x.options);
        }

        let db = DB::open_cf(db_opt, path, cfs_v.into_iter().zip(cfs_opts_v).collect())?;
        return Ok(Rocks::from_db(Arc::new(db)));
    }

    // Opens db.
    let mut cfs_v: Vec<&str> = Vec::new();
    let mut cfs_opts_v: Vec<ColumnFamilyOptions> = Vec::new();
    for cf in &existed {
        cfs_v.push(cf);
        match cfs_opts.iter().find(|x| x.cf == *cf) {
            Some(x) => {
                let mut tmp = CFOptions::new(x.cf, x.options.clone());
                adjust_dynamic_level_bytes(&cf_descs, &mut tmp);
                cfs_opts_v.push(tmp.options);
            }
            None => {
                cfs_opts_v.push(ColumnFamilyOptions::new());
            }
        }
    }
    let cfds = cfs_v.into_iter().zip(cfs_opts_v).collect();
    let mut db = DB::open_cf(db_opt, path, cfds).unwrap();

    // Drops discarded column families.
    //    for cf in existed.iter().filter(|x| needed.iter().find(|y| y == x).is_none()) {
    for cf in cfs_diff(&existed, &needed) {
        // Never drop default column families.
        if cf != CF_DEFAULT {
            db.drop_cf(cf)?;
        }
    }

    // Creates needed column families if they don't exist.
    for cf in cfs_diff(&needed, &existed) {
        db.create_cf((
            cf,
            cfs_opts
                .iter()
                .find(|x| x.cf == cf)
                .unwrap()
                .options
                .clone(),
        ))?;
    }
    Ok(Rocks::from_db(Arc::new(db)))
}

/// Returns a Vec of cf which is in `a' but not in `b'.
fn cfs_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| b.iter().find(|y| y == x).is_none())
        .cloned()
        .collect()
}

/// Turns "dynamic level size" off for the existing column family which was off before.
/// Column families are small, HashMap isn't necessary.
fn adjust_dynamic_level_bytes(
    cf_descs: &[CColumnFamilyDescriptor],
    cf_options: &mut CFOptions,
) {
    if let Some(ref cf_desc) = cf_descs
        .iter()
        .find(|cf_desc| cf_desc.name() == cf_options.cf)
    {
        let existed_dynamic_level_bytes =
            cf_desc.options().get_level_compaction_dynamic_level_bytes();
        if existed_dynamic_level_bytes
            != cf_options
                .options
                .get_level_compaction_dynamic_level_bytes()
        {
            warn!(
                "change dynamic_level_bytes for existing column family is danger";
                "old_value" => existed_dynamic_level_bytes,
                "new_value" => cf_options.options.get_level_compaction_dynamic_level_bytes(),
            );
        }
        cf_options
            .options
            .set_level_compaction_dynamic_level_bytes(existed_dynamic_level_bytes);
    }
}

fn test_upgrade_from_v2_to_v3(fp: &str) {
    let tmp_dir = Builder::new().prefix("test_upgrade").tempdir().unwrap();
    let tmp_path_kv = tmp_dir.path().join("kv");

    // Create a raft engine.
    let tmp_path_raft = tmp_dir.path().join(Path::new("raft"));
    let raft_engine =
        Arc::new(rocksdb_util::new_engine(tmp_path_raft.to_str().unwrap(), None, &[], None).unwrap());
    let mut cache_opts = LRUCacheOptions::new();
    cache_opts.set_capacity(8 * MB);

    // No need to upgrade an empty node.
    let kv_cfg = &DbConfig::default();
    maybe_upgrade_from_2_to_3(
        Arc::clone(raft_engine),
        tmp_path_kv.to_str().unwrap(),
        DBOptions::new(),
        kv_cfg.build_cf_opts_v2(Some(Cache::new_lru_cache(cache_opts))),
    )
    .unwrap();
    // Check whether there is a kv engine.
    assert!(
        DB::list_column_families(&DBOptions::new(), tmp_path_kv.to_str().unwrap())
            .unwrap_err()
            .contains("No such file or directory")
    );

    let all_cfs_v2 = &[CF_LOCK, CF_RAFT, CF_WRITE, CF_DEFAULT];
    // Create a v2 kv engine.
    let kv_engine =
        rocksdb_util::new_engine(tmp_path_kv.to_str().unwrap(), None, all_cfs_v2, None).unwrap();

    // For meta data in the default CF.
    //
    //  1. store_ident_key: 0x01 0x01
    //  2. prepare_bootstrap_key: 0x01 0x02
    let store_ident = write_store_ident(&kv_engine, CF_DEFAULT, keys::STORE_IDENT_KEY);
    let region = write_region(&kv_engine, CF_DEFAULT, keys::PREPARE_BOOTSTRAP_KEY);

    // For meta data in the raft CF.
    //
    //  1. apply_state_key:         0x01 0x02 region_id 0x03
    //  2. snapshot_raft_state_key: 0x01 0x02 region_id 0x04
    //  3. region_state_key:        0x01 0x03 region_id 0x01
    let apply_state = write_apply_state(&kv_engine, CF_RAFT, &keys::apply_state_key(REGION_ID));
    let mut snap_raft_state = write_snap_raft_state(
        &kv_engine,
        CF_RAFT,
        &keys::snapshot_raft_state_key(REGION_ID),
    );
    let region_state = write_region_state(
        &kv_engine,
        CF_RAFT,
        &keys::region_state_key(REGION_ID),
        region.clone(),
    );
    kv_engine.sync_wal().unwrap();

    // Data in the raft engine.
    //
    //  1. raft logs
    //  2. raft state
    // A stale raft local state in the raft engine to whether the upgrade procedure can correctly
    // merge RaftLocalState.
    write_stale_raft_state(
        &raft_engine,
        CF_DEFAULT,
        &keys::raft_state_key(REGION_ID),
        snap_raft_state.clone(),
    );
    for idx in APPLIED_INDEX..=LAST_INDEX {
        write_log_entry(
            &raft_engine,
            CF_DEFAULT,
            &keys::raft_log_key(REGION_ID, idx),
            idx,
        );
    }
    raft_engine.sync_wal().unwrap();

    drop(kv_engine);

    // Return early.
    fail::cfg(fp, "return").unwrap();
    let kv_cfg = &DbConfig::default();
    let res = maybe_upgrade_from_2_to_3(
        Arc::clone(raft_engine),
        tmp_path_kv.to_str().unwrap(),
        DBOptions::new(),
        kv_cfg.build_cf_opts_v2(Some(Cache::new_lru_cache(cache_opts))),
    );
    // `unwrap` or `unwrap_err` depends on whether we enable a fail point.
    if fp.is_empty() {
        res.unwrap();
    } else {
        res.unwrap_err();
    }
    fail::remove(fp);
    // Retry upgrade.
    let kv_cfg = &DbConfig::default();
    maybe_upgrade_from_2_to_3(
        Arc::clone(raft_engine),
        tmp_path_kv.to_str().unwrap(),
        DBOptions::new(),
        kv_cfg.build_cf_opts_v2(Some(Cache::new_lru_cache(cache_opts))),
    ).unwrap();

    // Check kv engine, no RAFT cf.
    let cfs = DB::list_column_families(&DBOptions::new(), tmp_path_kv.to_str().unwrap()).unwrap();
    assert! {
        cfs
        .iter()
        .find(|cf| *cf == CF_RAFT)
        .is_none(), "{:?}", cfs
    }

    // Check raft engine.
    assert_eq!(
        store_ident,
        raft_engine.get_msg(keys::STORE_IDENT_KEY).unwrap().unwrap(),
    );
    assert_eq!(
        region,
        raft_engine
            .get_msg(keys::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        snap_raft_state,
        raft_engine
            .get_msg(&keys::raft_state_key(REGION_ID))
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        apply_state,
        raft_engine
            .get_msg(&keys::apply_state_key(REGION_ID))
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        region_state,
        raft_engine
            .get_msg(&keys::region_state_key(REGION_ID))
            .unwrap()
            .unwrap(),
    );

    // To start a cluster, we need to delete the PREPARE_BOOTSTRAP_KEY and
    // update the last index in RaftLocalState.
    raft_engine.delete(keys::PREPARE_BOOTSTRAP_KEY).unwrap();
    snap_raft_state.set_last_index(LAST_INDEX);
    raft_engine
        .put_msg(&keys::raft_state_key(REGION_ID), &snap_raft_state)
        .unwrap();

    let pd_client = Arc::new(TestPdClient::new(CLUSTER_ID, false));
    pd_client
        .bootstrap_cluster(new_store(STOER_ID, "".to_owned()), region)
        .unwrap();
    for _ in 0..PEER_ID {
        // Advance the id allocator.
        pd_client.alloc_id().unwrap();
    }

    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&pd_client))));
    let mut cluster = Cluster::new(CLUSTER_ID, 5, sim, pd_client);
    cluster.create_engines();

    // Update upgraded engines.
    // Create a kv engine.
    let kv_engine =
        rocksdb_util::new_engine(tmp_path_kv.to_str().unwrap(), None, ALL_CFS, None).unwrap();
    let shared_block_cache = false;
    let engines = Engines::new(Arc::new(kv_engine), Arc::new(raft_engine), shared_block_cache);
    cluster.dbs[0] = engines.clone();
    cluster.paths[0] = tmp_dir;
    cluster.engines.insert(STOER_ID, engines);
    cluster.start().unwrap();

    let k = b"k1";
    let v = b"v1";
    cluster.must_put(k, v);
    must_get_equal(&cluster.get_engine(STOER_ID), k, v);
    for id in cluster.engines.keys() {
        if *id == STOER_ID {
            continue;
        }
        must_get_equal(&cluster.get_engine(*id), k, v);
    }
}

#[test]
fn test_upgrade_2_3_return_before_update_raft() {
    let _guard = crate::setup();
    test_upgrade_from_v2_to_v3("upgrade_2_3_before_update_raft");
}

#[test]
fn test_upgrade_2_3_return_before_update_kv() {
    let _guard = crate::setup();
    test_upgrade_from_v2_to_v3("upgrade_2_3_before_update_kv");
}

#[test]
fn test_upgrade_2_3_return_before_drop_raft_cf() {
    let _guard = crate::setup();
    test_upgrade_from_v2_to_v3("upgrade_2_3_before_drop_raft_cf");
}

#[test]
fn test_double_upgrade_2_3() {
    let _guard = crate::setup();
    // An empty fail point injects nothing.
    test_upgrade_from_v2_to_v3("");
}
