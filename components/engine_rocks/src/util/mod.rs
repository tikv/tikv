// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::path::Path;
use std::time::Instant;
use std::fs;
use std::sync::Arc;

use rocksdb::{CFHandle, DBIterator, Writable, WriteBatch as RawWriteBatch, DB, DBOptions, load_latest_options, Env, ColumnFamilyOptions, CColumnFamilyDescriptor};
use protobuf::Message;

use kvproto::raft_serverpb::{RaftLocalState, StoreIdent};
use crate::{Rocks};
use engine_traits::{Error, IterOptions, Result, CF_RAFT, WriteOptions, CF_DEFAULT, CF_LOCK, MAX_DELETE_BATCH_SIZE, Peekable, Mutable, Iterable, KvEngine};
use tikv_util::keybuilder::KeyBuilder;

use crate::options::RocksReadOptions;

pub struct CFOptions<'a> {
    cf: &'a str,
    options: ColumnFamilyOptions,
}

impl<'a> CFOptions<'a> {
    pub fn new(cf: &'a str, options: ColumnFamilyOptions) -> Self {
        Self { cf, options }
    }
}

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle> {
    let handle = db
        .cf_handle(cf)
        .ok_or_else(|| Error::Engine(format!("cf {} not found", cf)))?;
    Ok(handle)
}

pub fn delete_all_in_range_cf(
    db: &DB,
    cf: &str,
    start_key: &[u8],
    end_key: &[u8],
    use_delete_range: bool,
) -> Result<()> {
    let handle = get_cf_handle(db, cf)?;
    let wb = RawWriteBatch::default();
    if use_delete_range && cf != CF_LOCK {
        wb.delete_range_cf(handle, start_key, end_key)?;
    } else {
        let start = KeyBuilder::from_slice(start_key, 0, 0);
        let end = KeyBuilder::from_slice(end_key, 0, 0);
        let mut iter_opt = IterOptions::new(Some(start), Some(end), false);
        if db.is_titan() {
            // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
            // to avoid referring to missing blob files.
            iter_opt.set_key_only(true);
        }
        let handle = get_cf_handle(db, cf)?;
        let opts: RocksReadOptions = iter_opt.into();
        let mut it = DBIterator::new_cf(db, handle, opts.into_raw());
        it.seek(start_key.into());
        while it.valid() {
            wb.delete_cf(handle, it.key())?;
            if wb.data_size() >= MAX_DELETE_BATCH_SIZE {
                // Can't use write_without_wal here.
                // Otherwise it may cause dirty data when applying snapshot.
                db.write(&wb)?;
                wb.clear();
            }

            if !it.next() {
                break;
            }
        }
        it.status()?;
    }

    if wb.count() > 0 {
        db.write(&wb)?;
    }

    Ok(())
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

/// Upgreade from v2.x to v3.x
///
/// For backward compatibility, it needs to check whether there are any
/// meta data in the raft cf of the kv engine, if there are, it moves them
/// into raft engine.
pub fn maybe_upgrade_from_2_to_3(
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
