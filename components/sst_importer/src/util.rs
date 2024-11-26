// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ops::Bound, path::Path};

use encryption::DataKeyManager;
use external_storage::ExternalStorage;
use file_system::File;
use kvproto::import_sstpb::RewriteRule;

use crate::{Error, Result};

/// Prepares the SST file for ingestion.
/// The purpose is to make the ingestion retryable when using the `move_files`
/// option. Things we need to consider here:
/// 1. We need to access the original file on retry, so we should make a clone
///    before ingestion.
/// 2. `RocksDB` will modified the global seqno of the ingested file, so we need
///    to modified the global seqno back to 0 so that we can pass the checksum
///    validation.
/// 3. If the file has been ingested to `RocksDB`, we should not modified the
///    global seqno directly, because that may corrupt RocksDB's data.
pub fn prepare_sst_for_ingestion<P: AsRef<Path>, Q: AsRef<Path>>(
    path: P,
    clone: Q,
    encryption_key_manager: Option<&DataKeyManager>,
) -> Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;

    let path = path.as_ref().to_str().unwrap();
    let clone = clone.as_ref().to_str().unwrap();

    if Path::new(clone).exists() {
        file_system::remove_file(clone).map_err(|e| format!("remove {}: {:?}", clone, e))?;
    }
    // always try to remove the file from key manager because the clean up in
    // rocksdb is not atomic, thus the file may be deleted but key in key
    // manager is not.
    if let Some(key_manager) = encryption_key_manager {
        key_manager.delete_file(clone, None)?;
    }

    #[cfg(unix)]
    let nlink = file_system::metadata(path)
        .map_err(|e| format!("read metadata from {}: {:?}", path, e))?
        .nlink();
    #[cfg(not(unix))]
    let nlink = 0;

    if nlink == 1 {
        // RocksDB must not have this file, we can make a hard link.
        file_system::hard_link(path, clone)
            .map_err(|e| format!("link from {} to {}: {:?}", path, clone, e))?;
        File::open(clone)?.sync_all()?;
    } else {
        // RocksDB may have this file, we should make a copy.
        file_system::copy_and_sync(path, clone)
            .map_err(|e| format!("copy from {} to {}: {:?}", path, clone, e))?;
    }
    // sync clone dir
    File::open(Path::new(clone).parent().unwrap())?.sync_all()?;
    if let Some(key_manager) = encryption_key_manager {
        key_manager.link_file(path, clone)?;
    }
    Ok(())
}

/// Just like prepare_sst_for_ingestion, but
/// * always use copy instead of hard link;
/// * add write permission on the copied file if necessary.
pub fn copy_sst_for_ingestion<P: AsRef<Path>, Q: AsRef<Path>>(
    path: P,
    clone: Q,
    encryption_key_manager: Option<&DataKeyManager>,
) -> Result<()> {
    let path = path.as_ref();
    let clone = clone.as_ref();
    if clone.exists() {
        file_system::remove_file(clone)
            .map_err(|e| format!("remove {}: {:?}", clone.display(), e))?;
    }
    // always try to remove the file from key manager because the clean up in
    // rocksdb is not atomic, thus the file may be deleted but key in key
    // manager is not.
    if let Some(key_manager) = encryption_key_manager {
        key_manager.delete_file(clone.to_str().unwrap(), None)?;
    }

    file_system::copy_and_sync(path, clone).map_err(|e| {
        format!(
            "copy from {} to {}: {:?}",
            path.display(),
            clone.display(),
            e
        )
    })?;

    let mut pmts = file_system::metadata(clone)?.permissions();
    if pmts.readonly() {
        #[allow(clippy::permissions_set_readonly_false)]
        pmts.set_readonly(false);
        file_system::set_permissions(clone, pmts)?;
    }

    // sync clone dir
    File::open(clone.parent().unwrap())?.sync_all()?;
    if let Some(key_manager) = encryption_key_manager {
        key_manager.link_file(path.to_str().unwrap(), clone.to_str().unwrap())?;
    }

    Ok(())
}

pub fn url_for<E: ExternalStorage>(storage: &E) -> String {
    storage
        .url()
        .map(|url| url.to_string())
        .unwrap_or_else(|err| format!("ErrUrl({})", err))
}

fn key_to_bound(key: &[u8]) -> Bound<&[u8]> {
    if key.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Included(key)
    }
}

fn key_to_exclusive_bound(key: &[u8]) -> Bound<&[u8]> {
    if key.is_empty() {
        Bound::Unbounded
    } else {
        Bound::Excluded(key)
    }
}

pub struct PrefixReplacer<'a> {
    rewrite_rules: &'a [RewriteRule],
    next_index: usize,

    user_key: Vec<u8>,
    new_timestamp: u64,
    new_prefix_len: usize,
    old_prefix_len: usize,
}

impl<'a> PrefixReplacer<'a> {
    pub fn new(rewrite_rules: &'a [RewriteRule]) -> Result<Self> {
        let mut prefix_replacer = PrefixReplacer {
            rewrite_rules,
            next_index: 0,
            user_key: vec![],
            new_timestamp: 0,
            new_prefix_len: 0,
            old_prefix_len: 0,
        };
        prefix_replacer.update_rewrite_rule()?;
        Ok(prefix_replacer)
    }

    pub fn rewrite_start(&self, new_range_start: &[u8]) -> Result<Bound<Vec<u8>>> {
        let rewrite_rule =
            self.rewrite_rules
                .first()
                .ok_or(Error::WrongRewriteRules(String::from(
                    "Rewrite rules is empty",
                )))?;
        let range_start_bound = key_to_bound(new_range_start);
        let old_prefix = rewrite_rule.get_old_key_prefix();
        let new_prefix = rewrite_rule.get_new_key_prefix();
        let range_start =
            keys::rewrite::rewrite_prefix_of_start_bound(new_prefix, old_prefix, range_start_bound)
                .map_err(|_| Error::WrongKeyPrefix {
                    what: "SST start range",
                    key: new_range_start.to_vec(),
                    prefix: new_prefix.to_vec(),
                })?;
        Ok(range_start)
    }

    pub fn rewrite_end(
        &self,
        new_range_end: &[u8],
        end_key_exclusive: bool,
    ) -> Result<Bound<Vec<u8>>> {
        let rewrite_rule =
            self.rewrite_rules
                .last()
                .ok_or(Error::WrongRewriteRules(String::from(
                    "Rewrite rules is empty",
                )))?;
        let range_end_bound = if end_key_exclusive {
            key_to_exclusive_bound(new_range_end)
        } else {
            key_to_bound(new_range_end)
        };
        let old_prefix = rewrite_rule.get_old_key_prefix();
        let new_prefix = rewrite_rule.get_new_key_prefix();
        let range_end =
            keys::rewrite::rewrite_prefix_of_end_bound(new_prefix, old_prefix, range_end_bound)
                .map_err(|_| Error::WrongKeyPrefix {
                    what: "SST end range",
                    key: new_range_end.to_vec(),
                    prefix: new_prefix.to_vec(),
                })?;
        Ok(range_end)
    }

    pub fn need_to_replace(&self) -> bool {
        for rewrite_rule in self.rewrite_rules {
            if rewrite_rule.old_key_prefix != rewrite_rule.new_key_prefix
                || rewrite_rule.new_timestamp != 0
            {
                return true;
            }
        }
        false
    }

    pub fn try_update_rewrite_rule(&mut self, old_key: &[u8]) -> Result<(&[u8], u64)> {
        let last_index = self.next_index;
        while self.next_index < self.rewrite_rules.len() {
            let rewrite_rule = &self.rewrite_rules[self.next_index];
            if old_key.starts_with(rewrite_rule.get_old_key_prefix()) {
                if last_index != self.next_index {
                    self.update_rewrite_rule()?;
                }
                self.update_user_key(old_key);
                return Ok((&self.user_key, self.new_timestamp));
            }
            self.next_index += 1;
        }
        Err(Error::WrongKeyPrefix {
            what: "Key in SST does not match any rewrite rule",
            key: old_key.to_vec(),
            prefix: vec![],
        })
    }

    fn update_rewrite_rule(&mut self) -> Result<()> {
        let rewrite_rule = self.rewrite_rules
            .get(self.next_index)
            .ok_or(Error::WrongRewriteRules(
                format!(
                    "Rewrite rule index is out of bound. The size of rewrite rules is {}, but the index is {}", 
                    self.rewrite_rules.len(), self.next_index,
                ),
            ))?;
        let new_prefix = rewrite_rule.get_new_key_prefix();
        self.user_key = new_prefix.to_vec();
        self.new_timestamp = rewrite_rule.new_timestamp;
        self.new_prefix_len = new_prefix.len();
        self.old_prefix_len = rewrite_rule.get_old_key_prefix().len();
        Ok(())
    }

    fn update_user_key(&mut self, old_key: &[u8]) {
        self.user_key.truncate(self.new_prefix_len);
        self.user_key
            .extend_from_slice(&old_key[self.old_prefix_len..]);
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use encryption::DataKeyManager;
    use engine_rocks::{
        util::new_engine_opt, RocksCfOptions, RocksDbOptions, RocksEngine, RocksSstWriterBuilder,
        RocksTitanDbOptions,
    };
    use engine_traits::{
        CfName, CfOptions, DbOptions, ImportExt, Peekable, SstWriter, SstWriterBuilder,
        TitanCfOptions, CF_DEFAULT,
    };
    use kvproto::import_sstpb::RewriteRule;
    use tempfile::Builder;
    use test_util::encryption::new_test_key_manager;

    use super::{copy_sst_for_ingestion, prepare_sst_for_ingestion, PrefixReplacer};

    #[cfg(unix)]
    fn check_hard_link<P: AsRef<Path>>(path: P, nlink: u64) {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(file_system::metadata(path).unwrap().nlink(), nlink);
    }

    #[cfg(not(unix))]
    fn check_hard_link<P: AsRef<Path>>(_: P, _: u64) {
        // Just do nothing
    }

    fn check_db_with_kvs(db: &RocksEngine, cf: &str, kvs: &[(&str, &str)]) {
        for &(k, v) in kvs {
            assert_eq!(
                db.get_value_cf(cf, k.as_bytes()).unwrap().unwrap(),
                v.as_bytes()
            );
        }
    }

    fn gen_sst_with_kvs(db: &RocksEngine, cf: CfName, path: &str, kvs: &[(&str, &str)]) {
        let mut writer = RocksSstWriterBuilder::new()
            .set_db(db)
            .set_cf(cf)
            .build(path)
            .unwrap();
        for &(k, v) in kvs {
            writer.put(k.as_bytes(), v.as_bytes()).unwrap();
        }
        writer.finish().unwrap();
    }

    fn check_prepare_sst_for_ingestion(
        db_opts: Option<RocksDbOptions>,
        cf_opts: Option<Vec<(&str, RocksCfOptions)>>,
        key_manager: Option<&DataKeyManager>,
        was_encrypted: bool,
    ) {
        let path = Builder::new()
            .prefix("_util_rocksdb_test_prepare_sst_for_ingestion")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let sst_dir = Builder::new()
            .prefix("_util_rocksdb_test_prepare_sst_for_ingestion_sst")
            .tempdir()
            .unwrap();
        let sst_path = sst_dir.path().join("abc.sst");
        let sst_clone = sst_dir.path().join("abc.sst.clone");

        let kvs = [("k1", "v1"), ("k2", "v2"), ("k3", "v3")];

        let db_opts = db_opts.unwrap_or_default();
        let cf_opts = cf_opts.unwrap_or_else(|| vec![(CF_DEFAULT, RocksCfOptions::default())]);
        let db = new_engine_opt(path_str, db_opts, cf_opts).unwrap();

        gen_sst_with_kvs(&db, CF_DEFAULT, sst_path.to_str().unwrap(), &kvs);

        if was_encrypted {
            // Add the file to key_manager to simulate an encrypted file.
            if let Some(manager) = key_manager {
                manager.new_file(sst_path.to_str().unwrap()).unwrap();
            }
        }

        // The first ingestion will hard link sst_path to sst_clone.
        check_hard_link(&sst_path, 1);
        prepare_sst_for_ingestion(&sst_path, &sst_clone, key_manager).unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 2);
        // If we prepare again, it will use hard link too.
        prepare_sst_for_ingestion(&sst_path, &sst_clone, key_manager).unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 2);
        db.ingest_external_file_cf(CF_DEFAULT, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, CF_DEFAULT, &kvs);
        assert!(!sst_clone.exists());
        // Since we are not using key_manager in db, simulate the db deleting the file
        // from key_manager.
        if let Some(manager) = key_manager {
            manager
                .delete_file(sst_clone.to_str().unwrap(), None)
                .unwrap();
        }

        // The second ingestion will copy sst_path to sst_clone.
        check_hard_link(&sst_path, 2);
        prepare_sst_for_ingestion(&sst_path, &sst_clone, key_manager).unwrap();
        check_hard_link(&sst_path, 2);
        check_hard_link(&sst_clone, 1);
        db.ingest_external_file_cf(CF_DEFAULT, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, CF_DEFAULT, &kvs);
        assert!(!sst_clone.exists());
    }

    #[test]
    fn test_prepare_sst_for_ingestion() {
        check_prepare_sst_for_ingestion(
            None, None, None,  // key_manager
            false, // was encrypted
        );
    }

    #[test]
    fn test_prepare_sst_for_ingestion_titan() {
        let mut db_opts = RocksDbOptions::new();
        let mut titan_opts = RocksTitanDbOptions::new();
        // Force all values write out to blob files.
        titan_opts.set_min_blob_size(0);
        db_opts.set_titandb_options(&titan_opts);
        let mut cf_opts = RocksCfOptions::new();
        cf_opts.set_titan_cf_options(&titan_opts);
        check_prepare_sst_for_ingestion(
            Some(db_opts),
            Some(vec![(CF_DEFAULT, cf_opts)]),
            None,  // key_manager
            false, // was_encrypted
        );
    }

    #[test]
    fn test_prepare_sst_for_ingestion_with_key_manager_plaintext() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let key_manager = new_test_key_manager(&tmp_dir, None, None, None);
        let manager = Arc::new(key_manager.unwrap().unwrap());
        check_prepare_sst_for_ingestion(None, None, Some(&manager), false /* was_encrypted */);
    }

    #[test]
    fn test_prepare_sst_for_ingestion_with_key_manager_encrypted() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let key_manager = new_test_key_manager(&tmp_dir, None, None, None);
        let manager = Arc::new(key_manager.unwrap().unwrap());
        check_prepare_sst_for_ingestion(None, None, Some(&manager), true /* was_encrypted */);
    }

    #[test]
    fn test_copy_sst_for_ingestion() {
        let path = Builder::new()
            .prefix("_util_rocksdb_test_copy_sst_for_ingestion")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let sst_dir = Builder::new()
            .prefix("_util_rocksdb_test_copy_sst_for_ingestion_sst")
            .tempdir()
            .unwrap();
        let sst_path = sst_dir.path().join("abc.sst");
        let sst_clone = sst_dir.path().join("abc.sst.clone");

        let kvs = [("k1", "v1"), ("k2", "v2"), ("k3", "v3")];

        let db_opts = RocksDbOptions::default();
        let cf_opts = vec![(CF_DEFAULT, RocksCfOptions::default())];
        let db = new_engine_opt(path_str, db_opts, cf_opts).unwrap();

        gen_sst_with_kvs(&db, CF_DEFAULT, sst_path.to_str().unwrap(), &kvs);

        copy_sst_for_ingestion(&sst_path, &sst_clone, None).unwrap();
        check_hard_link(&sst_path, 1);
        check_hard_link(&sst_clone, 1);

        copy_sst_for_ingestion(&sst_path, &sst_clone, None).unwrap();
        check_hard_link(&sst_path, 1);
        check_hard_link(&sst_clone, 1);

        db.ingest_external_file_cf(CF_DEFAULT, &[sst_clone.to_str().unwrap()])
            .unwrap();
        check_db_with_kvs(&db, CF_DEFAULT, &kvs);
        assert!(!sst_clone.exists());
    }

    fn new_rewrite_rule(old_prefix: &[u8], new_prefix: &[u8], new_timestamp: u64) -> RewriteRule {
        let mut rewrite_rule = RewriteRule::default();
        rewrite_rule.set_old_key_prefix(old_prefix.to_vec());
        rewrite_rule.set_new_key_prefix(new_prefix.to_vec());
        rewrite_rule.set_new_timestamp(new_timestamp);
        rewrite_rule
    }

    #[test]
    fn test_prefix_replacer() {
        assert!(PrefixReplacer::new(&[]).is_err());
        let rewrite_rules = vec![new_rewrite_rule(b"aa", b"A", 1)];
        let prefix_replacer = PrefixReplacer::new(&rewrite_rules).unwrap();
        assert!(prefix_replacer.need_to_replace());
        let rewrite_rules = vec![new_rewrite_rule(b"aa", b"aa", 1)];
        let prefix_replacer = PrefixReplacer::new(&rewrite_rules).unwrap();
        assert!(prefix_replacer.need_to_replace());
        let rewrite_rules = vec![new_rewrite_rule(b"aa", b"aa", 0)];
        let prefix_replacer = PrefixReplacer::new(&rewrite_rules).unwrap();
        assert!(!prefix_replacer.need_to_replace());
        let rewrite_rules = vec![new_rewrite_rule(b"aa", b"AA", 0)];
        let prefix_replacer = PrefixReplacer::new(&rewrite_rules).unwrap();
        assert!(prefix_replacer.need_to_replace());
        let rewrite_rules = vec![
            new_rewrite_rule(b"aa", b"A", 1),
            new_rewrite_rule(b"bb", b"BB", 2),
            new_rewrite_rule(b"cc", b"CCC", 3),
        ];
        let mut prefix_replacer = PrefixReplacer::new(&rewrite_rules).unwrap();
        assert!(prefix_replacer.need_to_replace());
        prefix_replacer.rewrite_start(b"1").unwrap_err();
        prefix_replacer.rewrite_start(b"aa").unwrap_err();
        let std::ops::Bound::Included(range_start) = prefix_replacer.rewrite_start(b"A").unwrap()
        else {
            panic!("Can't rewrite the range start key");
        };
        assert_eq!(&range_start, b"aa");
        let std::ops::Bound::Included(range_start) = prefix_replacer.rewrite_start(b"Aa").unwrap()
        else {
            panic!("Can't rewrite the range start key");
        };
        assert_eq!(&range_start, b"aaa");
        let std::ops::Bound::Included(range_start) = prefix_replacer.rewrite_start(b"AAa").unwrap()
        else {
            panic!("Can't rewrite the range start key");
        };
        assert_eq!(&range_start, b"aaAa");
        prefix_replacer.rewrite_end(b"1", false).unwrap_err();
        prefix_replacer.rewrite_end(b"aa", false).unwrap_err();
        let std::ops::Bound::Included(range_start) =
            prefix_replacer.rewrite_end(b"CCC", false).unwrap()
        else {
            panic!("Can't rewrite the range start key");
        };
        assert_eq!(&range_start, b"cc");
        let std::ops::Bound::Excluded(range_start) =
            prefix_replacer.rewrite_end(b"CCC", true).unwrap()
        else {
            panic!("Can't rewrite the range start key");
        };
        assert_eq!(&range_start, b"cc");
        let std::ops::Bound::Included(range_start) =
            prefix_replacer.rewrite_end(b"CCCCc", false).unwrap()
        else {
            panic!("Can't rewrite the range start key");
        };
        assert_eq!(&range_start, b"ccCc");
        let std::ops::Bound::Excluded(range_start) =
            prefix_replacer.rewrite_end(b"CCCCc", true).unwrap()
        else {
            panic!("Can't rewrite the range start key");
        };
        assert_eq!(&range_start, b"ccCc");

        let cases = vec![
            (b"aa".to_vec(), b"A".to_vec(), 1),
            (b"aaa".to_vec(), b"Aa".to_vec(), 1),
            (b"aabc".to_vec(), b"Abc".to_vec(), 1),
            (b"bbcd".to_vec(), b"BBcd".to_vec(), 2),
            (b"ccde".to_vec(), b"CCCde".to_vec(), 3),
            (b"dddd".to_vec(), vec![], 0),
        ];
        for (old_key, expect_user_key, expect_new_timestamp) in cases {
            let res = prefix_replacer.try_update_rewrite_rule(&old_key);
            if expect_user_key.is_empty() {
                assert!(res.is_err());
            } else {
                let (user_key, new_timestamp) = res.unwrap();
                assert_eq!(new_timestamp, expect_new_timestamp);
                assert_eq!(user_key, expect_user_key);
            }
        }
    }
}
