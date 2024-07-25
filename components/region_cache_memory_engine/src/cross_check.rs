// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Display, sync::Arc, time::Duration};

use engine_rocks::{RocksEngine, RocksEngineIterator, RocksSnapshot};
use engine_traits::{
    iter_option, CacheRange, Iterable, Iterator, KvEngine, Peekable, RangeCacheEngine,
    SnapshotMiscExt, CF_LOCK, CF_WRITE,
};
use pd_client::PdClient;
use slog_global::{error, info, warn};
use tikv_util::{
    future::block_on_timeout,
    time::Instant,
    worker::{Runnable, RunnableWithTimer},
};
use txn_types::{Key, TimeStamp, WriteRef, WriteType};

use crate::{
    background::{parse_write, split_ts},
    read::{RangeCacheIterator, RangeCacheSnapshot},
    RangeCacheMemoryEngine,
};

#[derive(Debug)]
pub(crate) enum CrossCheckTask {
    CrossCheck,
}

impl Display for CrossCheckTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CrossCheckTask::CrossCheck => f.debug_struct("CrossCheck").finish(),
        }
    }
}

// Checks the data consistency in the mvcc snapshot semantics between in-memory
// engine and rocksdb. It compares keys one by one in the in-memory engine
// with keys in the rocksdb and for those keys that are missed or redundant
// in the in-memory engine, check the validity.
pub(crate) struct CrossChecker {
    pd_client: Arc<dyn PdClient>,
    memory_engine: RangeCacheMemoryEngine,
    rocks_engine: RocksEngine,
    interval: Duration,
}

impl CrossChecker {
    pub(crate) fn new(
        pd_client: Arc<dyn PdClient>,
        memory_engine: RangeCacheMemoryEngine,
        rocks_engine: RocksEngine,
        interval: Duration,
    ) -> CrossChecker {
        CrossChecker {
            pd_client,
            memory_engine,
            rocks_engine,
            interval,
        }
    }

    fn cross_check_range(&self, range_snap: &RangeCacheSnapshot, rocks_snap: &RocksSnapshot) {
        info!(
            "cross check range";
            "range" => ?range_snap.snapshot_meta().range,
        );
        let opts = iter_option(
            &range_snap.snapshot_meta().range.start,
            &range_snap.snapshot_meta().range.end,
            false,
        );
        let mut safe_point = {
            let core = self.memory_engine.core().read();
            let Some(s) = core
                .range_manager
                .get_safe_point(&range_snap.snapshot_meta().range)
            else {
                return;
            };
            s
        };

        for cf in &[CF_LOCK, CF_WRITE] {
            let mut mem_iter = range_snap.iterator_opt(cf, opts.clone()).unwrap();
            let mut disk_iter = rocks_snap.iterator_opt(cf, opts.clone()).unwrap();

            let mem_valid = mem_iter.seek_to_first().unwrap();
            let disk_valid = disk_iter.seek_to_first().unwrap();
            if !mem_valid {
                let mut last_disk_user_key = vec![];
                let mut last_disk_user_key_delete = false;
                let mut prev_key_info = KeyCheckingInfo::default();
                if !CrossChecker::check_remain_disk_key(
                    cf,
                    &range_snap.snapshot_meta().range,
                    &mut safe_point,
                    &mem_iter,
                    &mut disk_iter,
                    &mut prev_key_info,
                    &mut last_disk_user_key,
                    &mut last_disk_user_key_delete,
                    &self.memory_engine,
                ) {
                    return;
                }
                continue;
            }
            if !disk_valid {
                panic!(
                    "cross check fail(key should not exist): {:?} cf not match when seek_to_first;
                    lower={:?}, upper={:?}; cache_key={:?}; sequence_numer={};",
                    cf,
                    log_wrappers::Value(&mem_iter.lower_bound),
                    log_wrappers::Value(&mem_iter.upper_bound),
                    log_wrappers::Value(mem_iter.key()),
                    mem_iter.sequence_number,
                );
            }

            let check_default = |iter: &RangeCacheIterator| {
                let write = WriteRef::parse(iter.value()).unwrap();
                if write.write_type == WriteType::Put && write.short_value.is_none() {
                    let start_ts = write.start_ts;
                    let (user_key, _) = split_ts(iter.key()).unwrap();
                    let default_key = Key::from_encoded(user_key.to_vec()).append_ts(start_ts);
                    if let Ok(Some(_)) = range_snap.get_value(default_key.as_encoded()) {
                    } else {
                        // check again
                        if let Ok(Some(_)) = range_snap.get_value_cf(CF_WRITE, iter.key()) {
                            panic!(
                                "cross check fail(key should exist): default not found;
                                lower={:?}, upper={:?}; default_key={:?}, write_key={:?}, start_ts={}; sequence_numer={};",
                                log_wrappers::Value(&iter.lower_bound),
                                log_wrappers::Value(&iter.upper_bound),
                                log_wrappers::Value(default_key.as_encoded()),
                                log_wrappers::Value(iter.key()),
                                start_ts,
                                iter.sequence_number,
                            );
                        }
                    }
                }
            };

            let mut last_disk_user_key = vec![];
            // We can have this state:
            // Safe point: 6
            // IME:                       [k2-7]
            // Rocks: k1-5-delete, [k1-3], k2-7
            // where k1-5-delete and k1-3 are filtered which is legal as k1-5 is a delete
            // type. At some time, rocksdb iterator points to k1-3 while IME iterator points
            // to k2-7 and we need last_disk_user_key_delete being true to verify the
            // legality.
            let mut last_disk_user_key_delete = false;

            let mut cur_key_info = KeyCheckingInfo {
                user_key: vec![],
                // Used to record mvcc versions of same user keys. So if safe point changed, we
                // can found the last_mvcc_before_safe_point_of_cur_user_key and
                // last_mvcc_before_safe_point_of_last_user_key
                mvcc_recordings: vec![],
                // We can have intermediate state:
                // Safe point: 6
                // IME:   k1-7, k1-5,       k1-2
                // Rocks: k1-7, k1-5, k1-3, k1-2
                // where k1-3 is gced but k1-2 is not. It's safe becase safe point is 6 and we
                // have k1-5 so both k1-3 and k1-2 are not visible.
                // So we record last_mvcc_before_safe_point_of_cur_user_key = 5 and we reject
                // any version of this user key with mvcc between 5 and safe point 6.
                last_mvcc_version_before_safe_point: 0,
            };

            let mut prev_key_info = KeyCheckingInfo {
                user_key: vec![],
                mvcc_recordings: vec![],
                // We can have this sate:
                // Safe point: 6
                // IME:   k1-7, k1-5,              [k2-7]
                // Rocks: k1-7, k1-5, [k1-3], k1-2, k2-7
                // where k1-3 and k1-2 are filtered which is valid. At some time, rocksdb
                // iterator points to k1-3 and IME iterator points to k2-7. We need
                // to record last_mvcc_before_safe_point_of_last_user_key = 5 and
                // reject any version of user key k1 (which is the last user key of
                // IME) with mvcc between 5 and 6.
                last_mvcc_version_before_safe_point: 0,
            };

            if *cf == CF_WRITE {
                let write = match parse_write(mem_iter.value()) {
                    Ok(write) => write,
                    Err(e) => {
                        panic!(
                            "cross check fail(parse error); 
                            lower={:?}, upper={:?}; cache_key={:?}, cache_val={:?}; sequence_numer={}; Error={:?}",
                            log_wrappers::Value(&mem_iter.lower_bound),
                            log_wrappers::Value(&mem_iter.upper_bound),
                            log_wrappers::Value(mem_iter.key()),
                            log_wrappers::Value(mem_iter.value()),
                            mem_iter.sequence_number,
                            e,
                        );
                    }
                };
                let (user_key, ts) = split_ts(mem_iter.key()).unwrap();

                if write.write_type != WriteType::Lock && write.write_type != WriteType::Rollback {
                    cur_key_info.mvcc_recordings.push(ts);
                }

                cur_key_info.user_key = user_key.to_vec();
            }

            CrossChecker::check_with_key_in_disk_iter(
                cf,
                &mem_iter,
                &mut disk_iter,
                false,
                &mut safe_point,
                &self.memory_engine,
                &range_snap.snapshot_meta().range,
                &mut prev_key_info,
                &mut cur_key_info,
                &mut last_disk_user_key_delete,
                &mut last_disk_user_key,
            );

            if *cf == CF_WRITE {
                check_default(&mem_iter);
            }

            while mem_iter.next().unwrap() {
                if *cf == CF_WRITE {
                    let (user_key, ts) = split_ts(mem_iter.key()).unwrap();
                    let write = match parse_write(mem_iter.value()) {
                        Ok(write) => write,
                        Err(e) => {
                            panic!(
                                "cross check fail(parse error); 
                                lower={:?}, upper={:?}; cache_key={:?}, cache_val={:?}; sequence_numer={}; Error={:?}",
                                log_wrappers::Value(&mem_iter.lower_bound),
                                log_wrappers::Value(&mem_iter.upper_bound),
                                log_wrappers::Value(mem_iter.key()),
                                log_wrappers::Value(mem_iter.value()),
                                mem_iter.sequence_number,
                                e,
                            );
                        }
                    };

                    if cur_key_info.user_key != user_key {
                        prev_key_info = cur_key_info;
                        cur_key_info = KeyCheckingInfo {
                            user_key: user_key.to_vec(),
                            mvcc_recordings: vec![],
                            last_mvcc_version_before_safe_point: 0,
                        };
                    }

                    if write.write_type != WriteType::Lock
                        && write.write_type != WriteType::Rollback
                    {
                        cur_key_info.mvcc_recordings.push(ts);
                    }
                }

                CrossChecker::check_with_key_in_disk_iter(
                    cf,
                    &mem_iter,
                    &mut disk_iter,
                    true,
                    &mut safe_point,
                    &self.memory_engine,
                    &range_snap.snapshot_meta().range,
                    &mut prev_key_info,
                    &mut cur_key_info,
                    &mut last_disk_user_key_delete,
                    &mut last_disk_user_key,
                );

                if *cf == CF_WRITE {
                    check_default(&mem_iter);
                }
            }
            prev_key_info = cur_key_info;
            disk_iter.next().unwrap();
            if !CrossChecker::check_remain_disk_key(
                cf,
                &range_snap.snapshot_meta().range,
                &mut safe_point,
                &mem_iter,
                &mut disk_iter,
                &mut prev_key_info,
                &mut last_disk_user_key,
                &mut last_disk_user_key_delete,
                &self.memory_engine,
            ) {
                return;
            }
        }
        info!(
            "cross check range done";
            "range" => ?range_snap.snapshot_meta().range,
        );
    }

    // IME iterator has reached to end, now check the validity of the remaining keys
    // in rocksdb iterator.
    // Return false means the range has been evicted, and for simplicity, stop the
    // check.
    fn check_remain_disk_key(
        cf: &&str,
        range: &CacheRange,
        safe_point: &mut u64,
        mem_iter: &RangeCacheIterator,
        disk_iter: &mut RocksEngineIterator,
        prev_key_info: &mut KeyCheckingInfo,
        last_disk_user_key: &mut Vec<u8>,
        last_disk_user_key_delete: &mut bool,
        engine: &RangeCacheMemoryEngine,
    ) -> bool {
        while disk_iter.valid().unwrap() {
            if *cf == CF_LOCK {
                panic!(
                    "cross check fail(key should exist): lock cf not match; 
                    lower={:?}, upper={:?}; disk_key={:?}; sequence_numer={};",
                    log_wrappers::Value(&mem_iter.lower_bound),
                    log_wrappers::Value(&mem_iter.upper_bound),
                    log_wrappers::Value(disk_iter.key()),
                    mem_iter.sequence_number,
                );
            }

            let (disk_user_key, disk_mvcc) = split_ts(disk_iter.key()).unwrap();
            // We cannot miss any types of write if the mvcc version is larger than
            // safe_point of the relevant range. But the safe point can be updated during
            // the cross check. Fetch it and check again.
            if disk_mvcc > *safe_point {
                *safe_point = {
                    let Some(s) = engine.core().read().range_manager().get_safe_point(range) else {
                        return false;
                    };
                    assert!(s >= *safe_point);
                    s
                };
                if disk_mvcc > *safe_point {
                    panic!(
                        "cross check fail(key should exist): write cf not match;
                        lower={:?}, upper={:?}; disk_key={:?}, disk_mvcc={}; sequence_numer={}; prev_key_info={:?}",
                        log_wrappers::Value(&mem_iter.lower_bound),
                        log_wrappers::Value(&mem_iter.upper_bound),
                        log_wrappers::Value(disk_iter.key()),
                        disk_mvcc,
                        mem_iter.sequence_number,
                        prev_key_info,
                    );
                }
            }
            let write = match parse_write(disk_iter.value()) {
                Ok(write) => write,
                Err(e) => {
                    panic!(
                        "cross check fail(parse error); 
                        lower={:?}, upper={:?}; cache_key={:?}, cache_val={:?}; sequence_numer={}; Error={:?}",
                        log_wrappers::Value(&mem_iter.lower_bound),
                        log_wrappers::Value(&mem_iter.upper_bound),
                        log_wrappers::Value(mem_iter.key()),
                        log_wrappers::Value(mem_iter.value()),
                        mem_iter.sequence_number,
                        e,
                    );
                }
            };

            if !CrossChecker::check_with_last_user_key(
                cf,
                range,
                mem_iter,
                &write,
                safe_point,
                disk_iter.key(),
                disk_mvcc,
                disk_user_key,
                prev_key_info,
                last_disk_user_key,
                last_disk_user_key_delete,
                engine,
            ) {
                return false;
            };

            disk_iter.next().unwrap();
        }

        true
    }

    // In-memory engine may have gced some versions, so we should call next of
    // disk_iter for some times to get aligned with mem_iter.
    // After each call of disk_iter, we will check whether the key missed in the
    // in-memory engine will not make it compromise data consistency.
    // `next_fisrt` denotes whether disk_iter should call next before comparison.
    fn check_with_key_in_disk_iter(
        cf: &str,
        mem_iter: &RangeCacheIterator,
        disk_iter: &mut RocksEngineIterator,
        next_fisrt: bool,
        safe_point: &mut u64,
        engine: &RangeCacheMemoryEngine,
        range: &CacheRange,
        prev_key_info: &mut KeyCheckingInfo,
        cur_key_info: &mut KeyCheckingInfo,
        last_disk_user_key_delete: &mut bool,
        last_disk_user_key: &mut Vec<u8>,
    ) -> bool {
        let read_ts = mem_iter.snapshot_read_ts;
        let mem_key = mem_iter.key();
        if next_fisrt && !disk_iter.next().unwrap() {
            panic!(
                "cross check fail(key should not exist): disk iterator next failed;
                    lower={:?}, upper={:?}; cache_key={:?}; sequence_numer={}; cf={:?}",
                log_wrappers::Value(&mem_iter.lower_bound),
                log_wrappers::Value(&mem_iter.upper_bound),
                log_wrappers::Value(mem_key),
                mem_iter.sequence_number,
                cf,
            );
        }

        loop {
            let disk_key = disk_iter.key();
            if cf == "lock" {
                // lock cf should always have the same view
                if disk_key != mem_key {
                    panic!(
                        "cross check fail(key not equal): lock cf not match; 
                        lower={:?}, upper={:?}; cache_key={:?}, disk_key={:?}; sequence_numer={};",
                        log_wrappers::Value(&mem_iter.lower_bound),
                        log_wrappers::Value(&mem_iter.upper_bound),
                        log_wrappers::Value(mem_key),
                        log_wrappers::Value(disk_key),
                        mem_iter.sequence_number,
                    );
                }
                if mem_iter.value() != disk_iter.value() {
                    panic!(
                        "cross check fail(value not equal): lock cf not match; 
                        lower={:?}, upper={:?}; key={:?}, mem_value={:?} disk_key={:?};",
                        log_wrappers::Value(&mem_iter.lower_bound),
                        log_wrappers::Value(&mem_iter.upper_bound),
                        log_wrappers::Value(mem_key),
                        log_wrappers::Value(mem_iter.value()),
                        log_wrappers::Value(disk_iter.value()),
                    );
                }
                break;
            }

            if disk_key == mem_key {
                if mem_iter.value() != disk_iter.value() {
                    panic!(
                        "cross check fail(value not equal): write cf not match; 
                        lower={:?}, upper={:?}; key={:?}, mem_value={:?} disk_key={:?};",
                        log_wrappers::Value(&mem_iter.lower_bound),
                        log_wrappers::Value(&mem_iter.upper_bound),
                        log_wrappers::Value(mem_key),
                        log_wrappers::Value(mem_iter.value()),
                        log_wrappers::Value(disk_iter.value()),
                    );
                }
                break;
            }

            let (mem_user_key, mem_mvcc) = split_ts(mem_key).unwrap();
            let (disk_user_key, disk_mvcc) = split_ts(disk_key).unwrap();

            let write = match parse_write(disk_iter.value()) {
                Ok(write) => write,
                Err(e) => {
                    panic!(
                        "cross check fail(parse error); 
                        lower={:?}, upper={:?}; cache_key={:?}, cache_val={:?}; sequence_numer={}; Error={:?}",
                        log_wrappers::Value(&mem_iter.lower_bound),
                        log_wrappers::Value(&mem_iter.upper_bound),
                        log_wrappers::Value(mem_iter.key()),
                        log_wrappers::Value(mem_iter.value()),
                        mem_iter.sequence_number,
                        e,
                    );
                }
            };
            if mem_user_key == disk_user_key {
                if disk_mvcc > mem_mvcc {
                    if write.write_type == WriteType::Rollback
                        || write.write_type == WriteType::Lock
                    {
                        // todo(SpadeA): figure out this before review(merge)
                        info!(
                            "meet gced rollback or lock";
                            "cache_key" => log_wrappers::Value(mem_key),
                            "disk_key" => log_wrappers::Value(disk_key),
                            "lower" => log_wrappers::Value(&mem_iter.lower_bound),
                            "upper" => log_wrappers::Value(&mem_iter.upper_bound),
                            "seqno" => mem_iter.sequence_number,
                            "cf" => ?cf,
                        );
                    } else {
                        // [k1-10, k1-8, k1-5(mvcc delete), k1-4, k1-3]
                        // safe_point: 6
                        // If we gc this range, we will filter k-5, k1-4, and k1-3 but with k1-5
                        // deleted at last, so we may see an intermediate
                        // state [k1-10, k1-8, k1-5(mvcc delete), k1-3] where k1-4 is filtered so we
                        // have a lower mvcc key k1-3 and a higher mvcc key
                        // k1-5. So we should use the safe_point to compare
                        // the mvcc version.
                        if disk_mvcc >= *safe_point {
                            if disk_mvcc < read_ts {
                                // get safe point again as it may be updated
                                *safe_point = {
                                    let Some(s) =
                                        engine.core().read().range_manager().get_safe_point(range)
                                    else {
                                        return false;
                                    };
                                    assert!(s >= *safe_point);
                                    s
                                };
                            }
                            // check again
                            if disk_mvcc >= *safe_point {
                                panic!(
                                    "cross check fail(key should exist): miss valid mvcc version(larger than safe point);
                                    lower={:?}, upper={:?}; cache_key={:?}, disk_key={:?}; sequence_numer={}; read_ts={}, safe_point={}; cur_key_info={:?}",
                                    log_wrappers::Value(&mem_iter.lower_bound),
                                    log_wrappers::Value(&mem_iter.upper_bound),
                                    log_wrappers::Value(mem_key),
                                    log_wrappers::Value(disk_key),
                                    mem_iter.sequence_number,
                                    read_ts,
                                    *safe_point,
                                    cur_key_info,
                                );
                            }
                        }

                        cur_key_info.update_last_mvcc_version_before_safe_point(*safe_point);
                        // We record the largest mvcc version below safe_point for each user_key --
                        // and there should not be any version between it and safe_point
                        // So,   for [k1-10, k1-8, k1-5, k1-4, k1-3]
                        // safe_point: 6
                        // If we see [k1-10, k1-8, k1-4, k1-3] in the in-memory engine, and we
                        // record the last_mvcc_version_before_safe_point be 4. When we see k1-5
                        // from rocksdb, we have this version 5 which is between 6 and 4 which
                        // denotes we have gced a version that should not be gced.
                        if disk_mvcc < *safe_point
                            && disk_mvcc > cur_key_info.last_mvcc_version_before_safe_point
                            && (write.write_type != WriteType::Rollback
                                && write.write_type != WriteType::Lock)
                        {
                            panic!(
                                "cross check fail(key should exist): miss valid mvcc version(less than safe point);
                                lower={:?}, upper={:?}; cache_key={:?}, disk_key={:?}; sequence_numer={}; read_ts={}, safe_point={}; cur_key_info={:?}",
                                log_wrappers::Value(&mem_iter.lower_bound),
                                log_wrappers::Value(&mem_iter.upper_bound),
                                log_wrappers::Value(mem_key),
                                log_wrappers::Value(disk_key),
                                mem_iter.sequence_number,
                                read_ts,
                                *safe_point,
                                cur_key_info,
                            );
                        }
                    }
                }
            } else {
                if disk_mvcc > *safe_point {
                    *safe_point = {
                        let Some(s) = engine.core().read().range_manager().get_safe_point(range)
                        else {
                            return false;
                        };
                        assert!(s >= *safe_point);
                        s
                    };
                    if disk_mvcc > *safe_point {
                        panic!(
                            "cross check fail(key should exist): keys newer than safe_point have been gced;
                            lower={:?}, upper={:?}; disk_key={:?}; sequence_numer={}; read_ts={}, safe_point={}",
                            log_wrappers::Value(&mem_iter.lower_bound),
                            log_wrappers::Value(&mem_iter.upper_bound),
                            log_wrappers::Value(disk_key),
                            mem_iter.sequence_number,
                            read_ts,
                            *safe_point,
                        );
                    }
                }

                if !CrossChecker::check_with_last_user_key(
                    cf,
                    range,
                    mem_iter,
                    &write,
                    safe_point,
                    disk_key,
                    disk_mvcc,
                    disk_user_key,
                    prev_key_info,
                    last_disk_user_key,
                    last_disk_user_key_delete,
                    engine,
                ) {
                    return false;
                }
            }

            if disk_key > mem_key {
                panic!(
                    "cross check fail(key should not exist): write cf not match;
                    lower={:?}, upper={:?}; cache_key={:?}, disk_key={:?}; sequence_numer={}; read_ts={}, safe_point={}",
                    log_wrappers::Value(&mem_iter.lower_bound),
                    log_wrappers::Value(&mem_iter.upper_bound),
                    log_wrappers::Value(mem_key),
                    log_wrappers::Value(disk_key),
                    mem_iter.sequence_number,
                    read_ts,
                    *safe_point,
                );
            }

            assert!(disk_iter.next().unwrap());
        }

        true
    }

    // mem_iter has pointed to the next user key whereas disk_iter still has some
    // versions.
    #[allow(clippy::collapsible_else_if)]
    fn check_with_last_user_key(
        cf: &str,
        range: &CacheRange,
        mem_iter: &RangeCacheIterator,
        write: &WriteRef<'_>,
        safe_point: &mut u64,
        disk_key: &[u8],
        disk_mvcc: u64,
        disk_user_key: &[u8],
        prev_key_info: &mut KeyCheckingInfo,
        last_disk_user_key: &mut Vec<u8>,
        last_disk_user_key_delete: &mut bool,
        engine: &RangeCacheMemoryEngine,
    ) -> bool {
        if write.write_type == WriteType::Rollback || write.write_type == WriteType::Lock {
            info!(
                "meet gced rollback or lock";
                "disk_key" => log_wrappers::Value(disk_key),
                "lower" => log_wrappers::Value(&mem_iter.lower_bound),
                "upper" => log_wrappers::Value(&mem_iter.upper_bound),
                "seqno" => mem_iter.sequence_number,
                "cf" => ?cf,
            );
            return true;
        }

        if disk_user_key == prev_key_info.user_key {
            prev_key_info.update_last_mvcc_version_before_safe_point(*safe_point);
            // It means all versions below safe point are GCed which means the
            // latest write below safe point is mvcc delete.
            // IME:  [k1-9, k2-9]
            // Rocks:[k1-9, k1-5, k1-3, k2-9]
            // Safe point: 6
            // In thias case, k1-5 must be MVCC delete.
            // So when disk points to k1-5 we set last_disk_user_key_delete be
            // true so that when we check k1-3 we can know it is deleted
            // legally.
            if prev_key_info.last_mvcc_version_before_safe_point == 0 {
                *safe_point = {
                    let Some(s) = engine.core().read().range_manager().get_safe_point(range) else {
                        return false;
                    };
                    assert!(s >= *safe_point);
                    s
                };
            }
            prev_key_info.update_last_mvcc_version_before_safe_point(*safe_point);
            if prev_key_info.last_mvcc_version_before_safe_point == 0 {
                if disk_user_key != last_disk_user_key {
                    *last_disk_user_key = disk_user_key.to_vec();
                    *last_disk_user_key_delete = false;
                }
                if !*last_disk_user_key_delete {
                    if write.write_type == WriteType::Delete {
                        *last_disk_user_key_delete = true;
                    } else {
                        panic!(
                            "cross check fail(key should exist): miss valid mvcc version;
                            lower={:?}, upper={:?}; disk_key={:?}; sequence_numer={}; read_ts={}, safe_point={}; prev_key_info={:?}",
                            log_wrappers::Value(&mem_iter.lower_bound),
                            log_wrappers::Value(&mem_iter.upper_bound),
                            log_wrappers::Value(disk_key),
                            mem_iter.sequence_number,
                            mem_iter.snapshot_read_ts,
                            safe_point,
                            prev_key_info,
                        );
                    }
                }
            } else {
                if disk_mvcc > prev_key_info.last_mvcc_version_before_safe_point {
                    if write.write_type == WriteType::Rollback
                        || write.write_type == WriteType::Lock
                    {
                        info!(
                            "meet gced rollback or lock";
                            "disk_key" => log_wrappers::Value(disk_key),
                            "lower" => log_wrappers::Value(&mem_iter.lower_bound),
                            "upper" => log_wrappers::Value(&mem_iter.upper_bound),
                            "seqno" => mem_iter.sequence_number,
                            "cf" => ?cf,
                        );
                    } else {
                        panic!(
                            "cross check fail(key should exist): miss valid mvcc version;
                            lower={:?}, upper={:?}; disk_key={:?}; sequence_numer={}; read_ts={}, safe_point={}",
                            log_wrappers::Value(&mem_iter.lower_bound),
                            log_wrappers::Value(&mem_iter.upper_bound),
                            log_wrappers::Value(disk_key),
                            mem_iter.sequence_number,
                            mem_iter.snapshot_read_ts,
                            safe_point,
                        );
                    }
                } else {
                    // It's ok
                }
            }
        } else {
            // IME:               k2-9
            // Rocks: k1-5, k1-3, k2-9
            // Safe point: 6
            // In this case, k1-5 must be MVCC delete.
            // So when disk points to k1-5 we set last_disk_user_key_delete be true so that
            // when we check k1-3 we can know it is deleted legally.
            if disk_user_key != last_disk_user_key {
                *last_disk_user_key = disk_user_key.to_vec();
                *last_disk_user_key_delete = false;
            }
            if !*last_disk_user_key_delete {
                if write.write_type == WriteType::Delete {
                    *last_disk_user_key_delete = true;
                } else {
                    panic!(
                        "cross check fail(key should exist): miss valid mvcc version;
                        lower={:?}, upper={:?}; disk_key={:?}; sequence_numer={}; read_ts={}, safe_point={}",
                        log_wrappers::Value(&mem_iter.lower_bound),
                        log_wrappers::Value(&mem_iter.upper_bound),
                        log_wrappers::Value(disk_key),
                        mem_iter.sequence_number,
                        mem_iter.snapshot_read_ts,
                        safe_point,
                    );
                }
            }
        }

        true
    }
}

impl Runnable for CrossChecker {
    type Task = CrossCheckTask;

    fn run(&mut self, _: Self::Task) {
        let ranges: Vec<_> = {
            let core = self.memory_engine.core().read();
            core.range_manager
                .ranges()
                .iter()
                .map(|(r, _)| r.clone())
                .collect()
        };

        let snap = self.rocks_engine.snapshot(None);

        let tso_timeout = Duration::from_secs(5);
        let now = match block_on_timeout(self.pd_client.get_tso(), tso_timeout) {
            Ok(Ok(ts)) => ts,
            err => {
                error!(
                    "schedule range cache engine gc failed ";
                    "timeout_duration" => ?tso_timeout,
                    "error" => ?err,
                );
                return;
            }
        };

        // Check the snapshot with read_ts one minute ago
        let read_ts = now.physical() - Duration::from_secs(60).as_millis() as u64;
        let read_ts = TimeStamp::compose(read_ts, 0).into_inner();

        let ranges_to_audit: Vec<_> = ranges
            .iter()
            .filter_map(|range| {
                match self
                    .memory_engine
                    .snapshot(range.clone(), read_ts, snap.sequence_number())
                {
                    Ok(range_snap) => Some(range_snap),
                    Err(_) => {
                        warn!(
                            "failed to get snap in cross check";
                            "range" => ?range,
                        );
                        None
                    }
                }
            })
            .collect();

        if ranges_to_audit.is_empty() {
            return;
        }

        let now = Instant::now();

        ranges_to_audit
            .into_iter()
            .for_each(|r| self.cross_check_range(&r, &snap));
        info!(
            "cross check finished";
            "duration" => ?now.saturating_elapsed(),
        );
    }
}

impl RunnableWithTimer for CrossChecker {
    fn get_interval(&self) -> Duration {
        self.interval
    }

    fn on_timeout(&mut self) {
        self.run(CrossCheckTask::CrossCheck);
    }
}

#[derive(Default)]
struct KeyCheckingInfo {
    user_key: Vec<u8>,
    mvcc_recordings: Vec<u64>,
    last_mvcc_version_before_safe_point: u64,
}

impl KeyCheckingInfo {
    fn update_last_mvcc_version_before_safe_point(&mut self, safe_point: u64) {
        self.last_mvcc_version_before_safe_point = *self
            .mvcc_recordings
            .iter()
            .find(|&mvcc| mvcc <= &safe_point)
            .unwrap_or(&0);
    }
}

impl std::fmt::Debug for KeyCheckingInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyCheckingInfo")
            .field("user_key", &log_wrappers::Value(&self.user_key))
            .field("mvcc_recordings", &self.mvcc_recordings)
            .field(
                "last_mvcc_version_before_safe_point",
                &self.last_mvcc_version_before_safe_point,
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use engine_rocks::{util::new_engine_opt, RocksDbOptions, RocksWriteBatchVec};
    use engine_traits::{
        CacheRange, KvEngine, Mutable, RangeCacheEngine, WriteBatch, WriteBatchExt, CF_DEFAULT,
        CF_LOCK, CF_WRITE,
    };
    use futures::future::ready;
    use kvproto::metapb::Region;
    use pd_client::PdClient;
    use raftstore::{
        coprocessor::{RegionInfoCallback, RegionInfoProvider},
        RegionInfo, SeekRegionCallback,
    };
    use tempfile::Builder;
    use tikv_util::config::VersionTrack;
    use txn_types::{Key, TimeStamp, Write, WriteType};

    use crate::{
        cross_check::CrossChecker, RangeCacheEngineConfig, RangeCacheEngineContext,
        RangeCacheMemoryEngine, RangeCacheWriteBatch,
    };

    #[derive(Clone)]
    struct MockRegionInfoProvider;
    impl RegionInfoProvider for MockRegionInfoProvider {
        fn seek_region(
            &self,
            _: &[u8],
            _: SeekRegionCallback,
        ) -> raftstore::coprocessor::Result<()> {
            Ok(())
        }
        fn find_region_by_id(
            &self,
            _: u64,
            _: RegionInfoCallback<Option<RegionInfo>>,
        ) -> raftstore::coprocessor::Result<()> {
            Ok(())
        }
        fn get_regions_in_range(
            &self,
            _start_key: &[u8],
            _end_key: &[u8],
            _: bool,
        ) -> raftstore::coprocessor::Result<Vec<Region>> {
            Ok(vec![])
        }
    }

    fn cross_check<F>(prepare_data: F)
    where
        F: FnOnce(&mut RangeCacheWriteBatch, &mut RocksWriteBatchVec),
    {
        let mut engine = RangeCacheMemoryEngine::with_region_info_provider(
            RangeCacheEngineContext::new_for_tests(Arc::new(VersionTrack::new(
                RangeCacheEngineConfig::config_for_test(),
            ))),
            Some(Arc::new(MockRegionInfoProvider {})),
        );
        let range = CacheRange::new(b"".to_vec(), b"z".to_vec());
        engine.new_range(range.clone());

        let path = Builder::new().prefix("temp").tempdir().unwrap();
        let db_opts = RocksDbOptions::default();
        let cf_opts = [CF_DEFAULT, CF_LOCK, CF_WRITE]
            .iter()
            .map(|name| (*name, Default::default()))
            .collect();
        let rocks_engine = new_engine_opt(path.path().to_str().unwrap(), db_opts, cf_opts).unwrap();

        engine.set_disk_engine(rocks_engine.clone());
        engine
            .core()
            .write()
            .mut_range_manager()
            .mut_range_meta(&range)
            .unwrap()
            .set_safe_point(6);

        struct MockPdClient {}
        impl PdClient for MockPdClient {
            fn get_tso(&self) -> pd_client::PdFuture<txn_types::TimeStamp> {
                Box::pin(ready(Ok(TimeStamp::compose(TimeStamp::physical_now(), 0))))
            }
        }

        let cross_checker = CrossChecker::new(
            Arc::new(MockPdClient {}),
            engine.clone(),
            rocks_engine.clone(),
            Duration::from_secs(100000),
        );

        {
            let mut wb = engine.write_batch();
            wb.prepare_for_range(range.clone());
            let mut disk_wb = rocks_engine.write_batch();

            prepare_data(&mut wb, &mut disk_wb);

            wb.set_sequence_number(1000).unwrap();
            wb.write().unwrap();
            disk_wb.write().unwrap();

            let snap = engine.snapshot(range.clone(), 10, 10000).unwrap();
            let disk_snap = rocks_engine.snapshot(None);

            cross_checker.cross_check_range(&snap, &disk_snap);
        }
    }

    fn write_key(k: &[u8], ts: u64, ty: WriteType) -> (Vec<u8>, Vec<u8>) {
        let raw_write_k = Key::from_raw(k).append_ts(ts.into());
        let val = Write::new(ty, ts.into(), Some(vec![])).as_ref().to_bytes();
        (raw_write_k.into_encoded(), val)
    }

    #[test]
    fn test_cross_check() {
        // Safe point: 6
        // IME:
        // Disk: k1-4-r,
        cross_check(|_wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 4, WriteType::Rollback);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:
        // Disk: k1-4-d,
        cross_check(|_wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 4, WriteType::Delete);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:
        // Disk: k1-4-d, k1-3
        cross_check(|_wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 4, WriteType::Delete);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 3, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:
        // Disk: k1-5-r, k1-4-d, k1-3
        cross_check(|_wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 5, WriteType::Rollback);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Delete);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 3, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:  k1-9, k1-5,
        // Disk: k1-9, k1-5, k1-4, k1-2
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 5, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:  k2-5,
        // Disk: k2-5, k2-4, k2-2
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-2", 5, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:  k1-5,       k2-4,       k3-4,       k4-4
        // Disk: k1-5, k1-3, k2-4, k2-2, k3-4, k3-2, k4-4, k4-2
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 5, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 3, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 4, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-3", 4, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-3", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-4", 4, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-4", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:  k1-9, k1-5,             k2-7
        // Disk: k1-9, k1-5, k1-4, k1-2, k2-7,
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 5, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 7, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Temporary state in GC: k1-4 is filtered
        // Safe point: 6
        // IME:  k1-9, k1-5-d,       k1-2  k2-7
        // Disk: k1-9, k1-5-d, k1-4, k1-2, k2-7,
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 5, WriteType::Delete);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 2, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 7, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:  k1-9,                     k2-7
        // Disk: k1-9, k1-5-d, k1-4, k1-2, k2-7,
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 5, WriteType::Delete);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 7, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:  k1-9, k1-5,                           k3-7
        // Disk: k1-9, k1-5, k1-4, k1-2, k2-4-d, k2-3, k3-7
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 5, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 4, WriteType::Delete);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 3, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-3", 7, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:  k1-9,                     k2-4-d        k2-1 k3-7
        // Disk: k1-9, k1-5-d, k1-4, k1-2, k2-4-d, k2-3, k2-1 k3-7
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 5, WriteType::Delete);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 4, WriteType::Delete);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 3, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 2, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-3", 7, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });

        // Safe point: 6
        // IME:  k1-9,                                   k3-7
        // Disk: k1-9, k1-5-d, k1-4, k1-2, k2-4-d, k2-3, k3-7
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 5, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 4, WriteType::Delete);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 3, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-3", 7, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }

    #[test]
    #[should_panic]
    fn test_cross_check_panic1() {
        // Safe point: 6
        // IME:  k1-9, k1-5-r,             k2-7
        // Disk: k1-9, k1-5-r, k1-4, k1-2, k2-7,
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 5, WriteType::Rollback);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 7, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }

    #[test]
    #[should_panic]
    fn test_cross_check_panic2() {
        // Safe point: 6
        // IME:  k1-9,       k1-4,       k2-7
        // Disk: k1-9, k1-5, k1-4, k1-2, k2-7,
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 5, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 7, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }

    #[test]
    #[should_panic]
    fn test_cross_check_panic2_2() {
        // Safe point: 6
        // IME:  k1-9,
        // Disk: k1-9, k1-5, k1-4, k1-2, k-2-7
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 5, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 2, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 7, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }

    #[test]
    #[should_panic]
    fn test_cross_check_panic3_1() {
        // Safe point: 6
        // IME:        k2-7
        // Disk: k1-9, k2-7,
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 7, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }

    #[test]
    #[should_panic]
    fn test_cross_check_panic3_2() {
        // Safe point: 6
        // IME:
        // Disk: k1-9,
        cross_check(|_wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }

    #[test]
    #[should_panic]
    fn test_cross_check_panic3_3() {
        // Safe point: 6
        // IME:
        // Disk: k1-4,
        cross_check(|_wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 4, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }

    #[test]
    #[should_panic]
    fn test_cross_check_panic3_4() {
        // Safe point: 6
        // IME:
        // Disk: k1-4-r, k1-3
        cross_check(|_wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 4, WriteType::Rollback);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-1", 3, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }

    #[test]
    #[should_panic]
    fn test_cross_check_panic4_1() {
        // Safe point: 6
        // IME:  k1-4
        // Disk:
        cross_check(|wb, _disk_wb| {
            let (k, v) = write_key(b"k-1", 4, WriteType::Rollback);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }

    #[test]
    #[should_panic]
    fn test_cross_check_panic4_2() {
        // Safe point: 6
        // IME:  k1-7, k2-4
        // Disk: k1-7
        cross_check(|wb, _disk_wb| {
            let (k, v) = write_key(b"k-1", 4, WriteType::Rollback);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }

    #[test]
    #[should_panic]
    fn test_cross_check_panic5() {
        // Safe point: 6
        // IME:        k2-7
        // Disk: k1-9, k2-7,
        cross_check(|wb, disk_wb| {
            let (k, v) = write_key(b"k-1", 9, WriteType::Put);
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();

            let (k, v) = write_key(b"k-2", 7, WriteType::Put);
            wb.put_cf(CF_WRITE, &k, &v).unwrap();
            disk_wb.put_cf(CF_WRITE, &k, &v).unwrap();
        });
    }
}
