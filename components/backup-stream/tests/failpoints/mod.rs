// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_failpoint_tests)]

#[path = "../suite.rs"]
mod suite;
pub use suite::*;

mod all {

    use core::panic;
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use backup_stream::{
        errors::Error,
        metadata::{
            keys::MetaKey,
            store::{Keys, MetaStore},
            PauseStatus,
        },
        router::TaskSelector,
        GetCheckpointResult, RegionCheckpointOperation, RegionSet, Task,
    };
    use encryption::{FileConfig, MasterKeyConfig};
    use futures::executor::block_on;
    use kvproto::encryptionpb::EncryptionMethod;
    use serde_json::Value;
    use tempfile::TempDir;
    use tikv_util::{
        box_err,
        config::{ReadableDuration, ReadableSize},
        defer, HandyRwLock,
    };
    use txn_types::Key;
    use walkdir::WalkDir;

    use super::{
        make_record_key, make_split_key_at_record, mutation, run_async_test, SuiteBuilder,
    };
    use crate::{make_table_key, Suite};

    #[test]
    fn failed_register_task() {
        let suite = SuiteBuilder::new_named("failed_register_task").build();
        fail::cfg("load_task::error_when_fetching_ranges", "return").unwrap();
        let cli = suite.get_meta_cli();
        block_on(cli.insert_task_with_range(
            &suite.simple_task("failed_register_task"),
            &[(&make_table_key(1, b""), &make_table_key(2, b""))],
        ))
        .unwrap();

        for _ in 0..10 {
            if block_on(cli.get_last_error_of("failed_register_task", 1))
                .unwrap()
                .is_some()
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        suite.dump_slash_etc();
        panic!("No error uploaded when failed to comminate to PD.");
    }

    #[test]
    fn basic() {
        let mut suite = SuiteBuilder::new_named("basic").build();
        fail::cfg("try_start_observe", "1*return").unwrap();

        let (round1, round2) = run_async_test(async {
            // write data before the task starting, for testing incremental scanning.
            let round1 = suite.write_records(0, 128, 1).await;
            suite.must_register_task(1, "test_basic");
            suite.sync();
            let round2 = suite.write_records(256, 128, 1).await;
            suite.force_flush_files("test_basic");
            suite.wait_for_flush();
            (round1, round2)
        });
        suite.check_for_write_records(
            suite.flushed_files.path(),
            round1.union(&round2).map(Vec::as_slice),
        );

        suite.cluster.shutdown();
    }
    #[test]
    fn frequent_initial_scan() {
        let mut suite = SuiteBuilder::new_named("frequent_initial_scan")
            .cfg(|c| c.num_threads = 1)
            .build();
        let keys = (1..1024).map(|i| make_record_key(1, i)).collect::<Vec<_>>();
        let start_ts = suite.tso();
        suite.must_kv_prewrite(
            1,
            keys.clone()
                .into_iter()
                .map(|k| mutation(k, b"hello, world".to_vec()))
                .collect(),
            make_record_key(1, 886),
            start_ts,
        );
        fail::cfg("scan_after_get_snapshot", "pause").unwrap();
        suite.must_register_task(1, "frequent_initial_scan");
        let commit_ts = suite.tso();
        suite.commit_keys(keys, start_ts, commit_ts);
        suite.run(|| {
            Task::ModifyObserve(backup_stream::ObserveOp::Stop {
                region: suite.cluster.get_region(&make_record_key(1, 886)),
            })
        });
        suite.run(|| {
            Task::ModifyObserve(backup_stream::ObserveOp::Start {
                region: suite.cluster.get_region(&make_record_key(1, 886)),
            })
        });
        fail::cfg("scan_after_get_snapshot", "off").unwrap();
        std::thread::sleep(Duration::from_secs(1));
        suite.force_flush_files("frequent_initial_scan");
        suite.wait_for_flush();
        std::thread::sleep(Duration::from_secs(1));
        let c = suite.global_checkpoint();
        assert!(c > commit_ts.into_inner(), "{} vs {}", c, commit_ts);
    }
    #[test]
    fn region_failure() {
        defer! {{
            fail::remove("try_start_observe");
        }}
        let mut suite = SuiteBuilder::new_named("region_failure").build();
        let keys = run_async_test(suite.write_records(0, 128, 1));
        fail::cfg("try_start_observe", "1*return").unwrap();
        suite.must_register_task(1, "region_failure");
        suite.must_shuffle_leader(1);
        let keys2 = run_async_test(suite.write_records(256, 128, 1));
        suite.force_flush_files("region_failure");
        suite.wait_for_flush();
        suite.check_for_write_records(
            suite.flushed_files.path(),
            keys.union(&keys2).map(|s| s.as_slice()),
        );
    }
    #[test]
    fn initial_scan_failure() {
        defer! {{
            fail::remove("scan_and_async_send");
        }}

        let mut suite = SuiteBuilder::new_named("initial_scan_failure")
            .nodes(1)
            .build();
        let keys = run_async_test(suite.write_records(0, 128, 1));
        fail::cfg(
            "scan_and_async_send",
            "1*return(dive into the temporary dream, where the SLA never bothers)",
        )
        .unwrap();
        suite.must_register_task(1, "initial_scan_failure");
        let keys2 = run_async_test(suite.write_records(256, 128, 1));
        suite.force_flush_files("initial_scan_failure");
        suite.wait_for_flush();
        suite.check_for_write_records(
            suite.flushed_files.path(),
            keys.union(&keys2).map(|s| s.as_slice()),
        );
    }
    #[test]
    fn failed_during_refresh_region() {
        defer! {
            fail::remove("get_last_checkpoint_of")
        }

        let mut suite = SuiteBuilder::new_named("fail_to_refresh_region")
            .nodes(1)
            .build();

        suite.must_register_task(1, "fail_to_refresh_region");
        let keys = run_async_test(suite.write_records(0, 128, 1));
        fail::cfg(
            "get_last_checkpoint_of",
            "1*return(the stream handler wants to become a batch processor, and the batch processor wants to be a stream handler.)",
        ).unwrap();

        suite.must_split(b"SOLE");
        let keys2 = run_async_test(suite.write_records(256, 128, 1));
        // Let's make sure the retry has been triggered...
        std::thread::sleep(Duration::from_secs(2));
        suite.force_flush_files("fail_to_refresh_region");
        suite.wait_for_flush();
        suite.check_for_write_records(
            suite.flushed_files.path(),
            keys.union(&keys2).map(|s| s.as_slice()),
        );
        let leader = suite.cluster.leader_of_region(1).unwrap().store_id;
        let (tx, rx) = std::sync::mpsc::channel();
        suite.endpoints[&leader]
            .scheduler()
            .schedule(Task::RegionCheckpointsOp(RegionCheckpointOperation::Get(
                RegionSet::Universal,
                Box::new(move |rs| {
                    let _ = tx.send(rs);
                }),
            )))
            .unwrap();

        let regions = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert!(
            regions.iter().all(|item| {
                matches!(item, GetCheckpointResult::Ok { checkpoint, .. } if checkpoint.into_inner() > 500)
            }),
            "{:?}",
            regions
        );
    }
    #[test]
    fn test_retry_abort() {
        let mut suite = SuiteBuilder::new_named("retry_abort").nodes(1).build();
        defer! {
            fail::list().into_iter().for_each(|(name, _)| fail::remove(name))
        };

        suite.must_register_task(1, "retry_abort");
        fail::cfg("subscribe_mgr_retry_start_observe_delay", "return(10)").unwrap();
        fail::cfg("try_start_observe", "return()").unwrap();

        suite.must_split(&make_split_key_at_record(1, 42));
        std::thread::sleep(Duration::from_secs(2));

        let error =
            run_async_test(suite.get_meta_cli().get_last_error_of("retry_abort", 1)).unwrap();
        let error = error.expect("no error uploaded");
        error
            .get_error_message()
            .find("retry")
            .expect("error doesn't contain retry");
        fail::cfg("try_start_observe", "10*return()").unwrap();
        // Resume the task manually...
        run_async_test(async {
            suite
                .meta_store
                .delete(Keys::Key(MetaKey::pause_of("retry_abort")))
                .await?;
            suite
                .meta_store
                .delete(Keys::Prefix(MetaKey::last_errors_of("retry_abort")))
                .await?;
            backup_stream::errors::Result::Ok(())
        })
        .unwrap();

        suite.sync();
        suite.wait_with_router(move |r| r.get_task_handler("retry_abort").is_ok());
        let items = run_async_test(suite.write_records(0, 128, 1));
        suite.force_flush_files("retry_abort");
        suite.wait_for_flush();
        suite.check_for_write_records(suite.flushed_files.path(), items.iter().map(Vec::as_slice));
    }
    #[test]
    fn failure_and_split() {
        let mut suite = SuiteBuilder::new_named("failure_and_split")
            .nodes(1)
            .build();
        fail::cfg("try_start_observe0", "pause").unwrap();

        // write data before the task starting, for testing incremental scanning.
        let round1 = run_async_test(suite.write_records(0, 128, 1));
        suite.must_register_task(1, "failure_and_split");
        suite.sync();

        suite.must_split(&make_split_key_at_record(1, 42));
        suite.sync();
        std::thread::sleep(Duration::from_millis(200));
        fail::cfg("try_start_observe", "2*return").unwrap();
        fail::cfg("try_start_observe0", "off").unwrap();

        // Let's wait enough time for observing the split operation.
        std::thread::sleep(Duration::from_secs(2));
        let round2 = run_async_test(suite.write_records(256, 128, 1));
        suite.force_flush_files("failure_and_split");
        suite.wait_for_flush();
        suite.check_for_write_records(
            suite.flushed_files.path(),
            round1.union(&round2).map(Vec::as_slice),
        );
        let cp = suite.global_checkpoint();
        assert!(cp > 512, "it is {}", cp);
        suite.cluster.shutdown();
    }

    #[test]
    fn memory_quota() {
        let mut suite = SuiteBuilder::new_named("memory_quota")
            .cfg(|cfg| cfg.initial_scan_pending_memory_quota = ReadableSize::kb(2))
            .build();
        let keys = run_async_test(suite.write_records(0, 128, 1));
        let failed = Arc::new(AtomicBool::new(false));
        fail::cfg_callback("scan_and_async_send::about_to_consume", {
            let failed = failed.clone();
            move || {
                let v = backup_stream::metrics::HEAP_MEMORY.get();
                // Not greater than max key length * concurrent initial scan number.
                if v > 4096 * 6 {
                    println!("[[ FAILED ]] The memory usage is {v} which exceeds the quota");
                    failed.store(true, Ordering::SeqCst);
                }
            }
        })
        .unwrap();
        suite.must_register_task(1, "memory_quota");
        suite.force_flush_files("memory_quota");
        suite.wait_for_flush();
        suite.check_for_write_records(
            suite.flushed_files.path(),
            keys.iter().map(|v| v.as_slice()),
        );
        assert!(!failed.load(Ordering::SeqCst));
    }

    #[test]
    fn resolve_during_flushing() {
        let mut suite = SuiteBuilder::new_named("resolve_during_flushing")
            .cfg(|cfg| {
                cfg.min_ts_interval = ReadableDuration::days(1);
                cfg.initial_scan_concurrency = 1;
            })
            .nodes(2)
            .build();
        suite.must_register_task(1, "resolve_during_flushing");
        let key = make_record_key(1, 1);

        let start_ts = suite.tso();
        suite.must_kv_prewrite(
            1,
            vec![mutation(
                key.clone(),
                Suite::PROMISED_SHORT_VALUE.to_owned(),
            )],
            key.clone(),
            start_ts,
        );
        fail::cfg("after_moving_to_flushing_files", "pause").unwrap();
        suite.force_flush_files("resolve_during_flushing");
        let commit_ts = suite.tso();
        suite.just_commit_a_key(key.clone(), start_ts, commit_ts);
        suite.run(|| Task::RegionCheckpointsOp(RegionCheckpointOperation::PrepareMinTsForResolve));
        // Wait until the resolve done. Sadly for now we don't have good solutions :(
        std::thread::sleep(Duration::from_secs(2));
        fail::remove("after_moving_to_flushing_files");
        suite.wait_for_flush();
        assert_eq!(suite.global_checkpoint(), start_ts.into_inner());
        // transfer the leader, make sure everything has been flushed.
        suite.must_shuffle_leader(1);
        suite.wait_with(|cfg| cfg.initial_scan_semaphore.available_permits() > 0);
        suite.force_flush_files("resolve_during_flushing");
        suite.wait_for_flush();
        let enc_key = Key::from_raw(&key).append_ts(commit_ts);
        suite.check_for_write_records(
            suite.flushed_files.path(),
            std::iter::once(enc_key.as_encoded().as_slice()),
        );
    }

    #[test]
    fn commit_during_flushing() {
        let mut suite = SuiteBuilder::new_named("commit_during_flushing")
            .nodes(1)
            .build();
        suite.must_register_task(1, "commit_during_flushing");
        let key = make_record_key(1, 1);
        let start_ts = suite.tso();
        suite.must_kv_prewrite(
            1,
            vec![mutation(
                key.clone(),
                Suite::PROMISED_SHORT_VALUE.to_owned(),
            )],
            key.clone(),
            start_ts,
        );
        fail::cfg("subscription_manager_resolve_regions", "pause").unwrap();
        let commit_ts = suite.tso();
        suite.force_flush_files("commit_during_flushing");
        suite.sync();
        suite.sync();
        fail::cfg("log_backup_batch_delay", "return(2000)").unwrap();
        suite.just_commit_a_key(key.clone(), start_ts, commit_ts);
        fail::remove("subscription_manager_resolve_regions");
        suite.wait_for_flush();
        let enc_key = Key::from_raw(&key).append_ts(commit_ts);
        assert!(
            suite.global_checkpoint() > commit_ts.into_inner(),
            "{} {:?}",
            suite.global_checkpoint(),
            commit_ts
        );
        suite.check_for_write_records(
            suite.flushed_files.path(),
            std::iter::once(enc_key.as_encoded().as_slice()),
        )
    }

    #[test]
    fn encryption() {
        let key_folder = TempDir::new().unwrap();
        let key_file = key_folder.path().join("key.txt");
        fail::cfg("log_backup_always_swap_out", "return()").unwrap();
        std::fs::write(&key_file, "42".repeat(32) + "\n").unwrap();

        let mut suite = SuiteBuilder::new_named("encryption")
            .nodes(1)
            .cluster_cfg(move |cfg| {
                cfg.tikv.security.encryption.data_encryption_method = EncryptionMethod::Aes256Ctr;
                cfg.tikv.security.encryption.master_key = MasterKeyConfig::File {
                    config: FileConfig {
                        path: key_file
                            .to_str()
                            .expect("cannot convert OsStr to Rust string")
                            .to_owned(),
                    },
                };
            })
            .cfg(|cfg| cfg.temp_file_memory_quota = ReadableSize(16))
            .build();

        suite.must_register_task(1, "encryption");
        let items = run_async_test(suite.write_records_batched(0, 128, 1));
        // So the old files can be "flushed" to disk.
        let items2 = run_async_test(suite.write_records_batched(256, 128, 1));
        suite.sync();

        let files = WalkDir::new(suite.temp_files.path())
            .into_iter()
            .filter_map(|file| file.ok().filter(|f| f.file_type().is_file()))
            .collect::<Vec<_>>();
        assert!(!files.is_empty());
        for dir in files {
            let data = std::fs::read(dir.path()).unwrap();
            // assert it contains data...
            assert_ne!(data.len(), 0);
            // ... and is not plain zstd compression. (As it was encrypted.)
            assert_ne!(
                u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
                0xFD2FB528,
                "not encrypted: found plain zstd header"
            );
            // ... and doesn't contains the raw value.
            assert!(
                !data
                    .windows(Suite::PROMISED_SHORT_VALUE.len())
                    .any(|w| w == Suite::PROMISED_SHORT_VALUE)
            );
            assert!(
                !data
                    .windows(Suite::PROMISED_LONG_VALUE.len())
                    .any(|w| w == Suite::PROMISED_LONG_VALUE)
            );
        }

        fail::remove("log_backup_always_swap_out");

        suite.force_flush_files("encryption");
        suite.sync();
        suite.wait_for_flush();
        suite.check_for_write_records(
            suite.flushed_files.path(),
            items.union(&items2).map(Vec::as_slice),
        );
    }

    #[test]
    fn failed_to_get_task_when_pausing() {
        let suite = SuiteBuilder::new_named("resume_error").nodes(1).build();
        suite.must_register_task(1, "resume_error");
        let mcli = suite.get_meta_cli();
        run_async_test(mcli.pause("resume_error")).unwrap();
        suite.sync();
        fail::cfg("failed_to_get_task", "1*return").unwrap();
        run_async_test(mcli.resume("resume_error")).unwrap();
        suite.sync();
        // Make sure our suite doesn't panic.
        suite.sync();
    }

    #[test]
    fn fatal_error() {
        let mut suite = SuiteBuilder::new_named("fatal_error").nodes(3).build();
        suite.must_register_task(1, "test_fatal_error");
        suite.sync();
        run_async_test(suite.write_records(0, 1, 1));
        suite.force_flush_files("test_fatal_error");
        suite.wait_for_flush();
        run_async_test(suite.advance_global_checkpoint("test_fatal_error")).unwrap();
        fail::cfg("log-backup-upload-error", "1*return(not my fault)->off").unwrap();
        let (victim, endpoint) = suite.endpoints.iter().next().unwrap();
        endpoint
            .scheduler()
            .schedule(Task::FatalError(
                TaskSelector::ByName("test_fatal_error".to_owned()),
                Box::new(Error::Other(box_err!("everything is alright"))),
            ))
            .unwrap();
        suite.sync();
        fail::remove("log-backup-upload-error");
        // Wait retry...
        std::thread::sleep(Duration::from_secs(6));
        suite.sync();
        let cli = suite.get_meta_cli();
        let err = run_async_test(cli.get_last_error_of("test_fatal_error", *victim))
            .unwrap()
            .unwrap();

        let pause = match run_async_test(cli.pause_status("test_fatal_error")).unwrap() {
            PauseStatus::PausedV2Json(pause) => pause,
            val => {
                panic!("not paused: {:?}", val);
            }
        };
        let val = serde_json::from_str::<Value>(&pause).unwrap();
        assert_eq!(val["severity"].as_str().unwrap(), "ERROR");
        assert_eq!(
            val["operation_hostname"].as_str().unwrap(),
            tikv_util::sys::hostname().unwrap()
        );
        assert_eq!(
            val["operation_pid"].as_u64().unwrap(),
            std::process::id() as u64
        );
        assert_eq!(
            val["payload_type"].as_str().unwrap(),
            "application/x-protobuf;messageType=brpb.StreamBackupError"
        );

        assert_eq!(err.error_code, error_code::backup_stream::OTHER.code);
        assert!(err.error_message.contains("everything is alright"));
        assert_eq!(err.store_id, *victim);
        let paused =
            run_async_test(suite.get_meta_cli().check_task_paused("test_fatal_error")).unwrap();
        assert!(paused);
        let safepoints = suite.cluster.pd_client.gc_safepoints.rl();
        let checkpoint = suite.global_checkpoint();

        assert!(
            safepoints.iter().any(|sp| {
                sp.service.contains(&format!("{}", victim))
                    && sp.ttl >= Duration::from_secs(60 * 60 * 24)
                    && sp.safepoint.into_inner() == checkpoint - 1
            }),
            "{:?}",
            safepoints
        );
    }

    #[test]
    fn pending_flush_when_force_flush() {
        let mut suite = SuiteBuilder::new_named("pending_flush").nodes(1).build();
        fail::cfg("delay_on_flush", "sleep(5000)").unwrap();
        suite.must_register_task(1, "pending_flush");
        suite.sync();
        let keyset = run_async_test(suite.write_records(0, 1, 1));
        suite.force_flush_files("pending_flush");
        suite.for_each_log_backup_cli(|_id, c| {
            let res = c.flush_now(Default::default()).unwrap();
            assert_eq!(res.results.len(), 1, "{:?}", res.results);
            assert!(res.results[0].error_message.is_empty(), "{:?}", res);
            assert!(res.results[0].success, "{:?}", res);
        });
        suite.check_for_write_records(
            suite.flushed_files.path(),
            keyset.iter().map(|v| v.as_slice()),
        )
    }
}
