// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_failpoint_tests)]

#[path = "../suite.rs"]
mod suite;
pub use suite::*;

mod all {

    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };

    use backup_stream::{
        metadata::{
            keys::MetaKey,
            store::{Keys, MetaStore},
        },
        GetCheckpointResult, RegionCheckpointOperation, RegionSet, Task,
    };
    use futures::executor::block_on;
    use raftstore::coprocessor::ObserveHandle;
    use tikv_util::{
        config::{ReadableDuration, ReadableSize},
        defer,
    };
    use txn_types::Key;

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
        suite.wait_with_router(move |r| block_on(r.get_task_info("retry_abort")).is_ok());
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
        fail::cfg("router_on_event_delay_ms", "6*return(1000)").unwrap();
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
}
