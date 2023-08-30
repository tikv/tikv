// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_failpoint_tests)]

#[path = "../suite.rs"]
mod suite;
pub use suite::*;

mod all {

    use std::time::Duration;

    use backup_stream::{
        metadata::{
            keys::MetaKey,
            store::{Keys, MetaStore},
        },
        GetCheckpointResult, RegionCheckpointOperation, RegionSet, Task,
    };
    use futures::executor::block_on;
    use tikv_util::defer;

    use super::{
        make_record_key, make_split_key_at_record, mutation, run_async_test, SuiteBuilder,
    };

    #[test]
    fn basic() {
        let mut suite = SuiteBuilder::new_named("basic").build();
        fail::cfg("try_start_observe", "1*return").unwrap();

        run_async_test(async {
            // write data before the task starting, for testing incremental scanning.
            let round1 = suite.write_records(0, 128, 1).await;
            suite.must_register_task(1, "test_basic");
            suite.sync();
            let round2 = suite.write_records(256, 128, 1).await;
            suite.force_flush_files("test_basic");
            suite.wait_for_flush();
            suite
                .check_for_write_records(
                    suite.flushed_files.path(),
                    round1.union(&round2).map(Vec::as_slice),
                )
                .await;
        });
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
        run_async_test(suite.check_for_write_records(
            suite.flushed_files.path(),
            keys.union(&keys2).map(|s| s.as_slice()),
        ));
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
        run_async_test(suite.check_for_write_records(
            suite.flushed_files.path(),
            keys.union(&keys2).map(|s| s.as_slice()),
        ));
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
        run_async_test(suite.check_for_write_records(
            suite.flushed_files.path(),
            keys.union(&keys2).map(|s| s.as_slice()),
        ));
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

        let error = run_async_test(suite.get_meta_cli().get_last_error("retry_abort", 1)).unwrap();
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
        run_async_test(
            suite.check_for_write_records(
                suite.flushed_files.path(),
                items.iter().map(Vec::as_slice),
            ),
        );
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
        run_async_test(suite.check_for_write_records(
            suite.flushed_files.path(),
            round1.union(&round2).map(Vec::as_slice),
        ));
        let cp = suite.global_checkpoint();
        assert!(cp > 512, "it is {}", cp);
        suite.cluster.shutdown();
    }
}
