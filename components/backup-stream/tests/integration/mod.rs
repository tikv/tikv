// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(custom_test_frameworks)]
#![test_runner(test_util::run_tests)]

#[path = "../suite.rs"]
mod suite;

mod all {
    use std::{
        os::unix::ffi::OsStrExt,
        time::{Duration, Instant},
    };

    use backup_stream::{
        errors::Error, router::TaskSelector, GetCheckpointResult, RegionCheckpointOperation,
        RegionSet, Task,
    };
    use futures::{Stream, StreamExt};
    use pd_client::PdClient;
    use test_raftstore::IsolationFilterFactory;
    use tikv::config::BackupStreamConfig;
    use tikv_util::{box_err, defer, info, HandyRwLock};
    use tokio::time::timeout;
    use txn_types::{Key, TimeStamp};
    use walkdir::WalkDir;

    use super::suite::{
        make_record_key, make_split_key_at_record, mutation, run_async_test, SuiteBuilder,
    };

    #[test]
    fn with_split() {
        let mut suite = SuiteBuilder::new_named("with_split").build();
        let (round1, round2) = run_async_test(async {
            let round1 = suite.write_records(0, 128, 1).await;
            suite.must_split(&make_split_key_at_record(1, 42));
            suite.must_register_task(1, "test_with_split");
            let round2 = suite.write_records(256, 128, 1).await;
            (round1, round2)
        });
        suite.force_flush_files("test_with_split");
        suite.wait_for_flush();
        suite.check_for_write_records(
            suite.flushed_files.path(),
            round1.union(&round2).map(Vec::as_slice),
        );
        suite.cluster.shutdown();
    }

    #[test]
    fn region_boundaries() {
        let mut suite = SuiteBuilder::new_named("region_boundaries")
            .nodes(1)
            .build();
        let round = run_async_test(async {
            suite.must_split(&make_split_key_at_record(1, 42));
            let round1 = suite.write_records(0, 128, 1).await;
            suite.must_register_task(1, "region_boundaries");
            round1
        });
        suite.force_flush_files("region_boundaries");
        suite.wait_for_flush();
        suite.check_for_write_records(suite.flushed_files.path(), round.iter().map(Vec::as_slice));

        let a_meta = WalkDir::new(suite.flushed_files.path().join("v1/backupmeta"))
            .contents_first(true)
            .into_iter()
            .find(|v| {
                v.as_ref()
                    .is_ok_and(|v| v.file_name().as_bytes().ends_with(b".meta"))
            })
            .unwrap()
            .unwrap();
        let mut a_meta_content = protobuf::parse_from_bytes::<kvproto::brpb::Metadata>(
            &std::fs::read(a_meta.path()).unwrap(),
        )
        .unwrap();
        let dfs = a_meta_content.mut_file_groups()[0].mut_data_files_info();
        // Two regions, two CFs.
        assert_eq!(dfs.len(), 4);
        dfs.sort_by(|x1, x2| x1.start_key.cmp(&x2.start_key));
        let hnd_key = |hnd| make_split_key_at_record(1, hnd);
        assert_eq!(dfs[0].region_start_key, b"");
        assert_eq!(dfs[0].region_end_key, hnd_key(42));
        assert_eq!(dfs[2].region_start_key, hnd_key(42));
        assert_eq!(dfs[2].region_end_key, b"");
        suite.cluster.shutdown();
    }

    /// This test tests whether we can handle some weird transactions and their
    /// race with initial scanning.
    /// Generally, those transactions:
    /// - Has N mutations, which's values are all short enough to be inlined in
    ///   the `Write` CF. (N > 1024)
    /// - Commit the mutation set M first. (for all m in M: Nth-Of-Key(m) >
    ///   1024)
    /// ```text
    /// |--...-----^------*---*-*--*-*-*-> (The line is the Key Space - from "" to inf)
    ///            +The 1024th key  (* = committed mutation)
    /// ```
    /// - Before committing remaining mutations, PiTR triggered initial
    ///   scanning.
    /// - The remaining mutations are committed before the instant when initial
    ///   scanning get the snapshot.
    #[test]
    fn with_split_txn() {
        let mut suite = SuiteBuilder::new_named("split_txn").build();
        let (commit_ts, start_ts, keys) = run_async_test(async {
            let start_ts = suite.cluster.pd_client.get_tso().await.unwrap();
            let keys = (1..1960).map(|i| make_record_key(1, i)).collect::<Vec<_>>();
            suite.must_kv_prewrite(
                1,
                keys.clone()
                    .into_iter()
                    .map(|k| mutation(k, b"hello, world".to_vec()))
                    .collect(),
                make_record_key(1, 1913),
                start_ts,
            );
            let commit_ts = suite.cluster.pd_client.get_tso().await.unwrap();
            (commit_ts, start_ts, keys)
        });
        suite.commit_keys(keys[1913..].to_vec(), start_ts, commit_ts);
        suite.must_register_task(1, "test_split_txn");
        suite.commit_keys(keys[..1913].to_vec(), start_ts, commit_ts);
        suite.force_flush_files("test_split_txn");
        suite.wait_for_flush();
        let keys_encoded = keys
            .iter()
            .map(|v| {
                Key::from_raw(v.as_slice())
                    .append_ts(commit_ts)
                    .into_encoded()
            })
            .collect::<Vec<_>>();
        suite.check_for_write_records(
            suite.flushed_files.path(),
            keys_encoded.iter().map(Vec::as_slice),
        );
        suite.cluster.shutdown();
    }

    #[test]
    /// This case tests whether the backup can continue when the leader failes.
    fn leader_down() {
        let mut suite = SuiteBuilder::new_named("leader_down").build();
        suite.must_register_task(1, "test_leader_down");
        suite.sync();
        let round1 = run_async_test(suite.write_records(0, 128, 1));
        let leader = suite.cluster.leader_of_region(1).unwrap().get_store_id();
        suite.cluster.stop_node(leader);
        let round2 = run_async_test(suite.write_records(256, 128, 1));
        suite.force_flush_files("test_leader_down");
        suite.wait_for_flush();
        suite.check_for_write_records(
            suite.flushed_files.path(),
            round1.union(&round2).map(Vec::as_slice),
        );
        suite.cluster.shutdown();
    }

    #[test]
    /// This case tests whether the checkpoint ts (next backup ts) can be
    /// advanced correctly when async commit is enabled.
    fn async_commit() {
        let mut suite = SuiteBuilder::new_named("async_commit").nodes(3).build();
        run_async_test(async {
            suite.must_register_task(1, "test_async_commit");
            suite.sync();
            suite.write_records(0, 128, 1).await;
            let ts = suite.just_async_commit_prewrite(256, 1);
            suite.write_records(258, 128, 1).await;
            suite.force_flush_files("test_async_commit");
            std::thread::sleep(Duration::from_secs(4));
            assert_eq!(suite.global_checkpoint(), 256);
            suite.just_commit_a_key(make_record_key(1, 256), TimeStamp::new(256), ts);
            suite.force_flush_files("test_async_commit");
            suite.wait_for_flush();
            let cp = suite.global_checkpoint();
            assert!(cp > 256, "it is {:?}", cp);
        });
        suite.cluster.shutdown();
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
        let (victim, endpoint) = suite.endpoints.iter().next().unwrap();
        endpoint
            .scheduler()
            .schedule(Task::FatalError(
                TaskSelector::ByName("test_fatal_error".to_owned()),
                Box::new(Error::Other(box_err!("everything is alright"))),
            ))
            .unwrap();
        suite.sync();
        let err = run_async_test(
            suite
                .get_meta_cli()
                .get_last_error_of("test_fatal_error", *victim),
        )
        .unwrap()
        .unwrap();
        info!("err"; "err" => ?err);
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
    fn region_checkpoint_info() {
        let mut suite = SuiteBuilder::new_named("checkpoint_info").nodes(1).build();
        suite.must_register_task(1, "checkpoint_info");
        suite.must_split(&make_split_key_at_record(1, 42));
        run_async_test(suite.write_records(0, 128, 1));
        suite.force_flush_files("checkpoint_info");
        suite.wait_for_flush();
        std::thread::sleep(Duration::from_secs(1));
        let (tx, rx) = std::sync::mpsc::channel();
        suite.run(|| {
            let tx = tx.clone();
            Task::RegionCheckpointsOp(RegionCheckpointOperation::Get(
                RegionSet::Universal,
                Box::new(move |rs| {
                    tx.send(rs).unwrap();
                }),
            ))
        });
        let checkpoints = rx.recv().unwrap();
        assert!(!checkpoints.is_empty(), "{:?}", checkpoints);
        assert!(
            checkpoints
                .iter()
                .all(|cp| matches!(cp, GetCheckpointResult::Ok { checkpoint, .. } if checkpoint.into_inner() > 256)),
            "{:?}",
            checkpoints
        );
    }

    #[test]
    fn upload_checkpoint_exits_in_time() {
        defer! {{
            std::env::remove_var("LOG_BACKUP_UGC_SLEEP_AND_RETURN");
        }}
        let suite = SuiteBuilder::new_named("upload_checkpoint_exits_in_time")
            .nodes(1)
            .build();
        std::env::set_var("LOG_BACKUP_UGC_SLEEP_AND_RETURN", "meow");
        let (_, victim) = suite.endpoints.iter().next().unwrap();
        let sched = victim.scheduler();
        sched
            .schedule(Task::UpdateGlobalCheckpoint("greenwoods".to_owned()))
            .unwrap();
        let start = Instant::now();
        let (tx, rx) = std::sync::mpsc::channel();
        suite.wait_with(move |ep| {
            tx.send((Instant::now(), ep.abort_last_storage_save.is_some()))
                .unwrap();
            true
        });
        let (end, has_abort) = rx.recv().unwrap();
        assert!(
            end - start < Duration::from_secs(2),
            "take = {:?}",
            end - start
        );
        assert!(has_abort);
    }

    /// This test case tests whether we correctly handle the pessimistic locks.
    #[test]
    fn pessimistic_lock() {
        let mut suite = SuiteBuilder::new_named("pessimistic_lock").nodes(3).build();
        suite.must_kv_pessimistic_lock(
            1,
            vec![make_record_key(1, 42)],
            suite.tso(),
            make_record_key(1, 42),
        );
        suite.must_register_task(1, "pessimistic_lock");
        suite.must_kv_pessimistic_lock(
            1,
            vec![make_record_key(1, 43)],
            suite.tso(),
            make_record_key(1, 43),
        );
        let expected_tso = suite.tso().into_inner();
        suite.force_flush_files("pessimistic_lock");
        suite.wait_for_flush();
        std::thread::sleep(Duration::from_secs(1));
        run_async_test(suite.advance_global_checkpoint("pessimistic_lock")).unwrap();
        let checkpoint = run_async_test(
            suite
                .get_meta_cli()
                .global_progress_of_task("pessimistic_lock"),
        )
        .unwrap();
        // The checkpoint should be advanced: because PiTR is "Read" operation,
        // which shouldn't be blocked by pessimistic locks.
        assert!(
            checkpoint > expected_tso,
            "expected = {}; checkpoint = {}",
            expected_tso,
            checkpoint
        );
    }

    async fn collect_all_current<T>(
        mut s: impl Stream<Item = T> + Unpin,
        max_gap: Duration,
    ) -> Vec<T> {
        let mut r = vec![];
        while let Ok(Some(x)) = timeout(max_gap, s.next()).await {
            r.push(x);
        }
        r
    }

    async fn collect_current<T>(mut s: impl Stream<Item = T> + Unpin, goal: usize) -> Vec<T> {
        let mut r = vec![];
        while let Ok(Some(x)) = timeout(Duration::from_secs(10), s.next()).await {
            r.push(x);
            if r.len() >= goal {
                return r;
            }
        }
        r
    }

    #[test]
    fn subscribe_flushing() {
        let mut suite = SuiteBuilder::new_named("sub_flush").build();
        let stream = suite.flush_stream(true);
        for i in 1..10 {
            let split_key = make_split_key_at_record(1, i * 20);
            suite.must_split(&split_key);
            suite.must_shuffle_leader(suite.cluster.get_region_id(&split_key));
        }

        let round1 = run_async_test(suite.write_records(0, 128, 1));
        suite.must_register_task(1, "sub_flush");
        let round2 = run_async_test(suite.write_records(256, 128, 1));
        suite.sync();
        suite.force_flush_files("sub_flush");

        let mut items = run_async_test(async {
            collect_current(
                stream.flat_map(|(_, r)| futures::stream::iter(r.events.into_iter())),
                10,
            )
            .await
        });

        items.sort_by(|x, y| x.start_key.cmp(&y.start_key));

        println!("{:?}", items);
        assert_eq!(items.len(), 10);

        assert_eq!(items.first().unwrap().start_key, Vec::<u8>::default());
        for w in items.windows(2) {
            let a = &w[0];
            let b = &w[1];
            assert!(a.checkpoint > 512);
            assert!(b.checkpoint > 512);
            assert_eq!(a.end_key, b.start_key);
        }
        assert_eq!(items.last().unwrap().end_key, Vec::<u8>::default());

        suite.check_for_write_records(
            suite.flushed_files.path(),
            round1.union(&round2).map(|x| x.as_slice()),
        );
    }

    #[test]
    fn resolved_follower() {
        let mut suite = SuiteBuilder::new_named("r").build();
        let round1 = run_async_test(suite.write_records(0, 128, 1));
        suite.must_register_task(1, "r");
        suite.run(|| Task::RegionCheckpointsOp(RegionCheckpointOperation::PrepareMinTsForResolve));
        suite.sync();
        std::thread::sleep(Duration::from_secs(1));

        let leader = suite.cluster.leader_of_region(1).unwrap();
        suite.must_shuffle_leader(1);
        let round2 = run_async_test(suite.write_records(256, 128, 1));
        suite
            .endpoints
            .get(&leader.store_id)
            .unwrap()
            .scheduler()
            .schedule(Task::ForceFlush("r".to_owned()))
            .unwrap();
        suite.sync();
        std::thread::sleep(Duration::from_secs(2));
        suite.check_for_write_records(
            suite.flushed_files.path(),
            round1.iter().map(|x| x.as_slice()),
        );
        assert!(suite.global_checkpoint() > 256);
        suite.force_flush_files("r");
        suite.wait_for_flush();
        assert!(suite.global_checkpoint() > 512);
        suite.check_for_write_records(
            suite.flushed_files.path(),
            round1.union(&round2).map(|x| x.as_slice()),
        );
    }

    #[test]
    fn network_partition() {
        let mut suite = SuiteBuilder::new_named("network_partition")
            .nodes(3)
            .build();
        let stream = suite.flush_stream(true);
        suite.must_register_task(1, "network_partition");
        let leader = suite.cluster.leader_of_region(1).unwrap();
        let round1 = run_async_test(suite.write_records(0, 64, 1));

        suite
            .cluster
            .add_send_filter(IsolationFilterFactory::new(leader.store_id));
        suite.cluster.reset_leader_of_region(1);
        suite
            .cluster
            .must_wait_for_leader_expire(leader.store_id, 1);
        let leader2 = suite.cluster.leader_of_region(1).unwrap();
        assert_ne!(leader.store_id, leader2.store_id, "leader not switched.");
        let ts = suite.tso();
        suite.must_kv_prewrite(
            1,
            vec![mutation(make_record_key(1, 778), b"generator".to_vec())],
            make_record_key(1, 778),
            ts,
        );
        suite.sync();
        suite.force_flush_files("network_partition");
        suite.wait_for_flush();

        let cps = run_async_test(collect_all_current(stream, Duration::from_secs(2)));
        assert!(
            cps.iter()
                .flat_map(|(_s, cp)| cp.events.iter().map(|resp| resp.checkpoint))
                .all(|cp| cp <= ts.into_inner()),
            "ts={} cps={:?}",
            ts,
            cps
        );
        suite.check_for_write_records(
            suite.flushed_files.path(),
            round1.iter().map(|k| k.as_slice()),
        )
    }

    #[test]
    fn update_config() {
        let suite = SuiteBuilder::new_named("network_partition")
            .nodes(1)
            .build();
        let mut basic_config = BackupStreamConfig::default();
        basic_config.initial_scan_concurrency = 4;
        suite.run(|| Task::ChangeConfig(basic_config.clone()));
        suite.wait_with(|e| {
            assert_eq!(e.initial_scan_semaphore.available_permits(), 4,);
            true
        });

        basic_config.initial_scan_concurrency = 16;
        suite.run(|| Task::ChangeConfig(basic_config.clone()));
        suite.wait_with(|e| {
            assert_eq!(e.initial_scan_semaphore.available_permits(), 16,);
            true
        });
    }
}
