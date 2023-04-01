// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::WriteBatchExt;

use crate::utils::*;

mod pagestorage {
    use engine_tiflash::{PSEngineWriteBatch, PSLogEngine};
    use engine_traits::{RaftEngine, RaftEngineReadOnly};
    use raft::eraftpb::Entry;

    use super::*;
    #[test]
    fn test_raft_engine_wb() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 1);
        cluster.cfg.proxy_cfg.engine_store.enable_unips = true;
        disable_auto_gen_compact_log(&mut cluster);
        let _ = cluster.run();
        // Wait until ffi is inited.

        let engine = cluster.get_tiflash_engine(1);
        let mut raft_engine = PSLogEngine::new();
        let helper = engine.proxy_ext.engine_store_server_helper;

        {
            // RaftEngine::clean shall not consume the write batch.
            raft_engine.init(helper);
            let mut raft_wb = PSEngineWriteBatch::new(helper);
            let mut raft_state = RaftLocalState::default();
            raft_state.mut_hard_state().set_commit(114514);
            raft_wb.put_raft_state(20, &raft_state).unwrap();
            raft_engine.consume(&mut raft_wb, true).unwrap();

            let mut raft_wb_clean = PSEngineWriteBatch::new(helper);
            let mut raft_local_state2 = RaftLocalState::default();
            // Make sure no early return in clean.
            raft_local_state2.set_last_index(5);
            raft_engine
                .clean(20, 1, &raft_local_state2, &mut raft_wb_clean)
                .unwrap();

            let raft_state_read = raft_engine.get_raft_state(20).unwrap().unwrap();
            assert_eq!(raft_state_read.get_hard_state().get_commit(), 114514);
        }

        {
            // RaftEngine::gc shall not consume the write batch.
            let mut raft_wb = PSEngineWriteBatch::new(helper);
            let mut entries: Vec<Entry> = vec![];
            let mut entry: Entry = Entry::default();
            entry.set_index(6);
            entry.set_term(6);
            entries.push(entry);
            raft_wb.append(21, None, entries);
            raft_engine.consume(&mut raft_wb, true).unwrap();

            let mut raft_wb_clean = PSEngineWriteBatch::new(helper);
            raft_engine.gc(21, 0, 20, &mut raft_wb_clean).unwrap();

            let mut entries_read: Vec<Entry> = vec![];
            raft_engine
                .fetch_entries_to(21, 6, 7, None, &mut entries_read)
                .unwrap();
            assert_eq!(entries_read.len(), 1);
        }
        cluster.shutdown();
    }

    #[test]
    fn test_prefix() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 1);
        cluster.cfg.proxy_cfg.engine_store.enable_unips = true;
        let _ = cluster.run();
        // Wait until ffi is inited.
        cluster.must_put(b"k1", b"v1");

        let engine = cluster.get_tiflash_engine(1);
        let mut wb = engine.write_batch();
        // Work around `do_write` filter.
        wb.put_cf(engine_traits::CF_RAFT, &[0x01, 0x09], &[0x03, 0x04, 0x05])
            .unwrap();
        wb.write().unwrap();
        let v = engine
            .get_value_cf(engine_traits::CF_RAFT, &[0x01, 0x09])
            .unwrap()
            .unwrap();
        assert!(v == &[0x03, 0x04, 0x05]);

        iter_ffi_helpers(&cluster, Some(vec![1]), &mut |_, ffi: &mut FFIHelperSet| {
            // Will add 0x02 to kv inputs in PageStorage.
            // See `add_kv_engine_prefix`.
            let actual_key = vec![0x02, 0x01, 0x09];
            let guard = ffi.engine_store_server.page_storage.data.read();
            assert!(guard.as_ref().expect("read").get(&actual_key).is_some());
            assert_eq!(
                guard
                    .as_ref()
                    .expect("read")
                    .get(&actual_key)
                    .unwrap()
                    .data
                    .as_slice(),
                &[0x03, 0x04, 0x05]
            );
        });
        cluster.shutdown();
    }
}

mod rocks {
    use super::*;
    #[test]
    fn test_prefix() {
        let (mut cluster, _pd_client) = new_mock_cluster(0, 1);
        cluster.cfg.proxy_cfg.engine_store.enable_unips = false;
        let _ = cluster.run();
        // Wait until ffi is inited.
        cluster.must_put(b"k1", b"v1");

        let engine = cluster.get_tiflash_engine(1);
        let mut wb = engine.write_batch();
        // Work around `do_write` filter.
        wb.put_cf(engine_traits::CF_RAFT, &[0x01, 0x09], &[0x03, 0x04, 0x05])
            .unwrap();
        wb.write().unwrap();
        let v = engine
            .get_value_cf(engine_traits::CF_RAFT, &[0x01, 0x09])
            .unwrap()
            .unwrap();
        assert!(v == &[0x03, 0x04, 0x05]);
        cluster.shutdown();
    }
}
