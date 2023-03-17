// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::WriteBatchExt;

use crate::utils::*;

mod pagestorage {
    use super::*;
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
