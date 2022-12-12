// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::DerefMut;

use futures::executor::block_on;
use tikv_util::time::Duration;
use txn_types::WriteBatchFlags;

use crate::proxy::*;

fn must_get_error_flashback_in_progress(
    cluster: &mut Cluster<NodeCluster>,
    region: &metapb::Region,
    cmd: kvproto::raft_cmdpb::Request,
) {
    for _ in 0..10 {
        let mut reqs = vec![];
        for _ in 0..10 {
            reqs.push(cmd.clone());
        }
        match cluster.batch_put(b"k1", reqs) {
            Ok(_) => {}
            Err(e) => {
                assert_eq!(
                    e.get_flashback_in_progress(),
                    &kvproto::errorpb::FlashbackInProgress {
                        region_id: region.get_id(),
                        ..Default::default()
                    }
                );
            }
        }
    }
}

fn must_cmd_add_flashback_flag(
    cluster: &mut Cluster<NodeCluster>,
    region: &mut metapb::Region,
    cmd: kvproto::raft_cmdpb::Request,
) {
    // Verify the read can be executed if add flashback flag in request's
    // header.
    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![cmd],
        false,
    );
    let new_leader = cluster.query_leader(1, region.get_id(), Duration::from_secs(1));
    req.mut_header().set_peer(new_leader.unwrap());
    req.mut_header()
        .set_flags(WriteBatchFlags::FLASHBACK.bits());
    let resp = cluster.call_command(req, Duration::from_secs(5)).unwrap();
    assert!(!resp.get_header().has_error());
}

mod persist {
    use super::*;
    fn flashback_recover(do_persist: bool) {
        let (mut cluster, _) = new_mock_cluster(0, 3);
        disable_auto_gen_compact_log(&mut cluster);
        cluster.run();

        let region_id = 1;
        cluster.must_transfer_leader(region_id, new_peer(1, 1));

        // Write for cluster
        cluster.must_put(b"k1", b"v1");

        let prev_states = collect_all_states(&cluster, region_id);

        if !do_persist {
            fail::cfg("no_persist_flashback", "return(0)").unwrap();
        }

        // Prepare for flashback
        let region = cluster.get_region(b"k1");
        block_on(cluster.send_flashback_msg(
            region.get_id(),
            1, // store id
            kvproto::raft_cmdpb::AdminCmdType::PrepareFlashback,
            cluster.get_region_epoch(region_id),
            new_peer(1, 1),
        ));

        // Write will be blocked
        must_get_error_flashback_in_progress(&mut cluster, &region, new_put_cmd(b"k1", b"v2"));

        must_cmd_add_flashback_flag(&mut cluster, &mut region.clone(), new_put_cmd(b"k3", b"v3"));

        let victim = 1;
        stop_tiflash_node(&mut cluster, victim);
        restart_tiflash_node(&mut cluster, victim);

        let new_states = collect_all_states(&cluster, region_id);

        if !do_persist {
            // Check apply index is not persisted in disk.
            for i in vec![1] {
                let old = prev_states.get(&i).unwrap();
                let new = new_states.get(&i).unwrap();
                assert_eq!(
                    old.in_disk_apply_state.get_applied_index(),
                    new.in_disk_apply_state.get_applied_index()
                );
            }
            check_key(
                &cluster,
                b"k1",
                b"v1",
                Some(false),
                Some(false),
                Some(vec![victim]),
            );
        } else {
            check_key(&cluster, b"k1", b"v1", Some(true), None, Some(vec![victim]));
        }

        if !do_persist {
            fail::remove("no_persist_flashback");
        }

        cluster.shutdown();
    }

    #[test]
    fn test_flashback_persist() {
        flashback_recover(true);
    }

    #[test]
    fn test_flashback_nopersist() {
        flashback_recover(false);
    }
}
